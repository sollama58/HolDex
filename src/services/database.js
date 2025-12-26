const { Pool } = require('pg');
const config = require('../config/env');
const logger = require('./logger');
const { getClient } = require('./redis'); 

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: 20, 
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

async function smartCache(key, ttlSeconds, fetchFn) {
    const redis = getClient();
    if (redis && redis.status === 'ready') {
        try {
            const cached = await redis.get(key);
            if (cached) return JSON.parse(cached);
        } catch (e) { logger.warn(`Redis Cache Get Error: ${e.message}`); }
    }
    const value = await fetchFn();
    if (value && redis && redis.status === 'ready') {
        try {
            await redis.set(key, JSON.stringify(value), 'EX', ttlSeconds);
        } catch (e) { logger.warn(`Redis Cache Set Error: ${e.message}`); }
    }
    return value;
}

const dbWrapper = {
  pool, 
  async query(text, params) {
    try { return await pool.query(text, params); } 
    catch (error) { logger.error('Database query error', { text, error: error.message }); throw error; }
  },
  async get(text, params) { const res = await this.query(text, params); return res.rows[0]; },
  async all(text, params) { const res = await this.query(text, params); return res.rows; },
  async run(text, params) { const res = await this.query(text, params); return { rowCount: res.rowCount }; },
  async exec(text) { return this.query(text); }
};

const initDB = async () => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS tokens (
        mint TEXT PRIMARY KEY,
        symbol TEXT,
        name TEXT,
        image TEXT,
        decimals INTEGER,
        supply TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        metadata JSONB,
        k_score INTEGER DEFAULT 0,
        marketCap DOUBLE PRECISION DEFAULT 0,
        volume24h DOUBLE PRECISION DEFAULT 0,
        change24h DOUBLE PRECISION DEFAULT 0,
        change1h DOUBLE PRECISION DEFAULT 0,
        change5m DOUBLE PRECISION DEFAULT 0,
        priceUsd DOUBLE PRECISION DEFAULT 0,
        hasCommunityUpdate BOOLEAN DEFAULT FALSE,
        timestamp BIGINT DEFAULT 0
      );
    `);

    // Ensure columns exist
    await pool.query(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS image TEXT;`);
    await pool.query(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS k_score INTEGER DEFAULT 0;`);
    await pool.query(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS marketCap DOUBLE PRECISION DEFAULT 0;`);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS k_scores (
        mint TEXT PRIMARY KEY REFERENCES tokens(mint),
        score INTEGER,
        metrics JSONB,
        calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS token_updates (
        id SERIAL PRIMARY KEY,
        mint TEXT NOT NULL,
        twitter TEXT,
        website TEXT,
        telegram TEXT,
        banner TEXT,
        description TEXT,
        status TEXT DEFAULT 'pending', 
        signature TEXT UNIQUE,
        payer TEXT,
        submittedAt BIGINT
      );
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS pools (
          address TEXT PRIMARY KEY,       
          mint TEXT NOT NULL,             
          dex TEXT NOT NULL,              
          token_a TEXT NOT NULL,          
          token_b TEXT NOT NULL,          
          created_at BIGINT DEFAULT 0,
          liquidity_usd DOUBLE PRECISION DEFAULT 0,
          volume_24h DOUBLE PRECISION DEFAULT 0,
          price_usd DOUBLE PRECISION DEFAULT 0
      );
    `);
    
    // Ensure we don't have the old restrictive constraint
    try { await pool.query(`ALTER TABLE pools DROP CONSTRAINT IF EXISTS pools_mint_dex_key;`); } catch(e) {}

    await pool.query(`
      CREATE TABLE IF NOT EXISTS candles_1m (
          pool_address TEXT NOT NULL,
          timestamp BIGINT NOT NULL,
          open DOUBLE PRECISION,
          high DOUBLE PRECISION,
          low DOUBLE PRECISION,
          close DOUBLE PRECISION,
          volume DOUBLE PRECISION DEFAULT 0,
          PRIMARY KEY (pool_address, timestamp)
      );
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS active_trackers (
          pool_address TEXT PRIMARY KEY,
          last_check BIGINT DEFAULT 0,
          priority INTEGER DEFAULT 1
      );
    `);

    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_mcap ON tokens(marketCap DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_volume ON tokens(volume24h DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens(timestamp DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_candles_pool_time ON candles_1m(pool_address, timestamp DESC);`);
    // New index for finding best pool quickly
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_pools_mint_liquidity ON pools(mint, liquidity_usd DESC);`);

    logger.info('Database initialized (Multi-Pool Ready)');
  } catch (error) {
    logger.error('Database initialization failed', error);
    process.exit(1);
  }
};

// --- AGGREGATION LOGIC ---

// 1. Enable Indexing for a specific pool (Upsert Pool Data)
const enableIndexing = async (db, mint, pair) => {
    if (!pair || !pair.pairAddress) return;
    
    // Parse stats safely
    const liq = Number(pair.liquidity?.usd || 0);
    const vol = Number(pair.volume?.h24 || 0);
    const price = Number(pair.priceUsd || 0);

    // Update the POOL record
    await db.run(`
        INSERT INTO pools (address, mint, dex, token_a, token_b, created_at, liquidity_usd, volume_24h, price_usd)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT(address) DO UPDATE SET
            liquidity_usd = EXCLUDED.liquidity_usd,
            volume_24h = EXCLUDED.volume_24h,
            price_usd = EXCLUDED.price_usd
    `, [
        pair.pairAddress, 
        mint, 
        pair.dexId, 
        pair.baseToken.address, 
        pair.quoteToken.address, 
        Date.now(),
        liq, vol, price
    ]);
    
    // Enable Active Tracking for Snapshotter
    await db.run(`
        INSERT INTO active_trackers (pool_address, last_check) 
        VALUES ($1, $2) 
        ON CONFLICT (pool_address) DO NOTHING
    `, [pair.pairAddress, Date.now()]);
};

// 2. Aggregate Stats across ALL pools for a Mint
const aggregateAndSaveToken = async (db, mint, baseData) => {
    // A. Fetch all pools for this mint
    const pools = await db.all(`SELECT * FROM pools WHERE mint = $1`, [mint]);
    
    if (!pools || pools.length === 0) {
        // Fallback: Just save what we have if no pools indexed yet
        await saveTokenData(db, mint, baseData, Date.now());
        return;
    }

    // B. Calculate Aggregates
    let totalVolume = 0;
    let maxLiq = -1;
    let bestPool = null;

    for (const pool of pools) {
        // Sum Volume
        totalVolume += (pool.volume_24h || 0);
        
        // Find Largest Pool
        if ((pool.liquidity_usd || 0) > maxLiq) {
            maxLiq = pool.liquidity_usd;
            bestPool = pool;
        }
    }

    // C. Derive Final Stats
    // If we found a "best pool", use its price. Otherwise fallback to provided data.
    const finalPrice = bestPool ? (bestPool.price_usd || baseData.priceUsd) : baseData.priceUsd;
    
    // Recalculate Market Cap based on new Price (FDV = Supply * Price)
    // Note: We might not have supply easily, so we scale the input mcap by price ratio if needed,
    // or better: use the FDV from the best pool pair if we had it. 
    // For simplicity: We trust the input MarketCap but update it if price changed significantly?
    // Actually, sticking to the Best Pool's price for display is safest.
    
    const finalData = {
        ...baseData,
        volume24h: totalVolume, // AGGREGATED VOLUME
        priceUsd: finalPrice,   // PRICE FROM LARGEST POOL
        // Note: You might want to recalculate marketCap here if you have supply
    };

    await saveTokenData(db, mint, finalData, Date.now());
};

// 3. Raw Save (Low Level)
const saveTokenData = async (db, mint, data, timestamp) => {
    const cols = [
        'name', 'symbol', 'image', 'marketCap', 'volume24h', 
        'priceUsd', 'change1h', 'change24h', 'change5m', 'timestamp'
    ];
    const vals = [
        data.name, data.ticker, data.image, data.marketCap, data.volume24h,
        data.priceUsd, data.change1h, data.change24h, data.change5m, timestamp
    ];
    
    await db.query(`
      INSERT INTO tokens (mint, ${cols.join(',')})
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      ON CONFLICT(mint) DO UPDATE SET
          name = EXCLUDED.name,
          symbol = EXCLUDED.symbol,
          image = COALESCE(EXCLUDED.image, tokens.image),
          marketCap = EXCLUDED.marketCap,
          volume24h = EXCLUDED.volume24h,
          priceUsd = EXCLUDED.priceUsd,
          change1h = EXCLUDED.change1h,
          change24h = EXCLUDED.change24h,
          change5m = EXCLUDED.change5m,
          timestamp = GREATEST(tokens.timestamp, EXCLUDED.timestamp)
    `, [mint, ...vals]);
};

module.exports = {
  getDB: () => dbWrapper,
  initDB,
  smartCache,
  enableIndexing,
  aggregateAndSaveToken
};
