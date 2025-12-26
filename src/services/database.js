const { Pool } = require('pg');
const config = require('../config/env');
const logger = require('./logger');
const { getClient } = require('./redis'); 

// SCALABILITY: Limit max connections
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
    try {
      return await pool.query(text, params);
    } catch (error) {
      logger.error('Database query error', { text, error: error.message });
      throw error;
    }
  },
  async get(text, params) { const res = await this.query(text, params); return res.rows[0]; },
  async all(text, params) { const res = await this.query(text, params); return res.rows; },
  async run(text, params) { const res = await this.query(text, params); return { rowCount: res.rowCount }; },
  async exec(text) { return this.query(text); }
};

const initDB = async () => {
  try {
    // 1. Core Token Tables
    await pool.query(`
      CREATE TABLE IF NOT EXISTS tokens (
        mint TEXT PRIMARY KEY,
        symbol TEXT,
        name TEXT,
        image TEXT, -- Added explicitly
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

    // SELF-HEALING: Ensure 'image' column exists if table was created previously
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

    // 2. Indexer Tables
    await pool.query(`
      CREATE TABLE IF NOT EXISTS pools (
          address TEXT PRIMARY KEY,       
          mint TEXT NOT NULL,             
          dex TEXT NOT NULL,              
          token_a TEXT NOT NULL,          
          token_b TEXT NOT NULL,          
          created_at BIGINT DEFAULT 0,
          UNIQUE(mint, dex)
      );
    `);

    // SELF-HEALING: Ensure 'mint' exists in pools
    await pool.query(`ALTER TABLE pools ADD COLUMN IF NOT EXISTS mint TEXT;`);
    await pool.query(`ALTER TABLE pools ADD COLUMN IF NOT EXISTS dex TEXT;`);

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

    // 3. Indexes
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_mcap ON tokens(marketCap DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_volume ON tokens(volume24h DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens(timestamp DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_candles_pool_time ON candles_1m(pool_address, timestamp DESC);`);

    logger.info('Database initialized successfully with Optimized Schema (Self-Healed)');
  } catch (error) {
    logger.error('Database initialization failed', error);
    process.exit(1);
  }
};

// Helper: Upsert basic token data
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

// Helper: Enable Indexing
const enableIndexing = async (db, mint, pair) => {
    if (!pair || !pair.pairAddress) return;
    
    await db.run(`
        INSERT INTO pools (address, mint, dex, token_a, token_b, created_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT(mint, dex) DO NOTHING
    `, [
        pair.pairAddress, 
        mint, 
        pair.dexId, 
        pair.baseToken.address, 
        pair.quoteToken.address, 
        Date.now()
    ]);
    
    await db.run(`
        INSERT INTO active_trackers (pool_address, last_check) 
        VALUES ($1, $2) 
        ON CONFLICT (pool_address) DO NOTHING
    `, [pair.pairAddress, Date.now()]);
    
    logger.info(`✅ Indexing enabled for ${mint}`);
};

module.exports = {
  getDB: () => dbWrapper,
  initDB,
  smartCache,
  saveTokenData,
  enableIndexing
};
