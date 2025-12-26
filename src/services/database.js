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
    // This function assumes schema is created via reset script.
    // It is kept lightweight to avoid re-running CREATE statements on every boot
    // if we are relying on the reset script for the "Golden Schema".
    logger.info('Database Service Initialized');
};

// --- AGGREGATION LOGIC (The Core Fix) ---

// 1. Enable Indexing for a specific pool (Upsert Pool Data)
const enableIndexing = async (db, mint, pair) => {
    if (!pair || !pair.pairAddress) return;
    
    const liq = Number(pair.liquidity?.usd || 0);
    const vol = Number(pair.volume?.h24 || 0);
    const price = Number(pair.priceUsd || 0);

    // Update the POOL record
    // ON CONFLICT(address) handles updates to existing pools
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
        await saveTokenData(db, mint, baseData, Date.now());
        return;
    }

    // B. Calculate Aggregates
    let totalVolume = 0;
    let maxLiq = -1;
    let bestPool = null;

    for (const pool of pools) {
        // Sum Volume from all pools (Raydium + Orca + etc)
        totalVolume += (pool.volume_24h || 0);
        
        // Find Largest Pool by Liquidity for Price Source
        if ((pool.liquidity_usd || 0) > maxLiq) {
            maxLiq = pool.liquidity_usd;
            bestPool = pool;
        }
    }

    // C. Derive Final Stats
    // Price comes from the Largest Pool (most accurate)
    const finalPrice = bestPool ? (bestPool.price_usd || baseData.priceUsd) : baseData.priceUsd;
    
    // D. Recalculate Market Cap
    // If we have FDV from the base data (DexScreener), we prefer that.
    // Otherwise we can estimate via Supply * Price if available.
    // For now, we trust the input data but update the Price and Volume fields.
    
    const finalData = {
        ...baseData,
        volume24h: totalVolume, // AGGREGATED
        priceUsd: finalPrice,   // BEST SOURCE
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
