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
        // AUTO-MIGRATION: Ensure reserve columns exist
        await pool.query(`ALTER TABLE pools ADD COLUMN IF NOT EXISTS reserve_a TEXT;`);
        await pool.query(`ALTER TABLE pools ADD COLUMN IF NOT EXISTS reserve_b TEXT;`);
        logger.info('✅ Database Schema Verified (Reserves Columns Present)');
    } catch (e) {
        logger.warn('Schema check warning: ' + e.message);
    }
};

// --- AGGREGATION LOGIC ---

const enableIndexing = async (db, mint, pair) => {
    if (!pair || !pair.pairAddress) return;
    
    const liq = Number(pair.liquidity?.usd || 0);
    const vol = Number(pair.volume?.h24 || 0);
    const price = Number(pair.priceUsd || 0);

    // Update the POOL record
    await db.run(`
        INSERT INTO pools (
            address, mint, dex, 
            token_a, token_b, 
            reserve_a, reserve_b,
            created_at, liquidity_usd, volume_24h, price_usd
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT(address) DO UPDATE SET
            reserve_a = EXCLUDED.reserve_a,
            reserve_b = EXCLUDED.reserve_b,
            liquidity_usd = EXCLUDED.liquidity_usd,
            volume_24h = EXCLUDED.volume_24h,
            price_usd = EXCLUDED.price_usd
    `, [
        pair.pairAddress, 
        mint, 
        pair.dexId, 
        pair.baseToken.address, 
        pair.quoteToken.address, 
        pair.reserve_a || null, 
        pair.reserve_b || null, 
        Date.now(),
        liq, vol, price
    ]);
    
    await db.run(`
        INSERT INTO active_trackers (pool_address, last_check) 
        VALUES ($1, $2) 
        ON CONFLICT (pool_address) DO NOTHING
    `, [pair.pairAddress, Date.now()]);
};

// CRITICAL FIX: Update the parent TOKEN record based on child POOLS
const aggregateAndSaveToken = async (db, mint) => {
    try {
        // 1. Get all pools for this mint
        const pools = await db.all(`SELECT * FROM pools WHERE mint = $1`, [mint]);
        if (!pools || pools.length === 0) return;

        let totalVolume = 0;
        let totalLiquidity = 0;
        let maxLiq = -1;
        let bestPrice = 0;

        for (const pool of pools) {
            const pLiq = Number(pool.liquidity_usd || 0);
            const pVol = Number(pool.volume_24h || 0);
            const pPrice = Number(pool.price_usd || 0);

            totalVolume += pVol;
            totalLiquidity += pLiq;

            // Use price from the most liquid pool
            if (pLiq > maxLiq && pPrice > 0) {
                maxLiq = pLiq;
                bestPrice = pPrice;
            }
        }

        // If no liquid pool found, keep existing price or 0
        if (bestPrice === 0 && pools.length > 0) {
             // Fallback to any pool with price
             const anyPrice = pools.find(p => p.price_usd > 0);
             if (anyPrice) bestPrice = Number(anyPrice.price_usd);
        }

        // 2. Fetch current token info to calc Market Cap
        const token = await db.get(`SELECT supply, decimals FROM tokens WHERE mint = $1`, [mint]);
        let marketCap = 0;
        
        if (token && bestPrice > 0) {
            const supply = token.supply ? (Number(token.supply) / Math.pow(10, token.decimals || 9)) : 0;
            if (supply > 0) marketCap = supply * bestPrice;
        }

        // 3. Update Token Stats
        await db.run(`
            UPDATE tokens SET 
                liquidity = $1, 
                volume24h = $2, 
                priceUsd = $3, 
                marketCap = $4,
                timestamp = $5
            WHERE mint = $6
        `, [totalLiquidity, totalVolume, bestPrice, marketCap, Date.now(), mint]);

        // logger.info(`📊 Aggregated ${mint}: $${bestPrice} | Liq: $${totalLiquidity}`);

    } catch (err) {
        logger.error(`Aggregation Failed for ${mint}: ${err.message}`);
    }
};

module.exports = {
  getDB: () => dbWrapper,
  initDB,
  smartCache,
  enableIndexing,
  aggregateAndSaveToken
};
