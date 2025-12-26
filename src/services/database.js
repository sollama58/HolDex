const { Pool } = require('pg');
const config = require('../config/env');
const logger = require('./logger');
const { getClient } = require('./redis');

let pool = null;
let dbWrapper = null;

async function initDB() {
    if (pool) return dbWrapper;

    try {
        // SSL is often required for cloud Postgres (like Render), but not for local
        const isLocal = config.DATABASE_URL.includes('localhost') || config.DATABASE_URL.includes('127.0.0.1');
        
        pool = new Pool({
            connectionString: config.DATABASE_URL,
            ssl: isLocal ? false : { rejectUnauthorized: false },
            max: 20, // Connection pool limit
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 2000,
        });

        logger.info(`📦 Database: Connecting to PostgreSQL...`);
        
        // Test connection
        const client = await pool.connect();
        client.release();
        logger.info(`📦 Database: Connection Successful.`);

        // --- SCHEMA DEFINITIONS (PostgreSQL Dialect) ---
        await pool.query(`
            CREATE TABLE IF NOT EXISTS tokens (
                mint TEXT PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                image TEXT,
                supply TEXT,
                decimals INTEGER,
                priceUsd DOUBLE PRECISION,
                liquidity DOUBLE PRECISION,
                marketCap DOUBLE PRECISION,
                volume24h DOUBLE PRECISION,
                change24h DOUBLE PRECISION,
                change1h DOUBLE PRECISION,
                change5m DOUBLE PRECISION,
                k_score DOUBLE PRECISION DEFAULT 0,
                hasCommunityUpdate BOOLEAN DEFAULT FALSE,
                metadata TEXT,
                timestamp BIGINT
            );

            CREATE TABLE IF NOT EXISTS pools (
                address TEXT PRIMARY KEY,
                mint TEXT,
                dex TEXT,
                token_a TEXT NOT NULL,
                token_b TEXT NOT NULL,
                reserve_a TEXT,
                reserve_b TEXT,
                price_usd DOUBLE PRECISION DEFAULT 0,
                liquidity_usd DOUBLE PRECISION DEFAULT 0,
                volume_24h DOUBLE PRECISION DEFAULT 0,
                created_at BIGINT,
                FOREIGN KEY(mint) REFERENCES tokens(mint)
            );

            CREATE TABLE IF NOT EXISTS candles_1m (
                pool_address TEXT,
                timestamp BIGINT,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                PRIMARY KEY (pool_address, timestamp)
            );

            CREATE TABLE IF NOT EXISTS active_trackers (
                pool_address TEXT PRIMARY KEY,
                priority INTEGER DEFAULT 1,
                last_check BIGINT DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS token_updates (
                id SERIAL PRIMARY KEY,
                mint TEXT,
                twitter TEXT,
                website TEXT,
                telegram TEXT,
                banner TEXT,
                description TEXT,
                submittedAt BIGINT,
                status TEXT DEFAULT 'pending', 
                signature TEXT,
                payer TEXT
            );

            CREATE TABLE IF NOT EXISTS k_scores (
                mint TEXT PRIMARY KEY,
                score DOUBLE PRECISION,
                components TEXT, 
                updatedAt BIGINT
            );

            CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score DESC);
            CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens(timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_pools_mint ON pools(mint);
            CREATE INDEX IF NOT EXISTS idx_candles_pool_time ON candles_1m(pool_address, timestamp);
        `);

        // --- COMPATIBILITY LAYER ---
        // Maps the previous SQLite-style API (get, all, run) to PostgreSQL
        dbWrapper = {
            query: (text, params) => pool.query(text, params),
            
            // SQLite 'get' returns the first row or undefined
            get: async (text, params) => {
                const res = await pool.query(text, params);
                return res.rows[0];
            },

            // SQLite 'all' returns all rows
            all: async (text, params) => {
                const res = await pool.query(text, params);
                return res.rows;
            },

            // SQLite 'run' returns an object with lastID/changes (simplified here)
            run: async (text, params) => {
                const res = await pool.query(text, params);
                return { rowCount: res.rowCount };
            }
        };

        return dbWrapper;

    } catch (error) {
        logger.error(`❌ Database Init Failed: ${error.message}`);
        throw error;
    }
}

function getDB() {
    if (!dbWrapper) throw new Error("Database not initialized. Call initDB() first.");
    return dbWrapper;
}

// --- HELPER: Smart Caching ---
async function smartCache(key, ttlSeconds, fetchFn) {
    const redis = getClient();
    if (redis) {
        try {
            const cached = await redis.get(key);
            if (cached) return JSON.parse(cached);
        } catch (e) { logger.warn(`Redis Get Error: ${e.message}`); }
    }

    const data = await fetchFn();

    if (redis && data) {
        try {
            await redis.set(key, JSON.stringify(data), 'EX', ttlSeconds);
        } catch (e) { logger.warn(`Redis Set Error: ${e.message}`); }
    }
    return data;
}

// --- HELPER: Indexing Enabler ---
async function enableIndexing(db, mint, poolData) {
    if (!poolData || !poolData.pairAddress) return;

    // FIX: Explicitly extract token addresses to prevent NOT NULL violation
    const tokenA = poolData.baseToken?.address || poolData.baseToken || mint;
    const tokenB = poolData.quoteToken?.address || poolData.quoteToken || 'So11111111111111111111111111111111111111112';

    try {
        await db.run(`
            INSERT INTO pools (
                address, mint, dex, price_usd, liquidity_usd, volume_24h, created_at,
                token_a, token_b, reserve_a, reserve_b
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT(address) DO UPDATE SET
                price_usd = EXCLUDED.price_usd,
                liquidity_usd = EXCLUDED.liquidity_usd,
                volume_24h = EXCLUDED.volume_24h,
                reserve_a = EXCLUDED.reserve_a,
                reserve_b = EXCLUDED.reserve_b
        `, [
            poolData.pairAddress,
            mint,
            poolData.dexId,
            poolData.priceUsd || 0,
            poolData.liquidity?.usd || 0,
            poolData.volume?.h24 || 0,
            Date.now(),
            tokenA,
            tokenB,
            poolData.reserve_a || null,
            poolData.reserve_b || null
        ]);

        await db.run(`
            INSERT INTO active_trackers (pool_address, priority, last_check)
            VALUES ($1, 10, 0)
            ON CONFLICT(pool_address) DO UPDATE SET priority = 10
        `, [poolData.pairAddress]);

        logger.info(`✅ Indexed Pool: ${poolData.pairAddress} (${poolData.dexId})`);
    } catch (err) {
        logger.error(`Database Query Error [enableIndexing]: ${err.message}`);
        throw err;
    }
}

// --- HELPER: Aggregation ---
async function aggregateAndSaveToken(db, mint) {
    try {
        const pools = await db.all(`SELECT * FROM pools WHERE mint = $1`, [mint]);
        if (pools.length === 0) return;

        let totalLiq = 0;
        let totalVol = 0;
        let maxPrice = 0;
        
        let mainPool = pools[0];
        
        for (const p of pools) {
            totalLiq += parseFloat(p.liquidity_usd || 0);
            totalVol += parseFloat(p.volume_24h || 0);
            if (parseFloat(p.liquidity_usd || 0) > parseFloat(mainPool.liquidity_usd || 0)) {
                mainPool = p;
            }
        }
        maxPrice = parseFloat(mainPool.price_usd || 0);

        await db.run(`
            UPDATE tokens 
            SET liquidity = $1, volume24h = $2, priceUsd = $3, marketCap = ($3 * CAST(supply AS DOUBLE PRECISION) / POWER(10, decimals))
            WHERE mint = $4
        `, [totalLiq, totalVol, maxPrice, mint]);
        
    } catch (err) {
        logger.error(`Aggregation Error ${mint}: ${err.message}`);
    }
}

module.exports = {
    initDB,
    getDB,
    smartCache,
    enableIndexing,
    aggregateAndSaveToken
};
