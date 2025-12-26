const { Pool } = require('pg');
const config = require('../config/env');
const logger = require('./logger');
const { getClient } = require('./redis');

let pool = null;
let dbWrapper = null;
let initPromise = null; // Lock for concurrent calls

async function initDB() {
    // If already initialized, return immediately
    if (dbWrapper) return dbWrapper;
    
    // If initialization is in progress, return the pending promise
    if (initPromise) return initPromise;

    // Start initialization and assign to lock
    initPromise = (async () => {
        try {
            const isLocal = config.DATABASE_URL.includes('localhost') || config.DATABASE_URL.includes('127.0.0.1');
            
            pool = new Pool({
                connectionString: config.DATABASE_URL,
                ssl: isLocal ? false : { rejectUnauthorized: false },
                max: 20, 
                idleTimeoutMillis: 30000,
                connectionTimeoutMillis: 5000, // Increased timeout
            });

            // Handle unexpected errors on idle clients
            pool.on('error', (err, client) => {
                logger.error(`Unexpected error on idle DB client: ${err.message}`);
            });

            logger.info(`📦 Database: Connecting to PostgreSQL...`);
            const client = await pool.connect();
            client.release();
            logger.info(`📦 Database: Connection Successful.`);

            // Schema Creation (Idempotent)
            await pool.query(`
                CREATE TABLE IF NOT EXISTS tokens (
                    mint TEXT PRIMARY KEY,
                    name TEXT,
                    symbol TEXT,
                    image TEXT,
                    supply TEXT,
                    decimals INTEGER DEFAULT 9,
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

            dbWrapper = {
                query: (text, params) => pool.query(text, params),
                get: async (text, params) => { const res = await pool.query(text, params); return res.rows[0]; },
                all: async (text, params) => { const res = await pool.query(text, params); return res.rows; },
                run: async (text, params) => { const res = await pool.query(text, params); return { rowCount: res.rowCount }; }
            };

            return dbWrapper;

        } catch (error) {
            logger.error(`❌ Database Init Failed: ${error.message}`);
            initPromise = null; // Reset lock on failure so we can retry
            throw error;
        }
    })();

    return initPromise;
}

function getDB() {
    if (!dbWrapper) throw new Error("Database not initialized. Call initDB() first.");
    return dbWrapper;
}

async function smartCache(key, ttlSeconds, fetchFn) {
    const redis = getClient();
    if (redis) {
        try {
            const cached = await redis.get(key);
            if (cached) return JSON.parse(cached);
        } catch (e) {}
    }
    const data = await fetchFn();
    if (redis && data) {
        try { await redis.set(key, JSON.stringify(data), 'EX', ttlSeconds); } catch (e) {}
    }
    return data;
}

async function enableIndexing(db, mint, poolData) {
    if (!poolData || !poolData.pairAddress) return;

    const resolveToken = (val, fallback) => {
        if (!val) return fallback;
        if (typeof val === 'string') return val;
        if (typeof val === 'object' && val.address) return val.address;
        return fallback; 
    };

    const tokenA = resolveToken(poolData.baseToken, mint); 
    const tokenB = resolveToken(poolData.quoteToken, 'So11111111111111111111111111111111111111112'); 

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

async function aggregateAndSaveToken(db, mint) {
    try {
        const pools = await db.all(`SELECT * FROM pools WHERE mint = $1`, [mint]);
        if (pools.length === 0) return;

        let totalLiq = 0;
        let totalVol = 0;
        let mainPool = pools[0]; 
        
        // Find the most liquid pool to use as the price source
        for (const p of pools) {
            const liq = parseFloat(p.liquidity_usd || 0);
            const vol = parseFloat(p.volume_24h || 0);
            totalLiq += liq;
            totalVol += vol;
            if (liq > parseFloat(mainPool.liquidity_usd || 0)) {
                mainPool = p;
            }
        }

        const price = parseFloat(mainPool.price_usd || 0);

        // --- INTELLIGENT CHANGE CALCULATION ---
        let change24h = null;
        let change1h = null;
        let change5m = null;

        if (price > 0) {
            const now = Date.now();
            const time24h = now - (24 * 60 * 60 * 1000);
            const time1h = now - (60 * 60 * 1000);
            const time5m = now - (5 * 60 * 1000);

            const getPriceAt = async (ts) => {
                const row = await db.get(
                    `SELECT close FROM candles_1m WHERE pool_address = $1 AND timestamp <= $2 ORDER BY timestamp DESC LIMIT 1`, 
                    [mainPool.address, ts]
                );
                return row ? parseFloat(row.close) : null;
            };

            const [p24h, p1h, p5m] = await Promise.all([
                getPriceAt(time24h),
                getPriceAt(time1h),
                getPriceAt(time5m)
            ]);

            // Only calculate if we found a historical candle.
            // Explicitly cast to prevent math errors.
            if (p24h !== null && p24h > 0) change24h = ((price - p24h) / p24h) * 100;
            if (p1h !== null && p1h > 0) change1h = ((price - p1h) / p1h) * 100;
            if (p5m !== null && p5m > 0) change5m = ((price - p5m) / p5m) * 100;
        }

        // --- SAFE UPDATE ---
        // If change24h is null (because we have no history), we DO NOT update that column.
        // This prevents overwriting valid data fetched by the MetadataUpdater (GeckoTerminal) with '0'.
        
        const updates = [];
        const params = [totalLiq, totalVol, price, mint];
        let query = `UPDATE tokens SET liquidity = $1, volume24h = $2, priceUsd = $3`;
        
        // Always calculate Mcap based on new price.
        // COALESCE(decimals, 9) ensures we don't divide by null if decimals missing.
        query += `, marketCap = ($3 * CAST(supply AS DOUBLE PRECISION) / POWER(10, COALESCE(decimals, 9)))`;

        let pIdx = 5;

        if (change24h !== null) {
            query += `, change24h = $${pIdx++}`;
            params.push(change24h);
        }
        if (change1h !== null) {
            query += `, change1h = $${pIdx++}`;
            params.push(change1h);
        }
        if (change5m !== null) {
            query += `, change5m = $${pIdx++}`;
            params.push(change5m);
        }

        query += ` WHERE mint = $4`;

        await db.run(query, params);
        
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
