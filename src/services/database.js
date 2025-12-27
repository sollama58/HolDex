const { Pool } = require('pg');
const config = require('../config/env');
const logger = require('./logger');
const { getClient } = require('./redis');
const { getHolderCountFromRPC } = require('./solana'); 

let primaryPool = null;
let readPool = null; 
let dbWrapper = null;
let initPromise = null;

const pendingRequests = new Map();

async function initDB() {
    if (dbWrapper) return dbWrapper;
    if (initPromise) return initPromise;

    initPromise = (async () => {
        try {
            const isLocal = config.DATABASE_URL.includes('localhost') || config.DATABASE_URL.includes('127.0.0.1');
            const sslConfig = isLocal ? false : { rejectUnauthorized: false };

            // 1. Primary Connection (Writes)
            primaryPool = new Pool({
                connectionString: config.DATABASE_URL,
                ssl: sslConfig,
                max: 50, 
                idleTimeoutMillis: 30000,
                connectionTimeoutMillis: 5000,
            });

            primaryPool.on('error', (err) => logger.error(`Unexpected error on Primary DB: ${err.message}`));

            logger.info(`🔌 Database: Connecting to Primary...`);
            const client = await primaryPool.connect();
            client.release();
            logger.info(`🔌 Database: Primary Connection Successful.`);

            // 2. Read Replica Connection (Optional)
            if (process.env.READ_DATABASE_URL) {
                readPool = new Pool({
                    connectionString: process.env.READ_DATABASE_URL,
                    ssl: sslConfig,
                    max: 50, 
                    idleTimeoutMillis: 30000,
                    connectionTimeoutMillis: 5000,
                });
                readPool.on('error', (err) => logger.error(`Unexpected error on Read Replica: ${err.message}`));
                
                const readClient = await readPool.connect();
                readClient.release();
                logger.info(`🔌 Database: Read Replica Connected.`);
            } else {
                readPool = primaryPool; // Fallback to primary if no replica
                logger.info(`🔌 Database: No Read Replica configured. Using Primary for reads.`);
            }

            // --- TABLE CREATION ---
            await primaryPool.query(`
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
                    holders INTEGER DEFAULT 0,
                    last_holder_check BIGINT DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    k_score DOUBLE PRECISION DEFAULT 0,
                    hasCommunityUpdate BOOLEAN DEFAULT FALSE,
                    metadata TEXT,
                    timestamp BIGINT
                );

                CREATE TABLE IF NOT EXISTS holders_history (
                    mint TEXT,
                    count INTEGER,
                    timestamp BIGINT,
                    PRIMARY KEY (mint, timestamp)
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
                CREATE INDEX IF NOT EXISTS idx_holders_hist_mint ON holders_history(mint);
            `);

            // --- AUTO-MIGRATIONS ---
            try {
                await primaryPool.query(`
                    ALTER TABLE tokens ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                    ALTER TABLE tokens ADD COLUMN IF NOT EXISTS last_holder_check BIGINT DEFAULT 0;
                    ALTER TABLE tokens ADD COLUMN IF NOT EXISTS holders INTEGER DEFAULT 0;
                `);
                await primaryPool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_updated_at ON tokens(updated_at ASC)`);
                
                // --- FIX: RESET STUCK TOKENS ---
                // Reset the timer for any token that incorrectly has 0 holders so they update immediately
                await primaryPool.query(`UPDATE tokens SET last_holder_check = 0 WHERE holders = 0`);
                // logger.info("✅ Database: Reset holder check for tokens with 0 holders.");

            } catch (migErr) {
                logger.warn(`Migration Warning (non-fatal): ${migErr.message}`);
            }

            dbWrapper = {
                query: (text, params) => {
                    const isSelect = text.trim().toUpperCase().startsWith('SELECT');
                    return (isSelect ? readPool : primaryPool).query(text, params);
                },
                get: async (text, params) => { 
                    const res = await readPool.query(text, params); 
                    return res.rows[0]; 
                },
                all: async (text, params) => { 
                    const res = await readPool.query(text, params); 
                    return res.rows; 
                },
                run: async (text, params) => { 
                    const res = await primaryPool.query(text, params); 
                    return { rowCount: res.rowCount }; 
                }
            };

            return dbWrapper;

        } catch (error) {
            logger.error(`❌ Database Init Failed: ${error.message}`);
            initPromise = null;
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

    if (pendingRequests.has(key)) return pendingRequests.get(key);

    const fetchPromise = (async () => {
        try {
            const data = await fetchFn();
            if (redis && data) {
                redis.set(key, JSON.stringify(data), 'EX', ttlSeconds).catch(() => {}); 
            }
            return data;
        } finally {
            pendingRequests.delete(key);
        }
    })();

    pendingRequests.set(key, fetchPromise);
    return fetchPromise;
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
    } catch (err) {
        logger.error(`Database Query Error [enableIndexing]: ${err.message}`);
    }
}

async function aggregateAndSaveToken(db, mint) {
    try {
        const pools = await db.all(`SELECT * FROM pools WHERE mint = $1`, [mint]);
        if (pools.length === 0) return;

        let totalLiq = 0;
        let totalVol = 0;
        let mainPool = pools[0]; 
        
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

            if (p24h !== null && p24h > 0) change24h = ((price - p24h) / p24h) * 100;
            if (p1h !== null && p1h > 0) change1h = ((price - p1h) / p1h) * 100;
            if (p5m !== null && p5m > 0) change5m = ((price - p5m) / p5m) * 100;
        }

        // --- HOLDER CHECK LOGIC ---
        const now = Date.now();
        const oneHour = 60 * 60 * 1000;
        
        const tokenRow = await db.get(`SELECT last_holder_check FROM tokens WHERE mint = $1`, [mint]);
        const lastCheck = parseInt(tokenRow?.last_holder_check || 0);
        
        let holderCount = null;

        if (lastCheck === 0 || (now - lastCheck > oneHour)) {
            try {
                // Fetch from RPC (Heavy operation, hence the throttle)
                const count = await getHolderCountFromRPC(mint);
                
                // Only update if we got a valid number
                // IMPORTANT: We check >= 0 because count can legally be 0, but if it is 0 due to error it will persist.
                // However, the new solenoid.js fix ensures we don't falsely return 0 for Token2022.
                if (typeof count === 'number') {
                    holderCount = count;
                    // Save history
                    await db.run(`
                        INSERT INTO holders_history (mint, count, timestamp) 
                        VALUES ($1, $2, $3)
                        ON CONFLICT (mint, timestamp) DO NOTHING
                    `, [mint, holderCount, now]);
                }
            } catch (e) {
                logger.warn(`Holder check failed for ${mint}: ${e.message}`);
            }
        }

        const updates = [];
        const params = [totalLiq, totalVol, price, mint, Date.now()]; 
        let query = `UPDATE tokens SET liquidity = $1, volume24h = $2, priceUsd = $3`;
        query += `, marketCap = ($3 * CAST(supply AS DOUBLE PRECISION) / POWER(10, COALESCE(decimals, 9)))`;
        
        // Explicitly update timestamp
        query += `, timestamp = $5`;

        let pIdx = 6;

        if (change24h !== null) { query += `, change24h = $${pIdx++}`; params.push(change24h); }
        if (change1h !== null) { query += `, change1h = $${pIdx++}`; params.push(change1h); }
        if (change5m !== null) { query += `, change5m = $${pIdx++}`; params.push(change5m); }
        
        if (holderCount !== null) { 
            query += `, holders = $${pIdx++}, last_holder_check = $${pIdx++}`; 
            params.push(holderCount); 
            params.push(now);
        }

        query += ` WHERE mint = $4`;

        await db.run(query, params);
        
    } catch (err) {
        logger.error(`Aggregation Error ${mint}: ${err.message}`);
    }
}

module.exports = { initDB, getDB, smartCache, enableIndexing, aggregateAndSaveToken };
