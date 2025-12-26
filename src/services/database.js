const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite');
const path = require('path');
const config = require('../config/env');
const logger = require('./logger');
const { getClient } = require('./redis');

let dbInstance = null;

async function initDB() {
    if (dbInstance) return dbInstance;

    try {
        const dbPath = path.resolve(__dirname, '../../holdex.db');
        dbInstance = await open({
            filename: dbPath,
            driver: sqlite3.Database
        });

        logger.info(`📦 Database: Connected to ${dbPath}`);

        // --- SCHEMA DEFINITIONS ---
        await dbInstance.exec(`
            CREATE TABLE IF NOT EXISTS tokens (
                mint TEXT PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                image TEXT,
                supply TEXT,
                decimals INTEGER,
                priceUsd REAL,
                liquidity REAL,
                marketCap REAL,
                volume24h REAL,
                change24h REAL,
                change1h REAL,
                change5m REAL,
                k_score REAL DEFAULT 0,
                hasCommunityUpdate BOOLEAN DEFAULT FALSE,
                metadata TEXT,
                timestamp INTEGER
            );

            CREATE TABLE IF NOT EXISTS pools (
                address TEXT PRIMARY KEY,
                mint TEXT,
                dex TEXT,
                token_a TEXT NOT NULL,
                token_b TEXT NOT NULL,
                reserve_a TEXT,
                reserve_b TEXT,
                price_usd REAL DEFAULT 0,
                liquidity_usd REAL DEFAULT 0,
                volume_24h REAL DEFAULT 0,
                created_at INTEGER,
                FOREIGN KEY(mint) REFERENCES tokens(mint)
            );

            CREATE TABLE IF NOT EXISTS candles_1m (
                pool_address TEXT,
                timestamp INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY (pool_address, timestamp)
            );

            CREATE TABLE IF NOT EXISTS active_trackers (
                pool_address TEXT PRIMARY KEY,
                priority INTEGER DEFAULT 1,
                last_check INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS token_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mint TEXT,
                twitter TEXT,
                website TEXT,
                telegram TEXT,
                banner TEXT,
                description TEXT,
                submittedAt INTEGER,
                status TEXT DEFAULT 'pending', 
                signature TEXT,
                payer TEXT
            );

            CREATE TABLE IF NOT EXISTS k_scores (
                mint TEXT PRIMARY KEY,
                score REAL,
                components TEXT, 
                updatedAt INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score DESC);
            CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens(timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_pools_mint ON pools(mint);
            CREATE INDEX IF NOT EXISTS idx_candles_pool_time ON candles_1m(pool_address, timestamp);
        `);

        return dbInstance;
    } catch (error) {
        logger.error(`❌ Database Init Failed: ${error.message}`);
        throw error;
    }
}

function getDB() {
    if (!dbInstance) throw new Error("Database not initialized. Call initDB() first.");
    return dbInstance;
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

    // FIX: Explicitly extract token addresses. 
    // Handle both object structure { address: '...' } and raw strings.
    const tokenA = poolData.baseToken?.address || poolData.baseToken || mint;
    // Default to SOL if missing (common quote token)
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
        // Rethrow to ensure caller knows it failed
        throw err; 
    }
}

// --- HELPER: Aggregation ---
async function aggregateAndSaveToken(db, mint) {
    try {
        const pools = await db.all(`SELECT * FROM pools WHERE mint = $1`, [mint]);
        if (pools.length === 0) return;

        // Simple aggregation logic
        let totalLiq = 0;
        let totalVol = 0;
        let maxPrice = 0;
        
        // Find pool with highest liquidity for price reference
        let mainPool = pools[0];
        
        for (const p of pools) {
            totalLiq += p.liquidity_usd || 0;
            totalVol += p.volume_24h || 0;
            if ((p.liquidity_usd || 0) > (mainPool.liquidity_usd || 0)) {
                mainPool = p;
            }
        }
        maxPrice = mainPool.price_usd || 0;

        await db.run(`
            UPDATE tokens 
            SET liquidity = $1, volume24h = $2, priceUsd = $3, marketCap = ($3 * supply / POWER(10, decimals))
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
