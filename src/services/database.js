const { Pool } = require('pg');
const config = require('../config/env');

let pool = null;
let dbWrapper = null;

async function initDB() {
    if (dbWrapper) return dbWrapper;

    console.log(`🔌 Connecting to PostgreSQL Database...`);
    
    try {
        pool = new Pool({
            connectionString: config.DATABASE_URL,
            ssl: { rejectUnauthorized: false }, 
            max: 20, 
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 5000,
        });

        const client = await pool.connect();
        console.log("💾 PostgreSQL Connected Successfully");
        client.release();
    } catch (err) {
        console.error("❌ Fatal Database Connection Error:", err.message);
        throw err;
    }

    dbWrapper = {
        pool,
        get: async (text, params) => {
            const res = await pool.query(text, params);
            return res.rows[0];
        },
        all: async (text, params) => {
            const res = await pool.query(text, params);
            return res.rows;
        },
        run: async (text, params) => {
            const res = await pool.query(text, params);
            return { rowCount: res.rowCount }; 
        },
        exec: async (text) => {
            return await pool.query(text);
        }
    };

    await initSchema(dbWrapper);
    return dbWrapper;
}

async function initSchema(db) {
    try {
        // --- TOKENS TABLE (aligned with production) ---
        await db.exec(`
            CREATE TABLE IF NOT EXISTS tokens (
                mint TEXT PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                image TEXT,
                supply TEXT,
                decimals INTEGER,
                priceusd DOUBLE PRECISION DEFAULT 0,
                liquidity DOUBLE PRECISION DEFAULT 0,
                marketcap DOUBLE PRECISION DEFAULT 0,
                volume24h DOUBLE PRECISION DEFAULT 0,
                change24h DOUBLE PRECISION DEFAULT 0,
                change1h DOUBLE PRECISION DEFAULT 0,
                change5m DOUBLE PRECISION DEFAULT 0,
                holders INTEGER DEFAULT 0,
                last_holder_check BIGINT,
                k_score DOUBLE PRECISION DEFAULT 0,
                hascommunityupdate BOOLEAN DEFAULT FALSE,
                metadata TEXT,
                timestamp BIGINT,
                updated_at TIMESTAMP DEFAULT NOW(),
                last_k_score_update BIGINT,
                conviction_score INTEGER DEFAULT 0,
                conviction_accumulators INTEGER DEFAULT 0,
                conviction_holders INTEGER DEFAULT 0,
                conviction_reducers INTEGER DEFAULT 0,
                conviction_extractors INTEGER DEFAULT 0,
                conviction_analyzed INTEGER DEFAULT 0
            );
        `);

        // Add conviction columns if they don't exist (migration for existing DBs)
        try {
            await db.exec(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS conviction_score INTEGER DEFAULT 0;`);
            await db.exec(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS conviction_accumulators INTEGER DEFAULT 0;`);
            await db.exec(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS conviction_holders INTEGER DEFAULT 0;`);
            await db.exec(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS conviction_reducers INTEGER DEFAULT 0;`);
            await db.exec(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS conviction_extractors INTEGER DEFAULT 0;`);
            await db.exec(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS conviction_analyzed INTEGER DEFAULT 0;`);
        } catch(e) { /* columns may already exist */ }

        // --- POOLS TABLE (aligned with production) ---
        await db.exec(`
            CREATE TABLE IF NOT EXISTS pools (
                address TEXT PRIMARY KEY,
                mint TEXT,
                dex TEXT,
                token_a TEXT NOT NULL,
                token_b TEXT NOT NULL,
                reserve_a TEXT,
                reserve_b TEXT,
                price_usd DOUBLE PRECISION,
                liquidity_usd DOUBLE PRECISION,
                volume_24h DOUBLE PRECISION,
                created_at BIGINT,
                symbol TEXT
            );
        `);

        // --- CANDLES TABLE ---
        await db.exec(`
            CREATE TABLE IF NOT EXISTS candles (
                symbol TEXT,
                time BIGINT,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                PRIMARY KEY (symbol, time)
            );
        `);

        // --- HOLDER HISTORY TABLE (daily snapshots) ---
        await db.exec(`
            CREATE TABLE IF NOT EXISTS holder_history (
                mint TEXT,
                date DATE,
                holders INTEGER DEFAULT 0,
                real_holders INTEGER DEFAULT 0,
                PRIMARY KEY (mint, date)
            );
        `);

        // --- K-SCORE HISTORY TABLE (daily snapshots for credit rating trajectory) ---
        await db.exec(`
            CREATE TABLE IF NOT EXISTS k_score_history (
                mint TEXT,
                date DATE,
                k_score INTEGER DEFAULT 0,
                conviction_score INTEGER DEFAULT 0,
                holders INTEGER DEFAULT 0,
                PRIMARY KEY (mint, date)
            );
        `);

        try {
            await db.exec(`CREATE INDEX IF NOT EXISTS idx_candles_symbol_time ON candles(symbol, time DESC);`);
            await db.exec(`CREATE INDEX IF NOT EXISTS idx_pools_mint ON pools(mint);`);
            await db.exec(`CREATE INDEX IF NOT EXISTS idx_pools_liquidity ON pools(liquidity_usd DESC);`);
            await db.exec(`CREATE INDEX IF NOT EXISTS idx_holder_history_mint ON holder_history(mint, date DESC);`);
            await db.exec(`CREATE INDEX IF NOT EXISTS idx_kscore_history_mint ON k_score_history(mint, date DESC);`);
        } catch (e) { /* index may already exist */ }

        await db.exec(`CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score);`);

    } catch (err) {
        console.error("❌ Database Schema Init Error:", err);
    }
}

async function smartCache(key, durationSeconds, fetchFunction) {
    let redis = null;
    try {
        const redisModule = require('./redis');
        if (redisModule && redisModule.getClient) {
            redis = redisModule.getClient();
        }
    } catch (e) { }
    
    if (redis && redis.status === 'ready') {
        try {
            const cached = await redis.get(key);
            if (cached) return JSON.parse(cached);
        } catch (e) { }
    }

    const data = await fetchFunction();

    if (redis && redis.status === 'ready' && data) {
        try {
            await redis.setex(key, durationSeconds, JSON.stringify(data));
        } catch (e) { }
    }

    return data;
}

async function saveTokenData(db, mint, metadata, timestamp = Date.now()) {
    const d = db || dbWrapper;
    if (!d) return;
    try {
        await d.run(`
            INSERT INTO tokens (
                mint, name, symbol, image,
                marketcap, volume24h, priceusd,
                timestamp, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            ON CONFLICT(mint) DO UPDATE SET
                marketcap = EXCLUDED.marketcap,
                volume24h = EXCLUDED.volume24h,
                priceusd = EXCLUDED.priceusd,
                updated_at = NOW()
        `, [
            mint,
            metadata.name,
            metadata.ticker || metadata.symbol,
            metadata.image,
            metadata.marketCap || 0,
            metadata.volume24h || 0,
            metadata.priceUsd || 0,
            timestamp
        ]);
    } catch (e) {
        console.error(`Error saving token ${mint}:`, e.message);
    }
}

module.exports = { initDB, smartCache, saveTokenData };
