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
        // --- TOKENS TABLE ---
        await db.exec(`
            CREATE TABLE IF NOT EXISTS tokens (
                mint TEXT PRIMARY KEY,
                name TEXT,
                ticker TEXT,
                image TEXT,
                banner TEXT,
                description TEXT,
                website TEXT,
                twitter TEXT,
                tweetUrl TEXT,
                telegram TEXT,
                marketCap DOUBLE PRECISION DEFAULT 0,
                volume24h DOUBLE PRECISION DEFAULT 0,
                priceUsd DOUBLE PRECISION DEFAULT 0,
                change5m DOUBLE PRECISION DEFAULT 0,
                change1h DOUBLE PRECISION DEFAULT 0,
                change24h DOUBLE PRECISION DEFAULT 0,
                timestamp BIGINT,
                lastUpdated BIGINT,
                last_k_calc BIGINT DEFAULT 0,
                userPubkey TEXT,
                hasCommunityUpdate BOOLEAN DEFAULT FALSE,
                k_score INTEGER DEFAULT 0,
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

        // --- POOLS TABLE ---
        // Note: Production may have different schema, CREATE IF NOT EXISTS won't modify existing table
        await db.exec(`
            CREATE TABLE IF NOT EXISTS pools (
                mint TEXT PRIMARY KEY,
                symbol TEXT,
                pair_address TEXT,
                base_vault TEXT,
                quote_vault TEXT,
                base_decimals INTEGER,
                quote_decimals INTEGER
            );
        `);

        // Add symbol column if it doesn't exist (migration for existing DBs)
        try {
            await db.exec(`ALTER TABLE pools ADD COLUMN IF NOT EXISTS symbol TEXT;`);
        } catch (e) { /* column may already exist */ }

        // --- CANDLES TABLE ---
        // Uses 'symbol' as key (matches production schema)
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

        try {
            await db.exec(`CREATE INDEX IF NOT EXISTS idx_candles_symbol_time ON candles(symbol, time DESC);`);
        } catch (e) { /* index may already exist with different definition */ }

        await db.exec(`CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score);`);

        // --- SEED SOLANA (Example) ---
        // Seed only if pools table has symbol column
        try {
            await db.run(`
                INSERT INTO pools (mint, symbol, base_vault, quote_vault, base_decimals, quote_decimals)
                VALUES ('So11111111111111111111111111111111111111112', 'SOL', 'DQyrAcCrDXQ7NeoqGgDCZwBvWDcYmFCjSb9JtteuvPpz', 'HLmqeL62xR1QoZ1HKKbXRrdN1p3ph9EHDW6o72a6WzGV', 9, 6)
                ON CONFLICT (mint) DO NOTHING;
            `);
        } catch (e) { /* seed may fail on incompatible schema */ }
        
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
                mint, name, ticker, image, 
                marketCap, volume24h, priceUsd, 
                timestamp, lastUpdated
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT(mint) DO UPDATE SET
                marketCap = excluded.marketCap,
                volume24h = excluded.volume24h,
                priceUsd = excluded.priceUsd,
                lastUpdated = excluded.lastUpdated
        `, [
            mint, 
            metadata.name, 
            metadata.ticker, 
            metadata.image, 
            metadata.marketCap || 0, 
            metadata.volume24h || 0, 
            metadata.priceUsd || 0, 
            timestamp, 
            Date.now()
        ]);
    } catch (e) {
        console.error(`Error saving token ${mint}:`, e.message);
    }
}

module.exports = { initDB, smartCache, saveTokenData };
