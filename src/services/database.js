const { Pool } = require('pg');
const config = require('../config/env');

let pool = null;
let dbWrapper = null;

async function initDB() {
    if (dbWrapper) return dbWrapper;

    // 1. Initialize PostgreSQL Connection Pool
    console.log(`🔌 Connecting to PostgreSQL Database...`);
    
    try {
        pool = new Pool({
            connectionString: config.DATABASE_URL,
            ssl: { rejectUnauthorized: false }, // Required for hosted databases (Render/Neon)
            max: 20, // Connection pool size
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 5000,
        });

        // Test the connection
        const client = await pool.connect();
        console.log("💾 PostgreSQL Connected Successfully");
        client.release();
    } catch (err) {
        console.error("❌ Fatal Database Connection Error:", err.message);
        throw err; // Retry or crash
    }

    // 2. Create Compatibility Wrapper
    dbWrapper = {
        pool,
        // Fetch single row
        get: async (text, params) => {
            const res = await pool.query(text, params);
            return res.rows[0];
        },
        // Fetch all rows
        all: async (text, params) => {
            const res = await pool.query(text, params);
            return res.rows;
        },
        // Execute command (INSERT, UPDATE, DELETE)
        run: async (text, params) => {
            const res = await pool.query(text, params);
            return { rowCount: res.rowCount }; 
        },
        // Execute raw script
        exec: async (text) => {
            return await pool.query(text);
        }
    };

    // 3. Initialize Schema & Migrations
    await initSchema(dbWrapper);

    return dbWrapper;
}

async function initSchema(db) {
    try {
        // --- TOKENS TABLE (Existing) ---
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
                k_score INTEGER DEFAULT 0
            );
        `);

        // --- UPDATES TABLE (Existing) ---
        await db.exec(`
            CREATE TABLE IF NOT EXISTS token_updates (
                id SERIAL PRIMARY KEY,
                mint TEXT,
                twitter TEXT,
                website TEXT,
                telegram TEXT,
                banner TEXT,
                description TEXT,
                submittedAt BIGINT,
                status TEXT,
                signature TEXT,
                payer TEXT
            );
        `);

        // --- NEW: POOLS TABLE (For Indexer) ---
        // Stores the vault addresses we need to watch
        await db.exec(`
            CREATE TABLE IF NOT EXISTS pools (
                symbol TEXT PRIMARY KEY,
                name TEXT,
                base_vault TEXT,
                quote_vault TEXT,
                base_decimals INTEGER,
                quote_decimals INTEGER
            );
        `);

        // --- NEW: CANDLES TABLE (For Charts) ---
        // Stores OHLC data for the charts
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

        // --- INDEXES ---
        await db.exec(`CREATE INDEX IF NOT EXISTS idx_tokens_ticker ON tokens(ticker);`);
        await db.exec(`CREATE INDEX IF NOT EXISTS idx_tokens_name ON tokens(name);`);
        await db.exec(`CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens(timestamp);`);
        await db.exec(`CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score);`);
        await db.exec(`CREATE INDEX IF NOT EXISTS idx_candles_time ON candles(symbol, time DESC);`);

        // --- SEED INITIAL POOL (Example) ---
        // We seed SOL/USDC so the chart works immediately for testing
        await db.run(`
            INSERT INTO pools (symbol, name, base_vault, quote_vault, base_decimals, quote_decimals)
            VALUES ('SOL', 'SOL / USDC', 'DQyrAcCrDXQ7NeoqGgDCZwBvWDcYmFCjSb9JtteuvPpz', 'HLmqeL62xR1QoZ1HKKbXRrdN1p3ph9EHDW6o72a6WzGV', 9, 6)
            ON CONFLICT (symbol) DO NOTHING;
        `);
        
    } catch (err) {
        console.error("❌ Database Schema Init Error:", err);
    }
}

// Caching wrapper (Unchanged)
async function smartCache(key, durationSeconds, fetchFunction) {
    let redis = null;
    try {
        const redisModule = require('./redis');
        if (redisModule && redisModule.getClient) {
            redis = redisModule.getClient();
        }
    } catch (e) { /* ignore */ }
    
    if (redis && redis.status === 'ready') {
        try {
            const cached = await redis.get(key);
            if (cached) return JSON.parse(cached);
        } catch (e) { /* ignore */ }
    }

    const data = await fetchFunction();

    if (redis && redis.status === 'ready' && data) {
        try {
            await redis.setex(key, durationSeconds, JSON.stringify(data));
        } catch (e) { /* ignore */ }
    }

    return data;
}

// Save Token wrapper
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

module.exports = { 
    initDB, 
    smartCache,
    saveTokenData
};
