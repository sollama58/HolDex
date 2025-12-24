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
    // This allows us to keep using db.get, db.all, db.run syntax across the app
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
        // --- TOKENS TABLE ---
        // Converted types: REAL -> DOUBLE PRECISION, INTEGER -> BIGINT
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

        // --- UPDATES TABLE ---
        // Converted: AUTOINCREMENT -> SERIAL
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

        // --- INDEXES ---
        await db.exec(`CREATE INDEX IF NOT EXISTS idx_tokens_ticker ON tokens(ticker);`);
        await db.exec(`CREATE INDEX IF NOT EXISTS idx_tokens_name ON tokens(name);`);
        await db.exec(`CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens(timestamp);`);
        await db.exec(`CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score);`);
        await db.exec(`CREATE INDEX IF NOT EXISTS idx_tokens_mcap ON tokens(marketCap);`);

        // --- AUTO-MIGRATION (Column Repair) ---
        // PostgreSQL specific: Check information_schema
        const res = await db.all(`
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'tokens';
        `);
        const existingCols = res.map(r => r.column_name);

        const requiredColumns = [
            { name: 'last_k_calc', type: 'BIGINT DEFAULT 0' },
            { name: 'k_score', type: 'INTEGER DEFAULT 0' },
            { name: 'hascommunityupdate', type: 'BOOLEAN DEFAULT FALSE' }, // Postgres stores lowercase
            { name: 'banner', type: 'TEXT' },
            { name: 'description', type: 'TEXT' }
        ];

        for (const col of requiredColumns) {
            // Check lowercase because Postgres returns lowercase column names
            if (!existingCols.includes(col.name.toLowerCase())) {
                console.log(`⚠️ Auto-Migrating: Adding missing column '${col.name}' to tokens...`);
                await db.exec(`ALTER TABLE tokens ADD COLUMN ${col.name} ${col.type}`);
            }
        }
        
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
        // Postgres ON CONFLICT syntax is standard
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
