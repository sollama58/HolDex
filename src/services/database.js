const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite');
const path = require('path');
const config = require('../config/env');

let dbInstance = null;

async function initDB() {
    if (dbInstance) return dbInstance;

    // FIX: Ensure persistent path outside of src (one level up from src/services)
    // If running in Docker, map volume to /usr/src/app/data/database.sqlite
    const dbPath = process.env.DB_PATH || path.resolve(__dirname, '../../database.sqlite');
    
    console.log(`🔌 Connecting to SQLite at: ${dbPath}`);

    dbInstance = await open({
        filename: dbPath,
        driver: sqlite3.Database
    });

    console.log("💾 SQLite Database Connected");

    // WAL Mode for Concurrency
    await dbInstance.exec('PRAGMA journal_mode = WAL;');

    // Initialize Schema (IF NOT EXISTS ensures we don't reset data)
    await dbInstance.exec(`
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
            
            marketCap REAL DEFAULT 0,
            volume24h REAL DEFAULT 0,
            priceUsd REAL DEFAULT 0,
            
            change5m REAL DEFAULT 0,
            change1h REAL DEFAULT 0,
            change24h REAL DEFAULT 0,
            
            timestamp INTEGER,
            lastUpdated INTEGER,
            last_k_calc INTEGER DEFAULT 0,
            
            userPubkey TEXT,
            hasCommunityUpdate BOOLEAN DEFAULT 0,
            k_score INTEGER DEFAULT 0
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
            status TEXT,
            signature TEXT,
            payer TEXT
        );

        -- PERFORMANCE INDEXES
        CREATE INDEX IF NOT EXISTS idx_tokens_ticker ON tokens(ticker);
        CREATE INDEX IF NOT EXISTS idx_tokens_name ON tokens(name);
        CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens(timestamp);
        CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score);
    `);

    // Auto-Migration for missing columns (Non-Destructive)
    try {
        const columns = await dbInstance.all("PRAGMA table_info(tokens)");
        const hasLastKCalc = columns.some(c => c.name === 'last_k_calc');
        if (!hasLastKCalc) {
            console.log("⚠️ Migrating DB: Adding missing column 'last_k_calc'...");
            await dbInstance.exec("ALTER TABLE tokens ADD COLUMN last_k_calc INTEGER DEFAULT 0");
        }
    } catch (e) {
        console.error("Migration Error:", e.message);
    }

    return dbInstance;
}

// ... caching wrapper ...
async function smartCache(key, durationSeconds, fetchFunction) {
    const { getClient } = require('./redis'); 
    const redis = getClient();
    
    if (redis && redis.status === 'ready') {
        try {
            const cached = await redis.get(key);
            if (cached) return JSON.parse(cached);
        } catch (e) { console.warn("Redis Get Error", e.message); }
    }

    const data = await fetchFunction();

    if (redis && redis.status === 'ready' && data) {
        try {
            await redis.setex(key, durationSeconds, JSON.stringify(data));
        } catch (e) { console.warn("Redis Set Error", e.message); }
    }

    return data;
}

async function saveTokenData(db, mint, metadata, timestamp = Date.now()) {
    const d = db || dbInstance;
    await d.run(`
        INSERT INTO tokens (mint, name, ticker, image, marketCap, volume24h, priceUsd, timestamp, lastUpdated)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT(mint) DO UPDATE SET
            marketCap = excluded.marketCap,
            volume24h = excluded.volume24h,
            priceUsd = excluded.priceUsd,
            lastUpdated = excluded.lastUpdated
    `, [mint, metadata.name, metadata.ticker, metadata.image, metadata.marketCap, metadata.volume24h, metadata.priceUsd, timestamp, Date.now()]);
}

module.exports = { 
    initDB, 
    smartCache,
    saveTokenData
};
