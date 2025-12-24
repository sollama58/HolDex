const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite');
const path = require('path');
const config = require('../config/env');
const redisClient = require('./redis').getClient();

let dbInstance = null;

async function initDB() {
    if (dbInstance) return dbInstance;

    // Ensure we look for the DB in a place that persists (e.g., volume mount in Docker)
    const dbPath = path.resolve(__dirname, '../../database.sqlite');
    
    dbInstance = await open({
        filename: dbPath,
        driver: sqlite3.Database
    });

    console.log("💾 SQLite Database Connected");

    // --- CONCURRENCY OPTIMIZATION ---
    // WAL (Write-Ahead Logging) allows simultaneous Readers and Writers.
    // Critical for handling "hundreds of users" while updating metadata.
    await dbInstance.exec('PRAGMA journal_mode = WAL;');

    // Initialize Schema
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
            status TEXT, -- 'pending', 'approved', 'rejected'
            signature TEXT,
            payer TEXT
        );

        -- PERFORMANCE INDEXES (Phase 1 Stabilizer) --
        -- These speed up searches and sorting massively
        CREATE INDEX IF NOT EXISTS idx_tokens_ticker ON tokens(ticker);
        CREATE INDEX IF NOT EXISTS idx_tokens_name ON tokens(name);
        CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens(timestamp);
        CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score);
    `);

    return dbInstance;
}

// ... existing wrapper functions for caching ...
async function smartCache(key, durationSeconds, fetchFunction) {
    const redis = require('./redis').getClient();
    
    // Safety check for Redis connection
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
