const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite');
const path = require('path');
const config = require('../config/env');

let dbInstance = null;

async function initDB() {
    if (dbInstance) return dbInstance;

    // --- CRITICAL PATH FIX ---
    // We must ensure the DB file is stored in a PERSISTENT location.
    // In Docker, we typically mount a volume to /usr/src/app/data or the project root.
    // Locally, we want it in the project root.
    
    // Resolve path relative to THIS file (src/services/database.js) -> up two levels to root
    const dbPath = process.env.DB_PATH || path.resolve(__dirname, '../../database.sqlite');
    
    console.log(`🔌 Connecting to SQLite at: ${dbPath}`);

    try {
        dbInstance = await open({
            filename: dbPath,
            driver: sqlite3.Database
        });

        console.log("💾 SQLite Database Connected Successfully");

        // --- CONCURRENCY SETTINGS ---
        // WAL mode is essential for concurrent reads/writes
        await dbInstance.exec('PRAGMA journal_mode = WAL;');
        await dbInstance.exec('PRAGMA synchronous = NORMAL;'); // Safer for WAL

        // --- SCHEMA DEFINITION ---
        // We define the FULL schema here. The "IF NOT EXISTS" clause prevents resetting data.
        
        // 1. Tokens Table
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
        `);

        // 2. Token Updates Table
        await dbInstance.exec(`
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
        `);

        // --- INDEXES (Crucial for Search Performance) ---
        await dbInstance.exec(`
            CREATE INDEX IF NOT EXISTS idx_tokens_ticker ON tokens(ticker);
            CREATE INDEX IF NOT EXISTS idx_tokens_name ON tokens(name);
            CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens(timestamp);
            CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score);
            CREATE INDEX IF NOT EXISTS idx_tokens_mcap ON tokens(marketCap); -- Added for sorting by Mcap
        `);

        // --- AUTO-MIGRATION (Repair Schema if needed) ---
        // This block checks for columns that might be missing in older DB versions
        // and adds them without deleting data.
        
        const columns = await dbInstance.all("PRAGMA table_info(tokens)");
        const columnNames = columns.map(c => c.name);

        const requiredColumns = [
            'last_k_calc',
            'k_score',
            'hasCommunityUpdate',
            'marketCap' // Ensure basic fields exist
        ];

        for (const col of requiredColumns) {
            if (!columnNames.includes(col)) {
                console.log(`⚠️ Auto-Migrating: Adding missing column '${col}' to tokens table...`);
                // SQLite ALTER TABLE only allows adding one column at a time
                let type = 'INTEGER DEFAULT 0';
                if (col === 'hasCommunityUpdate') type = 'BOOLEAN DEFAULT 0';
                if (col === 'marketCap') type = 'REAL DEFAULT 0';
                
                await dbInstance.exec(`ALTER TABLE tokens ADD COLUMN ${col} ${type}`);
            }
        }
        
    } catch (err) {
        console.error("❌ Fatal Database Initialization Error:", err);
        throw err; // Stop server if DB fails
    }

    return dbInstance;
}

// ... Caching Wrapper ...
async function smartCache(key, durationSeconds, fetchFunction) {
    let redis = null;
    try {
        // Safe lazy load
        const redisModule = require('./redis');
        if (redisModule && redisModule.getClient) {
            redis = redisModule.getClient();
        }
    } catch (e) { /* ignore */ }
    
    if (redis && redis.status === 'ready') {
        try {
            const cached = await redis.get(key);
            if (cached) return JSON.parse(cached);
        } catch (e) { /* ignore cache error */ }
    }

    // Execute the actual data fetch
    const data = await fetchFunction();

    if (redis && redis.status === 'ready' && data) {
        try {
            await redis.setex(key, durationSeconds, JSON.stringify(data));
        } catch (e) { /* ignore cache error */ }
    }

    return data;
}

async function saveTokenData(db, mint, metadata, timestamp = Date.now()) {
    const d = db || dbInstance;
    if (!d) { console.error("DB not initialized in saveTokenData"); return; }
    
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
                -- We DO NOT update timestamp on conflict to preserve "Age"
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
        console.log(`✅ Saved token data for ${mint}`);
    } catch (e) {
        console.error(`Error saving token ${mint}:`, e.message);
    }
}

module.exports = { 
    initDB, 
    smartCache,
    saveTokenData
};
