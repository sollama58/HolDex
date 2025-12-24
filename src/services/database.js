const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite');
const path = require('path');
const fs = require('fs');
const config = require('../config/env');

let dbInstance = null;

async function initDB() {
    if (dbInstance) return dbInstance;

    // --- PERSISTENCE CONFIGURATION ---
    // Critical: Ensure we use the absolute path matching the Docker Volume if available.
    // Dockerfile VOLUME is ["/usr/src/app/data"]
    
    let dbPath;
    
    // 1. Check for Environment Variable Override
    if (process.env.DB_PATH) {
        dbPath = process.env.DB_PATH;
    } 
    // 2. Check for Standard Docker/Render Mount Path
    else if (fs.existsSync('/usr/src/app/data')) {
        console.log("📂 Detected Docker Volume at /usr/src/app/data");
        dbPath = '/usr/src/app/data/database.sqlite';
    }
    // 3. Fallback to Local Relative Path (Dev Mode)
    else {
        const projectRoot = path.resolve(__dirname, '../../');
        const dataDir = path.join(projectRoot, 'data');
        if (!fs.existsSync(dataDir)) {
            console.log(`📂 Creating local data directory: ${dataDir}`);
            fs.mkdirSync(dataDir, { recursive: true });
        }
        dbPath = path.join(dataDir, 'database.sqlite');
    }
    
    console.log(`🔌 Database Full Path: ${dbPath}`);

    try {
        dbInstance = await open({
            filename: dbPath,
            driver: sqlite3.Database
        });

        console.log("💾 SQLite Database Connected Successfully");

        // --- CONCURRENCY ---
        await dbInstance.exec('PRAGMA journal_mode = WAL;');
        await dbInstance.exec('PRAGMA synchronous = NORMAL;');
        await dbInstance.exec('PRAGMA foreign_keys = ON;');

        // --- SCHEMA DEFINITION (Idempotent) ---
        
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

        // --- INDEXES ---
        await dbInstance.exec(`
            CREATE INDEX IF NOT EXISTS idx_tokens_ticker ON tokens(ticker);
            CREATE INDEX IF NOT EXISTS idx_tokens_name ON tokens(name);
            CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens(timestamp);
            CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score);
            CREATE INDEX IF NOT EXISTS idx_tokens_mcap ON tokens(marketCap); 
        `);

        // --- AUTO-MIGRATION (Column Repair - Tokens) ---
        const columns = await dbInstance.all("PRAGMA table_info(tokens)");
        const columnNames = columns.map(c => c.name);

        const requiredColumns = [
            'last_k_calc', 'k_score', 'hasCommunityUpdate', 'marketCap', 'banner', 'description'
        ];

        for (const col of requiredColumns) {
            if (!columnNames.includes(col)) {
                console.log(`⚠️ Auto-Migrating: Adding missing column '${col}' to tokens...`);
                let type = 'INTEGER DEFAULT 0';
                if (col === 'hasCommunityUpdate') type = 'BOOLEAN DEFAULT 0';
                if (col === 'marketCap') type = 'REAL DEFAULT 0';
                if (col === 'banner' || col === 'description') type = 'TEXT';
                
                await dbInstance.exec(`ALTER TABLE tokens ADD COLUMN ${col} ${type}`);
            }
        }

        // --- AUTO-MIGRATION (Column Repair - Token Updates) ---
        // Critical for persistence of history across app versions
        const updateCols = await dbInstance.all("PRAGMA table_info(token_updates)");
        const updateColNames = updateCols.map(c => c.name);
        
        const requiredUpdateCols = ['signature', 'payer', 'status', 'description', 'banner', 'website', 'twitter', 'telegram'];

        for (const col of requiredUpdateCols) {
            if (!updateColNames.includes(col)) {
                console.log(`⚠️ Auto-Migrating: Adding missing column '${col}' to token_updates...`);
                await dbInstance.exec(`ALTER TABLE token_updates ADD COLUMN ${col} TEXT`);
            }
        }
        
    } catch (err) {
        console.error("❌ Fatal Database Initialization Error:", err);
        throw err;
    }

    return dbInstance;
}

// ... Caching Wrapper ...
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
