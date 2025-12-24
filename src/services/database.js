const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite');
const path = require('path');
const fs = require('fs');
const config = require('../config/env');

let dbInstance = null;

async function initDB() {
    if (dbInstance) return dbInstance;

    // --- PERSISTENCE CONFIGURATION ---
    // 1. Docker/Render often use specific paths for persistent disks.
    // 2. We default to a 'data' folder in the project root.
    // 3. __dirname is 'src/services', so '../../data' puts it in 'project/data'.
    
    // Explicit check for Render's persistent disk path if configured, else local.
    const projectRoot = path.resolve(__dirname, '../../');
    let dbPath = process.env.DB_PATH;
    
    if (!dbPath) {
        // If no env var, use project/data/database.sqlite
        const dataDir = path.join(projectRoot, 'data');
        if (!fs.existsSync(dataDir)) {
            console.log(`📂 Creating persistent data directory: ${dataDir}`);
            fs.mkdirSync(dataDir, { recursive: true });
        }
        dbPath = path.join(dataDir, 'database.sqlite');
    } else {
        // If env var provided, ensure its directory exists too
        const dbDir = path.dirname(dbPath);
        if (!fs.existsSync(dbDir)) {
            console.log(`📂 Creating configured DB directory: ${dbDir}`);
            fs.mkdirSync(dbDir, { recursive: true });
        }
    }
    
    console.log(`🔌 Database Path Resolved: ${dbPath}`);

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

        // --- AUTO-MIGRATION (Column Repair) ---
        const columns = await dbInstance.all("PRAGMA table_info(tokens)");
        const columnNames = columns.map(c => c.name);

        const requiredColumns = [
            'last_k_calc', 'k_score', 'hasCommunityUpdate', 'marketCap', 'banner', 'description'
        ];

        for (const col of requiredColumns) {
            if (!columnNames.includes(col)) {
                console.log(`⚠️ Auto-Migrating: Adding missing column '${col}'...`);
                let type = 'INTEGER DEFAULT 0';
                if (col === 'hasCommunityUpdate') type = 'BOOLEAN DEFAULT 0';
                if (col === 'marketCap') type = 'REAL DEFAULT 0';
                if (col === 'banner' || col === 'description') type = 'TEXT';
                
                await dbInstance.exec(`ALTER TABLE tokens ADD COLUMN ${col} ${type}`);
            }
        }
        
    } catch (err) {
        console.error("❌ Fatal Database Initialization Error:", err);
        // Do NOT throw here if we want the server to try and limp along or restart cleanly, 
        // but typically a DB fail is fatal.
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
