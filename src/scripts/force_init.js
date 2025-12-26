require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

async function forceInit() {
    console.log("🛠️  HolDEX Database Force Initialization");
    console.log("----------------------------------------");
    
    try {
        console.log("1. Creating 'tokens' table...");
        await pool.query(`
            CREATE TABLE IF NOT EXISTS tokens (
                mint TEXT PRIMARY KEY,
                symbol TEXT,
                name TEXT,
                image TEXT,
                decimals INTEGER,
                supply TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata JSONB,
                k_score INTEGER DEFAULT 0,
                marketCap DOUBLE PRECISION DEFAULT 0,
                volume24h DOUBLE PRECISION DEFAULT 0,
                change24h DOUBLE PRECISION DEFAULT 0,
                change1h DOUBLE PRECISION DEFAULT 0,
                change5m DOUBLE PRECISION DEFAULT 0,
                priceUsd DOUBLE PRECISION DEFAULT 0,
                hasCommunityUpdate BOOLEAN DEFAULT FALSE,
                timestamp BIGINT DEFAULT 0
            );
        `);
        
        console.log("2. Creating 'k_scores' table...");
        await pool.query(`
            CREATE TABLE IF NOT EXISTS k_scores (
                mint TEXT PRIMARY KEY REFERENCES tokens(mint),
                score INTEGER,
                metrics JSONB,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);

        console.log("3. Creating 'token_updates' table...");
        await pool.query(`
            CREATE TABLE IF NOT EXISTS token_updates (
                id SERIAL PRIMARY KEY,
                mint TEXT NOT NULL,
                twitter TEXT,
                website TEXT,
                telegram TEXT,
                banner TEXT,
                description TEXT,
                status TEXT DEFAULT 'pending', 
                signature TEXT UNIQUE,
                payer TEXT,
                submittedAt BIGINT
            );
        `);

        console.log("4. Creating 'pools' table...");
        await pool.query(`
            CREATE TABLE IF NOT EXISTS pools (
                address TEXT PRIMARY KEY,       
                mint TEXT NOT NULL,             
                dex TEXT NOT NULL,              
                token_a TEXT NOT NULL,          
                token_b TEXT NOT NULL,          
                created_at BIGINT DEFAULT 0,
                UNIQUE(mint, dex)
            );
        `);

        console.log("5. Creating 'active_trackers' table...");
        await pool.query(`
            CREATE TABLE IF NOT EXISTS active_trackers (
                pool_address TEXT PRIMARY KEY,
                last_check BIGINT DEFAULT 0,
                priority INTEGER DEFAULT 1
            );
        `);

        console.log("6. Creating 'candles_1m' table...");
        await pool.query(`
            CREATE TABLE IF NOT EXISTS candles_1m (
                pool_address TEXT NOT NULL,
                timestamp BIGINT NOT NULL,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY (pool_address, timestamp)
            );
        `);

        console.log("7. Creating Indexes...");
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score DESC);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_mcap ON tokens(marketCap DESC);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_volume ON tokens(volume24h DESC);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens(timestamp DESC);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_candles_pool_time ON candles_1m(pool_address, timestamp DESC);`);

        console.log("✅ Initialization Complete. Tables are ready.");

    } catch (error) {
        console.error("❌ Initialization Failed:", error.message);
    } finally {
        await pool.end();
    }
}

forceInit();
