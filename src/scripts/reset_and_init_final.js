require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

async function finalReset() {
    console.log("🚀 HolDEX FINAL DATABASE RESET & INIT");
    console.log("-------------------------------------");
    console.log("WARNING: This destroys ALL data and recreates the schema.");
    console.log("Waiting 3 seconds... (Ctrl+C to abort)");
    
    await new Promise(r => setTimeout(r, 3000));

    try {
        // 1. DROP EVERYTHING (Reverse Order of Dependencies)
        console.log("🔥 Dropping old tables...");
        await pool.query(`DROP TABLE IF EXISTS k_scores CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS token_updates CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS candles_1m CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS active_trackers CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS pools CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS tokens CASCADE;`);

        // 2. CREATE TOKENS
        console.log("🏗️  Creating 'tokens' table...");
        await pool.query(`
            CREATE TABLE tokens (
                mint TEXT PRIMARY KEY,
                symbol TEXT,
                name TEXT,
                image TEXT,
                decimals INTEGER DEFAULT 6,
                supply TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata JSONB,
                
                -- Aggregated Stats
                k_score INTEGER DEFAULT 0,
                marketCap DOUBLE PRECISION DEFAULT 0,
                volume24h DOUBLE PRECISION DEFAULT 0,
                change24h DOUBLE PRECISION DEFAULT 0,
                change1h DOUBLE PRECISION DEFAULT 0,
                change5m DOUBLE PRECISION DEFAULT 0,
                priceUsd DOUBLE PRECISION DEFAULT 0,
                liquidity DOUBLE PRECISION DEFAULT 0, -- CRITICAL: For sorting/filtering
                
                hasCommunityUpdate BOOLEAN DEFAULT FALSE,
                timestamp BIGINT DEFAULT 0
            );
        `);

        // 3. CREATE POOLS (Multi-Pool Support)
        console.log("🏗️  Creating 'pools' table...");
        await pool.query(`
            CREATE TABLE pools (
                address TEXT PRIMARY KEY,       
                mint TEXT NOT NULL,             
                dex TEXT NOT NULL,              
                token_a TEXT NOT NULL,          
                token_b TEXT NOT NULL,          
                created_at BIGINT DEFAULT 0,
                
                -- Pool Specific Stats
                liquidity_usd DOUBLE PRECISION DEFAULT 0,
                volume_24h DOUBLE PRECISION DEFAULT 0,
                price_usd DOUBLE PRECISION DEFAULT 0,
                
                -- Vault/Reserve Info (For Indexing)
                reserve_a TEXT,
                reserve_b TEXT
            );
        `);
        // Note: No unique constraint on (mint, dex) to allow multiple Raydium pools for one token
        
        // 4. CREATE CANDLES
        console.log("🏗️  Creating 'candles_1m' table...");
        await pool.query(`
            CREATE TABLE candles_1m (
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

        // 5. CREATE TRACKERS & UPDATES
        console.log("🏗️  Creating helper tables...");
        await pool.query(`
            CREATE TABLE active_trackers (
                pool_address TEXT PRIMARY KEY,
                last_check BIGINT DEFAULT 0,
                priority INTEGER DEFAULT 1
            );
        `);
        
        await pool.query(`
            CREATE TABLE token_updates (
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

        await pool.query(`
            CREATE TABLE k_scores (
                mint TEXT PRIMARY KEY REFERENCES tokens(mint),
                score INTEGER,
                metrics JSONB,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);

        // 6. INDEXES
        console.log("⚡ Applying Indexes...");
        // API Sorting Indexes
        await pool.query(`CREATE INDEX idx_tokens_kscore ON tokens(k_score DESC);`);
        await pool.query(`CREATE INDEX idx_tokens_mcap ON tokens(marketCap DESC);`);
        await pool.query(`CREATE INDEX idx_tokens_volume ON tokens(volume24h DESC);`);
        await pool.query(`CREATE INDEX idx_tokens_timestamp ON tokens(timestamp DESC);`);
        await pool.query(`CREATE INDEX idx_tokens_liquidity ON tokens(liquidity DESC);`);
        
        // Lookup Indexes
        await pool.query(`CREATE INDEX idx_pools_mint ON pools(mint);`); // Faster joins
        await pool.query(`CREATE INDEX idx_pools_liquidity ON pools(liquidity_usd DESC);`); // For "Best Pool" logic
        await pool.query(`CREATE INDEX idx_candles_pool_time ON candles_1m(pool_address, timestamp DESC);`);

        console.log("-------------------------------------");
        console.log("✅ SUCCESS. Database is ready for production.");
        console.log("👉 Restart your server to begin.");

    } catch (error) {
        console.error("❌ Init Failed:", error.message);
        process.exit(1);
    } finally {
        await pool.end();
    }
}

finalReset();
