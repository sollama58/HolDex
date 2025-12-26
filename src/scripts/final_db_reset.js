require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

async function finalReset() {
    console.log("🚀 HolDEX FINAL ARCHITECTURE RESET");
    console.log("----------------------------------");
    console.log("Policy: 1 Token (Mint) <-> N Pools");
    console.log("Data: Price from Largest Pool, Volume from All.");
    console.log("Waiting 3 seconds... (Ctrl+C to abort)");
    
    await new Promise(r => setTimeout(r, 3000));

    try {
        console.log("🔥 Dropping all existing tables...");
        await pool.query(`DROP TABLE IF EXISTS k_scores CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS token_updates CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS candles_1m CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS active_trackers CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS pools CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS tokens CASCADE;`);

        // 1. TOKENS TABLE (The Singular Identity)
        console.log("🏗️  Creating 'tokens' table...");
        await pool.query(`
            CREATE TABLE tokens (
                mint TEXT PRIMARY KEY, -- The Unique Contract Address
                symbol TEXT,
                name TEXT,
                image TEXT,
                decimals INTEGER,
                supply TEXT,
                
                -- Aggregated Stats (Computed from Pools)
                priceUsd DOUBLE PRECISION DEFAULT 0, -- From Largest Pool
                marketCap DOUBLE PRECISION DEFAULT 0, -- From Largest Pool
                volume24h DOUBLE PRECISION DEFAULT 0, -- Sum of All Pools
                
                change24h DOUBLE PRECISION DEFAULT 0,
                change1h DOUBLE PRECISION DEFAULT 0,
                change5m DOUBLE PRECISION DEFAULT 0,
                
                k_score INTEGER DEFAULT 0,
                hasCommunityUpdate BOOLEAN DEFAULT FALSE,
                
                metadata JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                timestamp BIGINT DEFAULT 0
            );
        `);

        // 2. POOLS TABLE (The Many Trading Venues)
        console.log("🏗️  Creating 'pools' table...");
        await pool.query(`
            CREATE TABLE pools (
                address TEXT PRIMARY KEY, -- Pool Address (Unique)
                mint TEXT NOT NULL,       -- Link to Token
                dex TEXT NOT NULL,        -- 'raydium', 'orca', 'pump'
                
                token_a TEXT NOT NULL,
                token_b TEXT NOT NULL,
                
                -- Pool Specific Data
                liquidity_usd DOUBLE PRECISION DEFAULT 0,
                volume_24h DOUBLE PRECISION DEFAULT 0,
                price_usd DOUBLE PRECISION DEFAULT 0,
                
                -- Indexing Helpers
                reserve_a TEXT,
                reserve_b TEXT,
                
                created_at BIGINT DEFAULT 0
            );
        `);
        // NOTE: No UNIQUE(mint, dex) constraint here. 
        // This allows multiple pools (SOL pair, USDC pair) on the same DEX.

        // 3. CANDLES TABLE (History per Pool)
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

        // 4. HELPER TABLES
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

        // 5. INDEXES (Optimized for Aggregation)
        console.log("⚡ Applying Indexes...");
        
        // Fast Lookups
        await pool.query(`CREATE INDEX idx_pools_mint ON pools(mint);`); // Find all pools for a token
        await pool.query(`CREATE INDEX idx_pools_liquidity ON pools(liquidity_usd DESC);`); // Find best pools
        
        // Sorting
        await pool.query(`CREATE INDEX idx_tokens_kscore ON tokens(k_score DESC);`);
        await pool.query(`CREATE INDEX idx_tokens_mcap ON tokens(marketCap DESC);`);
        await pool.query(`CREATE INDEX idx_tokens_volume ON tokens(volume24h DESC);`);
        await pool.query(`CREATE INDEX idx_tokens_timestamp ON tokens(timestamp DESC);`);
        
        // Charting
        await pool.query(`CREATE INDEX idx_candles_pool_time ON candles_1m(pool_address, timestamp DESC);`);

        console.log("-------------------------------------");
        console.log("✅ DATABASE RESET COMPLETE.");
        console.log("   Schema now supports: 1 Token -> Many Pools.");

    } catch (error) {
        console.error("❌ Init Failed:", error.message);
    } finally {
        await pool.end();
    }
}

finalReset();
