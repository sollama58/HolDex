require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false }
});

async function resetDB() {
    const client = await pool.connect();
    try {
        console.log("🗑️  Dropping all tables...");
        await client.query(`
            DROP TABLE IF EXISTS k_scores CASCADE;
            DROP TABLE IF EXISTS token_updates CASCADE;
            DROP TABLE IF EXISTS active_trackers CASCADE;
            DROP TABLE IF EXISTS candles_1m CASCADE;
            DROP TABLE IF EXISTS pools CASCADE;
            DROP TABLE IF EXISTS holders_history CASCADE;
            DROP TABLE IF EXISTS tokens CASCADE;
        `);

        console.log("🏗️  Creating tables...");

        // TOKENS TABLE (Updated with last_holder_check AND updated_at)
        await client.query(`
            CREATE TABLE IF NOT EXISTS tokens (
                mint TEXT PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                image TEXT,
                supply TEXT,
                decimals INTEGER DEFAULT 9,
                priceUsd DOUBLE PRECISION,
                liquidity DOUBLE PRECISION,
                marketCap DOUBLE PRECISION,
                volume24h DOUBLE PRECISION,
                change24h DOUBLE PRECISION,
                change1h DOUBLE PRECISION,
                change5m DOUBLE PRECISION,
                holders INTEGER DEFAULT 0,
                last_holder_check BIGINT DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                k_score DOUBLE PRECISION DEFAULT 0,
                hasCommunityUpdate BOOLEAN DEFAULT FALSE,
                metadata TEXT,
                timestamp BIGINT
            );
        `);

        // HOLDERS HISTORY TABLE
        await client.query(`
            CREATE TABLE IF NOT EXISTS holders_history (
                mint TEXT,
                count INTEGER,
                timestamp BIGINT,
                PRIMARY KEY (mint, timestamp)
            );
        `);

        // POOLS TABLE
        await client.query(`
            CREATE TABLE IF NOT EXISTS pools (
                address TEXT PRIMARY KEY,
                mint TEXT,
                dex TEXT,
                token_a TEXT NOT NULL,
                token_b TEXT NOT NULL,
                reserve_a TEXT,
                reserve_b TEXT,
                price_usd DOUBLE PRECISION DEFAULT 0,
                liquidity_usd DOUBLE PRECISION DEFAULT 0,
                volume_24h DOUBLE PRECISION DEFAULT 0,
                created_at BIGINT,
                FOREIGN KEY(mint) REFERENCES tokens(mint)
            );
        `);

        // CANDLES TABLE
        await client.query(`
            CREATE TABLE IF NOT EXISTS candles_1m (
                pool_address TEXT,
                timestamp BIGINT,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                PRIMARY KEY (pool_address, timestamp)
            );
        `);

        // ACTIVE TRACKERS TABLE
        await client.query(`
            CREATE TABLE IF NOT EXISTS active_trackers (
                pool_address TEXT PRIMARY KEY,
                priority INTEGER DEFAULT 1,
                last_check BIGINT DEFAULT 0
            );
        `);

        // TOKEN UPDATES TABLE
        await client.query(`
            CREATE TABLE IF NOT EXISTS token_updates (
                id SERIAL PRIMARY KEY,
                mint TEXT,
                twitter TEXT,
                website TEXT,
                telegram TEXT,
                banner TEXT,
                description TEXT,
                submittedAt BIGINT,
                status TEXT DEFAULT 'pending', 
                signature TEXT,
                payer TEXT
            );
        `);

        // K_SCORES TABLE
        await client.query(`
            CREATE TABLE IF NOT EXISTS k_scores (
                mint TEXT PRIMARY KEY,
                score DOUBLE PRECISION,
                components TEXT, 
                updatedAt BIGINT
            );
        `);

        console.log("📑  Creating indexes...");
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score DESC);
            CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens(timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_tokens_updated_at ON tokens(updated_at ASC);
            CREATE INDEX IF NOT EXISTS idx_pools_mint ON pools(mint);
            CREATE INDEX IF NOT EXISTS idx_candles_pool_time ON candles_1m(pool_address, timestamp);
            CREATE INDEX IF NOT EXISTS idx_holders_hist_mint ON holders_history(mint);
        `);

        console.log("✅ Database Reset & Init Complete.");

    } catch (e) {
        console.error("❌ Error:", e);
    } finally {
        client.release();
        pool.end();
    }
}

resetDB();
