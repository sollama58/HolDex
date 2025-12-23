/**
 * Database Service (PostgreSQL + Redis Version)
 * Optimized: Removed blocking I/O (legacy JSON writes)
 */
const { Pool } = require('pg');
const config = require('../config/env');
const logger = require('./logger');
const { getRedis } = require('./redis');

let pool = null;

// --- COMPATIBILITY WRAPPER ---
const dbWrapper = {
    async query(text, params) {
        if (!pool) throw new Error("DB not initialized");
        return pool.query(text, params);
    },
    async get(text, params = []) {
        const res = await this.query(text, params);
        return res.rows[0];
    },
    async all(text, params = []) {
        const res = await this.query(text, params);
        return res.rows;
    },
    async run(text, params = []) {
        const res = await this.query(text, params);
        return { rowCount: res.rowCount };
    }
};

// --- CACHING HELPER ---
async function smartCache(key, ttlSeconds, fetchFunction) {
    const redis = getRedis();
    
    // 1. Try Redis
    if (redis) {
        try {
            const cached = await redis.get(key);
            if (cached) return JSON.parse(cached);
        } catch (e) {
            logger.warn(`Redis Cache Miss/Error for ${key}: ${e.message}`);
        }
    }

    // 2. Fetch Fresh
    try {
        const value = await fetchFunction();
        
        // 3. Store in Redis
        if (value !== undefined && value !== null && redis) {
            // 'EX' sets expiry in seconds
            await redis.set(key, JSON.stringify(value), 'EX', ttlSeconds);
        }
        return value;
    } catch (e) {
        throw e;
    }
}

// --- INITIALIZATION ---
async function initDB() {
    if (!config.DATABASE_URL) {
        throw new Error("DATABASE_URL is missing in .env");
    }

    try {
        pool = new Pool({
            connectionString: config.DATABASE_URL,
            ssl: { rejectUnauthorized: false }, // Required for most cloud Postgres
            max: 20, // Limit connection pool size
            idleTimeoutMillis: 30000
        });

        // Test connection
        await pool.query('SELECT NOW()');
        logger.info('Connected to PostgreSQL');

        // Initialize Tables
        await pool.query(`
            CREATE TABLE IF NOT EXISTS tokens (
                id SERIAL PRIMARY KEY,
                userPubkey TEXT,
                mint TEXT UNIQUE,
                ticker TEXT,
                name TEXT,
                description TEXT,
                twitter TEXT,
                website TEXT,
                metadataUri TEXT,
                image TEXT,
                isMayhemMode BOOLEAN DEFAULT FALSE,
                signature TEXT,
                timestamp BIGINT,
                volume24h DOUBLE PRECISION DEFAULT 0,
                priceUsd DOUBLE PRECISION DEFAULT 0,
                marketCap DOUBLE PRECISION DEFAULT 0,
                holderCount INTEGER DEFAULT 0,
                change5m DOUBLE PRECISION DEFAULT 0,
                change1h DOUBLE PRECISION DEFAULT 0,
                change24h DOUBLE PRECISION DEFAULT 0,
                lastUpdated BIGINT,
                tweetUrl TEXT,
                complete BOOLEAN DEFAULT FALSE
            );
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS token_holders (
                id SERIAL PRIMARY KEY,
                mint TEXT,
                holderPubkey TEXT,
                balance TEXT,
                rank INTEGER,
                updatedAt BIGINT,
                UNIQUE(mint, holderPubkey)
            );
        `);

        // Index Creation (Crucial for performance)
        // We do this safely with IF NOT EXISTS logic handled by catch or specific checks
        try {
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_volume ON tokens(volume24h DESC)`);
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_mcap ON tokens(marketCap DESC)`);
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_time ON tokens(timestamp DESC)`);
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_holders_mint ON token_holders(mint)`);
        } catch (idxErr) {
            logger.warn('Index creation notice:', idxErr.message);
        }

        logger.info('Database Schema & Indices Initialized');

    } catch (e) {
        logger.error('Database initialization failed', { error: e.message });
        throw e;
    }
}

// --- DATA ACCESS ---

async function saveTokenData(pubkey, mint, metadata) {
    if (!pool) return;
    
    try {
        // Postgres Upsert
        // Removed the synchronous fs.writeFileSync() call that was blocking the event loop
        await pool.query(`
            INSERT INTO tokens (userPubkey, mint, ticker, name, description, twitter, website, metadataUri, image, isMayhemMode, marketCap, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (mint) DO UPDATE SET
                ticker = EXCLUDED.ticker,
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                twitter = EXCLUDED.twitter,
                website = EXCLUDED.website,
                metadataUri = EXCLUDED.metadataUri,
                image = EXCLUDED.image,
                -- Only update marketCap if the new one is higher/valid, or just overwrite? 
                -- Usually we trust the latest data.
                marketCap = GREATEST(tokens.marketCap, EXCLUDED.marketCap) 
        `, [
            pubkey, 
            mint, 
            metadata.ticker, 
            metadata.name, 
            metadata.description,
            metadata.twitter, 
            metadata.website, 
            metadata.metadataUri,
            metadata.image, 
            metadata.isMayhemMode ? true : false,
            metadata.marketCap || 0,
            Date.now()
        ]);

    } catch (e) {
        logger.error("Save Token Error", { error: e.message });
    }
}

module.exports = {
    initDB,
    getDB: () => dbWrapper,
    smartCache,
    saveTokenData
};
