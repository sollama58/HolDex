/**
 * Database Service
 * Updated: Added 'k_score' and 'last_k_calc' columns.
 */
const { Pool } = require('pg');
const config = require('../config/env');
const logger = require('./logger');
const { getRedis } = require('./redis');

let pool = null;

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

async function smartCache(key, ttlSeconds, fetchFunction) {
    const redis = getRedis();
    if (redis) {
        try {
            const cached = await redis.get(key);
            if (cached) return JSON.parse(cached);
        } catch (e) {
            logger.warn(`Redis Cache Miss/Error for ${key}: ${e.message}`);
        }
    }
    try {
        const value = await fetchFunction();
        if (value !== undefined && value !== null && redis) {
            await redis.set(key, JSON.stringify(value), 'EX', ttlSeconds);
        }
        return value;
    } catch (e) {
        throw e;
    }
}

async function initDB() {
    if (!config.DATABASE_URL) throw new Error("DATABASE_URL is missing in .env");

    try {
        pool = new Pool({
            connectionString: config.DATABASE_URL,
            ssl: { rejectUnauthorized: false }, 
            max: 20, 
            idleTimeoutMillis: 30000
        });

        await pool.query('SELECT NOW()');
        logger.info('Connected to PostgreSQL');

        // Main Token Table
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
                banner TEXT,
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
                complete BOOLEAN DEFAULT FALSE,
                hasCommunityUpdate BOOLEAN DEFAULT FALSE,
                k_score DOUBLE PRECISION DEFAULT 0,
                last_k_calc BIGINT DEFAULT 0
            );
        `);
        
        // Migration: Add columns if missing
        try { await pool.query(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS k_score DOUBLE PRECISION DEFAULT 0;`); } catch (e) {}
        try { await pool.query(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS last_k_calc BIGINT DEFAULT 0;`); } catch (e) {}

        await pool.query(`
            CREATE TABLE IF NOT EXISTS token_updates (
                id SERIAL PRIMARY KEY,
                mint TEXT NOT NULL,
                twitter TEXT,
                website TEXT,
                telegram TEXT,
                banner TEXT,
                submittedAt BIGINT,
                status TEXT DEFAULT 'pending' 
            );
        `);

        try {
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score DESC)`); // Index for K-Score sorting
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_volume ON tokens(volume24h DESC)`);
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_mcap ON tokens(marketCap DESC)`);
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_time ON tokens(timestamp DESC)`);
        } catch (idxErr) {
            logger.warn('Index creation notice:', idxErr.message);
        }

        logger.info('Database Schema & Indices Initialized');

    } catch (e) {
        logger.error('Database initialization failed', { error: e.message });
        throw e;
    }
}

async function saveTokenData(pubkey, mint, metadata, customTimestamp = null) {
    if (!pool) return;
    try {
        const ts = customTimestamp || Date.now();

        await pool.query(`
            INSERT INTO tokens (userPubkey, mint, ticker, name, description, twitter, website, metadataUri, image, isMayhemMode, marketCap, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (mint) DO UPDATE SET
                ticker = EXCLUDED.ticker,
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                metadataUri = EXCLUDED.metadataUri,
                image = EXCLUDED.image,
                marketCap = GREATEST(tokens.marketCap, EXCLUDED.marketCap) 
        `, [
            pubkey, mint, metadata.ticker, metadata.name, metadata.description,
            metadata.twitter, metadata.website, metadata.metadataUri,
            metadata.image, metadata.isMayhemMode ? true : false,
            metadata.marketCap || 0, 
            ts 
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
