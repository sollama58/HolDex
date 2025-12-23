/**
 * Database Service (PostgreSQL + Redis Version)
 * Replaces SQLite with a robust, scalable architecture.
 */
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
const config = require('../config/env');
const logger = require('./logger');
const { getRedis } = require('./redis');

// Paths for local JSON backups (legacy/fallback)
const DISK_ROOT = config.DISK_ROOT;
const DATA_DIR = path.join(DISK_ROOT, 'tokens');

const ensureDir = (dir) => {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
};
ensureDir(DISK_ROOT);
ensureDir(DATA_DIR);

let pool = null;

// --- COMPATIBILITY WRAPPER ---
// Allows us to use db.get, db.all, db.run syntax while using Postgres
const dbWrapper = {
    async query(text, params) {
        if (!pool) throw new Error("DB not initialized");
        return pool.query(text, params);
    },
    
    // Fetch single row
    async get(text, params = []) {
        const res = await this.query(text, params);
        return res.rows[0];
    },

    // Fetch all rows
    async all(text, params = []) {
        const res = await this.query(text, params);
        return res.rows;
    },

    // Execute (Insert/Update/Delete)
    async run(text, params = []) {
        const res = await this.query(text, params);
        return { rowCount: res.rowCount };
    }
};

// --- CACHING ---
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
            await redis.setex(key, ttlSeconds, JSON.stringify(value));
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
            ssl: { rejectUnauthorized: false } // Required for Render/Cloud Postgres
        });

        // Test connection
        await pool.query('SELECT NOW()');
        logger.info('Connected to PostgreSQL');

        // Create Tables (Migrated from SQLite syntax to Postgres)
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

        await pool.query(`
            CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                value DOUBLE PRECISION DEFAULT 0
            );
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS logs (
                id SERIAL PRIMARY KEY,
                type TEXT,
                data TEXT,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            );
        `);

        // Initialize Stats
        const statsKeys = [
            'accumulatedFeesLamports',
            'lifetimeFeesLamports',
            'totalPumpBoughtLamports',
            'totalPumpTokensBought',
            'lastClaimTimestamp',
            'lastClaimAmountLamports',
            'nextCheckTimestamp',
            'lifetimeCreatorFeesLamports'
        ];

        for (const key of statsKeys) {
            await pool.query(
                'INSERT INTO stats (key, value) VALUES ($1, 0) ON CONFLICT (key) DO NOTHING',
                [key]
            );
        }

        // Additional Tables
        await pool.query(`
            CREATE TABLE IF NOT EXISTS flywheel_logs (
                id SERIAL PRIMARY KEY,
                timestamp BIGINT,
                status TEXT,
                feesCollected DOUBLE PRECISION,
                solSpent DOUBLE PRECISION,
                tokensBought TEXT,
                pumpBuySig TEXT,
                transfer9_5 DOUBLE PRECISION,
                transfer0_5 DOUBLE PRECISION,
                reason TEXT
            );
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS airdrop_logs (
                id SERIAL PRIMARY KEY,
                amount TEXT,
                recipients INTEGER,
                totalPoints DOUBLE PRECISION,
                signatures TEXT,
                details TEXT,
                timestamp TEXT
            );
        `);

        logger.info('Database Schema Initialized');

    } catch (e) {
        logger.error('Database initialization failed', { error: e.message });
        throw e;
    }
}

// --- HELPER FUNCTIONS (Refactored for Postgres Syntax) ---

async function addFees(amount) {
    if (!pool) return;
    await pool.query('UPDATE stats SET value = value + $1 WHERE key = $2', [amount, 'accumulatedFeesLamports']);
    await pool.query('UPDATE stats SET value = value + $1 WHERE key = $2', [amount, 'lifetimeFeesLamports']);
}

async function addPumpBought(amount) {
    if (!pool) return;
    await pool.query('UPDATE stats SET value = value + $1 WHERE key = $2', [amount, 'totalPumpBoughtLamports']);
}

async function getTotalLaunches() {
    if (!pool) return 0;
    const res = await pool.query('SELECT COUNT(*) as count FROM tokens');
    return parseInt(res.rows[0].count);
}

async function getStats() {
    if (!pool) return {};
    const res = await pool.query('SELECT key, value FROM stats');
    return res.rows.reduce((acc, r) => ({ ...acc, [r.key]: r.value }), {});
}

async function resetAccumulatedFees(used) {
    if (!pool) return;
    await pool.query('UPDATE stats SET value = value - $1 WHERE key = $2', [used, 'accumulatedFeesLamports']);
}

async function recordClaim(amount) {
    if (!pool) return;
    await pool.query('UPDATE stats SET value = $1 WHERE key = $2', [Date.now(), 'lastClaimTimestamp']);
    await pool.query('UPDATE stats SET value = $1 WHERE key = $2', [amount, 'lastClaimAmountLamports']);
}

async function updateNextCheckTime() {
    if (!pool) return;
    const nextCheck = Date.now() + (5 * 60 * 1000);
    await pool.query('UPDATE stats SET value = $1 WHERE key = $2', [nextCheck, 'nextCheckTimestamp']);
    return nextCheck;
}

async function logFlywheelCycle(data) {
    if (!pool) return;
    await pool.query(`
        INSERT INTO flywheel_logs (timestamp, status, feesCollected, solSpent, tokensBought, pumpBuySig, transfer9_5, transfer0_5, reason)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    `, [Date.now(), data.status, data.feesCollected || 0, data.solSpent || 0, data.tokensBought || '0', data.pumpBuySig || null, data.transfer9_5 || 0, data.transfer0_5 || 0, data.reason || null]);
}

async function logPurchase(type, data) {
    if (!pool) return;
    try {
        await pool.query(
            'INSERT INTO logs (type, data, timestamp) VALUES ($1, $2, NOW())',
            [type, JSON.stringify(data)]
        );
    } catch (e) {
        logger.error("Log error", { error: e.message });
    }
}

async function saveTokenData(pubkey, mint, metadata) {
    if (!pool) return;
    
    try {
        // Postgres Upsert
        await pool.query(`
            INSERT INTO tokens (userPubkey, mint, ticker, name, description, twitter, website, metadataUri, image, isMayhemMode, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (mint) DO UPDATE SET
                userPubkey = EXCLUDED.userPubkey,
                ticker = EXCLUDED.ticker,
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                twitter = EXCLUDED.twitter,
                website = EXCLUDED.website,
                metadataUri = EXCLUDED.metadataUri,
                image = EXCLUDED.image
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
            Date.now()
        ]);

        // Legacy JSON Backup (Keep for safety)
        const shard = pubkey.slice(0, 2).toLowerCase();
        const dir = path.join(DATA_DIR, shard);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(
            path.join(dir, `${mint}.json`),
            JSON.stringify({ userPubkey: pubkey, mint, metadata, timestamp: new Date().toISOString() }, null, 2)
        );
    } catch (e) {
        logger.error("Save Token Error", { error: e.message });
    }
}

module.exports = {
    initDB,
    getDB: () => dbWrapper, // Returns the wrapper to maintain compat with other files
    smartCache,
    addFees,
    addPumpBought,
    getTotalLaunches,
    getStats,
    resetAccumulatedFees,
    recordClaim,
    updateNextCheckTime,
    logFlywheelCycle,
    logPurchase,
    saveTokenData,
    DATA_DIR
};
