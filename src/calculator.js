#!/usr/bin/env node
/**
 * HolDex Calculator Worker
 *
 * Standalone service that computes K-Scores for the shared DB.
 * Can run on a separate Render instance from the main API.
 *
 * Usage:
 *   DATABASE_URL=... HELIUS_API_KEY=... node src/calculator.js
 *
 * Philosophy: $asdfasdfa - just ship it, do the work, no overthinking.
 */

require('dotenv').config();
const { Pool } = require('pg');
const logger = require('./services/logger');
const config = require('./config/env');

// ============================================
// DATABASE CONNECTION
// ============================================

const pool = new Pool({
    connectionString: config.DATABASE_URL,
    ssl: config.DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false },
    max: 5, // Light footprint
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000
});

const db = {
    get: (q, p) => pool.query(q, p).then(r => r.rows[0]),
    all: (q, p) => pool.query(q, p).then(r => r.rows),
    run: (q, p) => pool.query(q, p)
};

// ============================================
// K-SCORE CALCULATOR
// ============================================

const kScoreUpdater = require('./tasks/kScoreUpdater');

// ============================================
// WORKER LOOP
// ============================================

const LOOP_INTERVAL = config.USE_WEBHOOKS ? 3600000 : 1800000; // 1h or 30min
const DEEP_REFRESH_INTERVAL = 86400000; // 24h

let isRunning = true;
let loopCount = 0;

async function runCalculatorLoop() {
    logger.info('🧮 [Calculator] Starting K-Score calculation cycle...');
    loopCount++;

    try {
        // Get verified tokens
        const tokens = await db.all(`
            SELECT mint, symbol, k_score, holders
            FROM tokens
            WHERE hascommunityupdate = true
            ORDER BY volume24h DESC NULLS LAST
        `);

        logger.info(`[Calculator] Processing ${tokens.length} verified tokens...`);

        let updated = 0;
        let errors = 0;

        for (const token of tokens) {
            if (!isRunning) break;

            try {
                const result = await kScoreUpdater.updateSingleToken({ db }, token.mint);
                if (result) {
                    updated++;
                    logger.info(`  ✓ ${token.symbol}: ${token.k_score} → ${result.score}`);
                }
            } catch (err) {
                errors++;
                logger.warn(`  ✗ ${token.symbol}: ${err.message}`);
            }

            // Rate limiting between tokens
            await sleep(1000);
        }

        logger.info(`🧮 [Calculator] Cycle #${loopCount} complete: ${updated} updated, ${errors} errors`);

    } catch (err) {
        logger.error('[Calculator] Cycle error:', err.message);
    }
}

async function runDeepRefresh() {
    logger.info('🔬 [Calculator] Starting daily deep refresh...');

    // Force deep refresh mode
    try {
        const tokens = await db.all(`
            SELECT mint, symbol
            FROM tokens
            WHERE hascommunityupdate = true
        `);

        for (const token of tokens) {
            if (!isRunning) break;

            try {
                // Deep refresh fetches fresh holder data from RPC
                await kScoreUpdater.updateSingleToken({ db, forceDeepRefresh: true }, token.mint);
                logger.info(`  🔬 ${token.symbol}: Deep refresh complete`);
            } catch (err) {
                logger.warn(`  ✗ ${token.symbol}: ${err.message}`);
            }

            await sleep(2000); // Slower for deep refresh
        }

        logger.info('🔬 [Calculator] Deep refresh complete');
    } catch (err) {
        logger.error('[Calculator] Deep refresh error:', err.message);
    }
}

// ============================================
// MAIN
// ============================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
    console.log(`
╔═══════════════════════════════════════════════════════════╗
║           HOLDEX CALCULATOR WORKER                        ║
║           Philosophy: $asdfasdfa                          ║
╠═══════════════════════════════════════════════════════════╣
║  DB: ${config.DATABASE_URL.split('@')[1]?.split('/')[0] || 'localhost'}
║  Helius: ${config.HELIUS_API_KEY ? '✓ configured' : '✗ missing'}
║  Mode: ${config.USE_WEBHOOKS ? 'WEBHOOK (light)' : 'POLLING'}
║  Interval: ${LOOP_INTERVAL / 60000} minutes
╚═══════════════════════════════════════════════════════════╝
    `);

    // Verify DB connection
    try {
        const result = await db.get('SELECT COUNT(*) as count FROM tokens WHERE hascommunityupdate = true');
        logger.info(`✅ Connected to DB. ${result.count} verified tokens.`);
    } catch (err) {
        logger.error('❌ DB connection failed:', err.message);
        process.exit(1);
    }

    // Graceful shutdown
    process.on('SIGTERM', () => {
        logger.info('🛑 [Calculator] Received SIGTERM, shutting down...');
        isRunning = false;
    });

    process.on('SIGINT', () => {
        logger.info('🛑 [Calculator] Received SIGINT, shutting down...');
        isRunning = false;
    });

    // Initial run after 10s
    setTimeout(() => runCalculatorLoop(), 10000);

    // Schedule regular loops
    setInterval(() => {
        if (isRunning) runCalculatorLoop();
    }, LOOP_INTERVAL);

    // Schedule daily deep refresh (offset by 6 hours to avoid overlap)
    setTimeout(() => {
        setInterval(() => {
            if (isRunning) runDeepRefresh();
        }, DEEP_REFRESH_INTERVAL);
    }, 6 * 3600000);

    logger.info('🧮 [Calculator] Worker started. First run in 10 seconds...');
}

main().catch(err => {
    logger.error('Fatal error:', err);
    process.exit(1);
});
