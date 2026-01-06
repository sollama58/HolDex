#!/usr/bin/env node
/**
 * HolDex Calculator - The Brain of Data Processing
 *
 * Runs as a dedicated background worker to handle:
 * - K-Score calculations (conviction analysis)
 * - Metadata updates (price/volume/liquidity)
 * - Grower scanning (token promotion)
 */

// CRITICAL: Force development mode to bypass ADMIN_PASSWORD validation in config/env
process.env.NODE_ENV = 'development';

require('dotenv').config();

const config = require('./config/env');
const logger = require('./services/logger');

// Startup info
logger.info('='.repeat(50));
logger.info('🧠 HolDex Calculator Starting');
logger.info(`   Node: ${process.version}`);
logger.info(`   DATABASE_URL: ${config.DATABASE_URL ? 'SET' : 'MISSING'}`);
logger.info(`   HELIUS_API_KEY: ${config.HELIUS_API_KEY ? 'SET' : 'MISSING'}`);
logger.info(`   REDIS_URL: ${config.REDIS_URL ? 'SET' : 'MISSING'}`);
logger.info('='.repeat(50));

// Memory monitoring
let heartbeatCount = 0;
function logHeartbeat() {
    heartbeatCount++;
    const mem = process.memoryUsage();
    logger.info(`💓 Heartbeat #${heartbeatCount} | RSS: ${Math.round(mem.rss / 1024 / 1024)}MB | Heap: ${Math.round(mem.heapUsed / 1024 / 1024)}MB`);
}

// Graceful shutdown
let isShuttingDown = false;
async function shutdown(signal) {
    if (isShuttingDown) return;
    isShuttingDown = true;
    logger.info(`\n📴 Received ${signal}, shutting down gracefully...`);

    // Give tasks time to complete current work
    await new Promise(r => setTimeout(r, 2000));
    process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Catch unhandled errors
process.on('uncaughtException', (err) => {
    logger.error(`❌ Uncaught Exception: ${err.message}`);
    logger.error(err.stack);
});

process.on('unhandledRejection', (reason, promise) => {
    logger.error(`❌ Unhandled Rejection at: ${promise}`);
    logger.error(`   Reason: ${reason}`);
});

// Main startup
async function main() {
    try {
        // 1. Initialize Database
        logger.info('📊 Initializing Database...');
        const { initDB, getDB } = require('./services/database');
        await initDB();
        const db = getDB();
        logger.info('✅ Database Ready');

        // 2. Initialize Redis (optional - graceful failure)
        logger.info('🔴 Initializing Redis...');
        const { connectRedis } = require('./services/redis');
        const redis = await connectRedis();
        if (redis) {
            logger.info('✅ Redis Ready');
        } else {
            logger.warn('⚠️ Redis unavailable - running in degraded mode');
        }

        // Build dependencies object
        const deps = { db };

        // 2.5. Re-sign any tampered tokens (after secret rotation)
        logger.info('🔐 Checking for tampered tokens...');
        const { signAllCategories } = require('./utils/dataSignature');
        const tamperedTokens = await db.all(`
            SELECT mint, symbol, name, image, decimals,
                   k_score, conviction_score, conviction_accumulators,
                   conviction_holders, conviction_reducers, conviction_extractors,
                   conviction_analyzed, holders, priceusd, marketcap, liquidity,
                   supply, initial_supply, burned_amount, burned_percent,
                   mint_authority_revoked, freeze_authority_revoked, is_mutable_supply,
                   hasCommunityUpdate, lp_burn_pct, lp_locked_pct, lp_status,
                   is_pump_fun, bonding_curve_complete, timestamp, metadata,
                   price_source, price_timestamp, price_pool,
                   mcap_calculated, liquidity_source, liquidity_timestamp,
                   holders_source, holders_timestamp, age_days
            FROM tokens WHERE hasCommunityUpdate = TRUE
        `);

        if (tamperedTokens.length > 0) {
            logger.info(`🔐 Re-signing ${tamperedTokens.length} tokens with current secret...`);
            const { saveSnapshot } = require('./tasks/integrityWatchdog');
            let resigned = 0;
            for (const token of tamperedTokens) {
                try {
                    const signatures = signAllCategories(token);
                    await db.run(`
                        UPDATE tokens SET
                            sig_identity = $1, sig_security = $2, sig_lp = $3,
                            sig_supply = $4, sig_kscore = $5, sig_market = $6,
                            sig_origin = $7, sig_full = $8, chaos_nonce = $9
                        WHERE mint = $10
                    `, [
                        signatures.sig_identity, signatures.sig_security, signatures.sig_lp,
                        signatures.sig_supply, signatures.sig_kscore, signatures.sig_market,
                        signatures.sig_origin, signatures.sig_full, signatures.chaos_nonce,
                        token.mint
                    ]);

                    // CRITICAL: Update Redis snapshot with new signatures
                    // Otherwise watchdog will "heal" by restoring OLD signatures
                    const tokenWithNewSigs = { ...token, ...signatures };
                    await saveSnapshot(token.mint, tokenWithNewSigs);

                    resigned++;
                } catch (e) {
                    logger.error(`Failed to re-sign ${token.symbol}: ${e.message}`);
                }
            }
            logger.info(`✅ Re-signed ${resigned}/${tamperedTokens.length} tokens (DB + Redis snapshots)`);
        } else {
            logger.info('✅ No tokens need re-signing');
        }

        // 3. Start K-Score Updater
        logger.info('📈 Starting K-Score Updater...');
        const kScoreUpdater = require('./tasks/kScoreUpdater');
        kScoreUpdater.start(deps);
        logger.info('✅ K-Score Updater Running');

        // 4. Start Metadata Updater
        logger.info('📊 Starting Metadata Updater...');
        const metadataUpdater = require('./tasks/metadataUpdater');
        metadataUpdater.start(deps);
        logger.info('✅ Metadata Updater Running');

        // 5. Start Grower Scanner
        logger.info('🌱 Starting Grower Scanner...');
        const growerScanner = require('./tasks/growerScanner');
        growerScanner.start(deps);
        logger.info('✅ Grower Scanner Running');

        // Start heartbeat
        setInterval(logHeartbeat, 60000); // Every minute
        logHeartbeat(); // First one immediately

        logger.info('='.repeat(50));
        logger.info('🧠 Calculator Brain ACTIVE');
        logger.info('   Running: K-Score, Metadata, Growers');
        logger.info('='.repeat(50));

    } catch (err) {
        logger.error(`❌ Calculator Failed to Start: ${err.message}`);
        logger.error(err.stack);
        process.exit(1);
    }
}

// Keep process alive
process.stdin.resume();

// Start
main();
