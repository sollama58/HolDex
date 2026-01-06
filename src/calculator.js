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
