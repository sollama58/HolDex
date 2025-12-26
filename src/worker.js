/**
 * Background Worker Process
 * Handles all data ingestion, listeners, and heavy calculation tasks.
 * Decoupled from the main API server to ensure responsiveness.
 */
require('dotenv').config();
const { initDB, getDB } = require('./services/database');
const { initRedis } = require('./services/redis');
const metadataUpdater = require('./tasks/metadataUpdater');
const newTokenListener = require('./tasks/newTokenListener');
const kScoreUpdater = require('./tasks/kScoreUpdater');
const { startSnapshotter } = require('./indexer/tasks/snapshotter'); 
const logger = require('./services/logger');

const globalState = {
    lastBackendUpdate: Date.now()
};

async function startWorker() {
    logger.info('⚙️ Starting HolDex Background Worker...');
    
    try {
        // 1. Initialize Database (WAIT for it to complete)
        // If this fails, the worker cannot function.
        await initDB();
        logger.info('✅ Database Initialized');

        // 2. Initialize Redis (Optional but recommended)
        const redis = initRedis ? initRedis() : null;
        if (redis) logger.info('✅ Redis Initialized');

        const deps = { db: getDB(), redis, globalState };

        // --- START TASKS ---
        logger.info('🚀 Launching Background Tasks...');
        
        // 3. Start Tasks (Wrap in try-catch blocks individually if they are async)
        
        // Task A: New Token Listener
        if (newTokenListener && typeof newTokenListener.start === 'function') {
            newTokenListener.start(deps);
        }

        // Task B: Metadata Updater
        if (metadataUpdater && typeof metadataUpdater.start === 'function') {
             metadataUpdater.start(deps);
        }

        // Task C: K-Score Updater
        if (kScoreUpdater && typeof kScoreUpdater.start === 'function') {
            kScoreUpdater.start(deps);
        }

        // Task D: Snapshotter (Price Engine)
        logger.info('📸 Starting Price Snapshotter Engine...');
        startSnapshotter(); // This sets up its own intervals, so it's safe to call synchronously

        logger.info('✅ Master Worker fully operational.');

    } catch (err) {
        logger.error(`❌ CRITICAL WORKER FAILURE: ${err.message}`, { stack: err.stack });
        process.exit(1);
    }
}

// Global Error Handlers
process.on('uncaughtException', (err) => {
    logger.error(`❌ Worker Uncaught Exception: ${err.message}`, { stack: err.stack });
    process.exit(1); 
});

process.on('unhandledRejection', (reason, promise) => {
    // Log the full reason (error object) to see the stack trace
    logger.error('❌ Worker Unhandled Rejection:', reason);
});

startWorker();
