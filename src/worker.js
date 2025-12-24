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
const { logger } = require('./services');

const globalState = {
    lastBackendUpdate: Date.now()
};

async function startWorker() {
    logger.info('⚙️ Starting HolDex Background Worker...');
    
    // Initialize shared resources
    await initDB();
    const redis = initRedis();

    const deps = { db: getDB(), redis, globalState };

    // --- START TASKS ---
    logger.info('🚀 Launching Background Tasks...');
    
    // 1. New Token Listener (DexScreener Feed)
    // Runs continuously to find new pairs/migrations
    newTokenListener.start(deps);  
    
    // 2. Metadata Updater (Price/Volume Refresh)
    // Runs every 60s to update existing tokens
    metadataUpdater.start(deps);   
    
    // 3. K-Score Updater (Helius Analysis)
    // Runs every 6h (rolling) to calculate conviction scores
    kScoreUpdater.start(deps);

    logger.info('✅ Worker fully operational.');
}

// Handle crashes
process.on('uncaughtException', (err) => {
    logger.error('❌ Worker Uncaught Exception:', err);
    // In production, use a process manager (PM2) to restart
    process.exit(1); 
});

process.on('unhandledRejection', (reason, promise) => {
    logger.error('❌ Worker Unhandled Rejection:', reason);
});

startWorker();
