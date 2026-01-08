/**
 * Background Worker Process
 * Handles all data ingestion, listeners, and heavy calculation tasks.
 * Decoupled from the main API server to ensure responsiveness.
 */
require('dotenv').config();
const { initDB } = require('./services/database');
const { initRedis } = require('./services/redis');
const metadataUpdater = require('./tasks/metadataUpdater');
const newTokenListener = require('./tasks/newTokenListener');
const kScoreUpdater = require('./tasks/kScoreUpdater');
const priceIndexer = require('./tasks/priceIndexer'); // NEW
const { logger } = require('./services');

const globalState = {
    lastBackendUpdate: Date.now()
};

async function startWorker() {
    logger.info('⚙️ Starting HolDex Background Worker...');
    
    // Initialize shared resources
    const db = await initDB(); // Updated: await the init
    const redis = await initRedis();

    const deps = { db, redis, globalState };

    // --- START TASKS ---
    logger.info('🚀 Launching Background Tasks...');
    
    // 1. New Price Indexer (The Main Engine)
    // Tracks prices via Helius RPC
    priceIndexer.start(deps);

    // 2. New Token Listener 
    // Finds new pairs on DexScreener to populate the DB
    newTokenListener.start(deps);  
    
    // 3. Metadata Updater (Legacy/Fallback)
    // Keeps non-indexed tokens fresh
    metadataUpdater.start(deps);   
    
    // 4. K-Score Updater (Helius Analysis)
    kScoreUpdater.start(deps);

    logger.info('✅ Worker fully operational.');
}

// Handle crashes
process.on('uncaughtException', (err) => {
    logger.error('❌ Worker Uncaught Exception:', err);
    process.exit(1); 
});

process.on('unhandledRejection', (reason, _promise) => {
    logger.error('❌ Worker Unhandled Rejection:', reason);
});

startWorker();
