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
// CRITICAL: Import the snapshotter to ensure prices update
const { startSnapshotter } = require('./indexer/tasks/snapshotter'); 
const logger = require('./services/logger');

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
    
    // 1. New Token Listener (On-Demand + Discovery)
    newTokenListener.start(deps);  
    
    // 2. Metadata Updater (Price/Volume Refresh)
    metadataUpdater.start(deps);   
    
    // 3. K-Score Updater (Helius Analysis)
    kScoreUpdater.start(deps);

    // 4. Snapshotter (Price Engine)
    // Runs regularly to fetch prices from On-Chain data
    logger.info('📸 Starting Price Snapshotter Engine...');
    startSnapshotter();

    logger.info('✅ Master Worker fully operational.');
}

// Handle crashes
process.on('uncaughtException', (err) => {
    logger.error('❌ Worker Uncaught Exception:', err);
    process.exit(1); 
});

process.on('unhandledRejection', (reason, promise) => {
    logger.error('❌ Worker Unhandled Rejection:', reason);
});

startWorker();
