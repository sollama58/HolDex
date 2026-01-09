console.log("🚀 LISTENER PROCESS LAUNCHING...");

require('dotenv').config();
const { initDB, getDB } = require('./services/database');
const { initRedis } = require('./services/redis');
const logger = require('./services/logger');

// --- TASKS ---
// K-Score updates now handled by calculator worker on 12h/24h schedule
// const { updateSingleToken } = require('./tasks/kScoreUpdater');
// DISABLED: Grower Scanner automatically adds tokens - tokens are now only added via CA search
// const growerScanner = require('./tasks/growerScanner');

// ============================================================================
// TOKEN ADDITION POLICY:
// Tokens are ONLY added to the database when users search by Contract Address (CA).
// Automatic discovery from Raydium/Pump.fun blockchain events is DISABLED.
// This ensures we only track tokens that users are actively interested in.
// See: routes/tokens.js GET /tokens (search) and GET /token/:mint endpoints
// ============================================================================

// DISABLED: New token listener removed - tokens are now only added when CA is searched
// const newTokenListener = require('./tasks/newTokenListener');
// DISABLED: Queue processor no longer needed since no automatic token discovery
// const { startQueueProcessor, stopQueueProcessor } = require('./services/tokenQueue');

// GLOBAL ERROR HANDLERS
process.on('uncaughtException', (err) => {
    console.error('❌ UNCAUGHT EXCEPTION:', err);
    // Keep running to avoid downtime, but log loudly
});

process.on('unhandledRejection', (reason, _promise) => {
    console.error('❌ UNHANDLED REJECTION:', reason);
});

// Attempt to load Indexer
let indexerService = null;
try {
    indexerService = require('./indexer');
} catch (_e) {
    logger.warn("ℹ️ Listener Worker: Could not load './indexer' module. Skipping indexer start.");
}

// K-SCORE LOOP DISABLED - K-Score updates now only happen on scheduled 12h/24h intervals
// via the calculator worker's kScoreUpdater task. This saves RPC credits and reduces load.
// On-demand updates are still available via admin panel endpoints.

async function startListenerWorker() {
    logger.info("🎧 LISTENER WORKER: Initializing Core Services...");
    
    try {
        // 1. Init Infrastructure
        await initDB();
        await initRedis();
        const db = getDB();

        // 2. Start Indexer (Optional)
        if (indexerService && typeof indexerService.start === 'function') {
            logger.info("📊 LISTENER: Starting Token Indexer...");
            indexerService.start();
        }

        // DISABLED: Token Queue Processor no longer needed - tokens only added via CA search
        // logger.info("📥 LISTENER: Starting Token Queue Processor...");
        // startQueueProcessor();

        // DISABLED: New token listener removed - tokens are now only added when CA is searched
        // if (newTokenListener && typeof newTokenListener.startNewTokenListener === 'function') {
        //     logger.info("🛰️ LISTENER: Starting New Token Discovery...");
        //     setTimeout(() => {
        //         newTokenListener.startNewTokenListener();
        //     }, 1000);
        // } else {
        //     logger.warn("⚠️ LISTENER: newTokenListener module missing startNewTokenListener function.");
        // }

        // DISABLED: Grower Scanner automatically adds tokens - tokens are now only added via CA search
        // if (growerScanner && typeof growerScanner.start === 'function') {
        //     logger.info("🌱 LISTENER: Starting Grower Scanner...");
        //     growerScanner.start({ db });
        // }

        // K-Score Loop DISABLED - now handled by calculator worker on 12h/24h schedule
        // runKScoreLoop(db).catch(err => {
        //     logger.error(`❌ LISTENER FATAL: K-Score Loop died: ${err.message}`);
        // });
        logger.info("🎧 LISTENER WORKER: Ready (K-Score handled by calculator worker)");

    } catch (err) {
        logger.error(`❌ LISTENER STARTUP FAILED: ${err.message}`);
        process.exit(1);
    }
}

process.on('SIGINT', () => {
    logger.info("🎧 LISTENER WORKER: Shutting down...");
    // stopQueueProcessor(); // DISABLED: Queue processor no longer running
    process.exit(0);
});

startListenerWorker();
