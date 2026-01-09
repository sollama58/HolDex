console.log("🚀 LISTENER PROCESS LAUNCHING...");

require('dotenv').config();
const { initDB, getDB } = require('./services/database');
const { initRedis } = require('./services/redis');
const logger = require('./services/logger');

// --- TASKS ---
const { updateSingleToken } = require('./tasks/kScoreUpdater');
const growerScanner = require('./tasks/growerScanner');
const newTokenListener = require('./tasks/newTokenListener');
const { startQueueProcessor, stopQueueProcessor } = require('./services/tokenQueue');

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

// --- K-SCORE LOOP ---
// This loop continuously re-evaluates tokens to update their "Conviction Score"
async function runKScoreLoop(db) {
    logger.info("🧠 LISTENER: K-Score Analysis Engine Started.");
    
    while (true) {
        try {
            const now = Date.now();
            // Tokens that haven't been updated in 2 hours (High Volume)
            const staleHighPriority = now - (2 * 60 * 60 * 1000);
            // Tokens that haven't been updated in 12 hours (Low Volume)
            const staleLowPriority = now - (12 * 60 * 60 * 1000);

            // Find the most eligible token to update
            // Prioritize: Verified Community Updates + High Volume
            const token = await db.get(`
                SELECT * FROM tokens 
                WHERE 
                    hasCommunityUpdate = TRUE
                    AND (
                        (volume24h > 10000 AND last_k_score_update < $1)
                        OR 
                        (volume24h <= 10000 AND volume24h > 500 AND last_k_score_update < $2)
                    )
                ORDER BY volume24h DESC
                LIMIT 1
            `, [staleHighPriority, staleLowPriority]);

            if (token) {
                logger.info(`🧠 LISTENER: Scoring ${token.symbol} ($${token.mint.slice(0,6)})...`);

                const startTime = Date.now();
                // Use updateSingleToken for full calculation + signatures ("Don't Trust, Verify")
                await updateSingleToken({ db, broadcast: () => {} }, token.mint);
                const duration = Date.now() - startTime;

                logger.info(`✅ SCORED: ${token.symbol} (took ${duration}ms)`);

                // Rate Limit: Wait 2s between updates to save RPC credits
                await new Promise(r => setTimeout(r, 2000));
            } else {
                // No work to do? Sleep for 30s to save CPU
                // logger.debug("💤 K-Score: No eligible tokens found. Sleeping...");
                await new Promise(r => setTimeout(r, 30000));
            }

        } catch (err) {
            logger.error(`❌ K-SCORE LOOP ERROR: ${err.message}`);
            // Sleep on error to prevent rapid restart loops
            await new Promise(r => setTimeout(r, 5000));
        }
    }
}

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

        // 3. Start Token Queue Processor (processes new tokens once metadata is available)
        logger.info("📥 LISTENER: Starting Token Queue Processor...");
        startQueueProcessor();

        // 4. Start New Token Listener (With Delay to let Redis settle)
        if (newTokenListener && typeof newTokenListener.startNewTokenListener === 'function') {
            logger.info("🛰️ LISTENER: Starting New Token Discovery...");
            setTimeout(() => {
                newTokenListener.startNewTokenListener();
            }, 1000);
        } else {
            logger.warn("⚠️ LISTENER: newTokenListener module missing startNewTokenListener function.");
        }

        // 4. Start Grower Scanner (Market Cap Check)
        if (growerScanner && typeof growerScanner.start === 'function') {
            logger.info("🌱 LISTENER: Starting Grower Scanner...");
            growerScanner.start({ db });
        }

        // 5. Start K-Score Loop (Background Analysis)
        runKScoreLoop(db).catch(err => {
            logger.error(`❌ LISTENER FATAL: K-Score Loop died: ${err.message}`);
        });

    } catch (err) {
        logger.error(`❌ LISTENER STARTUP FAILED: ${err.message}`);
        process.exit(1);
    }
}

process.on('SIGINT', () => {
    logger.info("🎧 LISTENER WORKER: Shutting down...");
    stopQueueProcessor();
    process.exit(0);
});

startListenerWorker();
