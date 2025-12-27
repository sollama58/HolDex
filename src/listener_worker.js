require('dotenv').config();
const { initDB, getDB } = require('./services/database');
const { initRedis } = require('./services/redis');
const logger = require('./services/logger');

// --- TASKS ---
const { calculateDeepScore } = require('./tasks/kScoreUpdater');
const growerScanner = require('./tasks/growerScanner');
const newTokenListener = require('./tasks/newTokenListener');

// Attempt to load the Indexer service if it exists
let indexerService = null;
try {
    indexerService = require('./indexer');
} catch (e) {
    logger.warn("ℹ️ Listener Worker: Could not load './indexer' module. Skipping indexer start.");
}

// --- K-SCORE CONFIGURATION ---
const RATE_LIMIT_DELAY_MS = 2000; // 2s delay between updates to save RPC
const HIGH_PRIORITY_AGE_MS = 2 * 60 * 60 * 1000; // Update hot tokens every 2h
const LOW_PRIORITY_AGE_MS = 12 * 60 * 60 * 1000; // Update quiet tokens every 12h

/**
 * The K-Score Loop runs continuously.
 * It performs heavy RPC analysis (Deep Holder Check) and updates the conviction score.
 */
async function runKScoreLoop(db) {
    logger.info("🧠 LISTENER: K-Score Analysis Engine Started.");
    
    while (true) {
        try {
            // STRATEGY: 
            // 1. FILTER: ONLY tokens with Community Updates are eligible.
            // 2. Prioritize Hot Tokens (Volume > $10k) that haven't updated in 2 hours.
            // 3. Then Warm Tokens (Volume > $500) that haven't updated in 12 hours.
            
            const now = Date.now();
            const staleHighPriority = now - HIGH_PRIORITY_AGE_MS;
            const staleLowPriority = now - LOW_PRIORITY_AGE_MS;

            // Updated Query: Added `hasCommunityUpdate = TRUE` constraint
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

                // 1. Calculate Score (Heavy RPC usage inside)
                const newScore = await calculateDeepScore(db, token);

                // 2. Update DB
                await db.run(`
                    UPDATE tokens 
                    SET k_score = $1, last_k_score_update = $2 
                    WHERE mint = $3
                `, [newScore, Date.now(), token.mint]);

                const duration = Date.now() - startTime;
                logger.info(`✅ LISTENER: ${token.symbol} Score: ${newScore} (took ${duration}ms)`);

                // 3. Rate Limit Sleep
                await new Promise(r => setTimeout(r, RATE_LIMIT_DELAY_MS));

            } else {
                // No eligible tokens found? Sleep longer to save CPU.
                await new Promise(r => setTimeout(r, 30000));
            }

        } catch (err) {
            logger.error(`❌ K-SCORE LOOP ERROR: ${err.message}`);
            // Sleep on crash to prevent rapid restart loops
            await new Promise(r => setTimeout(r, 5000));
        }
    }
}

async function startListenerWorker() {
    logger.info("🎧 LISTENER WORKER: Initializing Core Services...");
    
    try {
        // 1. Initialize Infrastructure
        await initDB();
        await initRedis();
        const db = getDB();

        // 2. Start the Indexer (Volume/Candles/Snapshots)
        if (indexerService && typeof indexerService.start === 'function') {
            logger.info("📊 LISTENER: Starting Token Indexer...");
            indexerService.start();
        } else {
            logger.info("ℹ️ LISTENER: No Indexer service 'start' function found.");
        }

        // 3. Start New Token Listener (Ingestion from Solana)
        if (newTokenListener && typeof newTokenListener.startNewTokenListener === 'function') {
            logger.info("🛰️ LISTENER: Starting New Token Discovery...");
            newTokenListener.startNewTokenListener();
        } else {
            logger.warn("⚠️ LISTENER: newTokenListener module missing startNewTokenListener function.");
        }

        // 4. Start Grower Scanner (Checks for pending tokens hitting mcap thresholds)
        if (growerScanner && typeof growerScanner.start === 'function') {
            logger.info("🌱 LISTENER: Starting Grower Scanner...");
            growerScanner.start({ db });
        } else {
            logger.warn("⚠️ LISTENER: Grower Scanner module missing start function.");
        }

        // 5. Start K-Score Analysis Loop (Heavy Background Task)
        runKScoreLoop(db).catch(err => {
            logger.error(`❌ LISTENER FATAL: K-Score Loop died: ${err.message}`);
        });

    } catch (err) {
        logger.error(`❌ LISTENER STARTUP FAILED: ${err.message}`);
        process.exit(1);
    }
}

// Handle Process Exit
process.on('SIGINT', () => {
    logger.info("🎧 LISTENER WORKER: Shutting down...");
    process.exit(0);
});

process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

startListenerWorker();
