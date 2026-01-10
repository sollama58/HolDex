/**
 * HolDex Listener Worker - Background Queue Processor
 *
 * Handles background processing queues:
 * - K-Score recalculation queue (tokens flagged for recalc after webhook activity)
 * - Token indexing queue overflow
 *
 * This worker complements the Calculator by handling on-demand recalculations
 * triggered by webhook events, while Calculator handles scheduled updates.
 */

console.log("🎧 LISTENER WORKER LAUNCHING...");

require('dotenv').config();

// Force development mode for env validation
process.env.NODE_ENV = 'development';

const { initDB, getDB } = require('./services/database');
const { connectRedis, getClient } = require('./services/redis');
const logger = require('./services/logger');

// Queue keys
const KSCORE_RECALC_QUEUE = 'kscore:recalc_queue';
const KSCORE_RECALC_PROCESSING = 'kscore:recalc_processing';

// Processing settings
const RECALC_BATCH_SIZE = 5;
const RECALC_INTERVAL_MS = 30000; // 30 seconds between batches
const RECALC_COOLDOWN_MS = 300000; // 5 min cooldown per token

// Track processing state
let isRunning = false;
let recalcInterval = null;

// GLOBAL ERROR HANDLERS
process.on('uncaughtException', (err) => {
    logger.error(`UNCAUGHT EXCEPTION: ${err.message}`);
    logger.error(err.stack);
});

process.on('unhandledRejection', (reason, _promise) => {
    logger.error(`UNHANDLED REJECTION: ${reason}`);
});

/**
 * Queue a token for K-Score recalculation
 * Called by webhook handler when significant activity is detected
 */
async function queueKScoreRecalc(mint) {
    const redis = getClient();
    if (!redis) return false;

    try {
        // Check cooldown - prevent spam recalcs
        const lastRecalc = await redis.get(`kscore:last_recalc:${mint}`);
        if (lastRecalc && Date.now() - parseInt(lastRecalc) < RECALC_COOLDOWN_MS) {
            return false; // Still in cooldown
        }

        // Add to queue (ZADD with timestamp as score for FIFO-like behavior)
        await redis.zadd(KSCORE_RECALC_QUEUE, Date.now(), mint);
        logger.debug(`[KScoreQueue] Queued ${mint.slice(0, 8)} for recalc`);
        return true;
    } catch (e) {
        logger.error(`[KScoreQueue] Failed to queue ${mint}: ${e.message}`);
        return false;
    }
}

/**
 * Process K-Score recalculation queue
 * Runs on interval, processes batch of tokens
 */
async function processKScoreQueue() {
    const redis = getClient();
    const db = getDB();
    if (!redis || !db) return;

    try {
        // Get oldest items from queue (ZPOPMIN for atomic pop)
        const items = await redis.zpopmin(KSCORE_RECALC_QUEUE, RECALC_BATCH_SIZE);
        if (!items || items.length === 0) return;

        // Items come as [member, score, member, score, ...]
        const mints = [];
        for (let i = 0; i < items.length; i += 2) {
            mints.push(items[i]);
        }

        if (mints.length === 0) return;

        logger.info(`[KScoreQueue] Processing ${mints.length} tokens for K-Score recalc`);

        // Load kScoreUpdater lazily to avoid circular deps
        const kScoreUpdater = require('./tasks/kScoreUpdater');

        // Process in parallel
        const results = await Promise.allSettled(
            mints.map(async (mint) => {
                try {
                    // Mark as processing
                    await redis.set(`kscore:processing:${mint}`, Date.now(), 'EX', 300);

                    // Get token data
                    const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
                    if (!token) {
                        logger.debug(`[KScoreQueue] Token not found: ${mint.slice(0, 8)}`);
                        return { mint, success: false, reason: 'not_found' };
                    }

                    // Only recalc verified tokens
                    if (!token.hasCommunityUpdate) {
                        logger.debug(`[KScoreQueue] Skipping unverified token: ${mint.slice(0, 8)}`);
                        return { mint, success: false, reason: 'not_verified' };
                    }

                    // Recalculate K-Score
                    await kScoreUpdater.updateSingleToken(db, mint);

                    // Update cooldown timestamp
                    await redis.set(`kscore:last_recalc:${mint}`, Date.now(), 'EX', RECALC_COOLDOWN_MS / 1000);

                    logger.info(`[KScoreQueue] Recalculated K-Score for ${token.symbol || mint.slice(0, 8)}`);
                    return { mint, success: true };
                } catch (e) {
                    logger.error(`[KScoreQueue] Failed to recalc ${mint.slice(0, 8)}: ${e.message}`);
                    return { mint, success: false, error: e.message };
                } finally {
                    // Clear processing flag
                    await redis.del(`kscore:processing:${mint}`);
                }
            })
        );

        // Log results
        const successful = results.filter(r => r.status === 'fulfilled' && r.value?.success).length;
        const failed = results.length - successful;
        if (failed > 0) {
            logger.warn(`[KScoreQueue] Batch complete: ${successful} success, ${failed} failed`);
        } else {
            logger.debug(`[KScoreQueue] Batch complete: ${successful} tokens updated`);
        }

    } catch (e) {
        logger.error(`[KScoreQueue] Queue processing error: ${e.message}`);
    }
}

/**
 * Get queue stats for health checks
 */
async function getQueueStats() {
    const redis = getClient();
    if (!redis) return { available: false };

    try {
        const queueSize = await redis.zcard(KSCORE_RECALC_QUEUE);
        return {
            available: true,
            recalcQueueSize: queueSize,
            isRunning
        };
    } catch (_e) {
        return { available: false };
    }
}

/**
 * Main startup function
 */
async function startListenerWorker() {
    logger.info("🎧 LISTENER WORKER: Initializing...");

    try {
        // 1. Init Database
        await initDB();
        const db = getDB();
        logger.info("✅ Database Ready");

        // 2. Init Redis
        await connectRedis();
        const redis = getClient();
        if (!redis) {
            logger.error("❌ Redis required for listener worker");
            process.exit(1);
        }
        logger.info("✅ Redis Ready");

        // 3. Start K-Score recalc queue processor
        isRunning = true;
        recalcInterval = setInterval(processKScoreQueue, RECALC_INTERVAL_MS);
        logger.info(`✅ K-Score Recalc Queue Started (interval: ${RECALC_INTERVAL_MS}ms)`);

        // Run initial processing
        await processKScoreQueue();

        // 4. Memory monitoring
        setInterval(() => {
            const mem = process.memoryUsage();
            logger.debug(`💓 Listener | RSS: ${Math.round(mem.rss / 1024 / 1024)}MB | Heap: ${Math.round(mem.heapUsed / 1024 / 1024)}MB`);
        }, 60000);

        logger.info("=".repeat(50));
        logger.info("🎧 LISTENER WORKER ACTIVE");
        logger.info("   Running: K-Score Recalc Queue");
        logger.info(`   Batch Size: ${RECALC_BATCH_SIZE}`);
        logger.info(`   Interval: ${RECALC_INTERVAL_MS / 1000}s`);
        logger.info("=".repeat(50));

    } catch (err) {
        logger.error(`LISTENER STARTUP FAILED: ${err.message}`);
        process.exit(1);
    }
}

// Graceful shutdown
process.on('SIGINT', () => {
    logger.info("🎧 LISTENER WORKER: Shutting down...");
    isRunning = false;
    if (recalcInterval) clearInterval(recalcInterval);
    process.exit(0);
});

process.on('SIGTERM', () => {
    logger.info("🎧 LISTENER WORKER: Shutting down...");
    isRunning = false;
    if (recalcInterval) clearInterval(recalcInterval);
    process.exit(0);
});

// Start
startListenerWorker();

// Export for use by other modules
module.exports = {
    queueKScoreRecalc,
    getQueueStats,
    KSCORE_RECALC_QUEUE
};
