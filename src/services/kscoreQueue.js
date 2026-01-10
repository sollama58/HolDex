/**
 * K-Score Recalculation Queue Service
 *
 * Provides functions to queue tokens for K-Score recalculation.
 * The actual processing is done by the listener_worker.js background process.
 *
 * Use this when:
 * - Webhook receives significant trading activity for a token
 * - Admin requests immediate K-Score refresh
 * - Token verification status changes
 */

const { getClient } = require('./redis');
const logger = require('./logger');

// Queue keys (must match listener_worker.js)
const KSCORE_RECALC_QUEUE = 'kscore:recalc_queue';
const RECALC_COOLDOWN_MS = 300000; // 5 min cooldown per token

/**
 * Queue a token for K-Score recalculation
 * @param {string} mint - Token mint address
 * @param {Object} options - Options
 * @param {boolean} options.priority - If true, bypasses cooldown
 * @param {string} options.reason - Reason for recalc (for logging)
 * @returns {Promise<boolean>} - True if queued successfully
 */
async function queueKScoreRecalc(mint, options = {}) {
    const redis = getClient();
    if (!redis) {
        logger.debug('[KScoreQueue] Redis unavailable, skipping queue');
        return false;
    }

    const { priority = false, reason = 'activity' } = options;

    try {
        // Validate mint
        if (!mint || typeof mint !== 'string' || mint.length < 32) {
            return false;
        }

        // Check cooldown unless priority
        if (!priority) {
            const lastRecalc = await redis.get(`kscore:last_recalc:${mint}`);
            if (lastRecalc && Date.now() - parseInt(lastRecalc, 10) < RECALC_COOLDOWN_MS) {
                logger.debug(`[KScoreQueue] ${mint.slice(0, 8)} in cooldown, skipping`);
                return false;
            }
        }

        // Check if already in queue
        const score = await redis.zscore(KSCORE_RECALC_QUEUE, mint);
        if (score !== null) {
            logger.debug(`[KScoreQueue] ${mint.slice(0, 8)} already in queue`);
            return false;
        }

        // Add to queue with timestamp score (lower = older = processed first)
        // Priority items get score 0 to be processed first
        const queueScore = priority ? 0 : Date.now();
        await redis.zadd(KSCORE_RECALC_QUEUE, queueScore, mint);

        logger.debug(`[KScoreQueue] Queued ${mint.slice(0, 8)} (${reason}${priority ? ', priority' : ''})`);
        return true;
    } catch (e) {
        logger.error(`[KScoreQueue] Failed to queue ${mint}: ${e.message}`);
        return false;
    }
}

/**
 * Queue multiple tokens for K-Score recalculation
 * @param {string[]} mints - Array of token mint addresses
 * @param {Object} options - Options passed to queueKScoreRecalc
 * @returns {Promise<number>} - Number of tokens queued
 */
async function queueMultipleKScoreRecalc(mints, options = {}) {
    if (!Array.isArray(mints) || mints.length === 0) return 0;

    let queued = 0;
    for (const mint of mints) {
        const success = await queueKScoreRecalc(mint, options);
        if (success) queued++;
    }
    return queued;
}

/**
 * Get current queue statistics
 * @returns {Promise<Object>} - Queue stats
 */
async function getQueueStats() {
    const redis = getClient();
    if (!redis) return { available: false };

    try {
        const queueSize = await redis.zcard(KSCORE_RECALC_QUEUE);
        return {
            available: true,
            recalcQueueSize: queueSize
        };
    } catch (_e) {
        return { available: false, error: 'Redis error' };
    }
}

/**
 * Clear the recalc queue (admin function)
 * @returns {Promise<number>} - Number of items removed
 */
async function clearQueue() {
    const redis = getClient();
    if (!redis) return 0;

    try {
        const size = await redis.zcard(KSCORE_RECALC_QUEUE);
        await redis.del(KSCORE_RECALC_QUEUE);
        logger.info(`[KScoreQueue] Cleared ${size} items from queue`);
        return size;
    } catch (e) {
        logger.error(`[KScoreQueue] Failed to clear queue: ${e.message}`);
        return 0;
    }
}

module.exports = {
    queueKScoreRecalc,
    queueMultipleKScoreRecalc,
    getQueueStats,
    clearQueue,
    KSCORE_RECALC_QUEUE
};
