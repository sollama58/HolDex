const { getClient } = require('./redis');
const logger = require('./logger');

const QUEUE_KEY = 'queue:token_indexing';

/**
 * Adds a mint to the indexing queue.
 * Uses Redis Set to ensure no duplicates in the queue.
 */
async function enqueueTokenUpdate(mint) {
    const redis = getClient();
    if (!redis) {
        logger.warn("Redis not available, skipping queue push");
        return false;
    }
    // SADD ensures uniqueness (we don't want to queue the same token 50 times)
    await redis.sadd(QUEUE_KEY, mint);
    return true;
}

/**
 * Pops a batch of tokens from the queue for processing.
 */
async function dequeueBatch(batchSize = 10) {
    const redis = getClient();
    if (!redis) return [];

    // SPOP pops random members. For strict FIFO, use LPUSH/RPOP, 
    // but for indexing, Set is better to prevent duplication flood.
    const batch = await redis.spop(QUEUE_KEY, batchSize);
    return batch || [];
}

async function getQueueLength() {
    const redis = getClient();
    return redis ? await redis.scard(QUEUE_KEY) : 0;
}

module.exports = { enqueueTokenUpdate, dequeueBatch, getQueueLength };
