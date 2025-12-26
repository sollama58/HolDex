const { getClient } = require('./redis');
const logger = require('./logger');

const QUEUE_KEY = 'queue:token_indexing';

/**
 * Adds a mint to the indexing queue.
 * Uses Redis Set to ensure no duplicates in the queue.
 */
async function enqueueTokenUpdate(mint) {
    const redis = getClient();
    if (!redis || redis.status !== 'ready') {
        // If Redis is down, we just log and return false. 
        // The calling function (tokens.js) falls back to immediate processing.
        // logger.warn("Redis not available/ready, skipping queue push"); 
        return false;
    }
    
    try {
        // SADD ensures uniqueness (we don't want to queue the same token 50 times)
        await redis.sadd(QUEUE_KEY, mint);
        return true;
    } catch (e) {
        logger.error(`Queue Push Error: ${e.message}`);
        return false;
    }
}

/**
 * Pops a batch of tokens from the queue for processing.
 */
async function dequeueBatch(batchSize = 10) {
    const redis = getClient();
    if (!redis || redis.status !== 'ready') return [];

    try {
        // SPOP pops random members. For strict FIFO, use LPUSH/RPOP, 
        // but for indexing, Set is better to prevent duplication flood.
        const batch = await redis.spop(QUEUE_KEY, batchSize);
        return batch || [];
    } catch (e) {
        logger.error(`Queue Pop Error: ${e.message}`);
        return [];
    }
}

async function getQueueLength() {
    const redis = getClient();
    if (!redis || redis.status !== 'ready') return 0;
    return await redis.scard(QUEUE_KEY);
}

module.exports = { enqueueTokenUpdate, dequeueBatch, getQueueLength };
