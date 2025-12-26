const { getClient } = require('./redis');
const logger = require('./logger');

const QUEUE_KEY = 'token_queue';

async function enqueueTokenUpdate(mint) {
    const redis = getClient();
    if (!redis) {
        // Fail silently if Redis is down to prevent crashing main API
        return;
    }

    try {
        // Add to the head of the list
        await redis.lpush(QUEUE_KEY, mint);
    } catch (e) {
        logger.error(`Queue Push Error: ${e.message}`);
    }
}

module.exports = {
    enqueueTokenUpdate
};
