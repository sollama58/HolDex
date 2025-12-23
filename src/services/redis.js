/**
 * Redis Service
 * Handles caching and temporary state storage
 */
const Redis = require('ioredis');
const config = require('../config/env');
const logger = require('./logger');

let redis = null;

function initRedis() {
    if (!config.REDIS_URL) {
        logger.warn('REDIS_URL not set. Caching will be disabled (or fallback to memory).');
        return null;
    }

    try {
        redis = new Redis(config.REDIS_URL, {
            maxRetriesPerRequest: 3,
            retryStrategy(times) {
                const delay = Math.min(times * 50, 2000);
                return delay;
            }
        });

        redis.on('connect', () => logger.info('Connected to Redis'));
        redis.on('error', (err) => logger.error('Redis Error', { error: err.message }));

        return redis;
    } catch (e) {
        logger.error('Failed to initialize Redis', { error: e.message });
        return null;
    }
}

// Get the singleton instance
const getRedis = () => {
    if (!redis) {
        // Try to init if not already done
        return initRedis();
    }
    return redis;
};

module.exports = {
    initRedis,
    getRedis
};
