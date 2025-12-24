/**
 * Redis Service
 * Uses 'ioredis' for robust connection handling and features.
 */
const Redis = require('ioredis');
const config = require('../config/env');
const logger = require('./logger');

let redisClient = null;

function initRedis() {
    if (redisClient) return redisClient;

    try {
        // Support for REDIS_URL string (e.g. from Render/Heroku)
        if (config.REDIS_URL) {
            redisClient = new Redis(config.REDIS_URL, {
                maxRetriesPerRequest: 3,
                retryStrategy: (times) => {
                    const delay = Math.min(times * 50, 2000);
                    return delay;
                }
            });
        } else {
            // Fallback for local dev if no URL provided
            redisClient = new Redis({
                host: 'localhost',
                port: 6379
            });
        }

        redisClient.on('connect', () => {
            logger.info('✅ Redis Connected');
        });

        redisClient.on('error', (err) => {
            logger.error('❌ Redis Error:', err);
        });

        return redisClient;
    } catch (e) {
        logger.error('Failed to initialize Redis:', e);
        return null;
    }
}

function getRedis() {
    if (!redisClient) {
        return initRedis();
    }
    return redisClient;
}

module.exports = {
    initRedis,
    getRedis
};
