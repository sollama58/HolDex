const Redis = require('ioredis');
const config = require('../config/env');
const logger = require('./logger');

let client = null;

function getClient() {
    if (client) return client;

    try {
        // Only initialize if a URL is provided
        if (!config.REDIS_URL) {
            logger.warn('Redis URL not set. Caching disabled.');
            return null;
        }

        client = new Redis(config.REDIS_URL, {
            maxRetriesPerRequest: 3,
            retryStrategy: (times) => {
                const delay = Math.min(times * 50, 2000);
                return delay;
            },
            reconnectOnError: (err) => {
                const targetError = 'READONLY';
                if (err.message.includes(targetError)) {
                    return true;
                }
                return false;
            }
        });

        client.on('connect', () => {
            logger.info('✅ Redis Connected');
        });

        client.on('error', (err) => {
            // Suppress connection refused logs in dev if not using redis
            if (err.code === 'ECONNREFUSED') {
                logger.warn('Redis Connection Refused. Ensure Redis is running.');
            } else {
                logger.error('Redis Error:', err.message);
            }
        });

        return client;
    } catch (e) {
        logger.error('Failed to initialize Redis client:', e.message);
        return null;
    }
}

module.exports = { getClient };
