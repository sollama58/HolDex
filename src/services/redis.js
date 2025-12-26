const Redis = require('ioredis');
const config = require('../config/env');
const logger = require('./logger');

let client = null;
let subscriber = null;

async function connectRedis() {
    if (client) return client;

    try {
        // Initialize Redis Client
        client = new Redis(config.REDIS_URL, {
            maxRetriesPerRequest: null,
            enableReadyCheck: false,
            retryStrategy: (times) => {
                const delay = Math.min(times * 50, 2000);
                return delay;
            },
            reconnectOnError: (err) => {
                const targetError = 'READONLY';
                if (err.message.slice(0, targetError.length) === targetError) {
                    return true;
                }
                return false;
            }
        });

        client.on('error', (err) => {
            // Suppress connection refused logs during local dev to keep console clean
            if (!err.message.includes('ECONNREFUSED')) {
                logger.error(`Redis Error: ${err.message}`);
            }
        });

        client.on('connect', () => {
            logger.info('✅ Redis Connected');
        });

        // Initialize Subscriber (Optional, for future Pub/Sub use)
        subscriber = client.duplicate();

        // Wait for ready state
        await new Promise((resolve) => {
            client.once('ready', resolve);
            // Fallback if ready event is delayed
            setTimeout(resolve, 1000); 
        });

        return client;
    } catch (e) {
        logger.error(`Redis Connection Failed: ${e.message}`);
        return null;
    }
}

function getClient() {
    return client;
}

function getSubscriber() {
    return subscriber;
}

module.exports = {
    connectRedis,
    getClient,
    getSubscriber
};
