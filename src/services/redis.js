const Redis = require('ioredis');
const config = require('../config/env');
const logger = require('./logger');

let client = null;
let subscriber = null;
let initPromise = null;

async function connectRedis() {
    if (client) return client;
    if (initPromise) return initPromise;

    initPromise = (async () => {
        try {
            // Initialize Redis Client
            // Force lazyConnect to ensure we catch connection errors in the Promise
            const tempClient = new Redis(config.REDIS_URL, {
                lazyConnect: true,
                maxRetriesPerRequest: null,
                retryStrategy: (times) => Math.min(times * 50, 2000),
                reconnectOnError: (err) => {
                    const targetError = 'READONLY';
                    return err.message.slice(0, targetError.length) === targetError;
                }
            });

            tempClient.on('error', (err) => {
                if (!err.message.includes('ECONNREFUSED')) {
                    logger.error(`Redis Error: ${err.message}`);
                }
            });

            await tempClient.connect();
            logger.info('✅ Redis Connected');
            
            client = tempClient;
            subscriber = client.duplicate();
            
            return client;
        } catch (e) {
            logger.error(`Redis Connection Failed: ${e.message}`);
            initPromise = null;
            return null; // Return null so app can run in "degraded" mode without crashing
        }
    })();

    return initPromise;
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
