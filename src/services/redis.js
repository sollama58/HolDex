const Redis = require('ioredis');
const config = require('../config/env');
const logger = require('./logger');
const { createCircuitBreaker } = require('../utils/circuitBreaker');

let client = null;
let subscriber = null;
let initPromise = null;
let redisCircuitBreaker = null;

async function connectRedis() {
    if (client) return client;
    if (initPromise) return initPromise;

    initPromise = (async () => {
        try {
            // Initialize Redis Client
            // Force lazyConnect to ensure we catch connection errors in the Promise
            // keyPrefix isolates HolDex keys from other apps sharing the same Redis
            const tempClient = new Redis(config.REDIS_URL, {
                lazyConnect: true,
                keyPrefix: 'holdex:',
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

            // Initialize circuit breaker for Redis
            redisCircuitBreaker = createCircuitBreaker('redis', {
                threshold: 5,
                cooldown: 30000,
                onStateChange: (name, oldState, newState) => {
                    if (newState === 'open') {
                        logger.warn(`[Redis] Circuit breaker OPEN - Redis operations will use fallbacks`);
                    } else if (newState === 'closed') {
                        logger.info(`[Redis] Circuit breaker CLOSED - Redis recovered`);
                    }
                }
            });

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

/**
 * Get circuit breaker state for health checks
 */
function getCircuitBreakerState() {
    return redisCircuitBreaker ? redisCircuitBreaker.getState() : null;
}

/**
 * Check if Redis is healthy
 */
function isHealthy() {
    if (!client) return false;
    if (redisCircuitBreaker && redisCircuitBreaker.isOpen) return false;
    return true;
}

/**
 * Execute Redis operation with circuit breaker protection
 */
async function executeWithBreaker(operation, fallback = null) {
    if (!client) {
        return fallback;
    }

    if (redisCircuitBreaker && redisCircuitBreaker.isOpen) {
        return fallback;
    }

    try {
        const result = await operation();
        if (redisCircuitBreaker) redisCircuitBreaker.recordSuccess();
        return result;
    } catch (err) {
        if (redisCircuitBreaker) redisCircuitBreaker.recordFailure(err);
        return fallback;
    }
}

module.exports = {
    connectRedis,
    initRedis: connectRedis, // ALIAS for backwards compatibility
    getClient,
    getRedis: getClient, // ALIAS for backwards compatibility
    getSubscriber,
    getCircuitBreakerState,
    isHealthy,
    executeWithBreaker
};
