const Redis = require('ioredis');
const config = require('../config/env');

let client = null;

async function initRedis() {
    if (client) return client;

    // Use REDIS_URL from env (Docker service name 'redis' resolves to IP automatically)
    // Fallback to localhost for local development
    const redisUrl = config.REDIS_URL || 'redis://localhost:6379';
    
    console.log(`🔌 Connecting to Redis at ${redisUrl}...`);
    
    try {
        client = new Redis(redisUrl, {
            maxRetriesPerRequest: 3,
            retryStrategy(times) {
                const delay = Math.min(times * 50, 2000);
                return delay;
            }
        });

        client.on('connect', () => console.log('✅ Redis Connected'));
        client.on('error', (err) => {
            // Suppress connection refused logs in dev if redis isn't running
            if (err.code === 'ECONNREFUSED') {
                console.warn('⚠️ Redis Connection Refused (Is Redis running?)');
            } else {
                console.warn('⚠️ Redis Error:', err.message);
            }
        });
    } catch (e) {
        console.error("Failed to initialize Redis client", e);
    }

    return client;
}

function getClient() {
    return client;
}

module.exports = { initRedis, getClient };
