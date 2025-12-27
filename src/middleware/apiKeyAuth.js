const { getDB } = require('../services/database');
const { getClient } = require('../services/redis');
const logger = require('../services/logger');

// Local memory cache to prevent slamming Redis/DB for key validation
// Cache valid keys for 60 seconds
const KEY_CACHE = new Map();

/**
 * @param {boolean} required - If true, blocks requests without valid keys.
 */
const apiKeyAuth = (required = true) => {
    return async (req, res, next) => {
        // 1. Allow Options/Preflight
        if (req.method === 'OPTIONS') return next();

        // 2. Extract Key
        const apiKey = req.headers['x-api-key'] || req.query.api_key;

        // 3. Handle Missing Key
        if (!apiKey) {
            if (required) {
                return res.status(401).json({ success: false, error: 'API Key Required (Header: x-api-key)' });
            }
            // If not required, attach 'anonymous' user and proceed to IP rate limiter (handled elsewhere)
            req.apiUser = { owner: 'anonymous', tier: 'public' };
            return next();
        }

        try {
            const now = Date.now();
            let keyData = KEY_CACHE.get(apiKey);

            // 4. Validate Key (Cache -> DB)
            if (!keyData || now > keyData.expiry) {
                const db = getDB();
                const record = await db.get('SELECT * FROM api_keys WHERE key = $1', [apiKey]);

                if (!record) return res.status(403).json({ success: false, error: 'Invalid API Key' });
                if (!record.is_active) return res.status(403).json({ success: false, error: 'API Key Revoked' });

                keyData = { 
                    ...record, 
                    expiry: now + 60000 // Cache for 60s
                };
                KEY_CACHE.set(apiKey, keyData);
            }

            // 5. Rate Limiting (Redis Optimized)
            const redis = getClient();
            if (redis) {
                const dateStr = new Date().toISOString().split('T')[0];
                const redisKey = `rate_limit:${apiKey}:${dateStr}`;

                // Atomic Increment
                const currentUsage = await redis.incr(redisKey);
                
                // Set expiry (24h) on first use of the day
                if (currentUsage === 1) await redis.expire(redisKey, 86400);

                // Add Headers
                res.setHeader('X-RateLimit-Limit', keyData.requests_limit);
                res.setHeader('X-RateLimit-Remaining', Math.max(0, keyData.requests_limit - currentUsage));

                // Block if exceeded
                if (currentUsage > keyData.requests_limit) {
                    return res.status(429).json({ 
                        success: false, 
                        error: 'Daily API Limit Exceeded',
                        tier: keyData.tier
                    });
                }

                // Lazy Sync to Postgres (Every 20 requests)
                if (currentUsage % 20 === 0) {
                    const db = getDB();
                    db.run('UPDATE api_keys SET requests_today = $1, last_reset = $2 WHERE key = $3', 
                        [currentUsage, now, apiKey]).catch(err => logger.error(`DB Sync Error: ${err.message}`));
                }
            } else {
                // FALLBACK: If Redis is down, we skip strict counting to keep API alive,
                // or you can implement the DB logic here as a slow fallback.
                logger.warn('Redis down: Skipping strict rate limit check');
            }

            // 6. Attach Context
            req.apiUser = { owner: keyData.owner, tier: keyData.tier };
            next();

        } catch (e) {
            logger.error(`Auth Middleware Error: ${e.message}`);
            // Fail open for authorized users if system error, or fail closed? 
            // Usually safer to fail closed 500.
            return res.status(500).json({ success: false, error: 'Internal Auth Error' });
        }
    };
};

module.exports = apiKeyAuth;
