const { getClient } = require('../services/redis');
const { getDB } = require('../services/database');
const logger = require('../services/logger');
const { hashApiKey } = require('../utils/apiKeyHash');

// Cache key details in memory for 60 seconds to avoid hitting Postgres on every request
const KEY_CACHE = new Map();

const rateLimiter = async (req, res, next) => {
    // 1. Get Key from Header or Query
    const apiKey = req.headers['x-api-key'] || req.query.api_key;
    if (!apiKey) return res.status(401).json({ success: false, error: 'API Key Required' });

    try {
        const redis = getClient();
        const keyHash = hashApiKey(apiKey);

        // 2. Validate Key (Memory Cache -> DB Fallback)
        let keyData = KEY_CACHE.get(keyHash);
        const now = Date.now();

        // If not in cache or expired, fetch from DB
        if (!keyData || now > keyData.expiry) {
            const db = getDB();
            // Compare by hash
            const record = await db.get('SELECT * FROM api_keys WHERE key_hash = $1', [keyHash]);

            if (!record) return res.status(403).json({ success: false, error: 'Invalid API Key' });
            if (!record.is_active) return res.status(403).json({ success: false, error: 'API Key Revoked' });

            keyData = {
                ...record,
                keyHash,
                // Cache for 60 seconds
                expiry: now + 60000
            };
            KEY_CACHE.set(keyHash, keyData);
        }

        // 3. Rate Limit Logic (Redis Window)
        if (redis) {
            // Key format: rate_limit:<key_hash>:<YYYY-MM-DD>
            const dateStr = new Date().toISOString().split('T')[0];
            const windowKey = `rate_limit:${keyHash}:${dateStr}`;

            // Atomic Increment
            const currentUsage = await redis.incr(windowKey);

            // Set expiry for 24 hours if this is the first request of the day
            if (currentUsage === 1) await redis.expire(windowKey, 86400);

            // 4. Async DB Sync (Lazy Update)
            // Update Postgres every 10 requests so Admin Panel stays roughly in sync
            // without choking the DB on every single hit.
            if (currentUsage % 10 === 0) {
                const db = getDB();
                // Fire and forget - don't await this
                db.run('UPDATE api_keys SET requests_today = $1, last_reset = $2 WHERE key_hash = $3',
                    [currentUsage, now, keyHash])
                    .catch(err => logger.error(`DB Sync Error: ${err.message}`));
            }

            // 5. Enforce Limit
            if (currentUsage > keyData.requests_limit) {
                res.setHeader('X-RateLimit-Limit', keyData.requests_limit);
                res.setHeader('X-RateLimit-Remaining', 0);
                return res.status(429).json({ 
                    success: false, 
                    error: 'Daily API Limit Exceeded',
                    limit: keyData.requests_limit,
                    usage: currentUsage,
                    tier: keyData.tier
                });
            }

            // Add headers for developer experience
            res.setHeader('X-RateLimit-Limit', keyData.requests_limit);
            res.setHeader('X-RateLimit-Remaining', Math.max(0, keyData.requests_limit - currentUsage));

            // Attach user info to request for downstream use
            req.apiUser = { owner: keyData.owner, tier: keyData.tier };
        } else {
            // Fallback if Redis is down: Allow request but log warning
            logger.warn('Redis unavailable for rate limiting. Request allowed.');
        }

        next();

    } catch (e) {
        logger.error(`RateLimit Error: ${e.message}`);
        // Fail open to avoid service disruption
        next(); 
    }
};

module.exports = rateLimiter;
