const { getClient } = require('../services/redis');
const { getDB } = require('../services/database');
const logger = require('../services/logger');
const { hashApiKey } = require('../utils/apiKeyHash');

// Cache key details in memory for 60 seconds to avoid hitting Postgres on every request
const KEY_CACHE = new Map();

// SECURITY: In-memory fallback rate limiting when Redis is down (H2)
const FALLBACK_WINDOW = 60 * 1000; // 1 minute window for fallback
const fallbackTracker = {};

// Clean up old fallback entries every 5 minutes
setInterval(() => {
    const now = Date.now();
    for (const key in fallbackTracker) {
        if (now > fallbackTracker[key].resetTime) {
            delete fallbackTracker[key];
        }
    }
}, 5 * 60 * 1000);

const rateLimiter = async (req, res, next) => {
    // 1. Get Key from Header or Query
    const headerKey = req.headers['x-api-key'];
    const queryKey = req.query.api_key;

    // SECURITY: Deprecate API key in query params (M1)
    // Query params are logged in access logs, browser history, and can be cached
    if (queryKey && !headerKey) {
        logger.warn(`[RateLimiter] DEPRECATED: API key used in query params from ${req.ip}`);
        res.setHeader('X-Deprecated', 'api_key query parameter - use x-api-key header instead');
    }

    const apiKey = headerKey || queryKey;
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
            // SECURITY: In-memory fallback rate limiting when Redis is down (H2)
            logger.warn('Redis unavailable - using in-memory fallback rate limiting');
            const keyHash = hashApiKey(apiKey);

            if (!fallbackTracker[keyHash]) {
                fallbackTracker[keyHash] = { count: 0, resetTime: Date.now() + FALLBACK_WINDOW };
            }

            // Reset window if expired
            if (Date.now() > fallbackTracker[keyHash].resetTime) {
                fallbackTracker[keyHash] = { count: 0, resetTime: Date.now() + FALLBACK_WINDOW };
            }

            fallbackTracker[keyHash].count++;

            // Apply stricter fallback limit (10% of normal limit)
            const fallbackLimit = Math.ceil(keyData.requests_limit * 0.1);
            if (fallbackTracker[keyHash].count > fallbackLimit) {
                logger.warn(`[RateLimiter] Fallback limit exceeded for key: ${keyHash.slice(0, 8)}...`);
                return res.status(429).json({
                    success: false,
                    error: 'Rate limit exceeded (degraded mode)',
                    fallback: true,
                    retryAfter: Math.ceil((fallbackTracker[keyHash].resetTime - Date.now()) / 1000)
                });
            }

            res.setHeader('X-RateLimit-Fallback', 'true');
            res.setHeader('X-RateLimit-Remaining', fallbackLimit - fallbackTracker[keyHash].count);
            req.apiUser = { owner: keyData.owner, tier: keyData.tier };
        }

        next();

    } catch (e) {
        logger.error(`RateLimit Error: ${e.message}`);
        // SECURITY: Fail closed - don't allow unauthenticated access on errors
        return res.status(503).json({
            success: false,
            error: 'Service temporarily unavailable',
            retry_after: 30
        });
    }
};

module.exports = rateLimiter;
