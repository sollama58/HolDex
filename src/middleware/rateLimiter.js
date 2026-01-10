const { getClient } = require('../services/redis');
const { getDB } = require('../services/database');
const logger = require('../services/logger');
const { hashApiKey } = require('../utils/apiKeyHash');
const crypto = require('crypto');

// Cache key details in memory for 60 seconds to avoid hitting Postgres on every request
const KEY_CACHE = new Map();
const KEY_CACHE_MAX_SIZE = 1000; // Max cached API keys (prevents memory leak)
const KEY_CACHE_TTL = 60000; // 60 seconds

// SECURITY: In-memory fallback rate limiting when Redis is down
// Improved for clustered deployment awareness
const FALLBACK_WINDOW = 60 * 1000; // 1 minute window for fallback
const FALLBACK_MAX_SIZE = 500; // Max tracked IPs
const fallbackTracker = new Map(); // Use Map for better cleanup

// SECURITY: Generate instance ID for cluster-aware logging
const INSTANCE_ID = crypto.randomBytes(4).toString('hex');

// Track when Redis was last available (for alerting on extended outages)
let lastRedisAvailable = Date.now();
let redisOutageAlerted = false;
const REDIS_OUTAGE_ALERT_THRESHOLD = 5 * 60 * 1000; // Alert after 5 minutes of outage

// Clean up expired entries every 5 minutes (prevents memory leak)
setInterval(() => {
    const now = Date.now();

    // Clean fallback tracker
    for (const [key, data] of fallbackTracker) {
        if (now > data.resetTime) {
            fallbackTracker.delete(key);
        }
    }

    // Clean KEY_CACHE expired entries
    for (const [key, data] of KEY_CACHE) {
        if (now > data.expiry) {
            KEY_CACHE.delete(key);
        }
    }

    logger.debug(`[RateLimiter] Cache cleanup: KEY_CACHE=${KEY_CACHE.size}, fallback=${fallbackTracker.size}`);
}, 5 * 60 * 1000);

/**
 * LRU-style eviction: remove oldest entries when cache is full
 */
function evictOldestEntries(cache, maxSize) {
    if (cache.size <= maxSize) return;

    // Maps maintain insertion order - delete from the beginning
    const toDelete = cache.size - maxSize;
    let deleted = 0;
    for (const key of cache.keys()) {
        if (deleted >= toDelete) break;
        cache.delete(key);
        deleted++;
    }
}

// ============================================
// ATOMIC RATE LIMITING - Lua script for Redis
// Prevents race conditions between INCR and EXPIRE
// ============================================
const RATE_LIMIT_LUA = `
local key = KEYS[1]
local ttl = tonumber(ARGV[1])

-- Atomic increment
local count = redis.call('INCR', key)

-- Set TTL only on first request (when count = 1)
if count == 1 then
    redis.call('EXPIRE', key, ttl)
end

return count
`;

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
                expiry: now + KEY_CACHE_TTL
            };
            // Evict oldest if cache is full (LRU-style)
            evictOldestEntries(KEY_CACHE, KEY_CACHE_MAX_SIZE);
            KEY_CACHE.set(keyHash, keyData);
        }

        // 3. Rate Limit Logic (Redis Window)
        if (redis) {
            // Key format: rate_limit:<key_hash>:<YYYY-MM-DD>
            const dateStr = new Date().toISOString().split('T')[0];
            const windowKey = `rate_limit:${keyHash}:${dateStr}`;

            // ATOMIC: Lua script handles INCR + conditional EXPIRE in one operation
            // Prevents race condition where key exists without TTL
            const currentUsage = await redis.eval(RATE_LIMIT_LUA, 1, windowKey, 86400);

            // Track that Redis is available (for fallback mode alerting)
            lastRedisAvailable = Date.now();
            if (redisOutageAlerted) {
                logger.info(`[RateLimiter] Redis connectivity restored (instance ${INSTANCE_ID})`);
                redisOutageAlerted = false;
            }

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
            // SECURITY: In-memory fallback rate limiting when Redis is down
            // WARNING: In clustered deployments, each instance tracks separately!
            // Use very conservative limits to compensate for N instances
            const now = Date.now();

            // Check for extended Redis outage
            if (now - lastRedisAvailable > REDIS_OUTAGE_ALERT_THRESHOLD && !redisOutageAlerted) {
                logger.error(`[RateLimiter] ALERT: Redis has been unavailable for ${Math.floor((now - lastRedisAvailable) / 1000)}s - rate limiting is degraded (instance ${INSTANCE_ID})`);
                redisOutageAlerted = true;
            }

            logger.warn(`[RateLimiter] Redis unavailable - using fallback rate limiting (instance ${INSTANCE_ID})`);

            let tracker = fallbackTracker.get(keyHash);

            // Atomic-style: get or create, reset if expired, increment - all in one block
            if (!tracker || now > tracker.resetTime) {
                tracker = { count: 1, resetTime: now + FALLBACK_WINDOW, instance: INSTANCE_ID };
            } else {
                tracker.count++;
            }
            // Evict oldest if tracker is full (LRU-style)
            evictOldestEntries(fallbackTracker, FALLBACK_MAX_SIZE);
            fallbackTracker.set(keyHash, tracker);

            // SECURITY: Apply VERY strict fallback limit
            // In a 3-node cluster, each node might allow 3.3%, so total is ~10%
            // This is intentionally conservative to prevent abuse during Redis outage
            const CLUSTER_NODES_ESTIMATE = 3; // Assume 3 nodes for safety margin
            const fallbackLimit = Math.max(1, Math.ceil(keyData.requests_limit * 0.033 / CLUSTER_NODES_ESTIMATE));

            if (tracker.count > fallbackLimit) {
                logger.warn(`[RateLimiter] Fallback limit exceeded for key: ${keyHash.slice(0, 8)}... (instance ${INSTANCE_ID})`);
                return res.status(429).json({
                    success: false,
                    error: 'Rate limit exceeded (degraded mode)',
                    fallback: true,
                    retryAfter: Math.ceil((tracker.resetTime - now) / 1000),
                    message: 'Service is operating in degraded mode. Please retry later.'
                });
            }

            res.setHeader('X-RateLimit-Fallback', 'true');
            res.setHeader('X-RateLimit-Remaining', Math.max(0, fallbackLimit - tracker.count));
            res.setHeader('X-RateLimit-Instance', INSTANCE_ID);
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
