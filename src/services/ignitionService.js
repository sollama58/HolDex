/**
 * IGNITION SERVICE - External Platform Integration
 *
 * Connects to the Ignition airdrop platform to check if tokens are part of
 * the rewards sharing ecosystem. Tokens can be:
 * - "ignition" type: Launched via IGNITION platform
 * - "robinhood" type: Fee-sharing partner tokens
 *
 * Benefits:
 * - Display reward sharing status on token pages
 * - Free community upgrade for Ignition-registered tokens
 */

const axios = require('axios');
const logger = require('./logger');
const config = require('../config/env');
const { getClient } = require('./redis');

// ============================================
// CONFIGURATION
// ============================================

const IGNITION_API_URL = config.IGNITION_API_URL || process.env.IGNITION_API_URL || null;
const IGNITION_TIMEOUT = 5000; // 5 second timeout
const CACHE_TTL = 300; // 5 minutes cache in Redis
const BATCH_SIZE = 50; // Max tokens per batch request

// In-memory cache for fast lookups (short TTL)
const memoryCache = new Map();
const MEMORY_CACHE_TTL = 60000; // 1 minute

// ============================================
// SINGLE TOKEN LOOKUP
// ============================================

/**
 * Check if a single token is registered in IGNITION
 * @param {string} mint - Token mint address
 * @returns {Object} Ignition registration data or null
 */
async function lookupToken(mint) {
    if (!IGNITION_API_URL) {
        return null; // Ignition integration not configured
    }

    if (!mint || typeof mint !== 'string' || mint.length < 32) {
        return null;
    }

    // Check memory cache first
    const memoryCached = memoryCache.get(mint);
    if (memoryCached && Date.now() - memoryCached.timestamp < MEMORY_CACHE_TTL) {
        return memoryCached.data;
    }

    // Check Redis cache
    const redis = getClient();
    if (redis) {
        try {
            const cached = await redis.get(`ignition:${mint}`);
            if (cached) {
                const data = JSON.parse(cached);
                memoryCache.set(mint, { data, timestamp: Date.now() });
                return data;
            }
        } catch (_e) { /* ignore cache errors */ }
    }

    // Fetch from Ignition API
    try {
        const response = await axios.get(
            `${IGNITION_API_URL}/token-lookup/${mint}`,
            { timeout: IGNITION_TIMEOUT }
        );

        const data = response.data;
        const result = normalizeIgnitionResponse(data);

        // Cache the result
        if (redis) {
            try {
                await redis.set(`ignition:${mint}`, JSON.stringify(result), 'EX', CACHE_TTL);
            } catch (_e) { /* ignore */ }
        }
        memoryCache.set(mint, { data: result, timestamp: Date.now() });

        return result;

    } catch (e) {
        if (e.response?.status === 404) {
            // Token not found - cache negative result
            const notFound = { registered: false, type: null, mint };
            if (redis) {
                try {
                    await redis.set(`ignition:${mint}`, JSON.stringify(notFound), 'EX', CACHE_TTL);
                } catch (_e) { /* ignore */ }
            }
            memoryCache.set(mint, { data: notFound, timestamp: Date.now() });
            return notFound;
        }

        if (e.response?.status === 429) {
            logger.warn('[Ignition] Rate limited - backing off');
            await new Promise(r => setTimeout(r, 5000));
        }

        logger.debug(`[Ignition] Lookup failed for ${mint.slice(0, 8)}: ${e.message}`);
        return null;
    }
}

// ============================================
// BATCH TOKEN LOOKUP
// ============================================

/**
 * Check multiple tokens at once (max 50 per request)
 * @param {string[]} mints - Array of token mint addresses
 * @returns {Map<string, Object>} Map of mint -> Ignition data
 */
async function lookupTokenBatch(mints) {
    if (!IGNITION_API_URL) {
        return new Map();
    }

    if (!mints || !Array.isArray(mints) || mints.length === 0) {
        return new Map();
    }

    const results = new Map();
    const uncachedMints = [];

    // Check caches first
    const redis = getClient();
    for (const mint of mints) {
        // Memory cache
        const memoryCached = memoryCache.get(mint);
        if (memoryCached && Date.now() - memoryCached.timestamp < MEMORY_CACHE_TTL) {
            results.set(mint, memoryCached.data);
            continue;
        }

        // Redis cache
        if (redis) {
            try {
                const cached = await redis.get(`ignition:${mint}`);
                if (cached) {
                    const data = JSON.parse(cached);
                    results.set(mint, data);
                    memoryCache.set(mint, { data, timestamp: Date.now() });
                    continue;
                }
            } catch (_e) { /* ignore */ }
        }

        uncachedMints.push(mint);
    }

    // Fetch uncached tokens in batches
    for (let i = 0; i < uncachedMints.length; i += BATCH_SIZE) {
        const batch = uncachedMints.slice(i, i + BATCH_SIZE);

        try {
            const response = await axios.get(
                `${IGNITION_API_URL}/token-lookup-batch`,
                {
                    params: { mints: batch.join(',') },
                    timeout: IGNITION_TIMEOUT * 2 // Longer timeout for batch
                }
            );

            const data = response.data;
            if (data && data.results) {
                for (const [mint, info] of Object.entries(data.results)) {
                    const normalized = normalizeIgnitionBatchItem(mint, info);
                    results.set(mint, normalized);

                    // Cache the result
                    if (redis) {
                        try {
                            await redis.set(`ignition:${mint}`, JSON.stringify(normalized), 'EX', CACHE_TTL);
                        } catch (_e) { /* ignore */ }
                    }
                    memoryCache.set(mint, { data: normalized, timestamp: Date.now() });
                }
            }

        } catch (e) {
            logger.warn(`[Ignition] Batch lookup failed: ${e.message}`);
            // Continue with next batch
        }

        // Small delay between batches
        if (i + BATCH_SIZE < uncachedMints.length) {
            await new Promise(r => setTimeout(r, 500));
        }
    }

    return results;
}

// ============================================
// RESPONSE NORMALIZATION
// ============================================

/**
 * Normalize single token response from Ignition API
 */
function normalizeIgnitionResponse(data) {
    if (!data || !data.registered) {
        return {
            registered: false,
            type: null
        };
    }

    const result = {
        registered: true,
        type: data.type, // 'ignition' or 'robinhood'
    };

    if (data.type === 'robinhood') {
        result.active = data.active !== false; // Default to true
        result.feeShareBps = data.token?.feeShareBps || 0;
        result.feeSharePercent = data.token?.feeSharePercent || '0';
    }

    // Include token metadata if available
    if (data.token) {
        result.tokenInfo = {
            ticker: data.token.ticker,
            name: data.token.name,
            image: data.token.image,
            marketCap: data.token.marketCap,
            volume24h: data.token.volume24h,
            registeredAt: data.token.registeredAt
        };
    }

    return result;
}

/**
 * Normalize batch response item
 */
function normalizeIgnitionBatchItem(mint, info) {
    if (!info || !info.registered) {
        return {
            registered: false,
            type: null,
            mint
        };
    }

    const result = {
        registered: true,
        type: info.type,
        mint
    };

    if (info.type === 'robinhood') {
        result.active = info.active !== false;
        result.feeSharePercent = info.feeSharePercent || '0';
    }

    if (info.ticker) {
        result.tokenInfo = {
            ticker: info.ticker,
            name: info.name,
            marketCap: info.marketCap,
            volume24h: info.volume24h
        };
    }

    return result;
}

// ============================================
// HELPER FUNCTIONS
// ============================================

/**
 * Check if a token is registered in Ignition (simple boolean check)
 * @param {string} mint - Token mint address
 * @returns {boolean}
 */
async function isIgnitionRegistered(mint) {
    const data = await lookupToken(mint);
    return data?.registered === true;
}

/**
 * Check if a token qualifies for free community upgrade
 * (Any token registered in Ignition, regardless of type)
 * @param {string} mint - Token mint address
 * @returns {boolean}
 */
async function qualifiesForFreeUpgrade(mint) {
    const data = await lookupToken(mint);
    return data?.registered === true;
}

/**
 * Get display-friendly Ignition status for token page
 * @param {string} mint - Token mint address
 * @returns {Object|null} Formatted status for frontend display
 */
async function getDisplayStatus(mint) {
    const data = await lookupToken(mint);

    if (!data || !data.registered) {
        return null;
    }

    if (data.type === 'ignition') {
        return {
            badge: 'IGNITION',
            badgeColor: '#00D1FF', // Cyan
            description: 'Launched via IGNITION Platform',
            rewardsSharing: true,
            rewardsPercent: '100' // Full rewards for platform tokens
        };
    }

    if (data.type === 'robinhood') {
        const isActive = data.active !== false;
        return {
            badge: 'ROBINHOOD',
            badgeColor: isActive ? '#10B981' : '#6B7280', // Green or gray
            description: isActive
                ? `Fee-sharing partner (${data.feeSharePercent}%)`
                : 'Fee-sharing partner (inactive)',
            rewardsSharing: isActive,
            rewardsPercent: data.feeSharePercent || '0',
            active: isActive
        };
    }

    return null;
}

/**
 * Invalidate cache for a specific token
 * @param {string} mint - Token mint address
 */
async function invalidateCache(mint) {
    memoryCache.delete(mint);
    const redis = getClient();
    if (redis) {
        try {
            await redis.del(`ignition:${mint}`);
        } catch (_e) { /* ignore */ }
    }
}

/**
 * Check if Ignition integration is configured
 * @returns {boolean}
 */
function isConfigured() {
    return !!IGNITION_API_URL;
}

// ============================================
// CACHE CLEANUP
// ============================================

// Periodic memory cache cleanup
setInterval(() => {
    const now = Date.now();
    const staleThreshold = 5 * 60 * 1000; // 5 minutes

    for (const [mint, cached] of memoryCache) {
        if (now - cached.timestamp > staleThreshold) {
            memoryCache.delete(mint);
        }
    }

    if (memoryCache.size > 0) {
        logger.debug(`[Ignition] Memory cache: ${memoryCache.size} entries`);
    }
}, 5 * 60 * 1000); // Run every 5 minutes

// ============================================
// EXPORTS
// ============================================

module.exports = {
    // Core lookups
    lookupToken,
    lookupTokenBatch,

    // Convenience functions
    isIgnitionRegistered,
    qualifiesForFreeUpgrade,
    getDisplayStatus,

    // Cache management
    invalidateCache,

    // Configuration check
    isConfigured
};
