/**
 * Data Verification Service - "Don't Trust, Verify"
 *
 * Verifies K-Score data integrity before serving to clients.
 * Detects tampering even if DB credentials are compromised.
 *
 * Modes:
 * - 'strict': Reject tampered data, return error
 * - 'warn': Log warning, return data with verification status
 * - 'off': Skip verification (dev only)
 */

const config = require('../config/env');
const logger = require('./logger');
const { verifyAllSignatures } = require('../utils/dataSignature');
const { getClient: getRedisClient } = require('./redis');

const VERIFY_MODE = config.VERIFY_DATA_MODE || 'strict';

// Recalculation queue key in Redis
const RECALC_QUEUE_KEY = 'kscore:recalc:queue';

/**
 * Add a token to the recalculation queue (for tampered tokens)
 * @param {string} mint - Token mint address
 * @param {string} reason - Reason for recalculation
 */
async function queueForRecalculation(mint, reason = 'tampered') {
    const redis = getRedisClient();
    if (!redis) {
        logger.warn(`[DataVerify] Cannot queue ${mint} - Redis not available`);
        return false;
    }

    try {
        // Add to sorted set with current timestamp as score
        await redis.zadd(RECALC_QUEUE_KEY, Date.now(), JSON.stringify({ mint, reason, queued: Date.now() }));
        logger.info(`[DataVerify] Queued for recalc: ${mint.slice(0, 8)}... (${reason})`);
        return true;
    } catch (e) {
        logger.error(`[DataVerify] Queue error: ${e.message}`);
        return false;
    }
}

/**
 * Get tokens that need recalculation
 * @param {number} limit - Max tokens to return
 * @returns {Array} List of { mint, reason, queued }
 */
async function getRecalcQueue(limit = 10) {
    const redis = getRedisClient();
    if (!redis) return [];

    try {
        const items = await redis.zrange(RECALC_QUEUE_KEY, 0, limit - 1);
        return items.map(item => JSON.parse(item));
    } catch (e) {
        logger.error(`[DataVerify] Get queue error: ${e.message}`);
        return [];
    }
}

/**
 * Remove a token from the recalculation queue
 * @param {string} mint - Token mint address
 */
async function removeFromRecalcQueue(mint) {
    const redis = getRedisClient();
    if (!redis) return;

    try {
        // Remove all entries for this mint
        const items = await redis.zrange(RECALC_QUEUE_KEY, 0, -1);
        for (const item of items) {
            const parsed = JSON.parse(item);
            if (parsed.mint === mint) {
                await redis.zrem(RECALC_QUEUE_KEY, item);
            }
        }
    } catch (e) {
        logger.error(`[DataVerify] Remove from queue error: ${e.message}`);
    }
}

/**
 * Verify a single token's data integrity (8-category system)
 * @param {Object} token - Token from database with signatures
 * @returns {Object} { token, verified, tampered, tamperedCategories, reason }
 */
function verifySingleToken(token) {
    if (!token) return { token: null, verified: false, tampered: false, reason: 'no_token' };

    // Skip verification if disabled
    if (VERIFY_MODE === 'off') {
        return { token, verified: true, tampered: false, reason: 'verification_disabled' };
    }

    const result = verifyAllSignatures(token);

    if (result.tampered.length > 0) {
        const tamperedStr = result.tampered.join(',');
        logger.warn(`[DataVerify] TAMPERED: ${token.mint?.slice(0, 8)} - categories: ${tamperedStr}`);

        // Queue for recalculation (fire-and-forget)
        queueForRecalculation(token.mint, `tampered:${tamperedStr}`).catch(() => {});

        if (VERIFY_MODE === 'strict') {
            // In strict mode, mark token as unverified
            return {
                token: {
                    ...token,
                    _integrity: 'tampered',
                    _verified: false,
                    _tamperedCategories: result.tampered
                },
                verified: false,
                tampered: true,
                tamperedCategories: result.tampered,
                reason: 'signature_mismatch'
            };
        }
    }

    if (result.unsigned.length > 0) {
        // Token hasn't been fully signed yet (migration period)
        return {
            token: {
                ...token,
                _integrity: 'unsigned',
                _verified: false,
                _unsignedCategories: result.unsigned
            },
            verified: false,
            tampered: false,
            reason: 'unsigned'
        };
    }

    return {
        token: {
            ...token,
            _integrity: result.chaosVerified ? 'chaos_verified' : 'verified',
            _verified: true
        },
        verified: result.valid,
        tampered: false,
        reason: result.chaosVerified ? 'chaos_verified' : 'verified'
    };
}

/**
 * Verify multiple tokens (8-category system)
 * @param {Array} tokens - Tokens from database with signatures
 * @returns {Object} { tokens, stats }
 */
function verifyTokens(tokens) {
    if (!tokens || tokens.length === 0) {
        return { tokens: [], stats: { total: 0, verified: 0, tampered: 0, unsigned: 0, chaosVerified: 0 } };
    }

    // Skip verification if disabled
    if (VERIFY_MODE === 'off') {
        return {
            tokens: tokens.map(t => ({ ...t, _integrity: 'unchecked', _verified: true })),
            stats: { total: tokens.length, verified: tokens.length, tampered: 0, unsigned: 0, chaosVerified: 0 }
        };
    }

    // Process each token through 8-category verification
    const verifiedTokens = [];
    const tamperedList = [];
    const unsignedList = [];
    let chaosVerifiedCount = 0;

    for (const token of tokens) {
        const result = verifyAllSignatures(token);

        if (result.tampered.length > 0) {
            tamperedList.push({ token, categories: result.tampered });
        } else if (result.unsigned.length > 0) {
            unsignedList.push({ token, categories: result.unsigned });
            verifiedTokens.push({
                ...token,
                _integrity: 'unsigned',
                _verified: false,
                _unsignedCategories: result.unsigned
            });
        } else {
            if (result.chaosVerified) chaosVerifiedCount++;
            verifiedTokens.push({
                ...token,
                _integrity: result.chaosVerified ? 'chaos_verified' : 'verified',
                _verified: true
            });
        }
    }

    const stats = {
        total: tokens.length,
        verified: tokens.length - tamperedList.length - unsignedList.length,
        tampered: tamperedList.length,
        unsigned: unsignedList.length,
        chaosVerified: chaosVerifiedCount
    };

    if (tamperedList.length > 0) {
        logger.warn(`[DataVerify] ALERT: ${tamperedList.length}/${tokens.length} tokens with invalid signatures`);
        for (const { token, categories } of tamperedList) {
            logger.warn(`[DataVerify] Tampered: ${token.mint?.slice(0, 8)} ${token.symbol} - categories: ${categories.join(',')}`);
            // Queue tampered tokens for recalculation (fire-and-forget)
            queueForRecalculation(token.mint, `tampered:${categories.join(',')}`).catch(() => {});
        }
    }

    // In warn mode, include tampered tokens with flag
    // In strict mode, exclude them
    if (VERIFY_MODE === 'warn') {
        for (const { token, categories } of tamperedList) {
            verifiedTokens.push({
                ...token,
                _integrity: 'tampered',
                _verified: false,
                _tamperedCategories: categories
            });
        }
    } else {
        // Strict mode: tampered tokens are excluded but logged
        for (const { token } of tamperedList) {
            logger.error(`[DataVerify] EXCLUDED tampered token: ${token.mint} ${token.symbol}`);
        }
    }

    return { tokens: verifiedTokens, stats };
}

/**
 * Get list of tampered token mints (for recalculation queue)
 * @param {Array} tokens - Tokens from database with signatures
 * @returns {Array} List of { mint, categories } for tampered tokens
 */
function getTamperedMints(tokens) {
    if (!tokens || tokens.length === 0) return [];
    if (VERIFY_MODE === 'off') return [];

    const tampered = [];
    for (const token of tokens) {
        const result = verifyAllSignatures(token);
        if (result.tampered.length > 0) {
            tampered.push({ mint: token.mint, categories: result.tampered });
        }
    }
    return tampered;
}

/**
 * Check if a token needs recalculation due to tampering
 * @param {Object} token - Token to check
 * @returns {boolean}
 */
function needsRecalculation(token) {
    if (!token) return false;
    if (VERIFY_MODE === 'off') return false;

    const result = verifyAllSignatures(token);
    return result.tampered.length > 0;
}

module.exports = {
    verifySingleToken,
    verifyTokens,
    getTamperedMints,
    needsRecalculation,
    queueForRecalculation,
    getRecalcQueue,
    removeFromRecalcQueue,
    VERIFY_MODE
};
