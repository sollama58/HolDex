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
const { verifyToken, batchVerify } = require('../utils/dataSignature');
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
 * Verify a single token's data integrity
 * @param {Object} token - Token from database
 * @returns {Object} { token, verified, tampered, reason }
 */
function verifySingleToken(token) {
    if (!token) return { token: null, verified: false, tampered: false, reason: 'no_token' };

    // Skip verification if disabled
    if (VERIFY_MODE === 'off') {
        return { token, verified: true, tampered: false, reason: 'verification_disabled' };
    }

    const result = verifyToken(token);

    if (result.tampered) {
        logger.warn(`[DataVerify] TAMPERED: ${token.mint?.slice(0, 8)} - signature mismatch`);

        // Queue for recalculation (fire-and-forget)
        queueForRecalculation(token.mint, 'signature_mismatch').catch(() => {});

        if (VERIFY_MODE === 'strict') {
            // In strict mode, mark token as unverified
            return {
                token: {
                    ...token,
                    _integrity: 'tampered',
                    _verified: false
                },
                verified: false,
                tampered: true,
                reason: 'signature_mismatch'
            };
        }
    }

    if (!result.verified && result.reason === 'missing_signature') {
        // Token hasn't been re-signed yet (migration period)
        return {
            token: {
                ...token,
                _integrity: 'unsigned',
                _verified: false
            },
            verified: false,
            tampered: false,
            reason: 'unsigned'
        };
    }

    return {
        token: {
            ...token,
            _integrity: 'verified',
            _verified: true
        },
        verified: result.verified,
        tampered: false,
        reason: result.reason
    };
}

/**
 * Verify multiple tokens
 * @param {Array} tokens - Tokens from database
 * @returns {Object} { tokens, stats }
 */
function verifyTokens(tokens) {
    if (!tokens || tokens.length === 0) {
        return { tokens: [], stats: { total: 0, verified: 0, tampered: 0, unsigned: 0 } };
    }

    // Skip verification if disabled
    if (VERIFY_MODE === 'off') {
        return {
            tokens: tokens.map(t => ({ ...t, _integrity: 'unchecked', _verified: true })),
            stats: { total: tokens.length, verified: tokens.length, tampered: 0, unsigned: 0 }
        };
    }

    const { valid, tampered, unsigned } = batchVerify(tokens);

    const stats = {
        total: tokens.length,
        verified: valid.length,
        tampered: tampered.length,
        unsigned: unsigned.length
    };

    if (tampered.length > 0) {
        logger.warn(`[DataVerify] ALERT: ${tampered.length}/${tokens.length} tokens with invalid signatures`);
        tampered.forEach(({ token, reason }) => {
            logger.warn(`[DataVerify] Tampered: ${token.mint?.slice(0, 8)} ${token.symbol} - ${reason}`);
            // Queue tampered tokens for recalculation (fire-and-forget)
            queueForRecalculation(token.mint, 'signature_mismatch').catch(() => {});
        });
    }

    // Build result array with integrity flags
    const verifiedTokens = [];

    for (const token of valid) {
        verifiedTokens.push({ ...token, _integrity: 'verified', _verified: true });
    }

    for (const { token } of unsigned) {
        verifiedTokens.push({ ...token, _integrity: 'unsigned', _verified: false });
    }

    // In strict mode, exclude tampered tokens
    // In warn mode, include them with flag
    if (VERIFY_MODE === 'warn') {
        for (const { token } of tampered) {
            verifiedTokens.push({ ...token, _integrity: 'tampered', _verified: false });
        }
    } else {
        // Strict mode: tampered tokens are excluded but logged
        for (const { token } of tampered) {
            logger.error(`[DataVerify] EXCLUDED tampered token: ${token.mint} ${token.symbol}`);
        }
    }

    return { tokens: verifiedTokens, stats };
}

/**
 * Get list of tampered token mints (for recalculation queue)
 * @param {Array} tokens - Tokens from database
 * @returns {Array} List of tampered mint addresses
 */
function getTamperedMints(tokens) {
    if (!tokens || tokens.length === 0) return [];
    if (VERIFY_MODE === 'off') return [];

    const { tampered } = batchVerify(tokens);
    return tampered.map(({ token }) => token.mint);
}

/**
 * Check if a token needs recalculation due to tampering
 * @param {Object} token - Token to check
 * @returns {boolean}
 */
function needsRecalculation(token) {
    if (!token) return false;
    if (VERIFY_MODE === 'off') return false;

    const { tampered } = verifyToken(token);
    return tampered;
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
