/**
 * Data Signature Utility - "Don't Trust, Verify"
 *
 * Proof-of-History style verification for K-Score data.
 * Signs critical token metrics with HMAC-SHA256.
 *
 * Even if DB credentials are leaked, attackers cannot forge valid signatures
 * without the DATA_SIGNING_SECRET.
 *
 * Signed fields:
 * - mint: token identifier
 * - k_score: the K-Score value
 * - conviction_score: conviction analysis result
 * - last_k_score_update: timestamp (proof-of-history)
 */

const crypto = require('crypto');
const config = require('../config/env');
const logger = require('../services/logger');

// Secret key for signing - MUST be set in production
const SIGNING_SECRET = config.DATA_SIGNING_SECRET || process.env.DATA_SIGNING_SECRET;

if (!SIGNING_SECRET && process.env.NODE_ENV === 'production') {
    logger.error('CRITICAL: DATA_SIGNING_SECRET not set in production!');
}

/**
 * Create canonical string from token data for signing
 * Order matters - must be deterministic
 */
function canonicalize(data) {
    const { mint, k_score, conviction_score, last_k_score_update } = data;

    // Canonical format: mint|k_score|conviction|timestamp
    return `${mint}|${k_score || 0}|${conviction_score || 0}|${last_k_score_update || 0}`;
}

/**
 * Sign token data with HMAC-SHA256
 * @param {Object} data - Token data to sign
 * @returns {string} Base64-encoded signature
 */
function signData(data) {
    if (!SIGNING_SECRET) {
        // In dev mode without secret, return placeholder
        logger.debug('[Signature] No secret configured, using dev mode');
        return 'dev_unsigned';
    }

    const canonical = canonicalize(data);
    const hmac = crypto.createHmac('sha256', SIGNING_SECRET);
    hmac.update(canonical);
    return hmac.digest('base64');
}

/**
 * Verify token data signature
 * @param {Object} data - Token data to verify
 * @param {string} signature - Expected signature
 * @returns {Object} { valid: boolean, reason?: string }
 */
function verifySignature(data, signature) {
    if (!SIGNING_SECRET) {
        // Dev mode - accept anything
        return { valid: true, reason: 'dev_mode' };
    }

    if (!signature) {
        return { valid: false, reason: 'missing_signature' };
    }

    if (signature === 'dev_unsigned') {
        return { valid: false, reason: 'dev_signature_in_prod' };
    }

    const expected = signData(data);

    // Constant-time comparison to prevent timing attacks
    const sigBuffer = Buffer.from(signature, 'base64');
    const expectedBuffer = Buffer.from(expected, 'base64');

    if (sigBuffer.length !== expectedBuffer.length) {
        return { valid: false, reason: 'length_mismatch' };
    }

    const isValid = crypto.timingSafeEqual(sigBuffer, expectedBuffer);

    return {
        valid: isValid,
        reason: isValid ? 'verified' : 'signature_mismatch'
    };
}

/**
 * Create signed token data object
 * @param {Object} tokenData - Raw token data
 * @returns {Object} Token data with signature
 */
function signTokenData(tokenData) {
    const signature = signData(tokenData);
    return {
        ...tokenData,
        data_signature: signature
    };
}

/**
 * Verify token and return verification status
 * @param {Object} token - Token with signature
 * @returns {Object} { verified: boolean, token: Object, tampered: boolean }
 */
function verifyToken(token) {
    if (!token) {
        return { verified: false, token: null, tampered: false };
    }

    const { data_signature, ...data } = token;
    const verification = verifySignature(data, data_signature);

    return {
        verified: verification.valid,
        reason: verification.reason,
        token,
        tampered: !verification.valid && verification.reason === 'signature_mismatch'
    };
}

/**
 * Batch verify multiple tokens
 * @param {Array} tokens - Array of tokens to verify
 * @returns {Object} { valid: Array, tampered: Array, unsigned: Array }
 */
function batchVerify(tokens) {
    const result = {
        valid: [],
        tampered: [],
        unsigned: []
    };

    for (const token of tokens) {
        const { verified, reason, tampered } = verifyToken(token);

        if (verified) {
            result.valid.push(token);
        } else if (tampered) {
            result.tampered.push({ token, reason });
        } else {
            result.unsigned.push({ token, reason });
        }
    }

    return result;
}

module.exports = {
    signData,
    verifySignature,
    signTokenData,
    verifyToken,
    batchVerify,
    canonicalize
};
