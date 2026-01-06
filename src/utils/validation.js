/**
 * Input Validation Utilities
 *
 * SECURITY: All external inputs must be validated before use.
 * Solana addresses are base58-encoded 32-byte public keys.
 */

const bs58 = require('bs58');

/**
 * Validate a Solana address (wallet or mint)
 * @param {string} address - The address to validate
 * @returns {boolean} - True if valid Solana address
 */
function isValidSolanaAddress(address) {
    if (!address || typeof address !== 'string') return false;

    // Solana addresses are 32-44 characters (base58 encoded 32 bytes)
    if (address.length < 32 || address.length > 44) return false;

    // Must only contain base58 characters (no 0, O, I, l)
    const base58Regex = /^[1-9A-HJ-NP-Za-km-z]+$/;
    if (!base58Regex.test(address)) return false;

    try {
        // Attempt to decode - must be exactly 32 bytes
        const decoded = bs58.decode(address);
        return decoded.length === 32;
    } catch (_e) {
        return false;
    }
}

/**
 * Validate transaction signature
 * @param {string} signature - The signature to validate
 * @returns {boolean} - True if valid signature format
 */
function isValidSignature(signature) {
    if (!signature || typeof signature !== 'string') return false;

    // Signatures are 64 bytes base58 encoded (87-88 chars)
    if (signature.length < 80 || signature.length > 90) return false;

    const base58Regex = /^[1-9A-HJ-NP-Za-km-z]+$/;
    return base58Regex.test(signature);
}

/**
 * Sanitize error messages for external responses
 * Removes sensitive information like file paths, stack traces, API keys
 */
function sanitizeError(error) {
    if (!error) return 'An error occurred';

    const message = error.message || String(error);

    // Remove file paths
    let sanitized = message.replace(/\/[^\s:]+\.(js|ts|json)/g, '[file]');

    // Remove potential API keys (long alphanumeric strings)
    sanitized = sanitized.replace(/[a-zA-Z0-9]{32,}/g, '[redacted]');

    // Remove stack traces
    sanitized = sanitized.replace(/at\s+.+\(.+\)/g, '');
    sanitized = sanitized.replace(/\s+at\s+.+:\d+:\d+/g, '');

    // Remove connection strings
    sanitized = sanitized.replace(/postgresql:\/\/[^\s]+/gi, '[db]');
    sanitized = sanitized.replace(/redis:\/\/[^\s]+/gi, '[redis]');

    // Trim and limit length
    sanitized = sanitized.trim().slice(0, 200);

    return sanitized || 'An error occurred';
}

/**
 * Validate integer within bounds
 */
function isValidInteger(value, min = 0, max = Number.MAX_SAFE_INTEGER) {
    const num = parseInt(value);
    return !isNaN(num) && num >= min && num <= max;
}

/**
 * Validate timestamp is reasonable (not in future, not too old)
 */
function isValidTimestamp(timestamp, maxAgeMs = 300000) { // 5 min default
    const now = Date.now();
    const ts = parseInt(timestamp);

    if (isNaN(ts)) return false;

    // Handle seconds vs milliseconds
    const normalizedTs = ts < 1e12 ? ts * 1000 : ts;

    // Must be in the past but not too old
    return normalizedTs <= now && normalizedTs >= (now - maxAgeMs);
}

module.exports = {
    isValidSolanaAddress,
    isValidSignature,
    sanitizeError,
    isValidInteger,
    isValidTimestamp
};
