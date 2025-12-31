/**
 * API Key Hashing Utility
 * Uses SHA256 for fast, secure hashing of high-entropy API keys
 */
const crypto = require('crypto');

/**
 * Hash an API key using SHA256
 * @param {string} key - The plaintext API key
 * @returns {string} - The hashed key (hex)
 */
function hashApiKey(key) {
    return crypto.createHash('sha256').update(key).digest('hex');
}

/**
 * Mask an API key for display (show first 4 and last 4 chars)
 * @param {string} key - The plaintext API key
 * @returns {string} - Masked key like "hx_a1b2...x9y0"
 */
function maskApiKey(key) {
    if (!key || key.length < 12) return '****';
    return key.substring(0, 7) + '...' + key.substring(key.length - 4);
}

module.exports = { hashApiKey, maskApiKey };
