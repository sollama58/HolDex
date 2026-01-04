/**
 * Helius Webhook Service
 * Manages webhook creation/deletion for real-time token transfer monitoring
 *
 * SECURITY NOTE: Helius Webhooks API requires API key in query string (not headers)
 * This is by design from Helius. RPC endpoints can use headers, but webhooks cannot.
 */
const crypto = require('crypto');
const config = require('../config/env');
const logger = require('./logger');

const HELIUS_API_URL = 'https://api.helius.xyz/v0';

// Mask API key for logging (show first 4 chars only)
function maskApiKey(key) {
    if (!key || key.length < 8) return '****';
    return key.slice(0, 4) + '...' + key.slice(-4);
}

// Build webhook URL with API key (required by Helius Webhooks API)
function getWebhookApiUrl(path = '') {
    if (!config.HELIUS_API_KEY) {
        throw new Error('HELIUS_API_KEY not configured');
    }
    return `${HELIUS_API_URL}/webhooks${path}?api-key=${config.HELIUS_API_KEY}`;
}

// SECURITY: Fetch with timeout to prevent hanging requests
const WEBHOOK_TIMEOUT = 15000; // 15 seconds
async function fetchWithTimeout(url, options = {}) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), WEBHOOK_TIMEOUT);
    try {
        const response = await fetch(url, { ...options, signal: controller.signal });
        return response;
    } finally {
        clearTimeout(timeout);
    }
}

/**
 * Create a webhook for monitoring token transfers
 * @param {string[]} mints - Array of token mint addresses (atomic creation)
 * @param {string} callbackUrl - URL to receive webhook events
 * @returns {Promise<{webhookID: string}>}
 */
async function createTokenWebhook(mints, callbackUrl) {
    // Ensure mints is always an array
    const mintArray = Array.isArray(mints) ? mints : [mints];

    if (mintArray.length === 0) {
        throw new Error('At least one mint address required');
    }

    logger.info(`[Webhook] Creating webhook for ${mintArray.length} tokens (key: ${maskApiKey(config.HELIUS_API_KEY)})`);

    const response = await fetchWithTimeout(getWebhookApiUrl(), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            webhookURL: callbackUrl,
            transactionTypes: ['TRANSFER'],
            accountAddresses: mintArray,  // All mints at once (atomic)
            webhookType: 'enhanced',
            encoding: 'jsonParsed'
        })
    });

    if (!response.ok) {
        const error = await response.text();
        logger.error(`[Webhook] Creation failed: ${error}`);
        throw new Error(`Helius webhook creation failed: ${error}`);
    }

    const data = await response.json();
    logger.info(`[Webhook] Created: ${data.webhookID} with ${mintArray.length} tokens`);
    return data;
}

/**
 * Add additional addresses to an existing webhook
 * @param {string} webhookId - Existing webhook ID
 * @param {string[]} mints - Array of mint addresses to add
 */
async function addToWebhook(webhookId, mints) {
    const mintArray = Array.isArray(mints) ? mints : [mints];

    logger.info(`[Webhook] Updating ${webhookId} with ${mintArray.length} addresses`);

    const response = await fetchWithTimeout(getWebhookApiUrl(`/${webhookId}`), {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            accountAddresses: mintArray
        })
    });

    if (!response.ok) {
        const error = await response.text();
        logger.error(`[Webhook] Update failed: ${error}`);
        throw new Error(`Helius webhook update failed: ${error}`);
    }

    return await response.json();
}

/**
 * Delete a webhook
 * @param {string} webhookId - Webhook ID to delete
 */
async function deleteWebhook(webhookId) {
    logger.info(`[Webhook] Deleting: ${webhookId}`);

    const response = await fetchWithTimeout(getWebhookApiUrl(`/${webhookId}`), {
        method: 'DELETE'
    });

    if (!response.ok) {
        const error = await response.text();
        logger.error(`[Webhook] Deletion failed: ${error}`);
        throw new Error(`Helius webhook deletion failed: ${error}`);
    }

    logger.info(`[Webhook] Deleted: ${webhookId}`);
    return true;
}

/**
 * List all webhooks for this API key
 * @returns {Promise<Array>}
 */
async function listWebhooks() {
    const response = await fetchWithTimeout(getWebhookApiUrl());

    if (!response.ok) {
        const error = await response.text();
        throw new Error(`Helius webhook list failed: ${error}`);
    }

    return await response.json();
}

/**
 * Verify webhook signature (CRITICAL for security)
 * Helius signs webhook payloads with HMAC-SHA256
 *
 * @param {string} payload - Raw request body (string)
 * @param {string} signature - X-Helius-Signature header
 * @param {string} secret - Webhook secret (from webhook creation)
 * @returns {boolean}
 */
function verifyWebhookSignature(payload, signature, secret) {
    if (!signature || !secret) {
        logger.warn('[Webhook] Missing signature or secret for verification');
        return false;
    }

    try {
        const expectedSig = crypto
            .createHmac('sha256', secret)
            .update(payload, 'utf8')
            .digest('hex');

        // Constant-time comparison to prevent timing attacks
        return crypto.timingSafeEqual(
            Buffer.from(signature, 'hex'),
            Buffer.from(expectedSig, 'hex')
        );
    } catch (e) {
        logger.error(`[Webhook] Signature verification error: ${e.message}`);
        return false;
    }
}

/**
 * Get or create a master webhook for all tracked tokens
 * This is more efficient than creating one webhook per token
 *
 * FIXED: Now creates webhook with ALL mints atomically (no race condition)
 *
 * @param {object} db - Database connection
 * @param {string} callbackUrl - Callback URL
 * @returns {Promise<string>} - Webhook ID
 */
async function getOrCreateMasterWebhook(db, callbackUrl) {
    // Check if we already have a master webhook
    const existing = await db.get('SELECT webhook_id FROM webhooks WHERE mint = $1', ['_master']);

    if (existing) {
        return existing.webhook_id;
    }

    // Get all verified tokens to add to webhook
    const tokens = await db.all('SELECT mint FROM tokens WHERE hasCommunityUpdate = TRUE');
    const mints = tokens.map(t => t.mint);

    if (mints.length === 0) {
        logger.warn('[Webhook] No verified tokens to create webhook for');
        return null;
    }

    // Create new master webhook with ALL mints atomically (no race condition)
    const webhook = await createTokenWebhook(mints, callbackUrl);

    // Save master webhook reference
    await db.run(
        'INSERT INTO webhooks (id, mint, webhook_id, created_at) VALUES ($1, $2, $3, $4)',
        ['_master', '_master', webhook.webhookID, Date.now()]
    );

    logger.info(`[Webhook] Master webhook created with ${mints.length} tokens`);
    return webhook.webhookID;
}

/**
 * Add a new token to the master webhook
 * @param {object} db - Database connection
 * @param {string} mint - Token mint to add
 */
async function addTokenToMasterWebhook(db, mint) {
    const master = await db.get('SELECT webhook_id FROM webhooks WHERE mint = $1', ['_master']);

    if (!master) {
        logger.warn('[Webhook] No master webhook exists, creating one...');
        const callbackUrl = config.WEBHOOK_URL || `${config.API_URL}/webhook/transfers`;
        await getOrCreateMasterWebhook(db, callbackUrl);
        return;
    }

    // Get all current tracked tokens + new one
    const tokens = await db.all('SELECT mint FROM tokens WHERE hasCommunityUpdate = TRUE');
    const mints = [...new Set([...tokens.map(t => t.mint), mint])]; // Dedupe

    // Update webhook with new token list
    await addToWebhook(master.webhook_id, mints);
    logger.info(`[Webhook] Added ${mint.slice(0, 8)}... to master webhook (total: ${mints.length})`);
}

module.exports = {
    createTokenWebhook,
    addToWebhook,
    deleteWebhook,
    listWebhooks,
    getOrCreateMasterWebhook,
    addTokenToMasterWebhook,
    verifyWebhookSignature  // Export for route handler
};
