/**
 * Helius Webhook Service
 * Manages webhook creation/deletion for real-time token transfer monitoring
 */
const config = require('../config/env');
const logger = require('./logger');

const HELIUS_API_URL = 'https://api.helius.xyz/v0';

/**
 * Create a webhook for monitoring token transfers
 * @param {string} mint - Token mint address
 * @param {string} callbackUrl - URL to receive webhook events
 * @returns {Promise<{webhookID: string}>}
 */
async function createTokenWebhook(mint, callbackUrl) {
    if (!config.HELIUS_API_KEY) {
        throw new Error('HELIUS_API_KEY not configured');
    }

    const response = await fetch(`${HELIUS_API_URL}/webhooks?api-key=${config.HELIUS_API_KEY}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            webhookURL: callbackUrl,
            transactionTypes: ['TRANSFER'],
            accountAddresses: [mint],
            webhookType: 'enhanced',
            encoding: 'jsonParsed'
        })
    });

    if (!response.ok) {
        const error = await response.text();
        throw new Error(`Helius webhook creation failed: ${error}`);
    }

    const data = await response.json();
    logger.info(`✅ Webhook created for ${mint}: ${data.webhookID}`);
    return data;
}

/**
 * Add additional addresses to an existing webhook
 * @param {string} webhookId - Existing webhook ID
 * @param {string[]} mints - Array of mint addresses to add
 */
async function addToWebhook(webhookId, mints) {
    if (!config.HELIUS_API_KEY) {
        throw new Error('HELIUS_API_KEY not configured');
    }

    const response = await fetch(`${HELIUS_API_URL}/webhooks/${webhookId}?api-key=${config.HELIUS_API_KEY}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            accountAddresses: mints
        })
    });

    if (!response.ok) {
        const error = await response.text();
        throw new Error(`Helius webhook update failed: ${error}`);
    }

    return await response.json();
}

/**
 * Delete a webhook
 * @param {string} webhookId - Webhook ID to delete
 */
async function deleteWebhook(webhookId) {
    if (!config.HELIUS_API_KEY) {
        throw new Error('HELIUS_API_KEY not configured');
    }

    const response = await fetch(`${HELIUS_API_URL}/webhooks/${webhookId}?api-key=${config.HELIUS_API_KEY}`, {
        method: 'DELETE'
    });

    if (!response.ok) {
        const error = await response.text();
        throw new Error(`Helius webhook deletion failed: ${error}`);
    }

    logger.info(`🗑️ Webhook deleted: ${webhookId}`);
    return true;
}

/**
 * List all webhooks for this API key
 * @returns {Promise<Array>}
 */
async function listWebhooks() {
    if (!config.HELIUS_API_KEY) {
        throw new Error('HELIUS_API_KEY not configured');
    }

    const response = await fetch(`${HELIUS_API_URL}/webhooks?api-key=${config.HELIUS_API_KEY}`);

    if (!response.ok) {
        const error = await response.text();
        throw new Error(`Helius webhook list failed: ${error}`);
    }

    return await response.json();
}

/**
 * Get or create a master webhook for all tracked tokens
 * This is more efficient than creating one webhook per token
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
        logger.warn('⚠️ No verified tokens to create webhook for');
        return null;
    }

    // Create new master webhook
    const webhook = await createTokenWebhook(mints[0], callbackUrl);

    // Add remaining mints if any
    if (mints.length > 1) {
        await addToWebhook(webhook.webhookID, mints);
    }

    // Save master webhook reference
    await db.run(
        'INSERT INTO webhooks (id, mint, webhook_id, created_at) VALUES ($1, $2, $3, $4)',
        ['_master', '_master', webhook.webhookID, Date.now()]
    );

    logger.info(`✅ Master webhook created with ${mints.length} tokens`);
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
        logger.warn('⚠️ No master webhook exists, creating one...');
        const callbackUrl = config.WEBHOOK_URL || `${config.API_URL}/webhook/transfers`;
        await getOrCreateMasterWebhook(db, callbackUrl);
        return;
    }

    // Get all current tracked tokens + new one
    const tokens = await db.all('SELECT mint FROM tokens WHERE hasCommunityUpdate = TRUE');
    const mints = [...tokens.map(t => t.mint), mint];

    // Update webhook with new token list
    await addToWebhook(master.webhook_id, mints);
    logger.info(`✅ Added ${mint} to master webhook`);
}

module.exports = {
    createTokenWebhook,
    addToWebhook,
    deleteWebhook,
    listWebhooks,
    getOrCreateMasterWebhook,
    addTokenToMasterWebhook
};
