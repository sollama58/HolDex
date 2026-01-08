/**
 * NEW TOKEN DISCOVERY WEBHOOK
 *
 * Helius webhook pour détecter les nouveaux tokens.
 * Remplace le listener WebSocket coûteux.
 *
 * Architecture:
 *   Helius filtre CREATE_POOL + TOKEN_MINT → POST /webhook/new-tokens
 *   Coût: 1 credit par VRAI nouveau token (pas par transaction)
 *
 * Programmes surveillés:
 *   - Raydium AMM V4: 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8
 *   - Pump.fun: 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
 *
 * φ Design: Module indépendant, drop-in ready
 *
 * @module newTokenWebhook
 */

'use strict';

const crypto = require('crypto');
const config = require('../config/env');
const logger = require('./logger');
const { getClient: getRedis } = require('./redis');

const HELIUS_API_URL = 'https://api.helius.xyz/v0';
const WEBHOOK_TIMEOUT = 15000;

// Programmes à surveiller
const PROGRAMS = {
    RAYDIUM_V4: '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
    PUMP_FUN: '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
};

// Types de transactions pour nouveaux tokens
const NEW_TOKEN_TX_TYPES = [
    'CREATE_POOL',      // Raydium pool creation
    'TOKEN_MINT',       // SPL token mint
    'SWAP',             // First swap (Pump.fun launch)
];

// Adresses système à ignorer
const IGNORED_MINTS = new Set([
    'So11111111111111111111111111111111111111112',  // wSOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', // Token Program
    '11111111111111111111111111111111',              // System Program
]);

// ═══════════════════════════════════════════════════════════════
// HELIUS API
// ═══════════════════════════════════════════════════════════════

function getWebhookApiUrl(path = '') {
    if (!config.HELIUS_API_KEY) {
        throw new Error('HELIUS_API_KEY not configured');
    }
    return `${HELIUS_API_URL}/webhooks${path}?api-key=${config.HELIUS_API_KEY}`;
}

async function fetchWithTimeout(url, options = {}) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), WEBHOOK_TIMEOUT);
    try {
        return await fetch(url, { ...options, signal: controller.signal });
    } finally {
        clearTimeout(timeout);
    }
}

/**
 * Créer le webhook pour nouveaux tokens
 *
 * @param {string} callbackUrl - URL de callback (ex: https://holdex.xyz/webhook/new-tokens)
 * @returns {Promise<{webhookID: string}>}
 */
async function createNewTokenWebhook(callbackUrl) {
    logger.info('[NewTokenWebhook] Creating webhook for new token discovery...');

    const response = await fetchWithTimeout(getWebhookApiUrl(), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            webhookURL: callbackUrl,
            transactionTypes: NEW_TOKEN_TX_TYPES,
            accountAddresses: [
                PROGRAMS.RAYDIUM_V4,
                PROGRAMS.PUMP_FUN
            ],
            webhookType: 'enhanced',
            encoding: 'jsonParsed'
        })
    });

    if (!response.ok) {
        const error = await response.text();
        logger.error(`[NewTokenWebhook] Creation failed: ${error}`);
        throw new Error(`Webhook creation failed: ${error}`);
    }

    const data = await response.json();
    logger.info(`[NewTokenWebhook] Created: ${data.webhookID}`);
    logger.info(`[NewTokenWebhook] Monitoring: Raydium V4 + Pump.fun`);
    logger.info(`[NewTokenWebhook] Transaction types: ${NEW_TOKEN_TX_TYPES.join(', ')}`);

    return data;
}

/**
 * Lister les webhooks existants
 */
async function listWebhooks() {
    const response = await fetchWithTimeout(getWebhookApiUrl());
    if (!response.ok) {
        throw new Error(`List webhooks failed: ${await response.text()}`);
    }
    return await response.json();
}

/**
 * Supprimer un webhook
 */
async function deleteWebhook(webhookId) {
    const response = await fetchWithTimeout(getWebhookApiUrl(`/${webhookId}`), {
        method: 'DELETE'
    });
    if (!response.ok) {
        throw new Error(`Delete webhook failed: ${await response.text()}`);
    }
    logger.info(`[NewTokenWebhook] Deleted: ${webhookId}`);
    return true;
}

/**
 * Trouver ou créer le webhook new-tokens
 */
async function getOrCreateNewTokenWebhook(db, callbackUrl) {
    // Vérifier si existe déjà
    const existing = await db.get(
        "SELECT webhook_id FROM webhooks WHERE mint = $1",
        ['_new_tokens']
    );

    if (existing) {
        logger.info(`[NewTokenWebhook] Using existing: ${existing.webhook_id}`);
        return existing.webhook_id;
    }

    // Créer nouveau
    const webhook = await createNewTokenWebhook(callbackUrl);

    // Sauvegarder référence
    await db.run(`
        INSERT INTO webhooks (id, mint, webhook_id, created_at)
        VALUES ($1, $2, $3, $4)
    `, ['_new_tokens', '_new_tokens', webhook.webhookID, Date.now()]);

    return webhook.webhookID;
}

// ═══════════════════════════════════════════════════════════════
// EVENT PROCESSING
// ═══════════════════════════════════════════════════════════════

/**
 * Extraire les mints candidats d'un événement Helius
 */
function extractMintsFromEvent(event) {
    const mints = new Set();

    // Token transfers contiennent les mints
    if (event.tokenTransfers) {
        event.tokenTransfers.forEach(t => {
            if (t.mint && !IGNORED_MINTS.has(t.mint)) {
                mints.add(t.mint);
            }
        });
    }

    // Account data peut contenir des mints
    if (event.accountData) {
        event.accountData.forEach(acc => {
            if (acc.tokenBalanceChanges) {
                acc.tokenBalanceChanges.forEach(change => {
                    if (change.mint && !IGNORED_MINTS.has(change.mint)) {
                        mints.add(change.mint);
                    }
                });
            }
        });
    }

    // Instructions peuvent contenir des mints
    if (event.instructions) {
        event.instructions.forEach(ix => {
            // Raydium initialize
            if (ix.programId === PROGRAMS.RAYDIUM_V4) {
                ix.accounts?.forEach(acc => {
                    if (acc && !IGNORED_MINTS.has(acc) && acc.length >= 32 && acc.length <= 44) {
                        mints.add(acc);
                    }
                });
            }
        });
    }

    return Array.from(mints);
}

/**
 * Déterminer la source (Raydium ou Pump.fun)
 */
function detectSource(event) {
    const programIds = new Set();

    if (event.instructions) {
        event.instructions.forEach(ix => {
            programIds.add(ix.programId);
        });
    }

    if (programIds.has(PROGRAMS.RAYDIUM_V4)) return 'Raydium';
    if (programIds.has(PROGRAMS.PUMP_FUN)) return 'Pump.fun';
    return 'Unknown';
}

/**
 * Vérifier signature webhook (sécurité)
 */
function verifySignature(payload, signature, secret) {
    if (!signature || !secret) return false;

    try {
        const expected = crypto
            .createHmac('sha256', secret)
            .update(payload, 'utf8')
            .digest('hex');

        return crypto.timingSafeEqual(
            Buffer.from(signature, 'hex'),
            Buffer.from(expected, 'hex')
        );
    } catch (_e) {
        return false;
    }
}

/**
 * Check replay attack (Redis)
 */
async function isReplayAttack(signature) {
    const redis = getRedis();
    if (!redis) return false;

    const key = `newtoken:sig:${signature}`;
    const isNew = await redis.set(key, '1', 'EX', 300, 'NX');
    return !isNew; // true si déjà vu
}

// ═══════════════════════════════════════════════════════════════
// STATS
// ═══════════════════════════════════════════════════════════════

const stats = {
    eventsReceived: 0,
    tokensDiscovered: 0,
    duplicatesSkipped: 0,
    errors: 0,
    lastEventTime: 0
};

function getStats() {
    return { ...stats };
}

function resetStats() {
    stats.eventsReceived = 0;
    stats.tokensDiscovered = 0;
    stats.duplicatesSkipped = 0;
    stats.errors = 0;
}

// ═══════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════

module.exports = {
    // Webhook management
    createNewTokenWebhook,
    getOrCreateNewTokenWebhook,
    listWebhooks,
    deleteWebhook,

    // Event processing
    extractMintsFromEvent,
    detectSource,
    verifySignature,
    isReplayAttack,

    // Stats
    getStats,
    resetStats,

    // Constants
    PROGRAMS,
    NEW_TOKEN_TX_TYPES,
    IGNORED_MINTS
};
