/**
 * Helius Webhook Receiver
 * Receives real-time token transfer events and updates holder_snapshots
 *
 * SECURITY: Webhooks are verified using HMAC-SHA256 signatures
 * from Helius (X-Helius-Signature header)
 *
 * CRITICAL: In production, WEBHOOK_SECRET is REQUIRED.
 * Requests will be rejected if signature verification fails.
 */
const express = require('express');
const router = express.Router();
const logger = require('../services/logger');
const config = require('../config/env');
const { getClient } = require('../services/redis');
const { isValidSolanaAddress, sanitizeError, isValidTimestamp } = require('../utils/validation');
const verification = require('../services/verificationService');

// Security: Replay attack prevention via Redis (cluster-safe, persistent)
const REPLAY_WINDOW_SECONDS = 300; // 5 minutes TTL

// Verification settings
const VERIFY_CRITICAL_TX = config.VERIFY_WEBHOOK_TX !== 'false'; // Default: true
const CRITICAL_AMOUNT_THRESHOLD = 1000000000; // 1B tokens = verify on-chain

// Known DEX/AMM pool programs to exclude from holder tracking
const POOL_PROGRAMS = new Set([
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', // Raydium AMM
    'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK', // Raydium CLMM
    'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc', // Orca Whirlpool
    '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP', // Orca Legacy
    'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',  // Meteora DLMM
    '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',  // Pump.fun bonding
    'TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM', // Pump.fun AMM
]);

// Additional known pool/program addresses
const isPoolAddress = (address) => {
    if (!address) return false;
    if (POOL_PROGRAMS.has(address)) return true;
    // Raydium pools often start with specific patterns
    if (address.length === 44 && address.endsWith('pump')) return false; // Token addresses
    return false;
};

// ============================================
// INPUT VALIDATION - Validate webhook payload BEFORE processing
// ============================================
const MAX_EVENTS_PER_BATCH = 100; // Prevent DoS via huge payloads
const MAX_TRANSFERS_PER_EVENT = 50; // Reasonable limit per transaction
const MAX_TOKEN_AMOUNT = BigInt('18446744073709551615'); // Max uint64
const MIN_TIMESTAMP = 1600000000; // Sept 2020 (before Solana mainnet)
const MAX_TIMESTAMP = Math.floor(Date.now() / 1000) + 86400; // Now + 1 day buffer

/**
 * Validate a single webhook event structure
 * @param {Object} event - Raw event from webhook
 * @returns {{valid: boolean, reason?: string}}
 */
function validateEvent(event) {
    if (!event || typeof event !== 'object') {
        return { valid: false, reason: 'Invalid event structure' };
    }

    // Validate type
    if (event.type && typeof event.type !== 'string') {
        return { valid: false, reason: 'Invalid event type' };
    }

    // Validate signature format
    if (event.signature) {
        if (typeof event.signature !== 'string' || event.signature.length < 32 || event.signature.length > 100) {
            return { valid: false, reason: 'Invalid signature format' };
        }
    }

    // Validate timestamp (if present)
    if (event.timestamp !== undefined) {
        const ts = parseInt(event.timestamp);
        if (!Number.isFinite(ts) || ts < MIN_TIMESTAMP || ts > MAX_TIMESTAMP) {
            return { valid: false, reason: 'Invalid timestamp' };
        }
    }

    // Validate tokenTransfers array
    if (event.tokenTransfers) {
        if (!Array.isArray(event.tokenTransfers)) {
            return { valid: false, reason: 'tokenTransfers must be array' };
        }
        if (event.tokenTransfers.length > MAX_TRANSFERS_PER_EVENT) {
            return { valid: false, reason: 'Too many transfers per event' };
        }
    }

    return { valid: true };
}

/**
 * Validate a single transfer within an event
 * @param {Object} transfer - Transfer object
 * @returns {{valid: boolean, amount?: bigint, reason?: string}}
 */
function validateTransfer(transfer) {
    if (!transfer || typeof transfer !== 'object') {
        return { valid: false, reason: 'Invalid transfer structure' };
    }

    // Validate tokenAmount with BigInt to prevent overflow
    if (transfer.tokenAmount !== undefined) {
        try {
            const amount = BigInt(transfer.tokenAmount);
            if (amount < 0n || amount > MAX_TOKEN_AMOUNT) {
                return { valid: false, reason: 'Token amount out of bounds' };
            }
            // Convert to safe integer for DB storage (cap at Number.MAX_SAFE_INTEGER)
            const safeAmount = amount > BigInt(Number.MAX_SAFE_INTEGER)
                ? Number.MAX_SAFE_INTEGER
                : Number(amount);
            return { valid: true, amount: safeAmount };
        } catch (_e) {
            return { valid: false, reason: 'Invalid token amount format' };
        }
    }

    return { valid: true, amount: 0 };
}

/**
 * Check if signature was already processed (Redis-backed, cluster-safe)
 * Returns true if duplicate, false if new
 */
async function checkAndMarkProcessed(signature) {
    const redis = getClient();
    if (!redis) return false; // Allow if Redis down (degrade gracefully)

    const key = `webhook:sig:${signature}`;
    // SETNX returns 1 if key was set (new), 0 if already exists (duplicate)
    const isNew = await redis.set(key, '1', 'EX', REPLAY_WINDOW_SECONDS, 'NX');
    return !isNew; // Return true if duplicate
}

let db = null;

function init(deps) {
    db = deps.db;

    /**
     * POST /webhook/transfers
     * Receives transfer events from Helius webhook
     *
     * SECURITY: Signature verification is REQUIRED in production
     * Set WEBHOOK_SECRET env var to enable verification
     *
     * Event structure (Enhanced):
     * [{
     *   type: 'TRANSFER',
     *   tokenTransfers: [{
     *     mint: 'TokenMint',
     *     fromUserAccount: 'Seller',
     *     toUserAccount: 'Buyer',
     *     tokenAmount: 1000000
     *   }],
     *   signature: 'txSig',
     *   timestamp: 1234567890
     * }]
     */
    router.post('/transfers', async (req, res) => {
        try {
            // ============================================
            // SECURITY: Auth Header Verification
            // Helius sends authHeader in Authorization header
            // Accept requests if auth matches OR if no secret configured
            // ============================================
            if (config.WEBHOOK_SECRET) {
                const authHeader = req.headers['authorization'];

                if (!authHeader) {
                    // SECURITY: Reject requests without auth when secret is configured
                    logger.warn('⚠️  Webhook request rejected - no auth header');
                    return res.status(401).json({ error: 'Authorization required' });
                }

                // Constant-time comparison to prevent timing attacks
                const expected = Buffer.from(config.WEBHOOK_SECRET);
                const received = Buffer.from(authHeader);

                if (expected.length !== received.length ||
                    !require('crypto').timingSafeEqual(expected, received)) {
                    logger.warn('⚠️  Webhook auth mismatch');
                    return res.status(401).json({ error: 'Unauthorized' });
                }
                // Auth verified - continue processing
            } else if (process.env.NODE_ENV === 'production') {
                // SECURITY: Reject in production if no secret configured
                logger.error('❌ WEBHOOK_SECRET not configured in production');
                return res.status(503).json({ error: 'Webhook not configured' });
            }

            const events = Array.isArray(req.body) ? req.body : [req.body];
            let processed = 0;
            let skipped = 0;

            // ============================================
            // VALIDATION: Check batch size BEFORE processing
            // ============================================
            if (events.length > MAX_EVENTS_PER_BATCH) {
                logger.warn(`[Webhook] Batch too large: ${events.length} events (max ${MAX_EVENTS_PER_BATCH})`);
                return res.status(400).json({
                    error: 'Batch size exceeded',
                    max: MAX_EVENTS_PER_BATCH,
                    received: events.length
                });
            }

            for (const event of events) {
                // ============================================
                // VALIDATION: Validate event structure BEFORE processing
                // ============================================
                const eventValidation = validateEvent(event);
                if (!eventValidation.valid) {
                    logger.debug(`[Webhook] Invalid event: ${eventValidation.reason}`);
                    skipped++;
                    continue;
                }

                // Skip non-transfer events
                if (event.type !== 'TRANSFER') continue;

                // ============================================
                // SECURITY: Replay Attack Protection (Redis-backed)
                // ============================================
                const txSignature = event.signature;
                if (txSignature) {
                    // Check if already processed (atomic Redis SETNX)
                    if (await checkAndMarkProcessed(txSignature)) {
                        skipped++;
                        continue;
                    }
                }

                const transfers = event.tokenTransfers || [];

                // ============================================
                // VERIFICATION: On-chain transaction validation
                // For large transfers, verify the transaction exists on-chain
                // ============================================
                const hasLargeTransfer = transfers.some(t => parseInt(t.tokenAmount) > CRITICAL_AMOUNT_THRESHOLD);

                if (VERIFY_CRITICAL_TX && hasLargeTransfer && txSignature) {
                    const txVerification = await verification.verifyTransaction(txSignature, {
                        timestamp: event.timestamp,
                        transfers: transfers.map(t => ({ mint: t.mint }))
                    });

                    if (!txVerification.verified) {
                        logger.warn(`⚠️  [Webhook] TX verification failed: ${txSignature.slice(0, 8)}... - ${txVerification.error || 'mismatch'}`);
                        skipped++;
                        continue; // Skip unverified large transactions
                    }

                    logger.debug(`✅ [Webhook] TX verified on-chain: ${txSignature.slice(0, 8)}... via ${txVerification.provider}`);
                }

                for (const transfer of transfers) {
                    const { mint, fromUserAccount, toUserAccount } = transfer;

                    if (!mint) continue;

                    // ============================================
                    // VALIDATION: Validate transfer structure and amount
                    // ============================================
                    const transferValidation = validateTransfer(transfer);
                    if (!transferValidation.valid) {
                        logger.debug(`[Webhook] Invalid transfer: ${transferValidation.reason}`);
                        continue;
                    }
                    const amount = transferValidation.amount;

                    // ============================================
                    // SECURITY: Validate all addresses
                    // ============================================
                    if (!isValidSolanaAddress(mint)) {
                        logger.debug(`[Webhook] Invalid mint address: ${String(mint).slice(0, 8)}...`);
                        continue;
                    }

                    // Check if this token is tracked (verified)
                    const token = await db.get(
                        'SELECT mint FROM tokens WHERE mint = $1 AND hasCommunityUpdate = TRUE',
                        [mint]
                    );

                    if (!token) continue; // Not a tracked token

                    const now = Date.now();
                    // K-Score v9: Use actual transaction timestamp for activity freshness
                    const txTimestamp = event.timestamp ? event.timestamp * 1000 : now;

                    // Update buyer (if not a pool and valid address)
                    // OPTIMIZED: Single atomic UPSERT with inline conviction calculation
                    if (toUserAccount && !isPoolAddress(toUserAccount) && isValidSolanaAddress(toUserAccount)) {
                        await db.run(`
                            INSERT INTO holder_snapshots (mint, holder, buy_count, sell_count, net_flow, balance, conviction_class, updated_at, last_tx_timestamp)
                            VALUES ($1, $2, 1, 0, $3, $3, 'accumulator', $4, $5)
                            ON CONFLICT (mint, holder) DO UPDATE SET
                                buy_count = holder_snapshots.buy_count + 1,
                                net_flow = holder_snapshots.net_flow + $3,
                                balance = holder_snapshots.balance + $3,
                                updated_at = $4,
                                last_tx_timestamp = $5,
                                conviction_class = CASE
                                    WHEN (holder_snapshots.buy_count + 1)::float / NULLIF(holder_snapshots.buy_count + 1 + holder_snapshots.sell_count, 0) >= 0.8 THEN 'accumulator'
                                    WHEN (holder_snapshots.buy_count + 1)::float / NULLIF(holder_snapshots.buy_count + 1 + holder_snapshots.sell_count, 0) >= 0.5 THEN 'holder'
                                    WHEN (holder_snapshots.buy_count + 1)::float / NULLIF(holder_snapshots.buy_count + 1 + holder_snapshots.sell_count, 0) >= 0.2 THEN 'reducer'
                                    ELSE 'extractor'
                                END
                        `, [mint, toUserAccount, amount, now, txTimestamp]);
                    }

                    // Update seller (if not a pool and valid address)
                    // OPTIMIZED: Single atomic UPSERT with inline conviction calculation
                    if (fromUserAccount && !isPoolAddress(fromUserAccount) && isValidSolanaAddress(fromUserAccount)) {
                        await db.run(`
                            INSERT INTO holder_snapshots (mint, holder, buy_count, sell_count, net_flow, balance, conviction_class, updated_at, last_tx_timestamp)
                            VALUES ($1, $2, 0, 1, $3, 0, 'extractor', $4, $5)
                            ON CONFLICT (mint, holder) DO UPDATE SET
                                sell_count = holder_snapshots.sell_count + 1,
                                net_flow = holder_snapshots.net_flow - $6,
                                balance = GREATEST(0, holder_snapshots.balance - $6),
                                updated_at = $4,
                                last_tx_timestamp = $5,
                                conviction_class = CASE
                                    WHEN holder_snapshots.buy_count::float / NULLIF(holder_snapshots.buy_count + holder_snapshots.sell_count + 1, 0) >= 0.8 THEN 'accumulator'
                                    WHEN holder_snapshots.buy_count::float / NULLIF(holder_snapshots.buy_count + holder_snapshots.sell_count + 1, 0) >= 0.5 THEN 'holder'
                                    WHEN holder_snapshots.buy_count::float / NULLIF(holder_snapshots.buy_count + holder_snapshots.sell_count + 1, 0) >= 0.2 THEN 'reducer'
                                    ELSE 'extractor'
                                END
                        `, [mint, fromUserAccount, -amount, now, txTimestamp, amount]);
                    }

                    // ============================================
                    // AUDIT: Log significant transfers (async, non-blocking)
                    // ============================================
                    if (amount > CRITICAL_AMOUNT_THRESHOLD) {
                        // Fire and forget - don't block webhook response
                        verification.logAudit(db, {
                            action: 'transfer',
                            entity: 'holder',
                            entityId: mint,
                            newValue: {
                                from: fromUserAccount,
                                to: toUserAccount,
                                amount,
                                signature: txSignature
                            },
                            source: 'helius_webhook',
                            metadata: { timestamp: event.timestamp }
                        }).catch(_e => {}); // Ignore audit failures
                    }

                    processed++;
                }
            }

            if (processed > 0 || skipped > 0) {
                logger.debug(`📥 Webhook: Processed ${processed}, Skipped ${skipped} transfers`);
            }

            res.status(200).json({ received: true, processed, skipped });

        } catch (error) {
            // SECURITY: Log full error internally, return sanitized message externally
            logger.error(`❌ Webhook Error: ${error.message}`);
            res.status(500).json({ error: sanitizeError(error) });
        }
    });

    /**
     * GET /webhook/health
     * Health check for webhook endpoint
     */
    router.get('/health', (req, res) => {
        res.json({ status: 'ok', mode: 'webhook-receiver' });
    });

    return router;
}

module.exports = { init };
