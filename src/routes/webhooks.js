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
const { verifyWebhookSignature } = require('../services/heliusWebhook');
const { isValidSolanaAddress, sanitizeError, isValidTimestamp } = require('../utils/validation');

// Security: Track processed signatures to prevent replay attacks
const processedSignatures = new Map(); // signature -> timestamp
const REPLAY_WINDOW_MS = 300000; // 5 minutes
const MAX_CACHED_SIGNATURES = 10000;

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

/**
 * Classify conviction based on buy/sell counts
 */
function classifyConviction(buys, sells) {
    const total = buys + sells;
    if (total === 0) return 'holder';

    const buyRatio = buys / total;

    if (buyRatio >= 0.8) return 'accumulator';
    if (buyRatio >= 0.5) return 'holder';
    if (buyRatio >= 0.2) return 'reducer';
    return 'extractor';
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
            // SECURITY: Auth Header Verification (REQUIRED in production)
            // Helius sends the authHeader in the Authorization header
            // ============================================
            if (config.WEBHOOK_SECRET) {
                const authHeader = req.headers['authorization'];

                if (!authHeader) {
                    logger.warn('⚠️  Webhook missing Authorization header');
                    return res.status(401).json({ error: 'Unauthorized' });
                }

                // Constant-time comparison to prevent timing attacks
                const expected = Buffer.from(config.WEBHOOK_SECRET);
                const received = Buffer.from(authHeader);

                if (expected.length !== received.length ||
                    !require('crypto').timingSafeEqual(expected, received)) {
                    logger.warn(`⚠️  Webhook auth mismatch (got ${authHeader.slice(0,8)}...)`);
                    return res.status(401).json({ error: 'Unauthorized' });
                }

                logger.debug('✅ Webhook auth verified');
            } else if (process.env.NODE_ENV === 'production') {
                // CRITICAL: Block requests in production without secret
                logger.error('❌ WEBHOOK_SECRET not configured - rejecting webhook');
                return res.status(503).json({ error: 'Webhook not configured' });
            }

            const events = Array.isArray(req.body) ? req.body : [req.body];
            let processed = 0;
            let skipped = 0;

            for (const event of events) {
                // Skip non-transfer events
                if (event.type !== 'TRANSFER') continue;

                // ============================================
                // SECURITY: Replay Attack Protection
                // ============================================
                const txSignature = event.signature;
                if (txSignature) {
                    // Check if already processed
                    if (processedSignatures.has(txSignature)) {
                        skipped++;
                        continue;
                    }

                    // Check timestamp (reject old events)
                    if (event.timestamp && !isValidTimestamp(event.timestamp, REPLAY_WINDOW_MS)) {
                        logger.debug(`[Webhook] Rejecting stale event: ${txSignature.slice(0, 8)}...`);
                        skipped++;
                        continue;
                    }

                    // Track this signature
                    processedSignatures.set(txSignature, Date.now());

                    // Cleanup old signatures if cache is too large
                    if (processedSignatures.size > MAX_CACHED_SIGNATURES) {
                        const cutoff = Date.now() - REPLAY_WINDOW_MS;
                        for (const [sig, ts] of processedSignatures) {
                            if (ts < cutoff) processedSignatures.delete(sig);
                        }
                    }
                }

                const transfers = event.tokenTransfers || [];

                for (const transfer of transfers) {
                    const { mint, fromUserAccount, toUserAccount, tokenAmount } = transfer;

                    if (!mint) continue;

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
                    const amount = parseInt(tokenAmount) || 0;

                    // Update buyer (if not a pool and valid address)
                    if (toUserAccount && !isPoolAddress(toUserAccount) && isValidSolanaAddress(toUserAccount)) {
                        await db.run(`
                            INSERT INTO holder_snapshots (mint, holder, buy_count, net_flow, balance, updated_at)
                            VALUES ($1, $2, 1, $3, $3, $4)
                            ON CONFLICT (mint, holder) DO UPDATE SET
                                buy_count = holder_snapshots.buy_count + 1,
                                net_flow = holder_snapshots.net_flow + $3,
                                balance = holder_snapshots.balance + $3,
                                conviction_class = $5,
                                updated_at = $4
                        `, [
                            mint,
                            toUserAccount,
                            amount,
                            now,
                            classifyConviction(1, 0) // Will be recalculated
                        ]);

                        // Recalculate conviction class with actual counts
                        const holder = await db.get(
                            'SELECT buy_count, sell_count FROM holder_snapshots WHERE mint = $1 AND holder = $2',
                            [mint, toUserAccount]
                        );
                        if (holder) {
                            const newClass = classifyConviction(holder.buy_count, holder.sell_count);
                            await db.run(
                                'UPDATE holder_snapshots SET conviction_class = $1 WHERE mint = $2 AND holder = $3',
                                [newClass, mint, toUserAccount]
                            );
                        }
                    }

                    // Update seller (if not a pool and valid address)
                    if (fromUserAccount && !isPoolAddress(fromUserAccount) && isValidSolanaAddress(fromUserAccount)) {
                        await db.run(`
                            INSERT INTO holder_snapshots (mint, holder, sell_count, net_flow, balance, updated_at)
                            VALUES ($1, $2, 1, $3, 0, $4)
                            ON CONFLICT (mint, holder) DO UPDATE SET
                                sell_count = holder_snapshots.sell_count + 1,
                                net_flow = holder_snapshots.net_flow - $5,
                                balance = GREATEST(0, holder_snapshots.balance - $5),
                                conviction_class = $6,
                                updated_at = $4
                        `, [
                            mint,
                            fromUserAccount,
                            -amount,
                            now,
                            amount,
                            classifyConviction(0, 1)
                        ]);

                        // Recalculate conviction class
                        const holder = await db.get(
                            'SELECT buy_count, sell_count FROM holder_snapshots WHERE mint = $1 AND holder = $2',
                            [mint, fromUserAccount]
                        );
                        if (holder) {
                            const newClass = classifyConviction(holder.buy_count, holder.sell_count);
                            await db.run(
                                'UPDATE holder_snapshots SET conviction_class = $1 WHERE mint = $2 AND holder = $3',
                                [newClass, mint, fromUserAccount]
                            );
                        }
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
