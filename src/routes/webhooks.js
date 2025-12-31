/**
 * Helius Webhook Receiver
 * Receives real-time token transfer events and updates holder_snapshots
 *
 * SECURITY: Webhooks are verified using HMAC-SHA256 signatures
 * from Helius (X-Helius-Signature header)
 */
const express = require('express');
const router = express.Router();
const logger = require('../services/logger');
const config = require('../config/env');
const { verifyWebhookSignature } = require('../services/heliusWebhook');

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
            // SECURITY: Verify Helius signature if WEBHOOK_SECRET is configured
            // Note: req.rawBody must be set by middleware (see index.js)
            if (config.WEBHOOK_SECRET) {
                const signature = req.headers['x-helius-signature'];
                const rawBody = req.rawBody || JSON.stringify(req.body);

                if (!verifyWebhookSignature(rawBody, signature, config.WEBHOOK_SECRET)) {
                    logger.warn('⚠️  Webhook signature verification FAILED');
                    return res.status(401).json({ error: 'Invalid signature' });
                }
            } else if (process.env.NODE_ENV === 'production') {
                // Warn in production if no secret configured
                logger.warn('⚠️  WEBHOOK_SECRET not set - signature verification disabled');
            }

            const events = Array.isArray(req.body) ? req.body : [req.body];
            let processed = 0;

            for (const event of events) {
                // Skip non-transfer events
                if (event.type !== 'TRANSFER') continue;

                const transfers = event.tokenTransfers || [];

                for (const transfer of transfers) {
                    const { mint, fromUserAccount, toUserAccount, tokenAmount } = transfer;

                    if (!mint) continue;

                    // Check if this token is tracked (verified)
                    const token = await db.get(
                        'SELECT mint FROM tokens WHERE mint = $1 AND hasCommunityUpdate = TRUE',
                        [mint]
                    );

                    if (!token) continue; // Not a tracked token

                    const now = Date.now();
                    const amount = parseInt(tokenAmount) || 0;

                    // Update buyer (if not a pool)
                    if (toUserAccount && !isPoolAddress(toUserAccount)) {
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

                    // Update seller (if not a pool)
                    if (fromUserAccount && !isPoolAddress(fromUserAccount)) {
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

            if (processed > 0) {
                logger.debug(`📥 Webhook: Processed ${processed} transfers`);
            }

            res.status(200).json({ received: true, processed });

        } catch (error) {
            logger.error(`❌ Webhook Error: ${error.message}`);
            res.status(500).json({ error: error.message });
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
