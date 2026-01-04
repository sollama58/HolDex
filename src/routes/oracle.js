/**
 * ORACLE ROUTES
 *
 * Public API endpoints for the $ASDFASDFA ecosystem.
 * Used by GASdf and external services for:
 * - K-Score lookups (token acceptance)
 * - E-Score lookups (discount calculation)
 * - Operation costs (efficiency floor)
 *
 * These endpoints are designed to be:
 * - Fast (heavily cached)
 * - Reliable (graceful degradation)
 * - Secure (rate limited, validated)
 */

'use strict';

const express = require('express');
const crypto = require('crypto');
const rateLimit = require('express-rate-limit');
const router = express.Router();
const { getHarmonyEngine, harmony } = require('../services/harmonyEngine');
const { getClient: getRedis } = require('../services/redis');
const config = require('../config/env');

// ═══════════════════════════════════════════════════════════════
// SECURITY: Oracle-Specific Rate Limiting
// ═══════════════════════════════════════════════════════════════

const oracleRateLimiter = rateLimit({
    windowMs: 60 * 1000, // 1 minute
    max: config.ORACLE_RATE_LIMIT || 100,
    standardHeaders: true,
    legacyHeaders: false,
    message: {
        success: false,
        error: 'Too many requests. Please slow down.'
    },
    keyGenerator: (req) => {
        // Rate limit by IP, but also consider x-api-key if present
        const apiKey = req.headers['x-api-key'];
        if (apiKey) {
            return `apikey:${apiKey}`;
        }
        return req.headers['x-forwarded-for'] || req.ip;
    }
});

// Stricter rate limit for write operations (webhooks, registration)
const writeRateLimiter = rateLimit({
    windowMs: 60 * 1000, // 1 minute
    max: 20, // Only 20 writes per minute
    standardHeaders: true,
    legacyHeaders: false,
    message: {
        success: false,
        error: 'Too many write requests. Please slow down.'
    },
    keyGenerator: (req) => req.headers['x-forwarded-for'] || req.ip
});

// ═══════════════════════════════════════════════════════════════
// SECURITY: Input Validation
// ═══════════════════════════════════════════════════════════════

// Base58 alphabet (Solana addresses)
const BASE58_ALPHABET = /^[1-9A-HJ-NP-Za-km-z]+$/;

/**
 * Validate Solana address format
 * - Length: 32-44 characters
 * - Characters: Base58 only (no 0, O, I, l)
 */
function isValidSolanaAddress(address) {
    if (!address || typeof address !== 'string') return false;
    if (address.length < 32 || address.length > 44) return false;
    return BASE58_ALPHABET.test(address);
}

/**
 * Validate operation type (alphanumeric + underscore only)
 * Prevents injection in cache keys and DB queries
 */
function isValidOperationType(operation) {
    if (!operation || typeof operation !== 'string') return false;
    if (operation.length > 64) return false;
    return /^[a-z0-9_]+$/.test(operation);
}

/**
 * Validate amount (positive finite number)
 */
function isValidAmount(amount) {
    if (typeof amount !== 'number') return false;
    if (!Number.isFinite(amount)) return false;
    if (amount <= 0) return false;
    if (amount > Number.MAX_SAFE_INTEGER) return false;
    return true;
}

// ═══════════════════════════════════════════════════════════════
// SECURITY: Webhook HMAC Verification
// ═══════════════════════════════════════════════════════════════

const WEBHOOK_SECRET = config.ORACLE_WEBHOOK_SECRET || null;

/**
 * Create canonical JSON for HMAC signing
 * CRITICAL: Key order must match exactly on both sides (GASdf + HolDex)
 */
function canonicalBurnPayload(payload) {
    return JSON.stringify({
        amount: payload.amount,
        source: payload.source,
        txSignature: payload.txSignature,
        wallet: payload.wallet
    });
}

/**
 * Verify HMAC signature for webhook requests
 * Header: x-holdex-signature: sha256=<hex>
 */
function verifyWebhookSignature(payload, signature) {
    if (!WEBHOOK_SECRET) {
        // If no secret configured, reject all webhooks in production
        return config.NODE_ENV !== 'production';
    }

    if (!signature || !signature.startsWith('sha256=')) {
        return false;
    }

    const providedHash = signature.slice(7);
    const expectedHash = crypto
        .createHmac('sha256', WEBHOOK_SECRET)
        .update(canonicalBurnPayload(payload))
        .digest('hex');

    // Timing-safe comparison to prevent timing attacks
    try {
        return crypto.timingSafeEqual(
            Buffer.from(providedHash, 'hex'),
            Buffer.from(expectedHash, 'hex')
        );
    } catch (_e) {
        return false;
    }
}

// Cache TTLs
const KSCORE_CACHE_TTL = 60;     // 1 minute
const ESCORE_CACHE_TTL = 300;    // 5 minutes
const COSTS_CACHE_TTL = 3600;    // 1 hour

// K-Score acceptance threshold (configurable)
const KSCORE_ACCEPTANCE_THRESHOLD = 50;

// Hardcoded accepted tokens (always pass K-Score check)
const HARDCODED_ACCEPTS = new Set([
    'So11111111111111111111111111111111111111112',      // SOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',    // USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',    // USDT
    config.FEE_TOKEN_MINT || '9zB5wRarXMj86MymwLumSKA1Dx35zPqqKfcZtK1Spump' // $ASDFASDFA
]);

/**
 * Initialize oracle routes
 */
function init(deps) {
    const { db, logger } = deps;
    const engine = getHarmonyEngine(db);

    // Apply rate limiting to all Oracle routes
    router.use(oracleRateLimiter);

    // ═══════════════════════════════════════════════════════════════
    // K-SCORE ORACLE (for token acceptance)
    // ═══════════════════════════════════════════════════════════════

    /**
     * GET /oracle/kscore/:mint
     *
     * Returns K-Score and acceptance status for a token.
     * Used by GASdf to determine if a token can be used for payment.
     *
     * Response:
     * {
     *   mint: string,
     *   k_score: number,
     *   tier: string,
     *   accepted: boolean,
     *   reason: string | null,
     *   cached: boolean,
     *   ttl: number
     * }
     */
    router.get('/kscore/:mint', async (req, res) => {
        try {
            const { mint } = req.params;

            // SECURITY: Validate mint address (Base58, proper length)
            if (!isValidSolanaAddress(mint)) {
                return res.status(400).json({
                    success: false,
                    error: 'Invalid mint address format'
                });
            }

            // Check hardcoded accepts first
            if (HARDCODED_ACCEPTS.has(mint)) {
                return res.json({
                    success: true,
                    data: {
                        mint,
                        k_score: 100,
                        tier: 'Diamond',
                        tier_icon: '💎',
                        accepted: true,
                        reason: 'Hardcoded acceptance (infrastructure token)',
                        cached: false,
                        ttl: 0
                    }
                });
            }

            // Try cache
            const redis = getRedis();
            const cacheKey = `oracle:kscore:${mint}`;
            let cached = false;

            if (redis) {
                try {
                    const cachedData = await redis.get(cacheKey);
                    if (cachedData) {
                        const data = JSON.parse(cachedData);
                        return res.json({
                            success: true,
                            data: { ...data, cached: true }
                        });
                    }
                } catch (_e) { /* continue */ }
            }

            // Fetch from database
            const token = await db.get(`
                SELECT mint, k_score, conviction_score, holders
                FROM tokens
                WHERE mint = $1
            `, [mint]);

            if (!token) {
                return res.json({
                    success: true,
                    data: {
                        mint,
                        k_score: 0,
                        tier: 'Unknown',
                        tier_icon: '❓',
                        accepted: false,
                        reason: 'Token not found in database',
                        cached: false,
                        ttl: 0
                    }
                });
            }

            const kScore = Number(token.k_score) || 0;
            const kRank = getKRank(kScore);
            const accepted = kScore >= KSCORE_ACCEPTANCE_THRESHOLD;

            const responseData = {
                mint,
                k_score: kScore,
                tier: kRank.tier,
                tier_icon: kRank.icon,
                accepted,
                reason: accepted
                    ? null
                    : `K-Score ${kScore} below threshold ${KSCORE_ACCEPTANCE_THRESHOLD}`,
                cached: false,
                ttl: KSCORE_CACHE_TTL
            };

            // Cache result
            if (redis) {
                redis.set(cacheKey, JSON.stringify(responseData), { EX: KSCORE_CACHE_TTL }).catch(() => {});
            }

            res.json({
                success: true,
                data: responseData
            });

        } catch (error) {
            logger.error(`[Oracle] K-Score lookup failed: ${error.message}`);
            res.status(500).json({
                success: false,
                error: 'K-Score lookup failed'
            });
        }
    });

    // ═══════════════════════════════════════════════════════════════
    // E-SCORE ORACLE (for participant benefits)
    // ═══════════════════════════════════════════════════════════════

    /**
     * GET /oracle/escore/:wallet
     *
     * Returns E-Score and benefits for a wallet.
     * Used by GASdf to calculate discounts.
     *
     * Response:
     * {
     *   wallet: string,
     *   e_score: number,
     *   tier: { name, icon, color },
     *   benefits: { discount, freeCalls, rateLimit, priority },
     *   cached: boolean
     * }
     */
    router.get('/escore/:wallet', async (req, res) => {
        try {
            const { wallet } = req.params;

            // SECURITY: Validate wallet address (Base58, proper length)
            if (!isValidSolanaAddress(wallet)) {
                return res.status(400).json({
                    success: false,
                    error: 'Invalid wallet address format'
                });
            }

            // Get E-Score from engine (handles caching internally)
            const eScoreResult = await engine.getEScore(wallet);
            const benefits = harmony.calculateBenefits(eScoreResult.score);

            res.json({
                success: true,
                data: {
                    wallet,
                    e_score: eScoreResult.score,
                    tier: eScoreResult.tier,
                    is_registered: eScoreResult.isRegistered,
                    benefits: benefits.benefits,
                    display: benefits.display,
                    progress: benefits.progress
                }
            });

        } catch (error) {
            logger.error(`[Oracle] E-Score lookup failed: ${error.message}`);
            res.status(500).json({
                success: false,
                error: 'E-Score lookup failed'
            });
        }
    });

    /**
     * GET /oracle/discount/:wallet/:operation
     *
     * Calculate discount for a specific operation.
     * Used by GASdf to get exact fee amount.
     *
     * Response:
     * {
     *   wallet: string,
     *   operation: string,
     *   e_score: number,
     *   discounts: { theoretical, maxAllowed, effective },
     *   finalFee: number,
     *   baseFee: number,
     *   isViable: boolean
     * }
     */
    router.get('/discount/:wallet/:operation', async (req, res) => {
        try {
            const { wallet, operation } = req.params;

            // SECURITY: Validate wallet address (Base58, proper length)
            if (!isValidSolanaAddress(wallet)) {
                return res.status(400).json({
                    success: false,
                    error: 'Invalid wallet address format'
                });
            }

            // SECURITY: Validate operation type (alphanumeric + underscore only)
            if (!isValidOperationType(operation)) {
                return res.status(400).json({
                    success: false,
                    error: 'Invalid operation type format'
                });
            }

            // Calculate fee
            const feeResult = await engine.calculateFee(wallet, operation);

            if (feeResult.error) {
                return res.status(400).json({
                    success: false,
                    error: feeResult.error
                });
            }

            res.json({
                success: true,
                data: {
                    wallet,
                    operation,
                    ...feeResult
                }
            });

        } catch (error) {
            logger.error(`[Oracle] Discount calculation failed: ${error.message}`);
            res.status(500).json({
                success: false,
                error: 'Discount calculation failed'
            });
        }
    });

    // ═══════════════════════════════════════════════════════════════
    // OPERATION COSTS
    // ═══════════════════════════════════════════════════════════════

    /**
     * GET /oracle/costs
     *
     * Returns all operation costs and efficiency parameters.
     * Used by GASdf for local calculation.
     *
     * Response:
     * {
     *   operations: { [type]: { baseFee, cost, minFee, maxDiscount } },
     *   constants: { PHI, RATIOS, SAFETY_MARGIN }
     * }
     */
    router.get('/costs', async (req, res) => {
        try {
            const costs = await engine.getOperationCosts();

            res.json({
                success: true,
                data: {
                    operations: costs,
                    constants: {
                        PHI: harmony.PHI,
                        RATIOS: harmony.RATIOS,
                        SAFETY_MARGIN: harmony.SAFETY_MARGIN,
                        MAX_DISCOUNT_CAP: harmony.MAX_DISCOUNT_CAP,
                        DISCOUNT_ASYMPTOTE: harmony.DISCOUNT_ASYMPTOTE
                    },
                    acceptance: {
                        threshold: KSCORE_ACCEPTANCE_THRESHOLD,
                        hardcodedTokens: Array.from(HARDCODED_ACCEPTS)
                    }
                }
            });

        } catch (error) {
            logger.error(`[Oracle] Costs lookup failed: ${error.message}`);
            res.status(500).json({
                success: false,
                error: 'Costs lookup failed'
            });
        }
    });

    // ═══════════════════════════════════════════════════════════════
    // WEBHOOK: BURN NOTIFICATIONS (from GASdf)
    // ═══════════════════════════════════════════════════════════════

    /**
     * POST /oracle/webhook/burns
     *
     * Receive burn notifications from GASdf.
     * Updates participant E-Score with new burn amount.
     *
     * Request body:
     * {
     *   wallet: string,
     *   amount: number,
     *   txSignature: string,
     *   source: 'gasdf'
     * }
     */
    router.post('/webhook/burns', writeRateLimiter, async (req, res) => {
        try {
            const { wallet, amount, txSignature, source } = req.body;

            // SECURITY: Verify HMAC signature (prevents forged requests)
            const signature = req.headers['x-holdex-signature'];
            if (!verifyWebhookSignature(req.body, signature)) {
                logger.warn(`[Oracle] SECURITY: Invalid webhook signature from ${req.ip}`);
                return res.status(401).json({
                    success: false,
                    error: 'Invalid signature'
                });
            }

            // Validate required fields
            if (!wallet || amount === undefined) {
                return res.status(400).json({
                    success: false,
                    error: 'Missing required fields: wallet, amount'
                });
            }

            // SECURITY: Validate wallet address (Base58, proper length)
            if (!isValidSolanaAddress(wallet)) {
                return res.status(400).json({
                    success: false,
                    error: 'Invalid wallet address format'
                });
            }

            // SECURITY: Validate amount (positive finite number)
            if (!isValidAmount(amount)) {
                return res.status(400).json({
                    success: false,
                    error: 'Invalid amount: must be positive number'
                });
            }

            // SECURITY: Validate transaction signature format (Base58, 87-88 chars)
            if (txSignature && (txSignature.length < 85 || txSignature.length > 90 || !BASE58_ALPHABET.test(txSignature))) {
                return res.status(400).json({
                    success: false,
                    error: 'Invalid transaction signature format'
                });
            }

            // Validate source (only accept from known sources)
            if (source !== 'gasdf') {
                return res.status(403).json({
                    success: false,
                    error: 'Unknown source'
                });
            }

            // Record burn contribution
            const result = await engine.recordContribution(wallet, 'burn', amount, {
                source,
                txSignature,
                details: { receivedAt: Date.now() }
            });

            logger.info(`[Oracle] Burn recorded: ${wallet.slice(0,8)}... burned ${amount} $ASDF via ${source}`);

            res.json({
                success: true,
                data: {
                    wallet,
                    amount,
                    newEScore: result.score,
                    tier: result.tier
                }
            });

        } catch (error) {
            logger.error(`[Oracle] Burn webhook failed: ${error.message}`);
            res.status(500).json({
                success: false,
                error: 'Burn recording failed'
            });
        }
    });

    // ═══════════════════════════════════════════════════════════════
    // PARTICIPANT ENDPOINTS
    // ═══════════════════════════════════════════════════════════════

    /**
     * GET /oracle/participant/:wallet
     *
     * Get full participant profile.
     */
    router.get('/participant/:wallet', async (req, res) => {
        try {
            const { wallet } = req.params;

            // SECURITY: Validate wallet address (Base58, proper length)
            if (!isValidSolanaAddress(wallet)) {
                return res.status(400).json({
                    success: false,
                    error: 'Invalid wallet address format'
                });
            }

            const participant = await engine.getParticipant(wallet);

            if (!participant) {
                return res.status(404).json({
                    success: false,
                    error: 'Participant not found'
                });
            }

            res.json({
                success: true,
                data: participant
            });

        } catch (error) {
            logger.error(`[Oracle] Participant lookup failed: ${error.message}`);
            res.status(500).json({
                success: false,
                error: 'Participant lookup failed'
            });
        }
    });

    /**
     * POST /oracle/participant/register
     *
     * Register a new participant.
     */
    // Valid participant types (whitelist)
    const VALID_PARTICIPANT_TYPES = ['user', 'holder', 'burner', 'dev', 'infra'];

    router.post('/participant/register', writeRateLimiter, async (req, res) => {
        try {
            const { wallet, type } = req.body;

            // SECURITY: Validate wallet address (Base58, proper length)
            if (!isValidSolanaAddress(wallet)) {
                return res.status(400).json({
                    success: false,
                    error: 'Invalid wallet address format'
                });
            }

            // SECURITY: Validate participant type (whitelist only)
            const participantType = type || 'user';
            if (!VALID_PARTICIPANT_TYPES.includes(participantType)) {
                return res.status(400).json({
                    success: false,
                    error: 'Invalid participant type'
                });
            }

            await engine.ensureParticipant(wallet, participantType);
            const participant = await engine.getParticipant(wallet);

            res.json({
                success: true,
                data: participant
            });

        } catch (error) {
            logger.error(`[Oracle] Participant registration failed: ${error.message}`);
            res.status(500).json({
                success: false,
                error: 'Registration failed'
            });
        }
    });

    // ═══════════════════════════════════════════════════════════════
    // STATS & LEADERBOARD
    // ═══════════════════════════════════════════════════════════════

    /**
     * GET /oracle/leaderboard
     *
     * Get E-Score leaderboard.
     */
    router.get('/leaderboard', async (req, res) => {
        try {
            const limit = Math.min(100, parseInt(req.query.limit) || 50);
            const leaderboard = await engine.getLeaderboard(limit);

            res.json({
                success: true,
                data: leaderboard
            });

        } catch (error) {
            logger.error(`[Oracle] Leaderboard failed: ${error.message}`);
            res.status(500).json({
                success: false,
                error: 'Leaderboard lookup failed'
            });
        }
    });

    /**
     * GET /oracle/stats
     *
     * Get ecosystem statistics.
     */
    router.get('/stats', async (req, res) => {
        try {
            const stats = await engine.getStats();

            res.json({
                success: true,
                data: stats
            });

        } catch (error) {
            logger.error(`[Oracle] Stats failed: ${error.message}`);
            res.status(500).json({
                success: false,
                error: 'Stats lookup failed'
            });
        }
    });

    return router;
}

// ═══════════════════════════════════════════════════════════════
// HELPER: K-Score tier (matches tokens.js)
// ═══════════════════════════════════════════════════════════════

function getKRank(score) {
    if (score >= 90) return { tier: 'Diamond', icon: '💎', level: 8 };
    if (score >= 80) return { tier: 'Platinum', icon: '💠', level: 7 };
    if (score >= 70) return { tier: 'Gold', icon: '🥇', level: 6 };
    if (score >= 60) return { tier: 'Silver', icon: '🥈', level: 5 };
    if (score >= 50) return { tier: 'Bronze', icon: '🥉', level: 4 };
    if (score >= 40) return { tier: 'Copper', icon: '🟤', level: 3 };
    if (score >= 20) return { tier: 'Iron', icon: '⚫', level: 2 };
    return { tier: 'Rust', icon: '🔩', level: 1 };
}

module.exports = { init };
