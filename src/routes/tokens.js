const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const rateLimit = require('express-rate-limit');
const { PublicKey } = require('@solana/web3.js');
const nacl = require('tweetnacl');
const _bs58 = require('bs58');
const { isValidPubkey } = require('../utils/solana');
const { hashApiKey } = require('../utils/apiKeyHash');
const { sanitizeError } = require('../utils/validation');

// Strict rate limit for API key generation (5 per hour per IP)
const apiKeyRateLimit = rateLimit({
    windowMs: 60 * 60 * 1000, // 1 hour
    max: 5,
    message: { success: false, error: 'Too many key requests. Try again in 1 hour.' },
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req) => req.headers['x-forwarded-for'] || req.ip
});

// SECURITY: Rate limit for proxy endpoints (prevents RPC abuse)
const proxyRateLimit = rateLimit({
    windowMs: 60 * 1000, // 1 minute
    max: 30, // 30 requests per minute
    message: { success: false, error: 'Rate limit exceeded. Try again shortly.' },
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req) => req.headers['x-forwarded-for'] || req.ip
});
const { smartCache, aggregateAndSaveToken } = require('../services/database');
const { getSolanaConnection } = require('../services/solana'); 
const config = require('../config/env');
const { updateSingleToken, updateKScores, getHealthStatus } = require('../tasks/kScoreUpdater'); 
const { getClient } = require('../services/redis'); 
const { snapshotPools } = require('../indexer/tasks/snapshotter'); 
const logger = require('../services/logger');
const cacheControl = require('../middleware/httpCache');
const unifiedRateLimiter = require('../middleware/unifiedRateLimiter');
const { indexTokenOnChain } = require('../services/indexer');
const { addTokenToMasterWebhook } = require('../services/heliusWebhook');
const verification = require('../services/verificationService');
const dataVerification = require('../services/dataVerification');
const nodeService = require('../services/nodeService');

// Lazy load canvas-based card generator (avoid build failures on workers without native deps)
let cardGeneratorModule = null;
function getCardGenerator() {
    if (!cardGeneratorModule) {
        try {
            cardGeneratorModule = require('../services/cardGenerator');
        } catch (err) {
            console.warn('[CardGenerator] Canvas not available:', err.message);
            cardGeneratorModule = {
                generateKScoreCard: () => { throw new Error('Card generator not available'); },
                styleFromMode: () => 'holdex'
            };
        }
    }
    return cardGeneratorModule;
}

const router = express.Router();
const solanaConnection = getSolanaConnection();

const pendingRefreshes = new Set();

/**
 * Native tokens - Infrastructure, not rated
 * "K-Score is for the jungle. SOL and USDC are the roads. We don't rate roads."
 */
const NATIVE_TOKENS = new Set([
    'So11111111111111111111111111111111111111112',  // Wrapped SOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
    'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So',  // mSOL (Marinade)
    'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn', // JitoSOL
    'bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1',  // bSOL (Blaze)
    'jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v',  // JupSOL
    '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj', // stSOL (Lido)
]);

/**
 * Check if a token is a native/infrastructure token
 */
function isNativeToken(mint) {
    return NATIVE_TOKENS.has(mint);
}

/**
 * K-Score Metal Rank Tiers (aligned with Credit Grades)
 */
function getKRank(score, mint = null) {
    // Native tokens are infrastructure - not rated
    if (mint && isNativeToken(mint)) {
        return { tier: 'Native', icon: '🏛️', level: 9, isNative: true };
    }
    if (score >= 90) return { tier: 'Diamond', icon: '💎', level: 8 };
    if (score >= 80) return { tier: 'Platinum', icon: '💠', level: 7 };
    if (score >= 70) return { tier: 'Gold', icon: '🥇', level: 6 };
    if (score >= 60) return { tier: 'Silver', icon: '🥈', level: 5 };
    if (score >= 50) return { tier: 'Bronze', icon: '🥉', level: 4 };
    if (score >= 40) return { tier: 'Copper', icon: '🟤', level: 3 };
    if (score >= 20) return { tier: 'Iron', icon: '⚫', level: 2 };
    return { tier: 'Rust', icon: '🔩', level: 1 };
}

/**
 * Credit Rating System - "Moody's for Memecoins"
 */
function getCreditRating(score, mint = null) {
    // Native tokens are infrastructure - not rated
    if (mint && isNativeToken(mint)) {
        return { grade: '—', label: 'Infrastructure', color: '#6366f1', isNative: true };
    }
    if (score >= 90) return { grade: 'A1', label: 'Prime', color: '#00ff88' };
    if (score >= 80) return { grade: 'A2', label: 'Excellent', color: '#00dd77' };
    if (score >= 70) return { grade: 'A3', label: 'Good', color: '#00bb66' };
    if (score >= 60) return { grade: 'B1', label: 'Fair', color: '#ffcc00' };
    if (score >= 50) return { grade: 'B2', label: 'Speculative', color: '#ffaa00' };
    if (score >= 40) return { grade: 'B3', label: 'Risky', color: '#ff8800' };
    if (score >= 20) return { grade: 'C', label: 'High Risk', color: '#ff4400' };
    return { grade: 'D', label: 'Junk', color: '#ff0000' };
}

/**
 * Calculate K-Score trajectory from historical data
 * Returns: 'improving' | 'stable' | 'declining'
 *
 * Analyzes 30-day trend to determine trajectory
 * - improving: score increased >5 points
 * - declining: score decreased >5 points
 * - stable: within ±5 points
 */
async function calculateTrajectory(db, mint, currentScore) {
    try {
        // Get K-Score history for last 30 days
        const history = await db.all(`
            SELECT k_score, date
            FROM k_score_history
            WHERE mint = $1
              AND date >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY date ASC
            LIMIT 30
        `, [mint]);

        if (!history || history.length < 3) {
            // Not enough data for trajectory
            return { trajectory: 'stable', delta30d: 0, dataPoints: 0 };
        }

        // Calculate average of first week vs current score
        const oldScores = history.slice(0, Math.min(7, Math.floor(history.length / 3)));
        const avgOldScore = oldScores.reduce((sum, h) => sum + (h.k_score || 0), 0) / oldScores.length;
        const delta = currentScore - avgOldScore;

        let trajectory = 'stable';
        if (delta > 5) trajectory = 'improving';
        else if (delta < -5) trajectory = 'declining';

        return {
            trajectory,
            delta30d: Math.round(delta * 10) / 10,
            dataPoints: history.length
        };
    } catch (_e) {
        return { trajectory: 'stable', delta30d: 0, dataPoints: 0 };
    }
}

/**
 * Get Credit Rating with Trajectory (for GASdf integration)
 */
async function getCreditRatingWithTrajectory(db, mint, score) {
    const base = getCreditRating(score, mint);

    // Native tokens don't have trajectory
    if (base.isNative) {
        return { ...base, trajectory: null, delta30d: null, dataPoints: 0 };
    }

    const { trajectory, delta30d, dataPoints } = await calculateTrajectory(db, mint, score);

    return {
        ...base,
        trajectory,
        delta30d,
        dataPoints
    };
}

const requireAdmin = (req, res, next) => {
    // Reject if ADMIN_PASSWORD not configured
    if (!config.ADMIN_PASSWORD) {
        logger.error('❌ Admin endpoint called but ADMIN_PASSWORD not set');
        return res.status(503).json({ success: false, error: 'Admin not configured' });
    }
    const authHeader = req.headers['x-admin-auth'];
    if (!authHeader) {
        return res.status(403).json({ success: false, error: 'Unauthorized' });
    }

    // SECURITY: Use timing-safe comparison to prevent timing attacks
    const headerBuffer = Buffer.from(String(authHeader));
    const passwordBuffer = Buffer.from(config.ADMIN_PASSWORD);

    // Ensure same length for timing-safe comparison
    if (headerBuffer.length !== passwordBuffer.length) {
        return res.status(403).json({ success: false, error: 'Unauthorized' });
    }

    if (!crypto.timingSafeEqual(headerBuffer, passwordBuffer)) {
        return res.status(403).json({ success: false, error: 'Unauthorized' });
    }
    next();
};

function init(deps) {
    const { db } = deps;

    // Burn Credits System - "Hold to enter. Burn to use."
    const burnCredits = require('../services/burnCredits');

    // --- HELPER FUNCTIONS ---
    function sanitizeUrl(url) {
        if (!url || typeof url !== 'string') return "";
        url = url.trim();
        if (url.match(/^(http:\/\/|https:\/\/)/i)) {
            return url;
        }
        if (url.match(/^[a-zA-Z0-9][a-zA-Z0-9-]{1,61}[a-zA-Z0-9]\.[a-zA-Z]{2,}/)) {
            return `https://${url}`;
        }
        return ""; 
    }

    async function fetchExternalCandles(poolAddress, resolution) {
        try {
            let timeframe = 'minute';
            let aggregate = 1;
            if (resolution === '5') aggregate = 5;
            else if (resolution === '15') aggregate = 15;
            else if (resolution === '60') { timeframe = 'hour'; aggregate = 1; }
            else if (resolution === '240') { timeframe = 'hour'; aggregate = 4; }
            else if (resolution === 'D') { timeframe = 'day'; aggregate = 1; }

            const url = `https://api.geckoterminal.com/api/v2/networks/solana/pools/${poolAddress}/ohlcv/${timeframe}?aggregate=${aggregate}&limit=100`;
            const response = await axios.get(url, { timeout: 5000 });
            const data = response.data.data.attributes.ohlcv_list;
            return data.map(c => ({ time: c[0], open: c[1], high: c[2], low: c[3], close: c[4], volume: c[5] })).reverse();
        } catch (_e) { return []; }
    }

    async function verifyPayment(signature, _payerPubkey) {
        if (!signature) throw new Error("Payment signature required");
        const existing = await db.get('SELECT id FROM token_updates WHERE signature = $1', [signature]);
        if (existing) throw new Error("Transaction signature already used");
        return true; 
    }

    // --- PROXY ROUTES ---
    // SECURITY: Rate limited to prevent RPC abuse
    router.get('/proxy/blockhash', proxyRateLimit, async (req, res) => {
        try {
            const { blockhash, lastValidBlockHeight } = await solanaConnection.getLatestBlockhash('confirmed');
            res.json({ success: true, blockhash, lastValidBlockHeight });
        } catch (_e) { res.status(500).json({ success: false, error: "Network Busy" }); }
    });

    router.post('/proxy/send-tx', proxyRateLimit, async (req, res) => {
        try {
            const { signedTx } = req.body;
            if (!signedTx) return res.status(400).json({ success: false, error: "No transaction data" });
            const txBuffer = Buffer.from(signedTx, 'base64');
            const signature = await solanaConnection.sendRawTransaction(txBuffer, { skipPreflight: false, preflightCommitment: 'confirmed' });
            res.json({ success: true, signature });
        } catch (_e) { res.status(500).json({ success: false, error: "Transaction Failed at RPC" }); }
    });

    // --- HEALTH CHECK ---
    // SECURITY: Rate limit health endpoint (M2)
    // Returns system health including Helius API status, circuit breaker, and rate limits
    router.get('/health', proxyRateLimit, async (req, res) => {
        try {
            const redis = getClient();
            const heliusHealth = getHealthStatus();

            // Check DB connectivity
            let dbStatus = 'ok';
            try {
                await db.get('SELECT 1');
            } catch (_e) {
                dbStatus = 'error';
            }

            // Check Redis connectivity
            let redisStatus = 'disconnected';
            if (redis) {
                try {
                    await redis.ping();
                    redisStatus = 'ok';
                } catch (_e) {
                    redisStatus = 'error';
                }
            }

            const health = {
                status: heliusHealth.helius.circuitBreaker.state === 'open' ? 'degraded' : 'ok',
                timestamp: new Date().toISOString(),
                uptime: process.uptime(),
                memory: {
                    heapUsed: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
                    heapTotal: Math.round(process.memoryUsage().heapTotal / 1024 / 1024),
                    rss: Math.round(process.memoryUsage().rss / 1024 / 1024)
                },
                services: {
                    database: dbStatus,
                    redis: redisStatus,
                    ...heliusHealth
                }
            };

            // Return 503 if circuit breaker is open (degraded service)
            const statusCode = health.status === 'degraded' ? 503 : 200;
            res.status(statusCode).json(health);

        } catch (e) {
            res.status(500).json({
                status: 'error',
                error: e.message,
                timestamp: new Date().toISOString()
            });
        }
    });

    // ============================================
    // VERIFICATION ENDPOINTS
    // "Don't Trust, Verify" - Data integrity checks
    // ============================================

    /**
     * GET /api/token/:mint/verify
     * Generate a verification report for a token
     * Shows: staleness, holder verification, RPC diversity, audit trail
     */
    router.get('/token/:mint/verify', cacheControl(60, 300), unifiedRateLimiter, async (req, res) => {
        const { mint } = req.params;

        if (!isValidPubkey(mint)) {
            return res.status(400).json({ success: false, error: 'Invalid mint address' });
        }

        try {
            const report = await verification.generateVerificationReport(db, mint);
            res.json({ success: true, report });
        } catch (e) {
            logger.error(`[Verify] Report failed for ${mint}: ${e.message}`);
            res.status(500).json({ success: false, error: 'Verification failed' });
        }
    });

    /**
     * GET /api/token/:mint/audit
     * Get audit history for a token
     */
    router.get('/token/:mint/audit', cacheControl(30, 60), unifiedRateLimiter, async (req, res) => {
        const { mint } = req.params;
        const limit = Math.min(parseInt(req.query.limit) || 50, 100);

        if (!isValidPubkey(mint)) {
            return res.status(400).json({ success: false, error: 'Invalid mint address' });
        }

        try {
            const entries = await verification.getAuditHistory(db, 'token', mint, limit);
            res.json({
                success: true,
                mint,
                entries,
                count: entries.length
            });
        } catch (e) {
            logger.error(`[Audit] History failed for ${mint}: ${e.message}`);
            res.status(500).json({ success: false, error: 'Audit lookup failed' });
        }
    });

    /**
     * GET /api/stale-tokens
     * List tokens with stale conviction data
     * SECURITY: Rate limited (M2)
     */
    router.get('/stale-tokens', cacheControl(300, 600), proxyRateLimit, async (req, res) => {
        try {
            const staleTokens = await verification.getStaleTokens(db);
            res.json({
                success: true,
                threshold_hours: verification.STALENESS_THRESHOLD_MS / (60 * 60 * 1000),
                tokens: staleTokens,
                count: staleTokens.length
            });
        } catch (e) {
            logger.error(`[Staleness] List failed: ${e.message}`);
            res.status(500).json({ success: false, error: 'Staleness check failed' });
        }
    });

    /**
     * GET /api/rpc-status
     * Show RPC provider health for transparency
     * SECURITY: Rate limited (M2)
     */
    router.get('/rpc-status', cacheControl(30, 60), proxyRateLimit, (req, res) => {
        const providers = verification.RPC_PROVIDERS.map(p => ({
            name: p.name,
            healthy: p.healthy,
            priority: p.priority,
            failures: p.failures
        }));

        res.json({
            success: true,
            providers,
            healthyCount: providers.filter(p => p.healthy).length,
            totalCount: providers.length
        });
    });

    // PUBLIC: Candle Chart
    router.get('/token/:mint/candles', cacheControl(30, 60), unifiedRateLimiter, async (req, res) => {
        const { mint } = req.params;
        const { resolution = '5', from, to, poolAddress } = req.query;

        // SECURITY: Validate mint address
        if (!isValidPubkey(mint)) {
            return res.status(400).json({ success: false, error: 'Invalid mint address' });
        }

        try {
            const resMinutes = parseInt(resolution === 'D' ? 1440 : resolution);
            const resMs = resMinutes * 60 * 1000;
            const cacheKey = `chart:${mint}:${poolAddress || 'best'}:${resolution}:${Math.floor(Date.now() / 10000)}`; 

            const result = await smartCache(cacheKey, 10, async () => {
                let targetPoolAddress = poolAddress;
                if (!targetPoolAddress) {
                    const bestPool = await db.get(`SELECT address FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1`, [mint]);
                    if (!bestPool) return { success: false, error: "Token not indexed" };
                    targetPoolAddress = bestPool.address;
                }

                const fromMs = parseInt(from) * 1000 || (Date.now() - 24 * 60 * 60 * 1000);
                const toMs = parseInt(to) * 1000 || Date.now();

                const rows = await db.all(`
                    SELECT timestamp, open, high, low, close, volume FROM candles_1m 
                    WHERE pool_address = $1 AND timestamp >= $2 AND timestamp <= $3 
                    ORDER BY timestamp ASC
                `, [targetPoolAddress, fromMs, toMs]);
                
                if (!rows || rows.length < 5) {
                    const extCandles = await fetchExternalCandles(targetPoolAddress, resolution);
                    if (extCandles.length > 0) return { success: true, candles: extCandles, source: 'external' };
                }

                if (!rows || rows.length === 0) return { success: true, candles: [] };

                const candles = [];
                let currentCandle = null;
                for (const r of rows) {
                    const time = parseInt(String(r.timestamp));
                    const bucketStart = Math.floor(time / resMs) * resMs;
                    if (!currentCandle || currentCandle.timeMs !== bucketStart) {
                        if (currentCandle) {
                            currentCandle.time = Math.floor(currentCandle.timeMs / 1000); 
                            delete currentCandle.timeMs;
                            candles.push(currentCandle);
                        }
                        currentCandle = { timeMs: bucketStart, open: r.open, high: r.high, low: r.low, close: r.close, volume: 0 };
                    }
                    if (r.high > currentCandle.high) currentCandle.high = r.high;
                    if (r.low < currentCandle.low) currentCandle.low = r.low;
                    currentCandle.close = r.close;
                    if (r.volume) currentCandle.volume += r.volume;
                }
                if (currentCandle) {
                    currentCandle.time = Math.floor(currentCandle.timeMs / 1000);
                    delete currentCandle.timeMs;
                    candles.push(currentCandle);
                }
                return { success: true, candles, source: 'internal' };
            });
            res.json(result);
        } catch (e) { res.status(500).json({ success: false, error: sanitizeError(e) }); }
    });

    // SECURITY: Rate limit config endpoints (M2)
    router.get('/config/fees', proxyRateLimit, (req, res) => {
        res.json({ success: true, solFee: config.FEE_SOL, tokenFee: config.FEE_TOKEN_AMOUNT, tokenMint: config.FEE_TOKEN_MINT, treasury: config.TREASURY_WALLET });
    });

    // SECURITY: Rate limit balance proxy (M2)
    router.get('/proxy/balance/:wallet', proxyRateLimit, async (req, res) => {
        try {
            const { wallet } = req.params;
            const tokenMint = req.query.tokenMint || config.FEE_TOKEN_MINT;
            if (!isValidPubkey(wallet)) return res.status(400).json({ success: false, error: "Invalid wallet" });

            const pubKey = new PublicKey(wallet);
            const [solBalance, tokenAccounts] = await Promise.all([
                solanaConnection.getBalance(pubKey),
                solanaConnection.getParsedTokenAccountsByOwner(pubKey, { mint: new PublicKey(tokenMint) })
            ]);
            res.json({ success: true, sol: solBalance / 1e9, tokens: tokenAccounts.value.length > 0 ? tokenAccounts.value[0].account.data.parsed.info.tokenAmount.uiAmount : 0 });
        } catch (_e) { res.status(500).json({ success: false, error: "Failed to fetch balance" }); }
    });

    // SECURITY: Rate limit update requests (M2)
    router.post('/request-update', proxyRateLimit, async (req, res) => {
        const { mint, twitter, website, telegram, banner, description, signature, userPublicKey, ctoUser } = req.body;
        try {
            // SECURITY: Validate mint address properly
            if (!mint || !isValidPubkey(mint)) {
                return res.status(400).json({ success: false, error: "Invalid mint address" });
            }
            
            try { await verifyPayment(signature, userPublicKey); } catch (payErr) { return res.status(402).json({ success: false, error: payErr.message }); }
            
            let finalDescription = description || "";
            if (ctoUser && typeof ctoUser === 'string' && ctoUser.trim().length > 0) {
                const cleanUser = ctoUser.replace(/^@/, '').trim();
                finalDescription += `\n\n(CTO by: @${cleanUser})`;
            }

            await db.run(`
                INSERT INTO token_updates (mint, twitter, website, telegram, banner, description, submittedAt, status, signature, payer)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9)
            `, [mint, twitter, website, telegram, banner, finalDescription, Date.now(), signature, userPublicKey]);
            
            try { await indexTokenOnChain(mint); } catch (_err) { /* ignore */ }
            res.json({ success: true, message: "Update queued." });
        } catch (_e) { res.status(500).json({ success: false, error: "Submission failed" }); }
    });

    // --- API KEY GENERATION (Wallet-Linked) ---
    // Key is just an alias for wallet - credits come from burns
    router.post('/request-api-key', apiKeyRateLimit, async (req, res) => {
        const { wallet, signature } = req.body;
        try {
            if (!wallet || !signature) return res.status(400).json({ success: false, error: "Wallet and Signature required" });

            const msg = new TextEncoder().encode("Request HolDex API Key");
            const sigBytes = Buffer.from(signature, 'base64');
            const pubBytes = new PublicKey(wallet).toBytes();
            const verified = nacl.sign.detached.verify(msg, sigBytes, pubBytes);
            if (!verified) return res.status(403).json({ success: false, error: "Invalid Signature" });

            const result = await db.get('SELECT COUNT(*) as count FROM api_keys WHERE owner = $1', [wallet]);
            const count = parseInt(result?.count || 0);

            if (count >= 5) {
                return res.status(400).json({ success: false, error: "Limit reached (5 Keys). Revoke old keys first." });
            }

            const key = 'hx_' + require('crypto').randomBytes(16).toString('hex');
            const keyHash = hashApiKey(key);
            const keyPrefix = key.substring(0, 7);

            // Store key linked to wallet (wallet column = owner for backwards compat)
            await db.run(
                `INSERT INTO api_keys (key_hash, key_prefix, owner, wallet, created_at) VALUES ($1, $2, $3, $3, $4)`,
                [keyHash, keyPrefix, wallet, Date.now()]
            );

            res.json({
                success: true,
                key, // Only time user sees the full key!
                wallet,
                message: "Key linked to wallet. Credits come from your burns.",
                system: {
                    gate: "Hold 10K+ $ASDFASDFA",
                    credits: "1 burn = 1 call forever"
                }
            });
        } catch (e) { console.error(e); res.status(500).json({ success: false, error: "Server Error" }); }
    });

    router.post('/request-my-keys', async (req, res) => {
        const { wallet, signature } = req.body;
        try {
            if (!wallet || !signature) return res.status(400).json({ success: false, error: "Wallet and Signature required" });

            const msg = new TextEncoder().encode("Request HolDex API Key");
            const sigBytes = Buffer.from(signature, 'base64');
            const pubBytes = new PublicKey(wallet).toBytes();
            const verified = nacl.sign.detached.verify(msg, sigBytes, pubBytes);

            if (!verified) return res.status(403).json({ success: false, error: "Invalid Signature" });

            // Fetch keys and credit status in parallel
            const [keys, creditStatus] = await Promise.all([
                db.all('SELECT key_hash, key_prefix, is_active, created_at FROM api_keys WHERE owner = $1 ORDER BY created_at DESC', [wallet]),
                burnCredits.getCreditStatus(getSolanaConnection(), db, wallet)
            ]);

            // Return masked keys (user can't see full key after creation)
            const maskedKeys = (keys || []).map(k => ({
                key_id: k.key_hash.substring(0, 8),
                key_preview: k.key_prefix + '...****',
                is_active: k.is_active,
                created_at: k.created_at
            }));

            res.json({
                success: true,
                wallet,
                keys: maskedKeys,
                credits: {
                    holdings: creditStatus.holdings,
                    burned: creditStatus.burned,
                    used: creditStatus.usedCalls,
                    remaining: creditStatus.remainingCalls,
                    eligible: creditStatus.holdingEligible && creditStatus.remainingCalls > 0
                },
                system: {
                    gate: `Hold ${burnCredits.MIN_HOLDINGS.toLocaleString()}+ $ASDFASDFA`,
                    credits: '1 burn = 1 call forever'
                }
            });
        } catch (e) { console.error(e); res.status(500).json({ success: false, error: "Server Error" }); }
    });

    // Burn credits already required at top of init()

    // Get burn credit pricing info
    // SECURITY: Rate limited (M2)
    router.get('/api-pricing', proxyRateLimit, (req, res) => {
        res.json({
            success: true,
            system: 'burn-credits',
            philosophy: 'Hold to enter. Burn to use.',
            tokenMint: config.FEE_TOKEN_MINT,
            requirements: {
                minHoldings: burnCredits.MIN_HOLDINGS,
                minHoldingsFormatted: `${burnCredits.MIN_HOLDINGS.toLocaleString()} $ASDFASDFA`
            },
            pricing: {
                ratio: '1:1',
                description: '1 token burned = 1 API call (lifetime)',
                burnMethods: [
                    'Transfer to treasury wallet',
                    'Transfer to burn address (1111...)',
                    'SPL Token burn instruction'
                ]
            },
            burnAddresses: Array.from(burnCredits.BURN_ADDRESSES)
        });
    });

    // Check wallet's burn credits
    // SECURITY: Rate limited (M2)
    router.get('/credits/:wallet', proxyRateLimit, async (req, res) => {
        const { wallet } = req.params;

        // SECURITY: Validate as proper Solana address, not just length check
        if (!wallet || !isValidPubkey(wallet)) {
            return res.status(400).json({ success: false, error: 'Invalid wallet address' });
        }

        try {
            const { getSolanaConnection } = require('../services/solana');
            const connection = getSolanaConnection();

            const status = await burnCredits.getCreditStatus(connection, db, wallet);

            res.json({
                success: true,
                credits: status,
                eligible: status.holdingEligible && status.remainingCalls > 0,
                message: !status.holdingEligible
                    ? `Hold ${burnCredits.MIN_HOLDINGS.toLocaleString()}+ $ASDFASDFA to access API`
                    : status.remainingCalls === 0
                        ? 'Burn $ASDFASDFA to earn API calls (1 token = 1 call)'
                        : `${status.remainingCalls.toLocaleString()} API calls remaining`
            });

        } catch (e) {
            logger.error(`[Credits] Error: ${e.message}`);
            res.status(500).json({ success: false, error: 'Failed to check credits' });
        }
    });

    // Refresh burn cache for a wallet (after new burn)
    // This triggers a Helius API call - use sparingly
    // SECURITY: Rate limited to prevent Helius API abuse
    router.post('/credits/:wallet/refresh', apiKeyRateLimit, async (req, res) => {
        const { wallet } = req.params;

        // SECURITY: Validate wallet address
        if (!wallet || !isValidPubkey(wallet)) {
            return res.status(400).json({ success: false, error: 'Invalid wallet address' });
        }

        try {
            burnCredits.invalidateCache(wallet);
            const burned = await burnCredits.refreshBurns(wallet, db);

            res.json({
                success: true,
                wallet,
                burned,
                message: `Refreshed. Total burned: ${burned.toLocaleString()} $ASDFASDFA`
            });

        } catch (e) {
            logger.error(`[Credits] Refresh error: ${e.message}`);
            res.status(500).json({ success: false, error: 'Failed to refresh' });
        }
    });

    // Legacy tier pricing (deprecated, redirects to new system)
    router.get('/api-tier-pricing', (req, res) => {
        res.json({
            success: true,
            deprecated: true,
            message: 'Tier system replaced by burn credits. See /api/api-pricing',
            redirect: '/api/api-pricing'
        });
    });

    // --- ADMIN ROUTES ---
    router.get('/admin/updates', requireAdmin, async (req, res) => { 
        const { type } = req.query; 
        try { 
            let sql = `
                SELECT u.*, t.symbol as ticker, t.image, 
                       CASE WHEN t.hasCommunityUpdate = TRUE THEN TRUE ELSE FALSE END as "hasCommunityUpdate"
                FROM token_updates u 
                LEFT JOIN tokens t ON u.mint = t.mint
            `; 
            if (type === 'history') { 
                sql += ` WHERE u.status != 'pending' ORDER BY u.submittedAt DESC LIMIT 50`; 
            } else { 
                sql += ` WHERE u.status = 'pending' ORDER BY u.submittedAt ASC`; 
            } 
            const updates = await db.all(sql); 
            res.json({ success: true, updates }); 
        } catch (e) { res.status(500).json({ success: false, error: sanitizeError(e) }); } 
    });

    router.post('/admin/approve-update', requireAdmin, async (req, res) => { 
        const { id } = req.body; 
        try { 
            const request = await db.get(`SELECT * FROM token_updates WHERE id = $1`, [id]); 
            if (!request) return res.status(404).json({ success: false, error: "Request not found" }); 
            const token = await db.get(`SELECT metadata FROM tokens WHERE mint = $1`, [request.mint]); 
            let currentMeta = {}; 
            if (token && token.metadata) { 
                try { currentMeta = typeof token.metadata === 'string' ? JSON.parse(token.metadata) : token.metadata; } catch (_e) { /* ignore */ } 
            } 
            
            const newCommunity = { 
                twitter: sanitizeUrl(request.twitter), 
                website: sanitizeUrl(request.website), 
                telegram: sanitizeUrl(request.telegram), 
                banner: sanitizeUrl(request.banner), 
                description: request.description
            }; 
            
            currentMeta.community = { ...(currentMeta.community || {}), ...newCommunity }; 
            const jsonStr = JSON.stringify(currentMeta); 
            
            await db.run(`UPDATE tokens SET metadata = $1, hasCommunityUpdate = TRUE, updated_at = CURRENT_TIMESTAMP WHERE mint = $2`, [jsonStr, request.mint]);
            await db.run(`UPDATE token_updates SET status = 'approved' WHERE id = $1`, [id]);

            // Add token to Helius webhook for real-time holder tracking (if enabled)
            if (config.USE_WEBHOOKS) {
                try {
                    await addTokenToMasterWebhook(db, request.mint);
                    logger.info(`[Webhook] Added ${request.mint.slice(0,8)} to master webhook`);
                } catch (webhookErr) {
                    logger.warn(`[Webhook] Failed to add ${request.mint.slice(0,8)}: ${webhookErr.message}`);
                }
            }

            await updateSingleToken({ db }, request.mint);
            res.json({ success: true });
        } catch (e) { res.status(500).json({ success: false, error: sanitizeError(e) }); }
    });

    router.post('/admin/reject-update', requireAdmin, async (req, res) => { const { id } = req.body; try { await db.run(`UPDATE token_updates SET status = 'rejected' WHERE id = $1`, [id]); res.json({ success: true }); } catch (e) { res.status(500).json({ success: false, error: sanitizeError(e) }); } });
    router.get('/admin/token/:mint', requireAdmin, async (req, res) => { const { mint } = req.params; if (!isValidPubkey(mint)) return res.status(400).json({ success: false, error: "Invalid mint" }); try { const token = await db.get(`SELECT * FROM tokens WHERE mint = $1`, [mint]); if (!token) return res.status(404).json({ success: false, error: "Token not found" }); let meta = {}; try { if (typeof token.metadata === 'string') meta = JSON.parse(token.metadata); else meta = token.metadata || {}; } catch(_e) { /* ignore */ } const community = meta.community || {}; res.json({ success: true, token: { ...token, ticker: token.symbol, twitter: community.twitter || meta.twitter, website: community.website || meta.website, telegram: community.telegram || meta.telegram, banner: community.banner || meta.banner, description: community.description || meta.description } }); } catch (e) { res.status(500).json({ success: false, error: sanitizeError(e) }); } });
    
    router.post('/admin/update-token', requireAdmin, async (req, res) => {
        const { mint, twitter, website, telegram, banner, description } = req.body;
        if (!mint || !isValidPubkey(mint)) return res.status(400).json({ success: false, error: "Invalid mint" });
        try {
            const token = await db.get(`SELECT metadata FROM tokens WHERE mint = $1`, [mint]); 
            let currentMeta = {}; 
            if (token && token.metadata) { 
                try { currentMeta = typeof token.metadata === 'string' ? JSON.parse(token.metadata) : token.metadata; } catch (_e) { /* ignore */ } 
            } 
            
            const newCommunity = { 
                twitter: sanitizeUrl(twitter), 
                website: sanitizeUrl(website), 
                telegram: sanitizeUrl(telegram), 
                banner: sanitizeUrl(banner), 
                description: description 
            }; 
            
            currentMeta.community = { ...(currentMeta.community || {}), ...newCommunity }; 
            const jsonStr = JSON.stringify(currentMeta); 
            await db.run(`UPDATE tokens SET metadata = $1, hasCommunityUpdate = TRUE WHERE mint = $2`, [jsonStr, mint]); 
            await updateSingleToken({ db }, mint); 
            res.json({ success: true }); 
        } catch (e) { res.status(500).json({ success: false, error: sanitizeError(e) }); } 
    });
    
    router.post('/admin/delete-token', requireAdmin, async (req, res) => {
        const { mint } = req.body;
        if (!mint || !isValidPubkey(mint)) return res.status(400).json({ success: false, error: "Invalid mint" });
        try {
            const pools = await db.all(`SELECT address FROM pools WHERE mint = $1`, [mint]); 
            const poolAddresses = pools.map(p => p.address); 
            
            if (poolAddresses.length > 0) { 
                await db.run(`DELETE FROM candles_1m WHERE pool_address = ANY($1)`, [poolAddresses]); 
                await db.run(`DELETE FROM active_trackers WHERE pool_address = ANY($1)`, [poolAddresses]); 
            } 
            
            await db.run(`DELETE FROM pools WHERE mint = $1`, [mint]); 
            await db.run(`DELETE FROM k_scores WHERE mint = $1`, [mint]); 
            await db.run(`DELETE FROM token_updates WHERE mint = $1`, [mint]); 
            await db.run(`DELETE FROM holders_history WHERE mint = $1`, [mint]); 
            await db.run(`DELETE FROM tokens WHERE mint = $1`, [mint]); 
            
            const redis = getClient(); 
            if (redis) { await redis.del(`token:detail:${mint}`); } 
            
            res.json({ success: true, message: "Token and all history permanently deleted." }); 
        } catch (e) { res.status(500).json({ success: false, error: sanitizeError(e) }); } 
    });
    
    // K-Score refresh with per-token rate limiting (1 hour cooldown)
    router.post('/admin/refresh-kscore', requireAdmin, async (req, res) => {
        const { mint, force } = req.body;
        if (!mint || !isValidPubkey(mint)) return res.status(400).json({ success: false, error: "Invalid mint" });

        try {
            // Per-token rate limiting (skip if force=true from admin)
            if (!force) {
                const redis = getClient();
                if (redis) {
                    const cooldownKey = `kscore:cooldown:${mint}`;
                    const lastRun = await redis.get(cooldownKey);
                    if (lastRun) {
                        const elapsed = Date.now() - parseInt(lastRun);
                        const remaining = Math.ceil((3600000 - elapsed) / 60000); // minutes
                        return res.status(429).json({
                            success: false,
                            error: `Rate limited. Try again in ${remaining} min.`,
                            lastRun: new Date(parseInt(lastRun)).toISOString()
                        });
                    }
                    // Set cooldown (1 hour TTL)
                    await redis.set(cooldownKey, Date.now().toString(), { EX: 3600 });
                }
            }

            const newScore = await updateSingleToken({ db }, mint);
            res.json({ success: true, message: `K-Score Updated: ${newScore}` });
        } catch (e) { res.status(500).json({ success: false, error: sanitizeError(e) }); }
    });

    // Bulk K-Score refresh (all verified tokens)
    // CAUTION: This will consume significant RPC credits
    router.post('/admin/refresh-all-kscores', requireAdmin, async (req, res) => {
        const { deepRefresh } = req.body;

        try {
            // Check cooldown (1 hour minimum between bulk refreshes)
            const redis = getClient();
            if (redis) {
                const cooldownKey = 'kscore:bulk:cooldown';
                const lastRun = await redis.get(cooldownKey);
                if (lastRun) {
                    const elapsed = Date.now() - parseInt(lastRun);
                    const remaining = Math.ceil((3600000 - elapsed) / 60000); // minutes
                    return res.status(429).json({
                        success: false,
                        error: `Bulk refresh rate limited. Try again in ${remaining} min.`,
                        lastRun: new Date(parseInt(lastRun)).toISOString()
                    });
                }
                // Set cooldown (1 hour TTL)
                await redis.set(cooldownKey, Date.now().toString(), { EX: 3600 });
            }

            // Trigger bulk update (runs async)
            const broadcast = () => {}; // No WebSocket broadcast from admin panel
            updateKScores({ db, broadcast, forceDeepRefresh: !!deepRefresh }).catch(err => {
                logger.error(`[Admin] Bulk K-Score refresh failed: ${err.message}`);
            });

            res.json({
                success: true,
                message: `Bulk K-Score refresh started${deepRefresh ? ' (deep mode)' : ''}. Check logs for progress.`
            });
        } catch (e) {
            res.status(500).json({ success: false, error: sanitizeError(e) });
        }
    });

    // --- API KEY ADMIN ---
    router.get('/admin/keys', requireAdmin, async (req, res) => {
        try {
            const keys = await db.all('SELECT key_hash, key_prefix, owner, tier, requests_limit, requests_today, is_active, created_at FROM api_keys ORDER BY created_at DESC');
            // Admin sees key_hash (first 8 chars) + prefix, not full keys
            const maskedKeys = keys.map(k => ({
                key_id: k.key_hash.substring(0, 8),
                key_preview: k.key_prefix + '...****',
                owner: k.owner,
                tier: k.tier,
                requests_limit: k.requests_limit,
                requests_today: k.requests_today,
                is_active: k.is_active,
                created_at: k.created_at
            }));
            res.json({ success: true, keys: maskedKeys });
        } catch (e) { res.status(500).json({ success: false, error: sanitizeError(e) }); }
    });

    router.post('/admin/generate-key', requireAdmin, async (req, res) => {
        const { owner, tier } = req.body;
        if (!owner) return res.status(400).json({ success: false, error: "Owner name required" });
        try {
            const key = 'hx_' + require('crypto').randomBytes(16).toString('hex');
            const keyHash = hashApiKey(key);
            const keyPrefix = key.substring(0, 7);
            const limit = tier === 'pro' ? 100000 : (tier === 'enterprise' ? 1000000 : 1000);
            await db.run(`INSERT INTO api_keys (key_hash, key_prefix, owner, tier, requests_limit, created_at) VALUES ($1, $2, $3, $4, $5, $6)`, [keyHash, keyPrefix, owner, tier || 'free', limit, Date.now()]);
            res.json({ success: true, key, message: "Save this key! It won't be shown again." });
        } catch (e) { res.status(500).json({ success: false, error: sanitizeError(e) }); }
    });

    // SECURITY: Escape LIKE special characters to prevent pattern injection
    const escapeLikePattern = (str) => str.replace(/[%_\\]/g, '\\$&');

    router.post('/admin/update-key', requireAdmin, async (req, res) => {
        const { key_id, tier, limit } = req.body;
        try {
            if (!key_id || typeof key_id !== 'string' || key_id.length < 8) {
                return res.status(400).json({ success: false, error: "Valid key_id required (min 8 chars)" });
            }
            // SECURITY: Escape LIKE pattern and validate tier
            const VALID_TIERS = ['free', 'pro', 'enterprise'];
            const safeTier = VALID_TIERS.includes(tier) ? tier : 'free';
            const safeLimit = Math.max(0, Math.min(parseInt(limit) || 1000, 10000000));
            const safePattern = escapeLikePattern(key_id) + '%';

            await db.run(`UPDATE api_keys SET tier = $1, requests_limit = $2 WHERE key_hash LIKE $3 ESCAPE '\\'`, [safeTier, safeLimit, safePattern]);
            res.json({ success: true, message: "Key Updated Successfully" });
        } catch (e) { res.status(500).json({ success: false, error: sanitizeError(e) }); }
    });

    router.post('/admin/revoke-key', requireAdmin, async (req, res) => {
        const { key_id } = req.body;
        try {
            if (!key_id || typeof key_id !== 'string' || key_id.length < 8) {
                return res.status(400).json({ success: false, error: "Valid key_id required" });
            }
            const safePattern = escapeLikePattern(key_id) + '%';
            await db.run(`UPDATE api_keys SET is_active = FALSE WHERE key_hash LIKE $1 ESCAPE '\\'`, [safePattern]);
            res.json({ success: true, message: "Key Revoked" });
        } catch (e) { res.status(500).json({ success: false, error: sanitizeError(e) }); }
    });

    router.post('/admin/delete-key', requireAdmin, async (req, res) => {
        const { key_id } = req.body;
        try {
            if (!key_id || typeof key_id !== 'string' || key_id.length < 8) {
                return res.status(400).json({ success: false, error: "Valid key_id required" });
            }
            const safePattern = escapeLikePattern(key_id) + '%';
            await db.run(`DELETE FROM api_keys WHERE key_hash LIKE $1 ESCAPE '\\'`, [safePattern]);
            res.json({ success: true, message: "Key Deleted" });
        } catch (e) { res.status(500).json({ success: false, error: sanitizeError(e) }); }
    });

    router.get('/admin/backup/updates', requireAdmin, async (req, res) => {
        try {
            const updates = await db.all('SELECT * FROM token_updates ORDER BY submittedAt DESC');
            // SECURITY: Never expose key_hash in backup - only metadata
            const keys = await db.all(`
                SELECT key_prefix, owner, wallet, tier, requests_limit, requests_today,
                       last_reset, is_active, created_at
                FROM api_keys
            `);
            res.setHeader('Content-Type', 'application/json');
            res.setHeader('Content-Disposition', `attachment; filename=holdex_backup_${Date.now()}.json`);
            res.json({
                success: true,
                timestamp: Date.now(),
                updates: updates,
                api_keys_metadata: keys,
                warning: 'API key hashes excluded for security. Keys must be regenerated after restore.'
            });
        } catch (e) {
            res.status(500).json({ success: false, error: sanitizeError(e) });
        }
    });
    
    router.post('/admin/restore/updates', requireAdmin, async (req, res) => { 
        const { updates, api_keys } = req.body; 
        
        if (!Array.isArray(updates) && !Array.isArray(api_keys)) {
             return res.status(400).json({ success: false, error: "Invalid data format." }); 
        }
        
        const results = {
            updates: { restored: 0, skipped: 0, merged: 0 },
            keys: { restored: 0, skipped: 0, merged: 0 }
        };

        const affectedMints = new Set();

        try {
            if (Array.isArray(updates)) {
                for (const u of updates) {
                    try {
                        if (u.mint) {
                            const tokenExists = await db.get('SELECT mint FROM tokens WHERE mint = $1', [u.mint]);
                            if (!tokenExists) {
                                try {
                                    await indexTokenOnChain(u.mint);
                                } catch (idxErr) {
                                    console.error(`Auto-index failed for restored token ${u.mint}: ${idxErr.message}`);
                                }
                            }
                        }

                        const sig = u.signature || u.txId || 'manual_' + Date.now();
                        const existing = await db.get('SELECT * FROM token_updates WHERE signature = $1', [sig]);
                        
                        if (existing) {
                            const fields = ['twitter', 'website', 'telegram', 'banner', 'description', 'payer', 'status'];
                            const sets = [];
                            const vals = [];
                            let idx = 1;

                            for(const f of fields) {
                                if ((existing[f] === null || existing[f] === '') && (u[f] !== null && u[f] !== undefined && u[f] !== '')) {
                                    sets.push(`${f} = $${idx++}`);
                                    vals.push(u[f]);
                                }
                            }

                            if (sets.length > 0) {
                                vals.push(existing.id);
                                await db.run(`UPDATE token_updates SET ${sets.join(', ')} WHERE id = $${idx}`, vals);
                                results.updates.merged++;
                                affectedMints.add(u.mint);
                            } else {
                                results.updates.skipped++;
                            }
                        } else {
                            const timestamp = u.submittedAt || u.submittedat || Date.now();
                            const status = u.status || 'pending';
                            await db.run(`
                                INSERT INTO token_updates (mint, twitter, website, telegram, banner, description, status, signature, payer, submittedAt)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                            `, [u.mint, u.twitter, u.website, u.telegram, u.banner, u.description, status, sig, u.payer || 'unknown', timestamp]);
                            results.updates.restored++;
                            affectedMints.add(u.mint);
                        }

                        const status = u.status || (existing ? existing.status : 'pending');
                        
                        if (status === 'approved') {
                            const token = await db.get('SELECT metadata, hasCommunityUpdate FROM tokens WHERE mint = $1', [u.mint]);
                            if (token) {
                                let meta = {};
                                try { meta = typeof token.metadata === 'string' ? JSON.parse(token.metadata) : token.metadata || {}; } catch(_e) { /* ignore */ }
                                
                                meta.community = meta.community || {};
                                let changed = false;
                                
                                const syncFields = ['twitter', 'website', 'telegram', 'banner', 'description'];
                                
                                for (const f of syncFields) {
                                    if ((!meta.community[f] || meta.community[f] === "") && (u[f] && u[f] !== "")) {
                                        meta.community[f] = u[f];
                                        changed = true;
                                    }
                                }

                                if (changed || !token.hasCommunityUpdate) {
                                    await db.run(`
                                        UPDATE tokens 
                                        SET metadata = $1, hasCommunityUpdate = TRUE, updated_at = CURRENT_TIMESTAMP 
                                        WHERE mint = $2
                                    `, [JSON.stringify(meta), u.mint]);
                                    affectedMints.add(u.mint);
                                }
                            }
                        }

                    } catch (e) { console.error(`Update Restore Error: ${e.message}`); }
                }
            }

            if (Array.isArray(api_keys)) {
                for (const k of api_keys) {
                    try {
                        const existing = await db.get('SELECT * FROM api_keys WHERE key = $1', [k.key]);
                        if (existing) {
                            const fields = ['owner', 'tier', 'requests_limit']; 
                            const sets = [];
                            const vals = [];
                            let idx = 1;

                            for(const f of fields) {
                                const inputVal = k[f] || k[f.replace(/_([a-z])/g, g => g[1].toUpperCase())];
                                if ((existing[f] === null || existing[f] === '') && (inputVal !== undefined && inputVal !== null)) {
                                    sets.push(`${f} = $${idx++}`);
                                    vals.push(inputVal);
                                }
                            }
                            
                            if (sets.length > 0) {
                                vals.push(existing.key);
                                await db.run(`UPDATE api_keys SET ${sets.join(', ')} WHERE key = $${idx}`, vals);
                                results.keys.merged++;
                            } else {
                                results.keys.skipped++;
                            }
                            continue;
                        }
                        
                        const createdAt = k.created_at || k.createdAt || Date.now();
                        const limit = k.requests_limit || k.requestsLimit || 1000;
                        const active = (k.is_active !== undefined) ? k.is_active : true;

                        await db.run(`
                            INSERT INTO api_keys (key, owner, tier, requests_limit, is_active, created_at)
                            VALUES ($1, $2, $3, $4, $5, $6)
                        `, [k.key, k.owner || 'Restored User', k.tier || 'free', limit, active, createdAt]);
                        
                        results.keys.restored++;
                    } catch (e) { console.error(`Key Restore Error: ${e.message}`); }
                }
            }

            if (affectedMints.size > 0) {
                (async () => {
                    for (const mint of affectedMints) {
                        await updateSingleToken({ db }, mint);
                    }
                })();
            }

            res.json({ success: true, results, message: `Update Log: ${results.updates.restored} added, ${results.updates.merged} merged. Keys: ${results.keys.restored} added, ${results.keys.merged} merged.` });

        } catch (e) { 
            res.status(500).json({ success: false, error: sanitizeError(e) }); 
        }
    });

    /**
     * GET /api/token/:mint/evolution
     * K-Score evolution with price correlation for overlay charts
     * SECURITY: Only available for verified tokens (hasCommunityUpdate=TRUE)
     * NOTE: Must be defined BEFORE /token/:mint to avoid route conflict
     * SECURITY: Rate limited (M2)
     */
    router.get('/token/:mint/evolution', cacheControl(60, 120), proxyRateLimit, async (req, res) => {
        const { mint } = req.params;
        const { days = 30 } = req.query;

        if (!isValidPubkey(mint)) {
            return res.status(400).json({ success: false, error: 'Invalid mint address' });
        }

        try {
            const token = await db.get(`
                SELECT symbol, name, hasCommunityUpdate, priceusd, marketcap, k_score
                FROM tokens WHERE mint = $1
            `, [mint]);

            if (!token) {
                return res.status(404).json({ success: false, error: 'Token not found' });
            }

            if (!(token.hascommunityupdate || token.hasCommunityUpdate)) {
                return res.json({
                    success: true,
                    mint,
                    verified: false,
                    message: 'K-Score evolution requires community verification',
                    evolution: []
                });
            }

            // SECURITY: Validate and clamp days parameter
            const daysNum = Math.min(Math.max(parseInt(days) || 30, 1), 365);

            const kScoreHistory = await db.all(`
                SELECT date, k_score, conviction_score, holders
                FROM k_score_history
                WHERE mint = $1
                  AND date >= CURRENT_DATE - $2 * INTERVAL '1 day'
                ORDER BY date ASC
            `, [mint, daysNum]);

            const holderHistory = await db.all(`
                SELECT date, holders, real_holders
                FROM holder_history
                WHERE mint = $1
                  AND date >= CURRENT_DATE - $2 * INTERVAL '1 day'
                ORDER BY date ASC
            `, [mint, daysNum]);

            const timeline = new Map();
            kScoreHistory.forEach(h => {
                const dateKey = new Date(h.date).toISOString().split('T')[0];
                timeline.set(dateKey, { date: dateKey, kScore: h.k_score, conviction: h.conviction_score, holders: h.holders });
            });
            holderHistory.forEach(h => {
                const dateKey = new Date(h.date).toISOString().split('T')[0];
                const existing = timeline.get(dateKey) || { date: dateKey };
                existing.holders = h.holders || existing.holders;
                existing.realHolders = h.real_holders;
                timeline.set(dateKey, existing);
            });

            const evolution = Array.from(timeline.values()).sort((a, b) => new Date(a.date) - new Date(b.date));

            let trajectory = 'stable';
            if (evolution.length >= 3) {
                const firstWeek = evolution.slice(0, Math.ceil(evolution.length / 3));
                const lastWeek = evolution.slice(-Math.ceil(evolution.length / 3));
                const avgFirst = firstWeek.reduce((sum, e) => sum + (e.kScore || 0), 0) / firstWeek.length;
                const avgLast = lastWeek.reduce((sum, e) => sum + (e.kScore || 0), 0) / lastWeek.length;
                const delta = avgLast - avgFirst;
                if (delta > 5) trajectory = 'improving';
                else if (delta < -5) trajectory = 'declining';
            }

            res.json({
                success: true, mint, symbol: token.symbol, name: token.name, verified: true,
                current: { kScore: token.k_score, price: token.priceusd, marketCap: token.marketcap },
                trajectory, dataPoints: evolution.length, evolution
            });

        } catch (e) {
            logger.error(`[Evolution] Error: ${e.message}`);
            res.status(500).json({ success: false, error: sanitizeError(e) });
        }
    });

    router.get('/token/:mint', cacheControl(3, 5), unifiedRateLimiter, async (req, res) => {
        const { mint } = req.params;

        // SECURITY: Validate mint address
        if (!isValidPubkey(mint)) {
            return res.status(400).json({ success: false, error: 'Invalid mint address' });
        }

        const cacheKey = `token:detail:${mint}`;
        try {
            const result = await smartCache(cacheKey, 5, async () => {
                // OPTIMIZATION: Parallel queries (3x faster)
                let [token, pairs, holderHistory] = await Promise.all([
                    db.get('SELECT * FROM tokens WHERE mint = $1', [mint]),
                    db.all('SELECT * FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC', [mint]),
                    db.all('SELECT count, timestamp FROM holders_history WHERE mint = $1 ORDER BY timestamp ASC LIMIT 100', [mint])
                ]);

                if (!token) {
                    try {
                        const indexed = await indexTokenOnChain(mint);
                        token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
                        pairs = indexed.pairs || [];
                    } catch (_e) { /* ignore */ }
                }

                if (!token) return { success: false, error: "Token not found" };

                // DATA INTEGRITY: Verify token signature (Proof-of-History)
                const { verified: dataVerified, tampered, reason } = dataVerification.verifySingleToken(token);
                if (tampered) {
                    logger.warn(`[DataVerify] Single token tampered: ${mint} - ${reason}`);
                }

                const now = Date.now();
                const isStale = !token.timestamp || (now - token.timestamp > 300000); 
                if (isStale && pairs.length > 0 && !pendingRefreshes.has(mint)) {
                    pendingRefreshes.add(mint);
                    
                    // FIRE-AND-FORGET
                    (async () => {
                        try {
                            const poolAddresses = pairs.map(p => p.address);
                            await snapshotPools(poolAddresses);
                            await aggregateAndSaveToken(db, mint);
                        } catch (err) { 
                            logger.warn(`Lazy refresh failed for ${mint}: ${err.message}`); 
                        } finally { 
                            pendingRefreshes.delete(mint); 
                        }
                    })();
                }

                let tokenData = { ...token };
                tokenData.marketCap = tokenData.marketCap || tokenData.marketcap || 0;
                tokenData.priceUsd = tokenData.priceUsd || tokenData.priceusd || 0;
                tokenData.volume24h = tokenData.volume24h || 0;
                tokenData.holders = tokenData.holders || 0;

                // SECURITY: K-Score and conviction only for verified tokens
                // Native tokens get special tier regardless of verification
                const isNative = isNativeToken(mint);
                const isVerified = token.hasCommunityUpdate || token.hascommunityupdate || false;
                tokenData.kScore = isVerified ? (tokenData.k_score || tokenData.kScore || 0) : null;
                tokenData.kRank = isNative ? getKRank(null, mint) : (tokenData.kScore !== null ? getKRank(tokenData.kScore, mint) : null);
                tokenData.creditRating = isNative
                    ? await getCreditRatingWithTrajectory(db, mint, null)
                    : (tokenData.kScore !== null ? await getCreditRatingWithTrajectory(db, mint, tokenData.kScore) : null);

                // GASdf Integration Fields
                // Burn data
                tokenData.burnedAmount = token.burned_amount || 0;
                tokenData.burnedPercent = token.burned_percent || 0;
                tokenData.initialSupply = token.initial_supply || token.supply;

                // Token category
                tokenData.isPumpFun = token.is_pump_fun || false;
                tokenData.bondingCurveComplete = token.bonding_curve_complete || false;
                tokenData.launchDate = token.timestamp ? new Date(parseInt(token.timestamp)).toISOString() : null;

                // Conviction breakdown - only for verified tokens
                // Holder Role Metals: Diamond (Accumulators), Gold (Holders), Silver (Reducers), Rust (Extractors)
                tokenData.conviction = isVerified ? {
                    score: token.conviction_score || 0,
                    accumulators: token.conviction_accumulators || 0,
                    holders: token.conviction_holders || 0,
                    reducers: token.conviction_reducers || 0,
                    extractors: token.conviction_extractors || 0,
                    analyzed: token.conviction_analyzed || 0,
                    // Holder role metals for UI display
                    roles: {
                        accumulator: { icon: '💎', metal: 'Diamond', impact: 'bullish' },
                        holder: { icon: '🥇', metal: 'Gold', impact: 'bullish' },
                        reducer: { icon: '🥈', metal: 'Silver', impact: 'neutral' },
                        extractor: { icon: '🔩', metal: 'Rust', impact: 'bearish' }
                    }
                } : null;

                // Mayhem Mode (mutable supply) fields
                tokenData.security = {
                    mintAuthorityRevoked: token.mint_authority_revoked || false,
                    freezeAuthorityRevoked: token.freeze_authority_revoked || false,
                    isMutableSupply: token.is_mutable_supply || false
                };

                // Supply change tracking
                tokenData.supplyChange24h = token.supply_change_24h || 0;
                tokenData.supplyLastCheck = token.supply_last_check || 0;

                // --- NORMALIZE UPDATED STATUS ---
                // Force boolean conversion for frontend consistency
                const rawStatus = token.hasCommunityUpdate || token.hascommunityupdate;
                tokenData.hasCommunityUpdate = (rawStatus === true || rawStatus === 1 || rawStatus === 'true');

                // --- FORMAT PAIRS FOR FRONTEND ---
                const formattedPairs = pairs.map(p => ({
                    dexId: p.dex || p.dex_id || p.dexId || 'unknown',
                    priceUsd: p.price_usd || p.priceUsd || 0,
                    liquidity: { usd: p.liquidity_usd || (p.liquidity && p.liquidity.usd) || 0 },
                    address: p.address
                }));

                if (tokenData.symbol) tokenData.ticker = tokenData.symbol;
                if (tokenData.metadata) { try { const meta = typeof tokenData.metadata === 'string' ? JSON.parse(tokenData.metadata) : tokenData.metadata; const comm = meta.community || {}; tokenData.banner = comm.banner || meta.banner; tokenData.description = comm.description || meta.description; tokenData.twitter = comm.twitter || meta.twitter; tokenData.telegram = comm.telegram || meta.telegram; tokenData.website = comm.website || meta.website; } catch (_e) { /* ignore */ } }
                if (pairs.length > 0) { const mainPool = pairs[0]; if (mainPool.price_usd > 0) tokenData.priceUsd = mainPool.price_usd; }

                // Add data integrity status
                tokenData._dataVerified = dataVerified;
                tokenData._integrityStatus = tampered ? 'tampered' : (dataVerified ? 'verified' : 'unsigned');

                // Add node validation info (non-blocking)
                let validation = null;
                try {
                    validation = await nodeService.getTokenValidation(db, mint);
                } catch (_e) {
                    // Non-critical, continue without validation info
                }

                return { success: true, token: { ...tokenData, pairs: formattedPairs, holderHistory, validation } };
            });
            res.json(result);
        } catch(e) { res.status(500).json({ success: false, error: sanitizeError(e) }); }
    });

    // --- K-SCORE CARD IMAGE (for Twitter/social sharing) ---
    // Styles: holdex (default), asdf, minimal
    // Legacy modes: full=holdex, simple/minimal=minimal, fire=asdf
    router.get('/token/:mint/card.png', async (req, res) => {
        const { mint } = req.params;
        const { style, mode } = req.query;

        // Validate mint address
        if (!isValidPubkey(mint)) {
            return res.status(400).json({ success: false, error: 'Invalid mint address' });
        }

        try {
            // Get token data
            const token = await db.get(`
                SELECT
                    mint, name, symbol, image,
                    k_score, holders, marketcap,
                    conviction_accumulators, conviction_holders,
                    conviction_reducers, conviction_extractors
                FROM tokens
                WHERE mint = $1
            `, [mint]);

            if (!token) {
                return res.status(404).json({ success: false, error: 'Token not found' });
            }

            // Determine style (new param) or convert from legacy mode
            const cardGen = getCardGenerator();
            const cardStyle = style || cardGen.styleFromMode(mode) || 'holdex';

            // Generate the card image
            const imageBuffer = await cardGen.generateKScoreCard({
                mint: token.mint,
                name: token.name,
                symbol: token.symbol,
                image: token.image,
                k_score: token.k_score || 0,
                holders: token.holders || 0,
                marketCap: token.marketcap || 0,
                conviction_accumulators: token.conviction_accumulators || 0,
                conviction_holders: token.conviction_holders || 0,
                conviction_reducers: token.conviction_reducers || 0,
                conviction_extractors: token.conviction_extractors || 0
            }, cardStyle);

            // Set caching headers (cache for 5 minutes)
            res.set({
                'Content-Type': 'image/png',
                'Cache-Control': 'public, max-age=300',
                'Content-Length': imageBuffer.length
            });

            res.send(imageBuffer);
        } catch (e) {
            logger.error(`Card generation failed for ${mint}: ${e.message}`);
            res.status(500).json({ success: false, error: 'Failed to generate card' });
        }
    });

    // --- TOP HOLDERS (for Orb AI integration) ---
    // SECURITY: Only return holder data for verified tokens (deep analysis done)
    router.get('/token/:mint/top-holders', cacheControl(60, 120), unifiedRateLimiter, async (req, res) => {
        const { mint } = req.params;

        // SECURITY: Validate mint address
        if (!isValidPubkey(mint)) {
            return res.status(400).json({ success: false, error: 'Invalid mint address' });
        }

        try {
            // SECURITY: Only return data for verified tokens
            const token = await db.get('SELECT decimals, hasCommunityUpdate FROM tokens WHERE mint = $1', [mint]);

            if (!token || !(token.hasCommunityUpdate || token.hascommunityupdate)) {
                return res.json({
                    success: true,
                    mint,
                    verified: false,
                    message: 'Top holder analysis requires community verification',
                    count: 0,
                    holders: []
                });
            }

            const decimals = token.decimals || 6;

            const holders = await db.all(`
                SELECT
                    holder,
                    balance,
                    conviction_class,
                    buy_count,
                    sell_count,
                    net_flow,
                    updated_at
                FROM holder_snapshots
                WHERE mint = $1
                ORDER BY balance DESC
                LIMIT 20
            `, [mint]);

            // Holder role metals mapping
            const ROLE_METALS = {
                accumulator: { icon: '💎', metal: 'Diamond', impact: 'bullish' },
                holder: { icon: '🥇', metal: 'Gold', impact: 'bullish' },
                reducer: { icon: '🥈', metal: 'Silver', impact: 'neutral' },
                extractor: { icon: '🔩', metal: 'Rust', impact: 'bearish' }
            };

            res.json({
                success: true,
                mint,
                decimals,
                count: holders.length,
                holders: holders.map(h => {
                    const role = ROLE_METALS[h.conviction_class] || ROLE_METALS.holder;
                    return {
                        address: h.holder,
                        balance: h.balance,
                        class: h.conviction_class || 'unknown',
                        role: {
                            icon: role.icon,
                            metal: role.metal,
                            impact: role.impact
                        },
                        buys: h.buy_count || 0,
                        sells: h.sell_count || 0,
                        netFlow: h.net_flow || 0,
                        orbUrl: `https://orbmarkets.io/address/${h.holder}`
                    };
                })
            });
        } catch(e) {
            res.status(500).json({ success: false, error: sanitizeError(e) });
        }
    });

    /**
     * PUBLIC Dashboard Endpoint
     * No auth required - for homepage display
     * K-Score only shown for verified tokens
     * Rate limited by IP (generous)
     */
    const publicRateLimit = rateLimit({
        windowMs: 60 * 1000,
        max: 60, // 60 req/min per IP
        message: { success: false, error: 'Rate limit. Try again shortly.' }
    });

    router.get('/tokens/public', cacheControl(5, 15), publicRateLimit, async (req, res) => {
        let { sort = 'volume', page = 1, direction = 'desc', limit = 20, filter, search = '' } = req.query;
        try {
            limit = Math.min(Math.max(parseInt(limit) || 20, 1), 50);
            page = Math.max(parseInt(page) || 1, 1);

            const redis = getClient();
            const cacheKey = `public:tokens:${sort}:${page}:${direction}:${limit}:${filter || 'all'}:${search}`;

            if (redis && !search) {
                try {
                    const cached = await redis.get(cacheKey);
                    if (cached) {
                        res.setHeader('X-Cache', 'HIT');
                        return res.json(JSON.parse(cached));
                    }
                } catch (_) { /* ignore cache errors */ }
            }

            let rows = [];

            // Handle search
            const selectFields = `mint, name, symbol, image, priceUsd, marketCap, volume24h, change24h, change1h, change5m, holders, timestamp, hasCommunityUpdate, k_score,
                                  conviction_score, conviction_accumulators, conviction_holders, conviction_reducers, conviction_extractors`;
            if (search.length > 0) {
                const isAddress = isValidPubkey(search);
                if (isAddress) {
                    rows = await db.all(`SELECT ${selectFields} FROM tokens WHERE mint = $1`, [search]);
                } else {
                    // SECURITY: Escape LIKE pattern to prevent injection (M9)
                    const safeSearch = `%${escapeLikePattern(search)}%`;
                    rows = await db.all(`SELECT ${selectFields} FROM tokens WHERE (symbol ILIKE $1 OR name ILIKE $1) LIMIT $2`, [safeSearch, limit]);
                }
            } else {
                const dir = direction === 'asc' ? 'ASC' : 'DESC';
                let sortCol = 'volume24h';
                switch(sort) {
                    case 'newest': sortCol = 'timestamp'; break;
                    case 'mcap': sortCol = 'marketCap'; break;
                    case 'volume': sortCol = 'volume24h'; break;
                    case '24h': sortCol = 'change24h'; break;
                    case '1h': sortCol = 'change1h'; break;
                    case 'holders': sortCol = 'holders'; break;
                    case 'kscore': sortCol = 'k_score'; break;
                    default: sortCol = 'volume24h';
                }

                // SECURITY: Whitelist filter
                let whereClause = 'WHERE volume24h > 0';
                if (filter === 'verified') {
                    whereClause += ' AND hasCommunityUpdate = TRUE';
                }

                const offset = (page - 1) * limit;
                rows = await db.all(
                    `SELECT ${selectFields} FROM tokens ${whereClause}
                     ORDER BY COALESCE(${sortCol}, 0) ${dir}
                     LIMIT $1 OFFSET $2`,
                    [limit, offset]
                );
            }

            const tokens = rows.map(r => {
                const isVerified = r.hascommunityupdate || r.hasCommunityUpdate || false;
                const isNative = isNativeToken(r.mint);
                const kScore = (isVerified || isNative) ? (r.k_score || 0) : null;
                const rank = kScore !== null ? getKRank(kScore, r.mint) : null;
                const credit = kScore !== null ? getCreditRating(kScore, r.mint) : null;

                return {
                    mint: r.mint,
                    name: r.name,
                    symbol: r.symbol,
                    // Aliases for frontend compatibility
                    ticker: r.symbol,
                    image: r.image,
                    // Price fields (both names for compatibility)
                    price: r.priceusd || r.priceUsd || 0,
                    priceUsd: r.priceusd || r.priceUsd || 0,
                    // MCap fields (both names)
                    mcap: r.marketcap || r.marketCap || 0,
                    marketCap: r.marketcap || r.marketCap || 0,
                    volume24h: r.volume24h || 0,
                    change24h: r.change24h || 0,
                    change1h: r.change1h || 0,
                    change5m: r.change5m || 0,
                    holders: r.holders || 0,
                    age: r.timestamp,
                    timestamp: r.timestamp,
                    // Verification (both names)
                    verified: isVerified,
                    hasCommunityUpdate: isVerified,
                    // K-Score (both names)
                    k_score: kScore,
                    kScore: kScore,
                    // Metal rank
                    metal_rank: rank?.tier || null,
                    metal_icon: rank?.icon || null,
                    // Credit rating
                    credit_rating: credit?.grade || null,
                    creditRating: credit?.grade || null,
                    // Conviction data (only for verified)
                    conviction_score: isVerified ? (r.conviction_score || 0) : null,
                    convictionScore: isVerified ? (r.conviction_score || 0) : null,
                    conviction_accumulators: isVerified ? (r.conviction_accumulators || 0) : null,
                    conviction_holders: isVerified ? (r.conviction_holders || 0) : null,
                    conviction_reducers: isVerified ? (r.conviction_reducers || 0) : null,
                    conviction_extractors: isVerified ? (r.conviction_extractors || 0) : null
                };
            });

            const response = { success: true, page, limit, tokens };

            if (redis) {
                try { await redis.setEx(cacheKey, 10, JSON.stringify(response)); } catch (_) { /* ignore */ }
            }

            res.setHeader('X-Cache', 'MISS');
            res.json(response);
        } catch(e) {
            logger.error('[PublicTokens]', e.message);
            res.status(500).json({ success: false, error: 'Server error' });
        }
    });

    // ============================================
    // PUBLIC TOKEN DETAIL (No Auth Required)
    // For frontend token detail view
    // ============================================
    router.get('/token/:mint/public', cacheControl(5, 15), publicRateLimit, async (req, res) => {
        const { mint } = req.params;

        // SECURITY: Validate mint address
        if (!isValidPubkey(mint)) {
            return res.status(400).json({ success: false, error: 'Invalid mint address' });
        }

        const cacheKey = `public:token:${mint}`;
        try {
            const redis = getClient();
            if (redis) {
                try {
                    const cached = await redis.get(cacheKey);
                    if (cached) {
                        res.setHeader('X-Cache', 'HIT');
                        return res.json(JSON.parse(cached));
                    }
                } catch (_) { /* ignore cache errors */ }
            }

            // Parallel queries for performance
            let [token, pairs, holderHistory] = await Promise.all([
                db.get('SELECT * FROM tokens WHERE mint = $1', [mint]),
                db.all('SELECT * FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 10', [mint]),
                db.all('SELECT count, timestamp FROM holders_history WHERE mint = $1 ORDER BY timestamp ASC LIMIT 100', [mint])
            ]);

            if (!token) {
                return res.status(404).json({ success: false, error: 'Token not found' });
            }

            // Build token data
            const isNative = isNativeToken(mint);
            const isVerified = token.hasCommunityUpdate || token.hascommunityupdate || false;

            const tokenData = {
                mint: token.mint,
                name: token.name,
                symbol: token.symbol,
                ticker: token.symbol,
                image: token.image,
                decimals: token.decimals || 9,
                // Market data with provenance
                priceUsd: parseFloat(token.priceusd) || 0,
                marketCap: parseFloat(token.marketcap) || 0,
                mcap: parseFloat(token.marketcap) || 0,
                liquidity: parseFloat(token.liquidity) || 0,
                volume24h: token.volume24h || 0,
                change24h: token.change24h || 0,
                change1h: token.change1h || 0,
                holders: token.holders || 0,
                // Data provenance (ONCHAIN IS TRUTH)
                priceSource: token.price_source || 'unknown',
                priceTimestamp: token.price_timestamp || 0,
                liquiditySource: token.liquidity_source || 'unknown',
                holdersSource: token.holders_source || 'unknown',
                ageDays: parseFloat(token.age_days) || 0,
                // K-Score (verified only)
                kScore: isVerified ? (token.k_score || 0) : null,
                kRank: isNative ? getKRank(null, mint) : (isVerified ? getKRank(token.k_score, mint) : null),
                creditRating: isNative
                    ? getCreditRating(null, mint)
                    : (isVerified ? getCreditRating(token.k_score, mint) : null),
                // Conviction breakdown
                conviction: isVerified ? {
                    score: token.conviction_score || 0,
                    accumulators: token.conviction_accumulators || 0,
                    holders: token.conviction_holders || 0,
                    reducers: token.conviction_reducers || 0,
                    extractors: token.conviction_extractors || 0,
                    analyzed: token.conviction_analyzed || 0
                } : null,
                // Security status
                security: {
                    mintAuthorityRevoked: token.mint_authority_revoked || false,
                    freezeAuthorityRevoked: token.freeze_authority_revoked || false,
                    isMutableSupply: token.is_mutable_supply || false
                },
                // LP status
                lp: {
                    burnPct: token.lp_burn_pct || 0,
                    lockedPct: token.lp_locked_pct || 0,
                    status: token.lp_status || 'unknown'
                },
                // Origin
                isPumpFun: token.is_pump_fun || false,
                bondingCurveComplete: token.bonding_curve_complete || false,
                launchDate: token.timestamp ? new Date(parseInt(token.timestamp)).toISOString() : null,
                hasCommunityUpdate: isVerified,
                verified: isVerified
            };

            // Parse metadata for banner, social links
            if (token.metadata) {
                try {
                    const meta = typeof token.metadata === 'string' ? JSON.parse(token.metadata) : token.metadata;
                    const comm = meta.community || {};
                    tokenData.banner = comm.banner || meta.banner;
                    tokenData.description = comm.description || meta.description;
                    tokenData.twitter = comm.twitter || meta.twitter;
                    tokenData.telegram = comm.telegram || meta.telegram;
                    tokenData.website = comm.website || meta.website;
                } catch (_e) { /* ignore */ }
            }

            // Format pairs
            const formattedPairs = pairs.map(p => ({
                dexId: p.dex || 'unknown',
                priceUsd: p.price_usd || 0,
                liquidity: { usd: p.liquidity_usd || 0 },
                address: p.address
            }));

            const response = { success: true, token: { ...tokenData, pairs: formattedPairs, holderHistory } };

            // Cache for 10 seconds
            if (redis) {
                try { await redis.setEx(cacheKey, 10, JSON.stringify(response)); } catch (_) { /* ignore */ }
            }

            res.setHeader('X-Cache', 'MISS');
            res.json(response);
        } catch(e) {
            logger.error('[PublicToken]', e.message);
            res.status(500).json({ success: false, error: 'Server error' });
        }
    });

    // ============================================
    // PUBLIC TOP HOLDERS (for frontend detail view)
    // ============================================
    router.get('/token/:mint/top-holders/public', cacheControl(30, 60), publicRateLimit, async (req, res) => {
        const { mint } = req.params;

        if (!isValidPubkey(mint)) {
            return res.status(400).json({ success: false, error: 'Invalid mint address' });
        }

        try {
            // Only return data for verified tokens
            const token = await db.get('SELECT decimals, hasCommunityUpdate FROM tokens WHERE mint = $1', [mint]);

            if (!token || !(token.hasCommunityUpdate || token.hascommunityupdate)) {
                return res.json({
                    success: true,
                    mint,
                    verified: false,
                    message: 'Top holder analysis requires community verification',
                    count: 0,
                    holders: []
                });
            }

            const decimals = token.decimals || 6;

            const holders = await db.all(`
                SELECT
                    holder,
                    balance,
                    conviction_class,
                    buy_count,
                    sell_count,
                    net_flow,
                    updated_at
                FROM holder_snapshots
                WHERE mint = $1
                ORDER BY balance DESC
                LIMIT 20
            `, [mint]);

            const ROLE_METALS = {
                accumulator: { icon: '💎', metal: 'Diamond', impact: 'bullish' },
                holder: { icon: '🥇', metal: 'Gold', impact: 'bullish' },
                reducer: { icon: '🥈', metal: 'Silver', impact: 'neutral' },
                extractor: { icon: '🔩', metal: 'Rust', impact: 'bearish' }
            };

            res.json({
                success: true,
                mint,
                decimals,
                count: holders.length,
                holders: holders.map(h => {
                    const role = ROLE_METALS[h.conviction_class] || ROLE_METALS.holder;
                    return {
                        address: h.holder,
                        balance: h.balance,
                        class: h.conviction_class || 'unknown',
                        role: {
                            icon: role.icon,
                            metal: role.metal,
                            impact: role.impact
                        },
                        buys: h.buy_count || 0,
                        sells: h.sell_count || 0,
                        netFlow: h.net_flow || 0,
                        orbUrl: `https://orbmarkets.io/address/${h.holder}`
                    };
                })
            });
        } catch(e) {
            logger.error('[PublicTopHolders]', e.message);
            res.status(500).json({ success: false, error: 'Server error' });
        }
    });

    // ============================================
    // PUBLIC CANDLES (GeckoTerminal - 0 RPC cost)
    // Philosophy: Charts are commodity data. K-Score is the signal.
    // ============================================
    router.get('/token/:mint/candles/public', cacheControl(30, 60), publicRateLimit, async (req, res) => {
        const { mint } = req.params;
        const { resolution = '5' } = req.query;

        if (!isValidPubkey(mint)) {
            return res.status(400).json({ success: false, error: 'Invalid mint address' });
        }

        try {
            // Get best pool for this token
            const bestPool = await db.get(
                'SELECT address FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1',
                [mint]
            );

            if (!bestPool) {
                return res.json({
                    success: false,
                    error: 'Token not indexed',
                    hint: 'Charts are noise. K-Score is signal.'
                });
            }

            // Fetch from GeckoTerminal (free, no RPC)
            const candles = await fetchExternalCandles(bestPool.address, resolution);

            res.json({
                success: true,
                candles,
                source: 'geckoterminal',
                disclaimer: 'Chart data via GeckoTerminal. For on-chain verified data, hold $ASDFASDFA.',
                philosophy: 'Charts are painted. K-Score is computed from on-chain behavior.'
            });

        } catch (e) {
            logger.error('[PublicCandles]', e.message);
            res.status(500).json({ success: false, error: 'Server error' });
        }
    });

    router.get('/tokens', cacheControl(2, 5), unifiedRateLimiter, async (req, res) => {
        let { search = '', sort = 'kscore', page = 1, filter, direction = 'desc', limit = 20 } = req.query;
        try {
            // Validate Limit
            limit = parseInt(limit);
            if (isNaN(limit) || limit < 1) limit = 20;
            if (limit > 100) limit = 100;
            
            page = parseInt(page);
            if (isNaN(page) || page < 1) page = 1;

            const isGenericView = !search && !filter && direction === 'desc' && limit === 20 && page === 1; 
            const cacheKey = `api:tokens:list:${sort}:${page}:${search}:${filter}:${direction}:${limit}`;
            const redis = getClient(); 
            if (isGenericView && redis) { try { const cached = await redis.get(cacheKey); if (cached) { res.setHeader('X-Cache', 'HIT'); return res.json(JSON.parse(cached)); } } catch(_e) { /* ignore */ } }

            const isAddressSearch = isValidPubkey(search);
            let rows = [];

            if (search.length > 0) {
                if (isAddressSearch) {
                    rows = await db.all(`SELECT * FROM tokens WHERE mint = $1`, [search]);
                    if (rows.length === 0) { await indexTokenOnChain(search); rows = await db.all(`SELECT * FROM tokens WHERE mint = $1`, [search]); }
                } else {
                    // SECURITY: Escape LIKE pattern to prevent injection (M9)
                    const safeSearch = `%${escapeLikePattern(search)}%`;
                    rows = await db.all(`SELECT * FROM tokens WHERE (symbol ILIKE $1 OR name ILIKE $1) LIMIT $2`, [safeSearch, limit]);
                }
            } else {
                const dir = direction.toLowerCase() === 'asc' ? 'ASC' : 'DESC';
                let sortColumn = 'k_score';

                switch(sort) {
                    case 'newest': sortColumn = 'timestamp'; break;
                    case 'age': sortColumn = 'timestamp'; break; 
                    case 'mcap': sortColumn = 'marketCap'; break;
                    case 'volume': sortColumn = 'volume24h'; break;
                    case '24h': sortColumn = 'change24h'; break;
                    case 'liquidity': sortColumn = 'liquidity'; break;
                    case '5m': sortColumn = 'change5m'; break;
                    case '1h': sortColumn = 'change1h'; break;
                    case 'holders': sortColumn = 'holders'; break;
                    default: sortColumn = 'k_score'; break;
                }

                let orderBy;
                if (sortColumn === 'k_score') {
                    orderBy = `CASE WHEN hasCommunityUpdate = TRUE THEN COALESCE(k_score, 0) ELSE 0 END ${dir}`;
                } else if (['timestamp', 'created_at'].includes(sortColumn)) {
                    orderBy = `${sortColumn} ${dir}`;
                } else {
                    orderBy = `COALESCE(${sortColumn}, 0) ${dir}`;
                }

                // SECURITY: Whitelist filter values to prevent injection
                const VALID_FILTERS = ['verified', 'all', undefined, ''];
                let whereClause = '';
                if (filter === 'verified') {
                    whereClause = 'WHERE hasCommunityUpdate = TRUE';
                } else if (filter && !VALID_FILTERS.includes(filter)) {
                    return res.status(400).json({ success: false, error: 'Invalid filter value' });
                }

                const offset = (page - 1) * limit;
                rows = await db.all(`SELECT * FROM tokens ${whereClause} ORDER BY ${orderBy} LIMIT $1 OFFSET $2`, [limit, offset]);
            }

            // DATA INTEGRITY: Verify token signatures (Proof-of-History)
            // Detects tampering even if DB credentials are compromised
            const { tokens: verifiedRows, stats: verifyStats } = dataVerification.verifyTokens(rows);

            // Log verification stats (only if there are issues)
            if (verifyStats.tampered > 0 || verifyStats.unsigned > verifyStats.total * 0.5) {
                logger.info(`[DataVerify] Tokens: ${verifyStats.verified}/${verifyStats.total} verified, ${verifyStats.tampered} tampered, ${verifyStats.unsigned} unsigned`);
            }

            const responsePayload = {
                success: true,
                lastUpdate: Date.now(),
                page,
                limit,
                // Include verification stats in response header
                _integrity: {
                    verified: verifyStats.verified,
                    total: verifyStats.total,
                    tampered: verifyStats.tampered
                },
                tokens: verifiedRows.map(r => {
                    // SECURITY: Only show K-Score/conviction for verified tokens
                    // Native tokens get special tier regardless of verification
                    const isNative = isNativeToken(r.mint);
                    const isVerified = r.hasCommunityUpdate || r.hascommunityupdate || false;
                    const kScore = isVerified ? (r.k_score || 0) : null;

                    return {
                        mint: r.mint,
                        name: r.name,
                        ticker: r.symbol,
                        image: r.image,
                        marketCap: r.marketcap || r.marketCap || 0,
                        volume24h: r.volume24h || 0,
                        priceUsd: r.priceusd || r.priceUsd || 0,
                        change24h: r.change24h || 0,
                        change1h: r.change1h || 0,
                        change5m: r.change5m || 0,
                        liquidity: r.liquidity || 0,
                        holders: r.holders || 0,
                        hasCommunityUpdate: isVerified,
                        timestamp: parseInt(r.timestamp),
                        // K-Score only for verified tokens (deep analysis done)
                        // Native tokens get special tier
                        kScore: kScore,
                        kRank: isNative ? getKRank(null, r.mint) : (kScore !== null ? getKRank(kScore, r.mint) : null),
                        creditRating: isNative ? getCreditRating(null, r.mint) : (kScore !== null ? getCreditRating(kScore, r.mint) : null),
                        // GASdf fields (lightweight version for list)
                        burnedPercent: r.burned_percent || 0,
                        isPumpFun: r.is_pump_fun || false,
                        // Conviction only for verified tokens
                        convictionScore: isVerified ? (r.conviction_score || 0) : null,
                        conviction_accumulators: isVerified ? (r.conviction_accumulators || 0) : null,
                        conviction_holders: isVerified ? (r.conviction_holders || 0) : null,
                        conviction_reducers: isVerified ? (r.conviction_reducers || 0) : null,
                        conviction_extractors: isVerified ? (r.conviction_extractors || 0) : null,
                        // Mayhem Mode indicator
                        isMutableSupply: r.is_mutable_supply || false,
                        supplyChange24h: r.supply_change_24h || 0,
                        // Data integrity status (Proof-of-History)
                        _dataVerified: r._verified || false
                    };
                })
            };

            if (isGenericView && redis) { try { await redis.set(cacheKey, JSON.stringify(responsePayload), 'EX', 3); } catch(_e) { /* ignore */ } }
            res.setHeader('X-Cache', 'MISS');
            return res.json(responsePayload);

        } catch (e) { res.status(500).json({ success: false, tokens: [], error: sanitizeError(e) }); }
    });

    // ==================================
    // WALLET PNL ENDPOINTS
    // ==================================

    const { calculateWalletPnL, getTokenPnL } = require('../services/pnlService');
    const pnlRateLimiter = require('../middleware/pnlRateLimiter');

    /**
     * GET /wallet/:address/pnl
     * Calculate on-chain PnL for a wallet
     *
     * Philosophy $asdfasdfa: Cache hits FREE, cache misses cost 1 credit
     *
     * Query params:
     *   - maxPages: Max pages of transactions to fetch (default 10, max 50)
     *   - since: Unix timestamp to filter transactions after
     */
    router.get('/wallet/:address/pnl', cacheControl(60, 120), pnlRateLimiter, async (req, res) => {
        const { address } = req.params;

        if (!isValidPubkey(address)) {
            return res.status(400).json({ success: false, error: 'Invalid wallet address' });
        }

        try {
            const options = {
                db, // Pass DB for pool price fallback
                maxPages: Math.min(parseInt(req.query.maxPages) || 10, 50),
                ...(req.query.since && { gtTime: parseInt(req.query.since) })
            };

            const pnl = await calculateWalletPnL(address, options);

            // Exclude _allTokens from API response (internal use only)
            const { _allTokens, ...publicPnl } = pnl;
            res.json({
                success: true,
                ...publicPnl
            });
        } catch (e) {
            logger.error(`[PnL] Wallet error: ${e.message}`);
            res.status(500).json({ success: false, error: sanitizeError(e) });
        }
    });

    /**
     * GET /wallet/:address/token/:mint/pnl
     * Calculate PnL for a specific token in a wallet
     *
     * Philosophy $asdfasdfa: Cache hits FREE, cache misses cost 1 credit
     */
    router.get('/wallet/:address/token/:mint/pnl', cacheControl(60, 120), pnlRateLimiter, async (req, res) => {
        const { address, mint } = req.params;

        if (!isValidPubkey(address)) {
            return res.status(400).json({ success: false, error: 'Invalid wallet address' });
        }
        if (!isValidPubkey(mint)) {
            return res.status(400).json({ success: false, error: 'Invalid token mint' });
        }

        try {
            const pnl = await getTokenPnL(address, mint, {
                db, // Pass DB for pool price fallback
                maxPages: 20 // More pages for specific token lookup
            });

            res.json({
                success: true,
                ...pnl
            });
        } catch (e) {
            logger.error(`[PnL] Token error: ${e.message}`);
            res.status(500).json({ success: false, error: sanitizeError(e) });
        }
    });

    // ==================================
    // TRACK RECORD - K-Score Predictions
    // ==================================

    /**
     * GET /api/track-record
     * Public track record of K-Score predictions vs reality
     * No API key required - this is proof of concept for organic growth
     * SECURITY: Rate limited (M2)
     */
    router.get('/track-record', cacheControl(300, 600), proxyRateLimit, async (req, res) => {
        try {
            const result = await smartCache('track-record', 300, async () => {
                // 1. K-Score vs Security correlation
                // SECURITY: Only show verified tokens (community submitted)
                // This ensures we only display tokens we've deeply analyzed
                const securityCorr = await db.all(`
                    SELECT
                        CASE
                            WHEN k_score >= 70 THEN 'HIGH'
                            WHEN k_score >= 50 THEN 'MID'
                            ELSE 'LOW'
                        END as tier,
                        COUNT(*) as total,
                        SUM(CASE WHEN mint_authority_revoked = true AND freeze_authority_revoked = true THEN 1 ELSE 0 END) as secure,
                        SUM(CASE WHEN mint_authority_revoked = false OR freeze_authority_revoked = false THEN 1 ELSE 0 END) as unsafe
                    FROM tokens
                    WHERE k_score IS NOT NULL AND hasCommunityUpdate = TRUE
                    GROUP BY tier
                    ORDER BY tier DESC
                `);

                // 2. Tier distribution with details (verified tokens only)
                const tierDetails = await db.all(`
                    SELECT
                        symbol,
                        name,
                        k_score,
                        conviction_score,
                        conviction_accumulators,
                        conviction_holders,
                        conviction_reducers,
                        conviction_extractors,
                        marketcap,
                        mint_authority_revoked,
                        freeze_authority_revoked,
                        hasCommunityUpdate as verified
                    FROM tokens
                    WHERE k_score IS NOT NULL AND hasCommunityUpdate = TRUE
                    ORDER BY k_score DESC
                `);

                // 3. K-Score evolution (verified tokens with history)
                const evolution = await db.all(`
                    SELECT
                        h.mint,
                        t.symbol,
                        t.k_score as current,
                        MIN(h.k_score) as min_score,
                        MAX(h.k_score) as max_score,
                        (SELECT h2.k_score FROM k_score_history h2 WHERE h2.mint = h.mint ORDER BY h2.date ASC LIMIT 1) as first_score,
                        COUNT(*) as data_points
                    FROM k_score_history h
                    JOIN tokens t ON t.mint = h.mint
                    WHERE t.hasCommunityUpdate = TRUE
                    GROUP BY h.mint, t.symbol, t.k_score
                    HAVING COUNT(*) >= 2
                    ORDER BY t.k_score DESC
                `);

                // 4. Extractor analysis (verified tokens only)
                const extractorAnalysis = await db.all(`
                    SELECT
                        symbol,
                        k_score,
                        conviction_extractors as extractors,
                        conviction_accumulators + conviction_holders as diamond_holders,
                        CASE
                            WHEN conviction_accumulators + conviction_holders + conviction_reducers + conviction_extractors > 0
                            THEN ROUND(conviction_extractors::numeric / (conviction_accumulators + conviction_holders + conviction_reducers + conviction_extractors) * 100, 1)
                            ELSE 0
                        END as extractor_pct
                    FROM tokens
                    WHERE k_score IS NOT NULL AND conviction_extractors IS NOT NULL AND hasCommunityUpdate = TRUE
                    ORDER BY extractor_pct DESC
                `);

                // 5. Calculate summary stats
                const rustTokens = tierDetails.filter(t => t.k_score < 20);
                const platinumTokens = tierDetails.filter(t => t.k_score >= 80);
                const highExtractorTokens = extractorAnalysis.filter(t => t.extractor_pct > 40);

                return {
                    generated: new Date().toISOString(),
                    summary: {
                        totalTracked: tierDetails.length,
                        rustTier: {
                            count: rustTokens.length,
                            allUnsafe: rustTokens.every(t => !t.mint_authority_revoked || !t.freeze_authority_revoked),
                            tokens: rustTokens.map(t => t.symbol)
                        },
                        platinumPlus: {
                            count: platinumTokens.length,
                            allSecure: platinumTokens.every(t => t.mint_authority_revoked && t.freeze_authority_revoked),
                            tokens: platinumTokens.map(t => ({ symbol: t.symbol, kScore: t.k_score }))
                        },
                        highSellPressure: {
                            count: highExtractorTokens.length,
                            avgKScore: highExtractorTokens.length > 0
                                ? Math.round(highExtractorTokens.reduce((a, t) => a + t.k_score, 0) / highExtractorTokens.length)
                                : 0,
                            tokens: highExtractorTokens.map(t => ({ symbol: t.symbol, extractorPct: t.extractor_pct }))
                        }
                    },
                    correlations: {
                        kScoreVsSecurity: securityCorr.map(row => ({
                            tier: row.tier,
                            total: parseInt(row.total),
                            secure: parseInt(row.secure),
                            unsafe: parseInt(row.unsafe),
                            securePercent: Math.round((row.secure / row.total) * 100)
                        }))
                    },
                    evolution: evolution.map(e => ({
                        symbol: e.symbol,
                        current: e.current,
                        first: e.first_score,
                        change: e.current - e.first_score,
                        dataPoints: parseInt(e.data_points)
                    })),
                    tokens: tierDetails.map(t => {
                        const totalAnalyzed = (t.conviction_accumulators || 0) + (t.conviction_holders || 0) +
                                             (t.conviction_reducers || 0) + (t.conviction_extractors || 0);
                        const extPct = totalAnalyzed > 0
                            ? Math.round((t.conviction_extractors / totalAnalyzed) * 100)
                            : 0;

                        return {
                            symbol: t.symbol,
                            name: t.name,
                            kScore: t.k_score,
                            tier: t.k_score >= 90 ? 'Diamond' : t.k_score >= 80 ? 'Platinum' :
                                  t.k_score >= 70 ? 'Gold' : t.k_score >= 60 ? 'Silver' :
                                  t.k_score >= 50 ? 'Bronze' : t.k_score >= 40 ? 'Copper' :
                                  t.k_score >= 20 ? 'Iron' : 'Rust',
                            conviction: t.conviction_score,
                            breakdown: {
                                accumulators: t.conviction_accumulators || 0,
                                holders: t.conviction_holders || 0,
                                reducers: t.conviction_reducers || 0,
                                extractors: t.conviction_extractors || 0
                            },
                            extractorPct: extPct,
                            secure: t.mint_authority_revoked && t.freeze_authority_revoked,
                            verified: t.verified || false,
                            marketCap: t.marketcap || 0
                        };
                    })
                };
            });

            res.json({ success: true, ...result });
        } catch (e) {
            logger.error(`[TrackRecord] Error: ${e.message}`);
            res.status(500).json({ success: false, error: sanitizeError(e) });
        }
    });

    return router;
}

module.exports = { init };
