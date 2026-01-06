/**
 * PnL Rate Limiter - Cache-Aware Credit Deduction + Rate Limiting
 *
 * Philosophy $asdfasdfa: "Pay for work, not for cache hits"
 *
 * Protection layers:
 * 1. Rate limit: 5 req/min per wallet (prevents spam)
 * 2. Cache HIT  → FREE (no credit deducted)
 * 3. Cache MISS → 1 credit (expensive RPC work)
 *
 * This is fair: you only pay when we do expensive computation.
 * Rate limiting prevents abuse even with valid credits.
 */

const { getSolanaConnection } = require('../services/solana');
const { getDB } = require('../services/database');
const { getClient: getRedisClient } = require('../services/redis');
const { hashApiKey } = require('../utils/apiKeyHash');
const {
    MIN_HOLDINGS,
    checkApiEligibility,
    deductCall
} = require('../services/burnCredits');
const logger = require('../services/logger');

// Must match pnlService.js
const PNL_CACHE_PREFIX = 'pnl:wallet:';

// Rate limit config
const PNL_RATE_LIMIT = 5;           // requests per window
const PNL_RATE_WINDOW = 60;         // window in seconds (1 minute)
const PNL_RATE_PREFIX = 'pnl:rate:';

/**
 * Check if PnL result is cached
 */
async function isPnLCached(wallet, maxPages) {
    const redis = getRedisClient();
    if (!redis) return false;

    try {
        const key = `${PNL_CACHE_PREFIX}${wallet}:${maxPages}`;
        const exists = await redis.exists(key);
        return exists === 1;
    } catch (_e) {
        return false;
    }
}

/**
 * Check and increment rate limit for wallet
 * Returns { allowed: boolean, current: number, limit: number, resetIn: number }
 */
async function checkRateLimit(authWallet) {
    const redis = getRedisClient();

    // Fallback: allow if no Redis (degraded mode)
    if (!redis) {
        return { allowed: true, current: 0, limit: PNL_RATE_LIMIT, resetIn: 0 };
    }

    const key = `${PNL_RATE_PREFIX}${authWallet}`;

    try {
        // Increment counter and set TTL atomically
        const current = await redis.incr(key);

        // Set expiry only on first request in window
        if (current === 1) {
            await redis.expire(key, PNL_RATE_WINDOW);
        }

        // Get TTL for reset info
        const ttl = await redis.ttl(key);

        if (current > PNL_RATE_LIMIT) {
            logger.warn(`[PnL] Rate limit exceeded for ${authWallet.slice(0, 8)}... (${current}/${PNL_RATE_LIMIT})`);
            return {
                allowed: false,
                current,
                limit: PNL_RATE_LIMIT,
                resetIn: ttl > 0 ? ttl : PNL_RATE_WINDOW
            };
        }

        return {
            allowed: true,
            current,
            limit: PNL_RATE_LIMIT,
            resetIn: ttl > 0 ? ttl : PNL_RATE_WINDOW
        };
    } catch (e) {
        logger.error(`[PnL] Rate limit check error: ${e.message}`);
        // Fail open on errors (allow request)
        return { allowed: true, current: 0, limit: PNL_RATE_LIMIT, resetIn: 0 };
    }
}

/**
 * PnL-specific rate limiter
 *
 * Flow:
 * 1. Authenticate (API key or wallet)
 * 2. Check rate limit (5 req/min per wallet)
 * 3. Check if result is cached
 * 4. If cached → allow without credit deduction
 * 5. If not cached → verify credits, deduct 1, proceed
 */
const pnlRateLimiter = async (req, res, next) => {
    const headerApiKey = req.headers['x-api-key'];
    const queryApiKey = req.query.api_key;
    const apiKey = headerApiKey || queryApiKey;
    const walletDirect = req.headers['x-wallet'] || req.query.wallet;

    let authWallet = null;
    const db = getDB();

    try {
        // 1. Resolve wallet from API key or direct header
        if (apiKey) {
            const keyHash = hashApiKey(apiKey);
            let keyRecord = await db.get(
                'SELECT wallet, owner, is_active FROM api_keys WHERE key_hash = $1',
                [keyHash]
            );
            if (!keyRecord) {
                keyRecord = await db.get(
                    'SELECT wallet, owner, is_active FROM api_keys WHERE key = $1',
                    [apiKey]
                );
            }

            if (!keyRecord) {
                return res.status(401).json({
                    success: false,
                    error: 'Invalid API key'
                });
            }
            if (!keyRecord.is_active) {
                return res.status(403).json({
                    success: false,
                    error: 'API key revoked'
                });
            }
            authWallet = keyRecord.wallet || keyRecord.owner;

        } else if (walletDirect) {
            authWallet = walletDirect;

        } else {
            return res.status(401).json({
                success: false,
                error: 'Authentication required',
                help: {
                    option1: 'x-api-key: YOUR_API_KEY',
                    option2: 'x-wallet: YOUR_WALLET_ADDRESS',
                    philosophy: 'Hold to enter. Burn to use. Cache hits are free.'
                }
            });
        }

        // 2. Check rate limit (5 req/min per wallet)
        const rateLimit = await checkRateLimit(authWallet);
        res.setHeader('X-RateLimit-Limit', rateLimit.limit);
        res.setHeader('X-RateLimit-Remaining', Math.max(0, rateLimit.limit - rateLimit.current));
        res.setHeader('X-RateLimit-Reset', rateLimit.resetIn);

        if (!rateLimit.allowed) {
            return res.status(429).json({
                success: false,
                error: 'PnL rate limit exceeded',
                limit: `${PNL_RATE_LIMIT} requests per minute`,
                current: rateLimit.current,
                resetIn: rateLimit.resetIn,
                help: 'PnL calculations are expensive. Please wait before retrying.'
            });
        }

        // 3. Get target wallet and maxPages from request
        const targetWallet = req.params.address;
        // Token-specific route uses maxPages=20, wallet route uses query param
        const isTokenRoute = !!req.params.mint;
        const maxPages = isTokenRoute ? 20 : Math.min(parseInt(req.query.maxPages) || 10, 50);

        // 3. Check if result is cached
        const isCached = await isPnLCached(targetWallet, maxPages);

        if (isCached) {
            // Cache HIT → FREE, no credit deduction
            logger.debug(`[PnL] Cache hit for ${targetWallet.slice(0, 8)}... - no credit charged`);
            res.setHeader('X-PnL-Cached', 'true');
            res.setHeader('X-Credits-Charged', '0');

            req.wallet = authWallet;
            req.pnlCached = true;
            return next();
        }

        // 4. Cache MISS → Check eligibility and deduct credit
        const connection = getSolanaConnection();
        const eligibility = await checkApiEligibility(connection, db, authWallet);

        // Gate 1: Holdings
        if (eligibility.holdings < MIN_HOLDINGS) {
            return res.status(403).json({
                success: false,
                error: `Hold ${MIN_HOLDINGS.toLocaleString()}+ $ASDFASDFA to access API`,
                gate: 'anti-sybil',
                wallet: authWallet,
                holdings: eligibility.holdings,
                required: MIN_HOLDINGS
            });
        }

        // Gate 2: Burns
        if (eligibility.burned === 0) {
            return res.status(402).json({
                success: false,
                error: 'Burn $ASDFASDFA to get API credits',
                gate: 'credits',
                wallet: authWallet,
                help: '1 burn = 1 PnL calculation (cache hits are free)'
            });
        }

        if (eligibility.remainingCalls <= 0) {
            return res.status(402).json({
                success: false,
                error: 'No credits remaining. Burn more $ASDFASDFA.',
                gate: 'credits',
                wallet: authWallet,
                burned: eligibility.burned,
                used: eligibility.usedCalls,
                remaining: 0
            });
        }

        // 5. Deduct 1 credit for cache miss
        await deductCall(db, authWallet);

        res.setHeader('X-PnL-Cached', 'false');
        res.setHeader('X-Credits-Charged', '1');
        res.setHeader('X-Wallet', authWallet);
        res.setHeader('X-Credits-Remaining', eligibility.remainingCalls - 1);

        req.wallet = authWallet;
        req.pnlCached = false;
        req.credits = {
            holdings: eligibility.holdings,
            burned: eligibility.burned,
            used: (eligibility.usedCalls || 0) + 1,
            remaining: eligibility.remainingCalls - 1
        };

        logger.debug(`[PnL] Cache miss for ${targetWallet.slice(0, 8)}... - 1 credit charged`);
        next();

    } catch (e) {
        logger.error(`[PnLRateLimiter] Error: ${e.message}`);
        return res.status(503).json({
            success: false,
            error: 'Service temporarily unavailable',
            retry_after: 30
        });
    }
};

module.exports = pnlRateLimiter;
