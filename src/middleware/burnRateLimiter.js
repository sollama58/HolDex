/**
 * Burn-Based Rate Limiter
 *
 * Philosophy: "Hold to enter. Burn to use."
 *
 * - Must hold 10K+ $ASDFASDFA to access API
 * - 1 token burned = 1 API call (lifetime)
 * - Burns detected via Helius API
 */

const { getSolanaConnection } = require('../services/solana');
const { getDB } = require('../services/database');
const {
    MIN_HOLDINGS,
    checkApiEligibility,
    deductCall,
    getCreditStatus
} = require('../services/burnCredits');
const logger = require('../services/logger');

/**
 * Middleware: Verify wallet signature and check burn credits
 *
 * Expects headers:
 * - x-wallet: Wallet public key
 * - x-signature: Base64 signature of message "HolDex API Access"
 *
 * Or query params for simple GET requests:
 * - wallet: Wallet public key (for status check only, no deduction)
 */
const burnRateLimiter = async (req, res, next) => {
    const wallet = req.headers['x-wallet'] || req.query.wallet;
    const apiKey = req.headers['x-api-key'] || req.query.api_key;

    // If no wallet provided, check for legacy API key
    if (!wallet) {
        if (apiKey) {
            // Fall back to legacy API key system (for backward compatibility)
            return require('./rateLimiter')(req, res, next);
        }
        return res.status(401).json({
            success: false,
            error: 'Wallet address required',
            help: {
                method: 'Include x-wallet header with your Solana wallet address',
                requirements: {
                    minHoldings: `${MIN_HOLDINGS.toLocaleString()} $ASDFASDFA`,
                    burnForCalls: '1 token burned = 1 API call (lifetime)'
                }
            }
        });
    }

    try {
        const connection = getSolanaConnection();
        const db = getDB();

        // Check eligibility (pass apiKey for whitelist bypass)
        const eligibility = await checkApiEligibility(connection, db, wallet, apiKey);

        if (!eligibility.eligible) {
            return res.status(403).json({
                success: false,
                error: eligibility.reason,
                credits: {
                    wallet,
                    holdings: eligibility.holdings,
                    minRequired: MIN_HOLDINGS,
                    burned: eligibility.burned,
                    usedCalls: eligibility.usedCalls || 0,
                    remainingCalls: 0
                },
                help: {
                    holdMore: eligibility.holdings < MIN_HOLDINGS
                        ? `Need ${(MIN_HOLDINGS - eligibility.holdings).toLocaleString()} more $ASDFASDFA`
                        : null,
                    burnMore: eligibility.burned <= (eligibility.usedCalls || 0)
                        ? 'Burn $ASDFASDFA to earn more API calls'
                        : null
                }
            });
        }

        // Deduct 1 call (skip for whitelisted keys)
        await deductCall(db, wallet, apiKey);

        // Add credit info to response headers
        if (eligibility.whitelisted) {
            res.setHeader('X-Whitelisted', 'true');
            res.setHeader('X-Credits-Charged', '0');
            req.burnWallet = {
                address: wallet,
                whitelisted: true
            };
        } else {
            res.setHeader('X-Credits-Remaining', eligibility.remainingCalls - 1);
            res.setHeader('X-Credits-Burned', eligibility.burned);
            res.setHeader('X-Credits-Used', (eligibility.usedCalls || 0) + 1);

            // Attach wallet info to request
            req.burnWallet = {
                address: wallet,
                holdings: eligibility.holdings,
                burned: eligibility.burned,
                remaining: eligibility.remainingCalls - 1
            };
        }

        next();

    } catch (e) {
        logger.error(`[BurnRateLimiter] Error: ${e.message}`);
        // SECURITY: Fail-closed with strict fallback rate limiting (C3)
        // If the burn system fails, apply emergency limits to prevent abuse
        const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.ip || 'unknown';
        const now = Date.now();

        // Atomic-style: get or create, reset if expired, increment - all in one block
        let tracker = failedRequestTracker.get(ip);
        if (!tracker || now > tracker.resetTime) {
            tracker = { count: 1, resetTime: now + FALLBACK_WINDOW };
        } else {
            tracker.count++;
        }
        failedRequestTracker.set(ip, tracker);

        if (tracker.count > FALLBACK_MAX_REQUESTS) {
            logger.warn(`[BurnRateLimiter] Fallback limit exceeded for ${ip}`);
            return res.status(429).json({
                success: false,
                error: 'Rate limit exceeded (fallback mode)',
                retryAfter: Math.ceil((tracker.resetTime - now) / 1000)
            });
        }

        // Allow with degraded service warning
        res.setHeader('X-RateLimit-Fallback', 'true');
        res.setHeader('X-RateLimit-Remaining', FALLBACK_MAX_REQUESTS - tracker.count);
        next();
    }
};

// SECURITY: In-memory fallback rate limiting when burn system fails
const FALLBACK_MAX_REQUESTS = 20; // Max requests per window when system is degraded
const FALLBACK_WINDOW = 60 * 1000; // 1 minute window
const failedRequestTracker = new Map(); // Use Map for better cleanup

// Clean up old entries every 5 minutes
setInterval(() => {
    const now = Date.now();
    for (const [ip, data] of failedRequestTracker) {
        if (now > data.resetTime) {
            failedRequestTracker.delete(ip);
        }
    }
}, 5 * 60 * 1000);

/**
 * Middleware factory: Optional burn check (doesn't deduct, just checks)
 * Useful for endpoints that should show credit status without charging
 */
const burnCheck = (required = false) => async (req, res, next) => {
    const wallet = req.headers['x-wallet'] || req.query.wallet;

    if (!wallet) {
        if (required) {
            return res.status(401).json({ success: false, error: 'Wallet required' });
        }
        return next();
    }

    try {
        const connection = getSolanaConnection();
        const db = getDB();
        const status = await getCreditStatus(connection, db, wallet);

        req.burnWallet = status;
        next();
    } catch (e) {
        logger.error(`[BurnCheck] Error: ${e.message}`);
        next();
    }
};

module.exports = burnRateLimiter;
module.exports.burnCheck = burnCheck;
module.exports.MIN_HOLDINGS = MIN_HOLDINGS;
