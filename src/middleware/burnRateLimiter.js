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

    // If no wallet provided, check for legacy API key
    if (!wallet) {
        const apiKey = req.headers['x-api-key'] || req.query.api_key;
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

        // Check eligibility
        const eligibility = await checkApiEligibility(connection, db, wallet);

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

        // Deduct 1 call
        await deductCall(db, wallet);

        // Add credit info to response headers
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

        next();

    } catch (e) {
        logger.error(`[BurnRateLimiter] Error: ${e.message}`);
        // Fail open for now (don't break API during transition)
        next();
    }
};

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
