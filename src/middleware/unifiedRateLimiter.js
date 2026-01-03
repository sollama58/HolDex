/**
 * Unified Rate Limiter
 *
 * $ASDFASDFA-Aligned API Access:
 * - Hold 10K+ = Anti-Sybil Gate (can enter)
 * - Burn = Credits (can use)
 * - API Key = Convenience layer linked to wallet
 *
 * Accepts: x-api-key OR x-wallet header
 */

const { getSolanaConnection } = require('../services/solana');
const { getDB } = require('../services/database');
const { hashApiKey } = require('../utils/apiKeyHash');
const {
    MIN_HOLDINGS,
    checkApiEligibility,
    deductCall,
    getWalletBalance
} = require('../services/burnCredits');
const logger = require('../services/logger');

/**
 * Unified middleware: API Key or Wallet → Check holdings → Check burns → Deduct
 */
const unifiedRateLimiter = async (req, res, next) => {
    const apiKey = req.headers['x-api-key'] || req.query.api_key;
    const walletDirect = req.headers['x-wallet'] || req.query.wallet;

    let wallet = null;
    const db = getDB();

    try {
        // 1. Resolve wallet from API key or direct header
        if (apiKey) {
            const keyHash = hashApiKey(apiKey);

            // Try new hashed key first, fallback to legacy plaintext key
            let keyRecord = await db.get(
                'SELECT wallet, owner, is_active FROM api_keys WHERE key_hash = $1',
                [keyHash]
            );

            // Fallback: Check legacy plaintext key column
            if (!keyRecord) {
                keyRecord = await db.get(
                    'SELECT wallet, owner, is_active FROM api_keys WHERE key = $1',
                    [apiKey]
                );
            }

            if (!keyRecord) {
                return res.status(401).json({
                    success: false,
                    error: 'Invalid API key',
                    help: 'Generate a key by connecting your wallet at holdex-api.onrender.com'
                });
            }

            if (!keyRecord.is_active) {
                return res.status(403).json({
                    success: false,
                    error: 'API key revoked'
                });
            }

            // Use wallet if set, otherwise fall back to owner (for legacy keys)
            wallet = keyRecord.wallet || keyRecord.owner;

        } else if (walletDirect) {
            wallet = walletDirect;

        } else {
            return res.status(401).json({
                success: false,
                error: 'Authentication required',
                help: {
                    option1: 'x-api-key: YOUR_API_KEY',
                    option2: 'x-wallet: YOUR_WALLET_ADDRESS',
                    requirements: {
                        gate: `Hold ${MIN_HOLDINGS.toLocaleString()}+ $ASDFASDFA (anti-sybil)`,
                        credits: '1 token burned = 1 API call (lifetime)'
                    }
                }
            });
        }

        // 2. Check eligibility (holdings + burns)
        const connection = getSolanaConnection();
        const eligibility = await checkApiEligibility(connection, db, wallet);

        // Gate 1: Anti-Sybil (holdings)
        if (eligibility.holdings < MIN_HOLDINGS) {
            return res.status(403).json({
                success: false,
                error: `Hold ${MIN_HOLDINGS.toLocaleString()}+ $ASDFASDFA to access API`,
                gate: 'anti-sybil',
                wallet,
                holdings: eligibility.holdings,
                required: MIN_HOLDINGS,
                need: MIN_HOLDINGS - eligibility.holdings
            });
        }

        // Gate 2: Credits (burns)
        if (eligibility.burned === 0) {
            return res.status(402).json({
                success: false,
                error: 'Burn $ASDFASDFA to get API credits',
                gate: 'credits',
                wallet,
                holdings: eligibility.holdings,
                burned: 0,
                help: 'Send $ASDFASDFA to burn address or treasury. 1 burn = 1 call forever.'
            });
        }

        if (eligibility.remainingCalls <= 0) {
            return res.status(402).json({
                success: false,
                error: 'No credits remaining. Burn more $ASDFASDFA.',
                gate: 'credits',
                wallet,
                burned: eligibility.burned,
                used: eligibility.usedCalls,
                remaining: 0
            });
        }

        // 3. Deduct 1 call
        await deductCall(db, wallet);

        // 4. Add headers
        res.setHeader('X-Wallet', wallet);
        res.setHeader('X-Credits-Remaining', eligibility.remainingCalls - 1);
        res.setHeader('X-Credits-Burned', eligibility.burned);
        res.setHeader('X-Credits-Used', (eligibility.usedCalls || 0) + 1);

        // 5. Attach to request
        req.wallet = wallet;
        req.credits = {
            holdings: eligibility.holdings,
            burned: eligibility.burned,
            used: (eligibility.usedCalls || 0) + 1,
            remaining: eligibility.remainingCalls - 1
        };

        next();

    } catch (e) {
        logger.error(`[UnifiedRateLimiter] Error: ${e.message}`);
        // Fail open during transition (remove this later)
        next();
    }
};

/**
 * Light check (no deduction) - for status endpoints
 */
const checkOnly = async (req, res, next) => {
    const apiKey = req.headers['x-api-key'] || req.query.api_key;
    const walletDirect = req.headers['x-wallet'] || req.query.wallet;

    if (!apiKey && !walletDirect) {
        return next();
    }

    try {
        const db = getDB();
        let wallet = walletDirect;

        if (apiKey && !wallet) {
            const keyHash = hashApiKey(apiKey);
            // Try new hashed key first, fallback to legacy plaintext
            let keyRecord = await db.get('SELECT wallet, owner FROM api_keys WHERE key_hash = $1', [keyHash]);
            if (!keyRecord) {
                keyRecord = await db.get('SELECT wallet, owner FROM api_keys WHERE key = $1', [apiKey]);
            }
            wallet = keyRecord?.wallet || keyRecord?.owner;
        }

        if (wallet) {
            const connection = getSolanaConnection();
            const eligibility = await checkApiEligibility(connection, db, wallet);
            req.wallet = wallet;
            req.credits = {
                holdings: eligibility.holdings,
                burned: eligibility.burned,
                used: eligibility.usedCalls || 0,
                remaining: eligibility.remainingCalls
            };
        }

        next();
    } catch (e) {
        logger.error(`[CheckOnly] Error: ${e.message}`);
        next();
    }
};

module.exports = unifiedRateLimiter;
module.exports.checkOnly = checkOnly;
module.exports.MIN_HOLDINGS = MIN_HOLDINGS;
