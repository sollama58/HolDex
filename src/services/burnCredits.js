/**
 * Burn Credits System
 *
 * Philosophy: "Burn = Value Creation = Lifetime API Access"
 *
 * - Hold 10K+ $ASDFASDFA = Eligibility
 * - 1 token burned = 1 API call forever
 * - Burns detected: CTO wallet, burn address, SPL burn instruction
 */

const { PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

// $ASDFASDFA token mint
const ASDF_MINT = config.FEE_TOKEN_MINT || '9zB5wRarXMj86MymwLumSKA1Dx35zPqqKfcZtK1Spump';

// Minimum holdings for API eligibility
const MIN_HOLDINGS = 10_000;

// Recognized burn destinations
const BURN_ADDRESSES = new Set([
    '1111111111111111111111111111111111',           // Solana null address
    '1nc1nerator11111111111111111111111111111111',  // Common burn address
    config.TREASURY_WALLET,                          // CTO/Treasury wallet counts as burn
].filter(Boolean));

// Cache burn totals (refresh every 5 min)
const burnCache = new Map();
const CACHE_TTL = 5 * 60 * 1000;

/**
 * Get wallet's $ASDFASDFA balance
 */
async function getWalletBalance(connection, walletAddress) {
    try {
        const wallet = new PublicKey(walletAddress);
        const mint = new PublicKey(ASDF_MINT);

        const accounts = await connection.getParsedTokenAccountsByOwner(wallet, { mint });

        if (accounts.value.length === 0) return 0;

        const balance = accounts.value[0].account.data.parsed.info.tokenAmount.uiAmount;
        return balance || 0;
    } catch (e) {
        logger.error(`[BurnCredits] Balance check failed: ${e.message}`);
        return 0;
    }
}

/**
 * Get total tokens burned by wallet (via Helius API)
 * Detects: transfers to burn addresses, SPL burn instructions
 */
async function getWalletBurns(walletAddress) {
    // Check cache first
    const cached = burnCache.get(walletAddress);
    if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
        return cached.total;
    }

    if (!config.HELIUS_API_KEY) {
        logger.warn('[BurnCredits] No Helius API key - cannot track burns');
        return 0;
    }

    try {
        let totalBurned = 0;
        let beforeSignature = null;
        let hasMore = true;

        // Paginate through transaction history
        while (hasMore) {
            const url = `https://api.helius.xyz/v0/addresses/${walletAddress}/transactions?api-key=${config.HELIUS_API_KEY}&type=TRANSFER${beforeSignature ? `&before=${beforeSignature}` : ''}`;

            const response = await fetch(url);
            const transactions = await response.json();

            if (!transactions || transactions.length === 0) {
                hasMore = false;
                break;
            }

            for (const tx of transactions) {
                // Check token transfers
                if (tx.tokenTransfers) {
                    for (const transfer of tx.tokenTransfers) {
                        // Must be $ASDFASDFA token
                        if (transfer.mint !== ASDF_MINT) continue;

                        // Must be FROM this wallet
                        if (transfer.fromUserAccount !== walletAddress) continue;

                        // Check if destination is a burn address
                        if (BURN_ADDRESSES.has(transfer.toUserAccount)) {
                            totalBurned += transfer.tokenAmount || 0;
                        }
                    }
                }

                // Check for SPL Token Burn instruction
                if (tx.type === 'BURN' || tx.type === 'TOKEN_BURN') {
                    const burnAmount = tx.tokenTransfers?.[0]?.tokenAmount || 0;
                    if (tx.tokenTransfers?.[0]?.mint === ASDF_MINT) {
                        totalBurned += burnAmount;
                    }
                }
            }

            // Pagination
            if (transactions.length < 100) {
                hasMore = false;
            } else {
                beforeSignature = transactions[transactions.length - 1].signature;
            }

            // Safety limit: max 10 pages (1000 transactions)
            if (beforeSignature && transactions.length >= 1000) {
                hasMore = false;
            }
        }

        // Cache result
        burnCache.set(walletAddress, {
            total: totalBurned,
            timestamp: Date.now()
        });

        logger.info(`[BurnCredits] ${walletAddress.slice(0, 8)}... burned ${totalBurned.toLocaleString()} $ASDF`);
        return totalBurned;

    } catch (e) {
        logger.error(`[BurnCredits] Failed to fetch burns: ${e.message}`);
        return 0;
    }
}

/**
 * Check if wallet is eligible for API access
 * Returns: { eligible, holdings, burned, remainingCalls, reason }
 */
async function checkApiEligibility(connection, db, walletAddress) {
    // 1. Check holdings
    const holdings = await getWalletBalance(connection, walletAddress);

    if (holdings < MIN_HOLDINGS) {
        return {
            eligible: false,
            holdings,
            burned: 0,
            remainingCalls: 0,
            reason: `Hold ${MIN_HOLDINGS.toLocaleString()}+ $ASDFASDFA to access API`
        };
    }

    // 2. Get total burns (lifetime)
    const burned = await getWalletBurns(walletAddress);

    if (burned === 0) {
        return {
            eligible: false,
            holdings,
            burned: 0,
            remainingCalls: 0,
            reason: 'Burn $ASDFASDFA to earn API calls (1 token = 1 call)'
        };
    }

    // 3. Get used calls from DB
    const record = await db.get(
        'SELECT used_calls FROM wallet_credits WHERE wallet = $1',
        [walletAddress]
    );
    const usedCalls = record?.used_calls || 0;

    // 4. Calculate remaining
    const remainingCalls = Math.max(0, burned - usedCalls);

    if (remainingCalls === 0) {
        return {
            eligible: false,
            holdings,
            burned,
            usedCalls,
            remainingCalls: 0,
            reason: 'No calls remaining. Burn more $ASDFASDFA for more calls.'
        };
    }

    return {
        eligible: true,
        holdings,
        burned,
        usedCalls,
        remainingCalls,
        reason: null
    };
}

/**
 * Deduct one API call from wallet's credits
 */
async function deductCall(db, walletAddress) {
    await db.run(`
        INSERT INTO wallet_credits (wallet, used_calls, last_call)
        VALUES ($1, 1, $2)
        ON CONFLICT (wallet)
        DO UPDATE SET used_calls = wallet_credits.used_calls + 1, last_call = $2
    `, [walletAddress, Date.now()]);
}

/**
 * Get wallet's credit status
 */
async function getCreditStatus(connection, db, walletAddress) {
    const holdings = await getWalletBalance(connection, walletAddress);
    const burned = await getWalletBurns(walletAddress);

    const record = await db.get(
        'SELECT used_calls, last_call FROM wallet_credits WHERE wallet = $1',
        [walletAddress]
    );

    const usedCalls = record?.used_calls || 0;
    const lastCall = record?.last_call || null;

    return {
        wallet: walletAddress,
        holdings,
        minRequired: MIN_HOLDINGS,
        holdingEligible: holdings >= MIN_HOLDINGS,
        burned,
        usedCalls,
        remainingCalls: Math.max(0, burned - usedCalls),
        lastCall: lastCall ? new Date(lastCall).toISOString() : null
    };
}

/**
 * Invalidate cache for a wallet (call after new burn detected)
 */
function invalidateCache(walletAddress) {
    burnCache.delete(walletAddress);
}

module.exports = {
    ASDF_MINT,
    MIN_HOLDINGS,
    BURN_ADDRESSES,
    getWalletBalance,
    getWalletBurns,
    checkApiEligibility,
    deductCall,
    getCreditStatus,
    invalidateCache
};
