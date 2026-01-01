/**
 * PnL Service - On-chain wallet profit/loss calculation
 *
 * Calculates PnL using Helius Enhanced Transactions API:
 * - Parses SWAP transactions to identify buys/sells
 * - Tracks cost basis in SOL (actual on-chain cost)
 * - Calculates realized PnL (from sells) and unrealized PnL (current holdings)
 * - Converts to USD using current SOL price
 *
 * Why SOL-based?
 * - Jupiter Price API = real-time only (no historical)
 * - Historical USD prices require paid APIs (Birdeye)
 * - SOL cost basis is accurate from actual swap data
 * - Solana traders think in SOL terms anyway
 */

const config = require('../config/env');
const { getSolPrice } = require('./priceService');
const logger = require('./logger');

const HELIUS_API_KEY = config.HELIUS_API_KEY;
const SOL_MINT = 'So11111111111111111111111111111111111111112';
const WSOL_MINT = 'So11111111111111111111111111111111111111112';

// Stablecoins (treat as ~$1 for cost basis)
const STABLECOINS = new Set([
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
]);

// System addresses to ignore
const IGNORED_ADDRESSES = new Set([
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', // Token Program
    '11111111111111111111111111111111',             // System Program
    'ComputeBudget111111111111111111111111111111',  // Compute Budget
]);

/**
 * Fetch swap transactions for a wallet using Helius Enhanced Transactions API
 */
async function fetchSwapTransactions(wallet, options = {}) {
    if (!HELIUS_API_KEY) {
        logger.warn('[PnL] No HELIUS_API_KEY configured');
        return [];
    }

    const allTxs = [];
    let before = null;
    const maxPages = options.maxPages || 10; // Default 10 pages = 500 txs
    const limit = options.limit || 50;

    for (let page = 0; page < maxPages; page++) {
        const params = new URLSearchParams({
            'api-key': HELIUS_API_KEY,
            'limit': limit.toString(),
            'type': 'SWAP' // Only fetch swap transactions
        });

        if (before) params.append('before-signature', before);
        if (options.gtTime) params.append('gt-time', options.gtTime.toString());

        const url = `https://api-mainnet.helius-rpc.com/v0/addresses/${wallet}/transactions?${params}`;

        try {
            const response = await fetch(url, {
                method: 'GET',
                signal: AbortSignal.timeout(15000)
            });

            if (!response.ok) {
                logger.warn(`[PnL] Helius API error: ${response.status}`);
                break;
            }

            const txs = await response.json();
            if (!txs || txs.length === 0) break;

            allTxs.push(...txs);
            before = txs[txs.length - 1]?.signature;

            // Rate limiting
            await new Promise(r => setTimeout(r, 100));

        } catch (error) {
            logger.error(`[PnL] Fetch error: ${error.message}`);
            break;
        }
    }

    return allTxs;
}

/**
 * Parse a swap transaction to extract buy/sell info
 * Returns: { mint, type: 'buy'|'sell', tokenAmount, solAmount, timestamp }
 */
function parseSwapTransaction(tx, wallet) {
    const result = [];

    if (!tx.tokenTransfers || tx.tokenTransfers.length === 0) {
        return result;
    }

    // Find SOL/WSOL movements (the cost/proceeds)
    let solDelta = 0;

    // Check native transfers for SOL
    if (tx.nativeTransfers) {
        for (const nt of tx.nativeTransfers) {
            if (nt.fromUserAccount === wallet) {
                solDelta -= (nt.amount || 0) / 1e9; // lamports to SOL
            }
            if (nt.toUserAccount === wallet) {
                solDelta += (nt.amount || 0) / 1e9;
            }
        }
    }

    // Check for wrapped SOL in token transfers
    for (const tt of tx.tokenTransfers) {
        if (tt.mint === WSOL_MINT || tt.mint === SOL_MINT) {
            if (tt.fromUserAccount === wallet) {
                solDelta -= tt.tokenAmount || 0;
            }
            if (tt.toUserAccount === wallet) {
                solDelta += tt.tokenAmount || 0;
            }
        }
    }

    // Check for stablecoin swaps (convert to SOL equivalent)
    let stableDelta = 0;
    for (const tt of tx.tokenTransfers) {
        if (STABLECOINS.has(tt.mint)) {
            if (tt.fromUserAccount === wallet) {
                stableDelta -= tt.tokenAmount || 0;
            }
            if (tt.toUserAccount === wallet) {
                stableDelta += tt.tokenAmount || 0;
            }
        }
    }

    // Process non-SOL/non-stable token transfers
    for (const tt of tx.tokenTransfers) {
        const mint = tt.mint;

        // Skip SOL, stables, and system addresses
        if (mint === SOL_MINT || mint === WSOL_MINT) continue;
        if (STABLECOINS.has(mint)) continue;
        if (IGNORED_ADDRESSES.has(mint)) continue;
        if (!mint || mint.length < 30) continue;

        const tokenAmount = tt.tokenAmount || 0;
        if (tokenAmount === 0) continue;

        const isBuy = tt.toUserAccount === wallet;
        const isSell = tt.fromUserAccount === wallet;

        if (isBuy) {
            // Bought tokens: SOL/stable went out
            const cost = Math.abs(solDelta) || (Math.abs(stableDelta) / 190); // rough SOL equiv
            result.push({
                mint,
                type: 'buy',
                tokenAmount,
                solAmount: cost,
                timestamp: tx.timestamp * 1000,
                signature: tx.signature
            });
        } else if (isSell) {
            // Sold tokens: SOL/stable came in
            const proceeds = Math.abs(solDelta) || (Math.abs(stableDelta) / 190);
            result.push({
                mint,
                type: 'sell',
                tokenAmount,
                solAmount: proceeds,
                timestamp: tx.timestamp * 1000,
                signature: tx.signature
            });
        }
    }

    return result;
}

/**
 * Calculate PnL for a wallet
 * Returns per-token breakdown + summary
 */
async function calculateWalletPnL(wallet, options = {}) {
    const startTime = Date.now();

    // Fetch swap transactions
    const txs = await fetchSwapTransactions(wallet, options);

    if (txs.length === 0) {
        return {
            wallet,
            totalRealizedPnlSol: 0,
            totalRealizedPnlUsd: 0,
            totalCostBasisSol: 0,
            tokens: [],
            txCount: 0,
            computeTimeMs: Date.now() - startTime
        };
    }

    // Parse all swaps
    const allSwaps = [];
    for (const tx of txs) {
        const parsed = parseSwapTransaction(tx, wallet);
        allSwaps.push(...parsed);
    }

    // Group by mint
    const byMint = new Map();
    for (const swap of allSwaps) {
        if (!byMint.has(swap.mint)) {
            byMint.set(swap.mint, { buys: [], sells: [] });
        }
        if (swap.type === 'buy') {
            byMint.get(swap.mint).buys.push(swap);
        } else {
            byMint.get(swap.mint).sells.push(swap);
        }
    }

    // Calculate PnL per token using FIFO
    const tokenPnLs = [];
    let totalRealizedPnlSol = 0;
    let totalCostBasisSol = 0;

    for (const [mint, data] of byMint) {
        // Sort chronologically
        data.buys.sort((a, b) => a.timestamp - b.timestamp);
        data.sells.sort((a, b) => a.timestamp - b.timestamp);

        // FIFO matching
        const buyQueue = data.buys.map(b => ({
            remaining: b.tokenAmount,
            costPerToken: b.solAmount / b.tokenAmount,
            timestamp: b.timestamp
        }));

        let realizedPnlSol = 0;
        let totalBought = 0;
        let totalSold = 0;
        let totalCost = 0;
        let totalProceeds = 0;

        for (const buy of data.buys) {
            totalBought += buy.tokenAmount;
            totalCost += buy.solAmount;
        }

        for (const sell of data.sells) {
            totalSold += sell.tokenAmount;
            totalProceeds += sell.solAmount;

            let remaining = sell.tokenAmount;
            const proceedsPerToken = sell.solAmount / sell.tokenAmount;

            // Match against FIFO buy queue
            while (remaining > 0 && buyQueue.length > 0) {
                const buy = buyQueue[0];
                const matched = Math.min(remaining, buy.remaining);

                const costBasis = matched * buy.costPerToken;
                const proceeds = matched * proceedsPerToken;
                realizedPnlSol += proceeds - costBasis;

                buy.remaining -= matched;
                remaining -= matched;

                if (buy.remaining <= 0) {
                    buyQueue.shift();
                }
            }
        }

        // Remaining holdings
        const remainingTokens = buyQueue.reduce((sum, b) => sum + b.remaining, 0);
        const remainingCostBasis = buyQueue.reduce((sum, b) => sum + (b.remaining * b.costPerToken), 0);

        totalRealizedPnlSol += realizedPnlSol;
        totalCostBasisSol += totalCost;

        tokenPnLs.push({
            mint,
            totalBought,
            totalSold,
            totalCostSol: totalCost,
            totalProceedsSol: totalProceeds,
            realizedPnlSol,
            remainingTokens,
            remainingCostBasisSol: remainingCostBasis,
            buyCount: data.buys.length,
            sellCount: data.sells.length,
            firstBuy: data.buys[0]?.timestamp,
            lastActivity: Math.max(
                data.buys[data.buys.length - 1]?.timestamp || 0,
                data.sells[data.sells.length - 1]?.timestamp || 0
            )
        });
    }

    // Sort by realized PnL
    tokenPnLs.sort((a, b) => b.realizedPnlSol - a.realizedPnlSol);

    // Get current SOL price for USD conversion
    const solPrice = await getSolPrice();

    return {
        wallet,
        totalRealizedPnlSol,
        totalRealizedPnlUsd: totalRealizedPnlSol * solPrice,
        totalCostBasisSol,
        totalCostBasisUsd: totalCostBasisSol * solPrice,
        solPrice,
        tokens: tokenPnLs.slice(0, 50), // Top 50 tokens
        tokenCount: tokenPnLs.length,
        txCount: txs.length,
        swapCount: allSwaps.length,
        computeTimeMs: Date.now() - startTime
    };
}

/**
 * Get PnL for a specific token in a wallet
 */
async function getTokenPnL(wallet, mint, options = {}) {
    const fullPnL = await calculateWalletPnL(wallet, options);
    const tokenData = fullPnL.tokens.find(t => t.mint === mint);

    if (!tokenData) {
        return {
            wallet,
            mint,
            found: false,
            realizedPnlSol: 0,
            realizedPnlUsd: 0
        };
    }

    return {
        wallet,
        mint,
        found: true,
        ...tokenData,
        realizedPnlUsd: tokenData.realizedPnlSol * fullPnL.solPrice,
        remainingCostBasisUsd: tokenData.remainingCostBasisSol * fullPnL.solPrice,
        solPrice: fullPnL.solPrice
    };
}

module.exports = {
    calculateWalletPnL,
    getTokenPnL,
    fetchSwapTransactions,
    parseSwapTransaction
};
