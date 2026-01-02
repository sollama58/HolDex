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

/**
 * Fetch actual on-chain token balances AND prices for a wallet using Helius DAS API
 * "On-chain is truth" - real balances + Helius price_info for top 10k tokens
 * Returns: { balances: Map<mint, balance>, prices: Map<mint, priceInSol> }
 */
async function fetchOnChainHoldings(wallet, solPrice) {
    if (!HELIUS_API_KEY) return { balances: new Map(), prices: new Map() };

    try {
        const url = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                jsonrpc: '2.0',
                id: 'holdings',
                method: 'getAssetsByOwner',
                params: {
                    ownerAddress: wallet,
                    page: 1,
                    limit: 1000,
                    displayOptions: { showFungible: true }
                }
            }),
            signal: AbortSignal.timeout(15000)
        });

        if (!response.ok) {
            logger.warn(`[PnL] Helius DAS API error: ${response.status}`);
            return { balances: new Map(), prices: new Map() };
        }

        const data = await response.json();
        const balanceMap = new Map();
        const priceMap = new Map();

        if (data.result?.items) {
            for (const item of data.result.items) {
                // Only fungible tokens
                if (item.interface === 'FungibleToken' || item.interface === 'FungibleAsset') {
                    const mint = item.id;
                    const balance = item.token_info?.balance || 0;
                    const decimals = item.token_info?.decimals || 0;
                    const actualBalance = balance / Math.pow(10, decimals);

                    if (actualBalance > 0) {
                        balanceMap.set(mint, actualBalance);

                        // Extract price_info if available (Helius provides for top 10k tokens)
                        const priceInfo = item.token_info?.price_info;
                        if (priceInfo?.price_per_token) {
                            // price_per_token is in USD, convert to SOL
                            const priceUsd = priceInfo.price_per_token;
                            const priceInSol = solPrice > 0 ? priceUsd / solPrice : 0;
                            priceMap.set(mint, priceInSol);
                        }
                    }
                }
            }
        }

        return { balances: balanceMap, prices: priceMap };
    } catch (error) {
        logger.error(`[PnL] Holdings fetch error: ${error.message}`);
        return { balances: new Map(), prices: new Map() };
    }
}

// Legacy function for compatibility
async function fetchOnChainBalances(wallet) {
    const { balances } = await fetchOnChainHoldings(wallet, 0);
    return balances;
}

/**
 * Fetch on-chain prices for multiple tokens using Jupiter Aggregator
 * Jupiter pulls prices directly from on-chain DEX pools (Raydium, Orca, PumpSwap, etc.)
 * Returns prices in SOL (using vsToken=SOL)
 *
 * Note: Jupiter is on-chain truth - it aggregates real pool reserves, not off-chain data
 */
async function fetchTokenPrices(mints) {
    if (!mints || mints.length === 0) return new Map();

    const priceMap = new Map();

    try {
        // Jupiter API accepts up to 100 tokens per request
        const chunks = [];
        for (let i = 0; i < mints.length; i += 100) {
            chunks.push(mints.slice(i, i + 100));
        }

        for (const chunk of chunks) {
            const ids = chunk.join(',');
            // Get prices in SOL terms (on-chain pool prices)
            const url = `https://api.jup.ag/price/v2?ids=${ids}&vsToken=So11111111111111111111111111111111111111112`;

            const response = await fetch(url, {
                method: 'GET',
                signal: AbortSignal.timeout(10000)
            });

            if (!response.ok) {
                logger.warn(`[PnL] Jupiter API error: ${response.status}`);
                continue;
            }

            const data = await response.json();
            if (data.data) {
                for (const [mint, priceData] of Object.entries(data.data)) {
                    if (priceData && priceData.price) {
                        priceMap.set(mint, parseFloat(priceData.price));
                    }
                }
            }

            // Rate limiting between chunks
            if (chunks.length > 1) {
                await new Promise(r => setTimeout(r, 200));
            }
        }

        return priceMap;
    } catch (error) {
        logger.error(`[PnL] Price fetch error: ${error.message}`);
        return priceMap;
    }
}

/**
 * Fetch prices from DB pools table (fastest fallback)
 * Uses pre-indexed pool prices from the snapshotter
 * Returns prices in SOL (converted from USD using solPrice)
 */
async function fetchPoolDbPrices(db, mints, solPrice) {
    const priceMap = new Map();
    if (!db || mints.length === 0 || solPrice <= 0) return priceMap;

    try {
        // Query pools with best liquidity for each mint
        const placeholders = mints.map((_, i) => `$${i + 1}`).join(',');
        const rows = await db.all(`
            SELECT DISTINCT ON (mint) mint, price_usd, dex, liquidity_usd
            FROM pools
            WHERE mint IN (${placeholders}) AND price_usd > 0
            ORDER BY mint, liquidity_usd DESC NULLS LAST
        `, mints);

        for (const row of rows) {
            // Convert USD price to SOL
            const priceInSol = row.price_usd / solPrice;
            priceMap.set(row.mint, priceInSol);
        }

        logger.debug(`[PnL] DB pools: found ${priceMap.size}/${mints.length} prices`);
    } catch (error) {
        logger.debug(`[PnL] DB pool query error: ${error.message}`);
    }

    return priceMap;
}

/**
 * Fetch prices from on-chain pool reserves (batched for speed)
 * For pump.fun tokens not covered by Helius/Jupiter
 * Uses batch RPC calls: 3 calls total instead of N*3
 */
async function fetchPoolReservePrices(mints) {
    const priceMap = new Map();
    if (!HELIUS_API_KEY || mints.length === 0) return priceMap;

    const url = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;

    try {
        // Step 1: Batch getTokenLargestAccounts for all mints
        const largestAccountsReqs = mints.map((mint, i) => ({
            jsonrpc: '2.0',
            id: `la-${i}`,
            method: 'getTokenLargestAccounts',
            params: [mint]
        }));

        const laResp = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(largestAccountsReqs),
            signal: AbortSignal.timeout(15000)
        });

        if (!laResp.ok) return priceMap;
        const laResults = await laResp.json();

        // Collect token accounts to query (top holder = likely pool)
        const accountsToQuery = [];
        const mintAccountMap = new Map(); // address -> { mint, tokenAmount }

        for (let i = 0; i < mints.length; i++) {
            const accounts = laResults[i]?.result?.value || [];
            const mint = mints[i];

            // Get top 2 holders (pools are usually largest)
            for (const acc of accounts.slice(0, 2)) {
                const tokenAmount = parseFloat(acc.uiAmountString || '0');
                if (tokenAmount > 0) {
                    accountsToQuery.push(acc.address);
                    mintAccountMap.set(acc.address, { mint, tokenAmount });
                }
            }
        }

        if (accountsToQuery.length === 0) return priceMap;

        // Step 2: Batch getAccountInfo for all token accounts
        const infoReqs = accountsToQuery.map((addr, i) => ({
            jsonrpc: '2.0',
            id: `info-${i}`,
            method: 'getAccountInfo',
            params: [addr, { encoding: 'jsonParsed' }]
        }));

        const infoResp = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(infoReqs),
            signal: AbortSignal.timeout(15000)
        });

        if (!infoResp.ok) return priceMap;
        const infoResults = await infoResp.json();

        // Collect unique owners (pool addresses)
        const owners = new Set();
        const ownerTokenMap = new Map(); // owner -> [{ mint, tokenAmount }]

        for (let i = 0; i < accountsToQuery.length; i++) {
            const owner = infoResults[i]?.result?.value?.data?.parsed?.info?.owner;
            if (owner) {
                const data = mintAccountMap.get(accountsToQuery[i]);
                owners.add(owner);
                if (!ownerTokenMap.has(owner)) ownerTokenMap.set(owner, []);
                ownerTokenMap.get(owner).push(data);
            }
        }

        if (owners.size === 0) return priceMap;

        // Step 3: Batch getBalance for all pool owners
        const ownerList = Array.from(owners);
        const balReqs = ownerList.map((owner, i) => ({
            jsonrpc: '2.0',
            id: `bal-${i}`,
            method: 'getBalance',
            params: [owner]
        }));

        const balResp = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(balReqs),
            signal: AbortSignal.timeout(15000)
        });

        if (!balResp.ok) return priceMap;
        const balResults = await balResp.json();

        // Step 4: Calculate price = SOL / tokens
        for (let i = 0; i < ownerList.length; i++) {
            const solLamports = balResults[i]?.result?.value || 0;
            const solAmount = solLamports / 1e9;

            // Need some SOL to be a real pool (0.1 SOL minimum)
            if (solAmount < 0.1) continue;

            const owner = ownerList[i];
            const tokenDatas = ownerTokenMap.get(owner) || [];

            for (const { mint, tokenAmount } of tokenDatas) {
                if (!priceMap.has(mint) && tokenAmount > 0) {
                    const priceInSol = solAmount / tokenAmount;
                    priceMap.set(mint, priceInSol);
                }
            }
        }

        logger.debug(`[PnL] Pool reserves: found ${priceMap.size}/${mints.length} prices`);
    } catch (error) {
        logger.debug(`[PnL] Pool reserve error: ${error.message}`);
    }

    return priceMap;
}

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
 * @param {string} wallet - Wallet address
 * @param {Object} options - { db, maxPages, limit }
 */
async function calculateWalletPnL(wallet, options = {}) {
    const { db } = options; // Optional DB connection for pool price fallback
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

    // Fetch ACTUAL on-chain balances AND prices (on-chain is truth)
    // Helius DAS returns price_info for top 10k tokens
    const { balances: onChainBalances, prices: heliusPrices } = await fetchOnChainHoldings(wallet, solPrice);

    // Update tokens with real on-chain balances
    for (const token of tokenPnLs) {
        const actualBalance = onChainBalances.get(token.mint);
        if (actualBalance !== undefined) {
            token.onChainBalance = actualBalance;
            // Flag if there's a discrepancy
            if (Math.abs(actualBalance - token.remainingTokens) > 0.01) {
                token.balanceDiscrepancy = actualBalance - token.remainingTokens;
            }
        }
    }

    // Add tokens that are on-chain but not in swap history (airdrops, transfers)
    for (const [mint, balance] of onChainBalances) {
        if (!tokenPnLs.find(t => t.mint === mint) && balance > 0.000001) {
            tokenPnLs.push({
                mint,
                totalBought: 0,
                totalSold: 0,
                totalCostSol: 0,
                totalProceedsSol: 0,
                realizedPnlSol: 0,
                remainingTokens: 0,
                remainingCostBasisSol: 0,
                onChainBalance: balance,
                buyCount: 0,
                sellCount: 0,
                source: 'airdrop/transfer' // Not from swaps
            });
        }
    }

    // Get prices: use Helius prices first, then Jupiter fallback
    const mintsWithHoldings = tokenPnLs
        .filter(t => (t.onChainBalance || t.remainingTokens) > 0.000001)
        .map(t => t.mint);

    let totalUnrealizedPnlSol = 0;
    let totalCurrentValueSol = 0;

    if (mintsWithHoldings.length > 0) {
        // Start with Helius prices (already fetched with balances)
        const priceMap = new Map(heliusPrices);

        // Fallback 1: Jupiter for tokens not covered by Helius
        let mintsNeedingPrices = mintsWithHoldings.filter(m => !priceMap.has(m));
        if (mintsNeedingPrices.length > 0) {
            const jupiterPrices = await fetchTokenPrices(mintsNeedingPrices);
            for (const [mint, price] of jupiterPrices) {
                priceMap.set(mint, price);
            }
        }

        // Fallback 2: DB pools table (pre-indexed prices from snapshotter)
        mintsNeedingPrices = mintsWithHoldings.filter(m => !priceMap.has(m));
        if (mintsNeedingPrices.length > 0 && db) {
            const dbPoolPrices = await fetchPoolDbPrices(db, mintsNeedingPrices, solPrice);
            for (const [mint, price] of dbPoolPrices) {
                priceMap.set(mint, price);
            }
        }

        // Fallback 3: On-chain pool reserves (for tokens not in DB yet)
        mintsNeedingPrices = mintsWithHoldings.filter(m => !priceMap.has(m));
        if (mintsNeedingPrices.length > 0) {
            const poolPrices = await fetchPoolReservePrices(mintsNeedingPrices);
            for (const [mint, price] of poolPrices) {
                priceMap.set(mint, price);
            }
        }

        for (const token of tokenPnLs) {
            // Use on-chain balance (truth) or fall back to calculated
            const actualHolding = token.onChainBalance ?? token.remainingTokens;

            if (actualHolding > 0.000001) {
                const priceInSol = priceMap.get(token.mint);
                if (priceInSol) {
                    const currentValueSol = actualHolding * priceInSol;
                    // Cost basis: use calculated if we have swap history, else 0 (airdrop)
                    const costBasis = token.remainingCostBasisSol || 0;
                    const unrealizedPnlSol = currentValueSol - costBasis;

                    token.currentPriceSol = priceInSol;
                    token.currentValueSol = currentValueSol;
                    token.actualHolding = actualHolding;
                    token.unrealizedPnlSol = unrealizedPnlSol;
                    token.unrealizedPnlUsd = unrealizedPnlSol * solPrice;
                    token.unrealizedPnlPercent = costBasis > 0
                        ? ((currentValueSol / costBasis) - 1) * 100
                        : (currentValueSol > 0 ? 100 : 0); // Airdrop = 100% gain

                    totalUnrealizedPnlSol += unrealizedPnlSol;
                    totalCurrentValueSol += currentValueSol;
                } else {
                    token.currentPriceSol = null;
                    token.currentValueSol = null;
                    token.actualHolding = actualHolding;
                    token.unrealizedPnlSol = null;
                }
            }
        }
    }

    // Re-sort by total PnL (realized + unrealized)
    tokenPnLs.sort((a, b) => {
        const aTotalPnl = a.realizedPnlSol + (a.unrealizedPnlSol || 0);
        const bTotalPnl = b.realizedPnlSol + (b.unrealizedPnlSol || 0);
        return bTotalPnl - aTotalPnl;
    });

    return {
        wallet,
        totalRealizedPnlSol,
        totalRealizedPnlUsd: totalRealizedPnlSol * solPrice,
        totalUnrealizedPnlSol,
        totalUnrealizedPnlUsd: totalUnrealizedPnlSol * solPrice,
        totalPnlSol: totalRealizedPnlSol + totalUnrealizedPnlSol,
        totalPnlUsd: (totalRealizedPnlSol + totalUnrealizedPnlSol) * solPrice,
        totalCostBasisSol,
        totalCostBasisUsd: totalCostBasisSol * solPrice,
        totalCurrentValueSol,
        totalCurrentValueUsd: totalCurrentValueSol * solPrice,
        solPrice,
        tokens: tokenPnLs.slice(0, 50), // Top 50 tokens for API response
        _allTokens: tokenPnLs, // Full list for internal use (getTokenPnL)
        tokenCount: tokenPnLs.length,
        holdingsCount: mintsWithHoldings.length,
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
    // Search in full list, not just top 50
    const tokenData = (fullPnL._allTokens || fullPnL.tokens).find(t => t.mint === mint);

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
