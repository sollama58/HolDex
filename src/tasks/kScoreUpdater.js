/**
 * K-Score Updater v2 - With Conviction Analysis
 *
 * Integrates asdf-oracle conviction logic directly.
 * Analyzes top 20 REAL holders (excluding DEX pools).
 *
 * K-Score = verification + volume + conviction + mcap
 */

const config = require('../config/env');
const { logger } = require('../services');

// ============================================
// HELIUS CONFIG
// ============================================

const HELIUS_API_KEY = config.HELIUS_API_KEY;
const HELIUS_RPC = HELIUS_API_KEY
    ? `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`
    : null;

// Rate limiting
const RATE_LIMIT = 50;
const REQUEST_INTERVAL = 1000 / RATE_LIMIT;
let lastRequestTime = 0;

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ============================================
// DEX PROGRAMS - Filter out pools from holders
// ============================================

const DEX_PROGRAMS = new Set([
    // Raydium
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
    'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',
    '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h',
    // Orca
    'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
    // Meteora
    'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',
    // OpenBook/Serum
    'srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX',
    '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin',
    'opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb',
    // PumpFun
    '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',
    '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg',
    'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA',
]);

const KNOWN_POOL_WALLETS = new Set([
    'CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM', // PumpFun Fee
]);

// Pool cache (address -> { isPool, ts })
const poolCache = new Map();
const POOL_CACHE_TTL = 3600000; // 1 hour

// ============================================
// HELIUS API FUNCTIONS
// ============================================

async function rateLimitedFetch(url, options) {
    const now = Date.now();
    const timeSince = now - lastRequestTime;
    if (timeSince < REQUEST_INTERVAL) {
        await sleep(REQUEST_INTERVAL - timeSince);
    }
    lastRequestTime = Date.now();
    return fetch(url, options);
}

async function heliusRpc(method, params) {
    if (!HELIUS_RPC) return null;

    try {
        const response = await rateLimitedFetch(HELIUS_RPC, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ jsonrpc: '2.0', id: Date.now(), method, params }),
        });
        const data = await response.json();
        if (data.error) throw new Error(data.error.message);
        return data.result;
    } catch (error) {
        logger.error(`[Helius] RPC error: ${error.message}`);
        return null;
    }
}

async function getEnhancedTransactions(address, options = {}) {
    if (!HELIUS_API_KEY) return [];

    const params = new URLSearchParams({ 'api-key': HELIUS_API_KEY });
    if (options.limit) params.append('limit', options.limit.toString());
    if (options.before) params.append('before', options.before);

    const url = `https://api-mainnet.helius-rpc.com/v0/addresses/${address}/transactions?${params}`;

    try {
        const response = await rateLimitedFetch(url, { method: 'GET' });
        if (!response.ok) return [];
        return await response.json();
    } catch (error) {
        return [];
    }
}

// ============================================
// POOL FILTERING
// ============================================

async function batchCheckPools(addresses) {
    const results = new Map();
    const uncached = [];

    for (const addr of addresses) {
        if (KNOWN_POOL_WALLETS.has(addr)) {
            results.set(addr, true);
            continue;
        }
        const cached = poolCache.get(addr);
        if (cached && Date.now() - cached.ts < POOL_CACHE_TTL) {
            results.set(addr, cached.isPool);
        } else {
            uncached.push(addr);
        }
    }

    if (uncached.length > 0) {
        try {
            const infos = await heliusRpc('getMultipleAccounts', [uncached, { encoding: 'base64' }]);
            for (let i = 0; i < uncached.length; i++) {
                const addr = uncached[i];
                const info = infos?.value?.[i];
                const isPool = info ? DEX_PROGRAMS.has(info.owner) : false;
                results.set(addr, isPool);
                poolCache.set(addr, { isPool, ts: Date.now() });
            }
        } catch (error) {
            for (const addr of uncached) {
                results.set(addr, false);
            }
        }
    }

    return results;
}

// ============================================
// CONVICTION CALCULATION (asdf-oracle logic)
// ============================================

async function fetchTokenHolders(mint) {
    const holders = [];
    let cursor = null;

    while (true) {
        const params = { mint, limit: 1000 };
        if (cursor) params.cursor = cursor;

        const result = await heliusRpc('getTokenAccounts', params);
        if (!result?.token_accounts) break;

        for (const acc of result.token_accounts) {
            if (acc.amount > 0) {
                holders.push({ address: acc.owner, balance: acc.amount });
            }
        }

        cursor = result.cursor;
        if (!cursor) break;
    }

    holders.sort((a, b) => b.balance - a.balance);
    return holders;
}

async function getHolderRetention(wallet, mint) {
    let firstBuyAmount = 0;
    let currentBalance = 0;
    let before = null;

    for (let page = 0; page < 5; page++) {
        const txs = await getEnhancedTransactions(wallet, { limit: 100, before });
        if (!txs || txs.length === 0) break;

        for (const tx of txs) {
            if (!tx.tokenTransfers) continue;
            for (const transfer of tx.tokenTransfers) {
                if (transfer.mint !== mint) continue;

                const amount = transfer.tokenAmount || 0;
                if (transfer.toUserAccount === wallet) {
                    currentBalance += amount;
                    firstBuyAmount = amount; // Going backwards, last seen = first buy
                }
                if (transfer.fromUserAccount === wallet) {
                    currentBalance -= amount;
                }
            }
        }

        before = txs[txs.length - 1]?.signature;
        if (!before || txs.length < 100) break;
    }

    if (currentBalance < 0) currentBalance = 0;
    return firstBuyAmount > 0 ? currentBalance / firstBuyAmount : 0;
}

function classifyRetention(retention) {
    if (retention >= 1.5) return 'accumulator';
    if (retention >= 1.0) return 'holder';
    if (retention >= 0.5) return 'reducer';
    return 'extractor';
}

/**
 * Calculate conviction score AND real holders count
 *
 * @param {string} mint - Token mint address
 * @param {number} priceUsd - Current token price in USD
 * @param {number} decimals - Token decimals (default 9 for SPL)
 * @returns {Object} { score, analyzed, accumulators, holders, reducers, extractors, realHoldersCount, totalHolders }
 */
async function calculateConvictionAndHolders(mint, priceUsd = 0, decimals = 9) {
    const TOP_HOLDERS = 20;
    const CANDIDATES = 50;
    const MIN_USD_VALUE = 1; // $1 minimum to count as "real holder"

    try {
        // 1. Fetch all holders
        const allHolders = await fetchTokenHolders(mint);
        if (allHolders.length === 0) {
            return { score: 0, analyzed: 0, realHoldersCount: 0, totalHolders: 0 };
        }

        // 2. Calculate real holders ($1+ balance)
        let realHoldersCount = 0;
        if (priceUsd > 0) {
            const divisor = Math.pow(10, decimals);
            for (const h of allHolders) {
                const usdValue = (h.balance / divisor) * priceUsd;
                if (usdValue >= MIN_USD_VALUE) {
                    realHoldersCount++;
                }
            }
        } else {
            // Fallback: count all holders with balance > 0
            realHoldersCount = allHolders.length;
        }

        logger.info(`[Holders] ${mint.slice(0,8)}: ${realHoldersCount} real ($1+) / ${allHolders.length} total`);

        // 3. Filter pools from top candidates for conviction analysis
        const candidates = allHolders.slice(0, CANDIDATES);
        const poolCheck = await batchCheckPools(candidates.map(h => h.address));

        const realCandidates = candidates.filter(h => !poolCheck.get(h.address));
        const poolsFiltered = candidates.length - realCandidates.length;

        logger.info(`[Conviction] ${mint.slice(0,8)}: ${poolsFiltered} pools filtered`);

        // 4. Analyze top 20 real holders for conviction
        const top20 = realCandidates.slice(0, TOP_HOLDERS);
        if (top20.length === 0) {
            return { score: 0, analyzed: 0, realHoldersCount, totalHolders: allHolders.length };
        }

        let accumulators = 0;
        let holders = 0;
        let reducers = 0;
        let extractors = 0;
        let analyzed = 0;

        for (const holder of top20) {
            try {
                const retention = await getHolderRetention(holder.address, mint);
                const classification = classifyRetention(retention);

                if (classification === 'accumulator') accumulators++;
                else if (classification === 'holder') holders++;
                else if (classification === 'reducer') reducers++;
                else if (classification === 'extractor') extractors++;

                analyzed++;
                await sleep(100); // Rate limit
            } catch (e) {
                // Skip failed holders
            }
        }

        if (analyzed === 0) {
            return { score: 0, analyzed: 0, realHoldersCount, totalHolders: allHolders.length };
        }

        const score = Math.round(((accumulators + holders) / analyzed) * 100);

        logger.info(`[Conviction] ${mint.slice(0,8)}: ${score}% (${accumulators} acc, ${holders} hold, ${analyzed} total)`);

        return {
            score,
            analyzed,
            accumulators,
            holders,
            reducers,
            extractors,
            realHoldersCount,
            totalHolders: allHolders.length
        };

    } catch (error) {
        logger.error(`[Conviction] Error for ${mint}: ${error.message}`);
        return {
            score: 0, analyzed: 0, accumulators: 0, holders: 0,
            reducers: 0, extractors: 0, realHoldersCount: 0, totalHolders: 0
        };
    }
}

// Backwards compatibility alias
async function calculateConviction(mint) {
    return calculateConvictionAndHolders(mint, 0, 9);
}

// ============================================
// K-SCORE CALCULATION v5 - WITH REAL HOLDERS
// ============================================

/**
 * Clamp value between min and max
 */
function clamp(val, min, max) {
    return Math.max(min, Math.min(max, val));
}

/**
 * Normalize value to 0-1 using logarithmic scale
 * Maps value from [min, max] to [0, 1] using log₁₀
 */
function logNormalize(value, min, max) {
    if (value <= 0 || value < min) return 0;
    const logRange = Math.log10(max / min);
    const logValue = Math.log10(value / min);
    return clamp(logValue / logRange, 0, 1);
}

/**
 * K-Score v5 - Weighted Geometric Mean with Real Holders
 *
 * Formula: K = 100 × C^0.45 × H^0.20 × L^0.15 × M^0.10 × V^0.10
 *
 * Weights (sum = 1):
 * - Conviction:    0.45 (on-chain, hard to fake)
 * - Real Holders:  0.20 (on-chain, costly to fake)
 * - Liquidity:     0.15 (on-chain)
 * - Market Cap:    0.10 (on-chain)
 * - Volume:        0.10 (DexScreener, can be wash traded)
 *
 * Normalization ranges:
 * - Conviction: 0-100% (linear)
 * - Holders: 100 → 100k (log scale)
 * - Liquidity: $1k → $10M (log scale)
 * - Market Cap: $10k → $1B (log scale)
 * - Volume: $1k → $10M (log scale)
 */
const WEIGHTS = {
    conviction: 0.45,
    holders: 0.20,
    liquidity: 0.15,
    mcap: 0.10,
    volume: 0.10
};

async function computeScoreInternal(mint, dbData = null, skipConviction = false) {
    // Normalized values (0-1)
    let normalized = { conviction: 0, holders: 0, liquidity: 0, mcap: 0, volume: 0 };

    try {
        // Get price and decimals for holder calculation
        const priceUsd = dbData?.priceusd || dbData?.priceUsd || 0;
        const decimals = dbData?.decimals || 9;

        // 1. CONVICTION + REAL HOLDERS (on-chain via Helius)
        let convictionData = null;
        if (!skipConviction && HELIUS_API_KEY) {
            convictionData = await calculateConvictionAndHolders(mint, priceUsd, decimals);
            normalized.conviction = convictionData.score / 100;
            // Holders: 100 → 100,000 log scale
            normalized.holders = logNormalize(convictionData.realHoldersCount, 100, 100000);
        }

        // 2. LIQUIDITY - Log scale: $1k → $10M → 0-1
        if (dbData) {
            const liq = dbData.liquidity || 0;
            normalized.liquidity = logNormalize(liq, 1000, 10000000);
        }

        // 3. MARKET CAP - Log scale: $10k → $1B → 0-1
        if (dbData) {
            const mcap = dbData.marketCap || dbData.marketcap || 0;
            normalized.mcap = logNormalize(mcap, 10000, 1000000000);
        }

        // 4. VOLUME 24H - Log scale: $1k → $10M → 0-1 (DexScreener, lower weight)
        if (dbData) {
            const vol = dbData.volume24h || 0;
            normalized.volume = logNormalize(vol, 1000, 10000000);
        }

        // Weighted Geometric Mean: K = 100 × C^w1 × H^w2 × L^w3 × M^w4 × V^w5
        // Add small epsilon to avoid zero (allows partial scores)
        const epsilon = 0.01;
        const C = Math.max(normalized.conviction, epsilon);
        const H = Math.max(normalized.holders, epsilon);
        const L = Math.max(normalized.liquidity, epsilon);
        const M = Math.max(normalized.mcap, epsilon);
        const V = Math.max(normalized.volume, epsilon);

        const geometricScore = Math.pow(C, WEIGHTS.conviction) *
                               Math.pow(H, WEIGHTS.holders) *
                               Math.pow(L, WEIGHTS.liquidity) *
                               Math.pow(M, WEIGHTS.mcap) *
                               Math.pow(V, WEIGHTS.volume);

        const score = Math.round(100 * geometricScore);

        // Display breakdown as percentages
        const displayBreakdown = {
            conviction: Math.round(normalized.conviction * 100),
            holders: Math.round(normalized.holders * 100),
            liquidity: Math.round(normalized.liquidity * 100),
            mcap: Math.round(normalized.mcap * 100),
            volume: Math.round(normalized.volume * 100)
        };

        logger.info(`[K-Score] ${mint.slice(0,8)}: ${score} (C:${displayBreakdown.conviction}% H:${displayBreakdown.holders}% L:${displayBreakdown.liquidity}% M:${displayBreakdown.mcap}% V:${displayBreakdown.volume}%)`);

        return {
            score,
            conviction: convictionData,
            breakdown: displayBreakdown
        };

    } catch (e) {
        logger.error(`[K-Score] Calc error ${mint}: ${e.message}`);
        return { score: 0, conviction: null, breakdown: {} };
    }
}

// ============================================
// PUBLIC API
// ============================================

/**
 * Update single token score immediately (for admin approval)
 */
async function updateSingleToken(deps, mint) {
    const { db } = deps;
    try {
        const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
        if (!token) return;

        logger.info(`[K-Score] Immediate calc for ${token.name || token.ticker}`);
        const result = await computeScoreInternal(mint, token);
        const conviction = result.conviction || {};

        await db.run(`
            UPDATE tokens
            SET k_score = $1,
                last_k_score_update = $2,
                conviction_score = $3,
                conviction_accumulators = $4,
                conviction_holders = $5,
                conviction_reducers = $6,
                conviction_extractors = $7,
                conviction_analyzed = $8,
                holders = $9,
                last_holder_check = $10
            WHERE mint = $11
        `, [
            result.score,
            Date.now().toString(),
            conviction.score || 0,
            conviction.accumulators || 0,
            conviction.holders || 0,
            conviction.reducers || 0,
            conviction.extractors || 0,
            conviction.analyzed || 0,
            conviction.realHoldersCount || 0,
            Date.now().toString(),
            mint
        ]);

        return result.score;
    } catch (e) {
        logger.error(`[K-Score] Failed single update for ${mint}:`, e);
        return 0;
    }
}

/**
 * Batch update K-Scores (scheduled task)
 */
async function updateKScores(deps) {
    const { db } = deps;

    logger.info("[K-Score v5] Starting cycle...");

    try {
        // Only calculate K-Score for verified tokens (saves Helius API credits)
        const tokens = await db.all(`
            SELECT * FROM tokens
            WHERE hascommunityupdate = TRUE
            ORDER BY volume24h DESC NULLS LAST
        `);

        if (!tokens || tokens.length === 0) {
            logger.info("[K-Score] No eligible tokens.");
            return;
        }

        logger.info(`[K-Score] Updating ${tokens.length} tokens...`);

        for (const t of tokens) {
            try {
                const result = await computeScoreInternal(t.mint, t);
                const conviction = result.conviction || {};

                await db.run(`
                    UPDATE tokens
                    SET k_score = $1,
                        last_k_score_update = $2,
                        conviction_score = $3,
                        conviction_accumulators = $4,
                        conviction_holders = $5,
                        conviction_reducers = $6,
                        conviction_extractors = $7,
                        conviction_analyzed = $8,
                        holders = $9,
                        last_holder_check = $10
                    WHERE mint = $11
                `, [
                    result.score,
                    Date.now().toString(),
                    conviction.score || 0,
                    conviction.accumulators || 0,
                    conviction.holders || 0,
                    conviction.reducers || 0,
                    conviction.extractors || 0,
                    conviction.analyzed || 0,
                    conviction.realHoldersCount || 0,
                    Date.now().toString(),
                    t.mint
                ]);

            } catch (err) {
                logger.warn(`[K-Score] Failed for ${t.mint}: ${err.message}`);
            }

            await sleep(500); // Slower between tokens (conviction is heavy)
        }

        logger.info("[K-Score v5] Cycle complete.");

    } catch (e) {
        logger.error("[K-Score] Cycle error", e);
    }
}

function start(deps) {
    if (!HELIUS_API_KEY) {
        logger.warn("[K-Score] No HELIUS_API_KEY - conviction analysis disabled");
    }

    // Run every 10 minutes
    setInterval(() => updateKScores(deps), 600000);
    // Run once after startup (delay 30s to let other services init)
    setTimeout(() => updateKScores(deps), 30000);
}

module.exports = {
    start,
    updateSingleToken,
    calculateTokenScore: async (mint) => {
        const result = await computeScoreInternal(mint, null, true);
        return result.score;
    },
    calculateConviction, // Exposed for testing
};
