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
 * Calculate conviction score for top 20 real holders
 * Returns 0-100 representing % of holders who maintained/accumulated
 */
async function calculateConviction(mint) {
    const TOP_HOLDERS = 20;
    const CANDIDATES = 50;

    try {
        // 1. Fetch all holders
        const allHolders = await fetchTokenHolders(mint);
        if (allHolders.length === 0) return { score: 0, analyzed: 0 };

        // 2. Filter pools from top candidates
        const candidates = allHolders.slice(0, CANDIDATES);
        const poolCheck = await batchCheckPools(candidates.map(h => h.address));

        const realHolders = candidates.filter(h => !poolCheck.get(h.address));
        const poolsFiltered = candidates.length - realHolders.length;

        logger.info(`[Conviction] ${mint.slice(0,8)}: ${poolsFiltered} pools filtered`);

        // 3. Analyze top 20 real holders
        const top20 = realHolders.slice(0, TOP_HOLDERS);
        if (top20.length === 0) return { score: 0, analyzed: 0 };

        let accumulators = 0;
        let holders = 0;
        let analyzed = 0;

        for (const holder of top20) {
            try {
                const retention = await getHolderRetention(holder.address, mint);
                const classification = classifyRetention(retention);

                if (classification === 'accumulator') accumulators++;
                else if (classification === 'holder') holders++;

                analyzed++;
                await sleep(100); // Rate limit
            } catch (e) {
                // Skip failed holders
            }
        }

        if (analyzed === 0) return { score: 0, analyzed: 0 };

        const score = Math.round(((accumulators + holders) / analyzed) * 100);

        logger.info(`[Conviction] ${mint.slice(0,8)}: ${score}% (${accumulators} acc, ${holders} hold, ${analyzed} total)`);

        return { score, analyzed, accumulators, holders };

    } catch (error) {
        logger.error(`[Conviction] Error for ${mint}: ${error.message}`);
        return { score: 0, analyzed: 0 };
    }
}

// ============================================
// K-SCORE CALCULATION
// ============================================

/**
 * Compute K-Score with conviction
 *
 * Breakdown (100 max):
 * - Verification:  30 pts (community verified)
 * - Volume:        20 pts (>100k = 20, >10k = 10)
 * - Conviction:    40 pts (scaled from 0-100 conviction)
 * - Market Cap:    10 pts (>100k)
 */
async function computeScoreInternal(mint, dbData = null, skipConviction = false) {
    let score = 0;

    try {
        // 1. Verification (Max 30 pts)
        if (dbData && (dbData.hasCommunityUpdate || dbData.hascommunityupdate)) {
            score += 30;
        } else {
            score += 5; // Base discovery
        }

        // 2. Volume (Max 20 pts)
        if (dbData) {
            const vol = dbData.volume24h || 0;
            if (vol > 100000) score += 20;
            else if (vol > 10000) score += 10;
        }

        // 3. Conviction (Max 40 pts) - Only if Helius configured
        if (!skipConviction && HELIUS_API_KEY) {
            const conviction = await calculateConviction(mint);
            // Scale: 100% conviction = 40 pts
            score += Math.round(conviction.score * 0.4);
        }

        // 4. Market Cap (Max 10 pts)
        if (dbData) {
            const mcap = dbData.marketCap || dbData.marketcap || 0;
            if (mcap > 100000) score += 10;
        }

        return Math.min(score, 100);

    } catch (e) {
        logger.error(`[K-Score] Calc error ${mint}: ${e.message}`);
        return 10;
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

        logger.info(`[K-Score] Immediate calc for ${token.ticker}`);
        const score = await computeScoreInternal(mint, token);

        await db.run(`
            UPDATE tokens
            SET k_score = $1, last_k_calc = $2
            WHERE mint = $3
        `, [score, Date.now(), mint]);

        return score;
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

    logger.info("[K-Score] Starting cycle...");

    try {
        const tokens = await db.all(`
            SELECT * FROM tokens
            WHERE hasCommunityUpdate = 1
            OR volume24h > 5000
        `);

        if (!tokens || tokens.length === 0) {
            logger.info("[K-Score] No eligible tokens.");
            return;
        }

        logger.info(`[K-Score] Updating ${tokens.length} tokens...`);

        for (const t of tokens) {
            try {
                const score = await computeScoreInternal(t.mint, t);

                await db.run(`
                    UPDATE tokens
                    SET k_score = $1, last_k_calc = $2
                    WHERE mint = $3
                `, [score, Date.now(), t.mint]);

            } catch (err) {
                logger.warn(`[K-Score] Failed for ${t.mint}: ${err.message}`);
            }

            await sleep(500); // Slower between tokens (conviction is heavy)
        }

        logger.info("[K-Score] Cycle complete.");

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
    calculateTokenScore: async (mint) => computeScoreInternal(mint, null, true),
    calculateConviction, // Exposed for testing
};
