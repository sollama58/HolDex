/**
 * K-Score Updater v8 - 3 Pillars Formula
 *
 * DIAMOND HANDS (50%):
 *   - conviction: % accumulators + holders among top 20
 *   - accExtRatio: accumulators / extractors ratio
 *
 * ORGANIC GROWTH (35%):
 *   - holders: real holders count ($1+ balance)
 *   - top10: distribution fairness (inverted concentration)
 *
 * LONGEVITY (15%):
 *   - age: token age in days
 *
 * Eliminates manipulable metrics (volume, mcap, liquidity)
 * Focus on on-chain behavior only
 */

const config = require('../config/env');
const { logger } = require('../services');
const priceService = require('../services/priceService');
const bs58 = require('bs58');
const { getClient: getRedisClient } = require('../services/redis');
const verification = require('../services/verificationService');

// ============================================
// HELIUS CONFIG
// ============================================

const HELIUS_API_KEY = config.HELIUS_API_KEY;
const HELIUS_RPC_URL = 'https://mainnet.helius-rpc.com/';

// Security: API key passed via header, not URL querystring
const HELIUS_HEADERS = HELIUS_API_KEY
    ? { 'Content-Type': 'application/json', 'Authorization': `Bearer ${HELIUS_API_KEY}` }
    : { 'Content-Type': 'application/json' };

// Request timeout (10 seconds)
const API_TIMEOUT_MS = 10000;

// Rate limiting with adaptive throttling
const BASE_RATE_LIMIT = 50; // requests per second
let currentRateLimit = BASE_RATE_LIMIT;
let requestInterval = 1000 / currentRateLimit;
let lastRequestTime = 0;
let rateLimitRemaining = BASE_RATE_LIMIT;
let rateLimitResetTime = 0;

// ============================================
// CIRCUIT BREAKER PATTERN
// ============================================
const circuitBreaker = {
    failures: 0,
    lastFailure: 0,
    state: 'closed', // closed, open, half-open
    threshold: 5,    // Open circuit after 5 consecutive failures
    cooldown: 30000, // Wait 30s before trying again
    halfOpenRequests: 0,
    halfOpenMax: 2   // Allow 2 test requests in half-open state
};

function checkCircuitBreaker() {
    const now = Date.now();

    if (circuitBreaker.state === 'open') {
        // Check if cooldown period passed
        if (now - circuitBreaker.lastFailure > circuitBreaker.cooldown) {
            circuitBreaker.state = 'half-open';
            circuitBreaker.halfOpenRequests = 0;
            logger.info('[CircuitBreaker] State: half-open (testing)');
            return true; // Allow request
        }
        return false; // Circuit is open, block request
    }

    if (circuitBreaker.state === 'half-open') {
        if (circuitBreaker.halfOpenRequests >= circuitBreaker.halfOpenMax) {
            return false; // Too many test requests pending
        }
        circuitBreaker.halfOpenRequests++;
        return true;
    }

    return true; // Circuit is closed, allow request
}

function recordSuccess() {
    if (circuitBreaker.state === 'half-open') {
        circuitBreaker.state = 'closed';
        circuitBreaker.failures = 0;
        logger.info('[CircuitBreaker] State: closed (recovered)');
    } else {
        circuitBreaker.failures = 0; // Reset on success
    }
}

function recordFailure() {
    circuitBreaker.failures++;
    circuitBreaker.lastFailure = Date.now();

    if (circuitBreaker.state === 'half-open') {
        circuitBreaker.state = 'open';
        logger.warn('[CircuitBreaker] State: open (failed during recovery)');
    } else if (circuitBreaker.failures >= circuitBreaker.threshold) {
        circuitBreaker.state = 'open';
        logger.warn(`[CircuitBreaker] State: open (${circuitBreaker.failures} consecutive failures)`);
    }
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Parse rate limit headers from Helius response
 * Headers: X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset
 */
function parseRateLimitHeaders(response) {
    try {
        const remaining = parseInt(response.headers.get('X-RateLimit-Remaining') || response.headers.get('x-ratelimit-remaining'));
        const resetTime = parseInt(response.headers.get('X-RateLimit-Reset') || response.headers.get('x-ratelimit-reset'));
        const limit = parseInt(response.headers.get('X-RateLimit-Limit') || response.headers.get('x-ratelimit-limit'));

        if (!isNaN(remaining)) rateLimitRemaining = remaining;
        if (!isNaN(resetTime)) rateLimitResetTime = resetTime * 1000; // Convert to ms
        if (!isNaN(limit) && limit > 0) {
            currentRateLimit = limit;
            requestInterval = 1000 / currentRateLimit;
        }

        // Adaptive throttling: slow down when approaching limit
        if (rateLimitRemaining < 10) {
            requestInterval = 200; // 5 req/s when almost exhausted
            logger.warn(`[RateLimit] Low credits: ${rateLimitRemaining} remaining, throttling to 5 req/s`);
        } else if (rateLimitRemaining < 25) {
            requestInterval = 100; // 10 req/s when getting low
        } else {
            requestInterval = 1000 / currentRateLimit; // Normal rate
        }
    } catch (_e) {
        // Ignore parsing errors, use defaults
    }
}

/**
 * Save holder history snapshot (one entry per day per token)
 */
async function saveHolderHistory(db, mint, totalHolders, realHolders) {
    try {
        await db.run(`
            INSERT INTO holder_history (mint, date, holders, real_holders)
            VALUES ($1, CURRENT_DATE, $2, $3)
            ON CONFLICT (mint, date) DO UPDATE SET
                holders = EXCLUDED.holders,
                real_holders = EXCLUDED.real_holders
        `, [mint, totalHolders, realHolders]);
    } catch (_e) {
        // Ignore errors (table might not exist on first run)
    }
}

/**
 * Save K-Score history snapshot (one entry per day per token)
 * Used for credit rating trajectory analysis (30/60/90 day trends)
 */
async function saveKScoreHistory(db, mint, kScore, convictionScore, holders) {
    try {
        await db.run(`
            INSERT INTO k_score_history (mint, date, k_score, conviction_score, holders)
            VALUES ($1, CURRENT_DATE, $2, $3, $4)
            ON CONFLICT (mint, date) DO UPDATE SET
                k_score = EXCLUDED.k_score,
                conviction_score = EXCLUDED.conviction_score,
                holders = EXCLUDED.holders
        `, [mint, kScore, convictionScore, holders]);
    } catch (_e) {
        // Ignore errors (table might not exist on first run)
    }
}

// ============================================
// DELTA ANALYSIS - Incremental conviction updates
// ============================================

const SNAPSHOT_TTL = 3600000; // 1 hour - use delta if snapshot is newer

/**
 * Save holder snapshots for delta analysis
 * OPTIMIZED: Uses batch UPSERT instead of individual inserts
 */
async function saveHolderSnapshots(db, mint, holders) {
    const now = Date.now();

    // Filter and prepare valid holders
    const validHolders = holders
        .filter(h => h.address)
        .map(h => ({
            address: h.address,
            lastSig: h.lastSignature || null,
            buyCount: Math.floor(Number(h.buyCount)) || 0,
            sellCount: Math.floor(Number(h.sellCount)) || 0,
            netFlow: Math.floor(Number(h.netFlow)) || 0,
            convClass: h.convictionClass || 'holder',
            balance: Math.floor(Number(h.balance)) || 0
        }));

    if (validHolders.length === 0) {
        return { saved: 0, failed: holders.length };
    }

    try {
        // Build batch UPSERT with UNNEST for PostgreSQL
        const addresses = validHolders.map(h => h.address);
        const lastSigs = validHolders.map(h => h.lastSig);
        const buyCounts = validHolders.map(h => h.buyCount);
        const sellCounts = validHolders.map(h => h.sellCount);
        const netFlows = validHolders.map(h => h.netFlow);
        const convClasses = validHolders.map(h => h.convClass);
        const balances = validHolders.map(h => h.balance);
        const mints = validHolders.map(() => mint);
        const timestamps = validHolders.map(() => now);

        await db.run(`
            INSERT INTO holder_snapshots (mint, holder, last_signature, buy_count, sell_count, net_flow, conviction_class, balance, updated_at)
            SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::int[], $5::int[], $6::bigint[], $7::text[], $8::bigint[], $9::bigint[])
            ON CONFLICT (mint, holder) DO UPDATE SET
                last_signature = EXCLUDED.last_signature,
                buy_count = EXCLUDED.buy_count,
                sell_count = EXCLUDED.sell_count,
                net_flow = EXCLUDED.net_flow,
                conviction_class = EXCLUDED.conviction_class,
                balance = EXCLUDED.balance,
                updated_at = EXCLUDED.updated_at
        `, [mints, addresses, lastSigs, buyCounts, sellCounts, netFlows, convClasses, balances, timestamps]);

        return { saved: validHolders.length, failed: holders.length - validHolders.length };
    } catch (e) {
        logger.warn(`[Snapshot] Batch save failed for ${mint.slice(0,8)}: ${e.message}, falling back to individual`);

        // Fallback to individual inserts on batch failure
        let saved = 0;
        let failed = 0;
        for (const h of validHolders) {
            try {
                await db.run(`
                    INSERT INTO holder_snapshots (mint, holder, last_signature, buy_count, sell_count, net_flow, conviction_class, balance, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (mint, holder) DO UPDATE SET
                        last_signature = EXCLUDED.last_signature, buy_count = EXCLUDED.buy_count,
                        sell_count = EXCLUDED.sell_count, net_flow = EXCLUDED.net_flow,
                        conviction_class = EXCLUDED.conviction_class, balance = EXCLUDED.balance,
                        updated_at = EXCLUDED.updated_at
                `, [mint, h.address, h.lastSig, h.buyCount, h.sellCount, h.netFlow, h.convClass, h.balance, now]);
                saved++;
            } catch (_e) {
                failed++;
            }
        }
        return { saved, failed: failed + (holders.length - validHolders.length) };
    }
}

/**
 * Prune stale holders - keep only top 20 by balance
 * Removes holders that are no longer in the top 20 on-chain
 */
async function pruneStaleHolders(db, mint) {
    try {
        const result = await db.run(`
            DELETE FROM holder_snapshots
            WHERE mint = $1 AND holder NOT IN (
                SELECT holder FROM holder_snapshots
                WHERE mint = $1
                ORDER BY balance DESC
                LIMIT 20
            )
        `, [mint]);

        const deleted = result?.rowCount || 0;
        if (deleted > 0) {
            logger.debug(`[Prune] ${mint.slice(0,8)}: Removed ${deleted} stale holders`);
        }
        return deleted;
    } catch (e) {
        logger.warn(`[Prune] ${mint.slice(0,8)}: Failed to prune: ${e.message}`);
        return 0;
    }
}

/**
 * Load existing holder snapshots for a token
 */
async function loadHolderSnapshots(db, mint) {
    try {
        const snapshots = await db.all(
            'SELECT * FROM holder_snapshots WHERE mint = $1 ORDER BY balance DESC',
            [mint]
        );
        return snapshots || [];
    } catch (_e) {
        return [];
    }
}

/**
 * Get NEW transactions for a holder since last snapshot
 * Optimized with Helius time-based filtering when available
 *
 * @param {string} wallet - Wallet address
 * @param {string} lastSignature - Last analyzed signature (fallback stop condition)
 * @param {string} mint - Token mint to filter for
 * @param {number} sinceTimestamp - Optional: Unix timestamp (ms) to filter from
 */
async function getNewTransactions(wallet, lastSignature, mint, sinceTimestamp = null) {
    const newTxs = [];
    let before = null;

    // Build query options with time filter optimization
    const baseOptions = {
        limit: 50,
        // Use time filter if we have a snapshot timestamp (reduces API calls)
        ...(sinceTimestamp && { gtTime: Math.floor(sinceTimestamp / 1000) })
    };

    for (let page = 0; page < 3; page++) { // Max 3 pages of new txs
        const txs = await getEnhancedTransactions(wallet, { ...baseOptions, before });
        if (!txs || txs.length === 0) break;

        let foundLast = false;
        for (const tx of txs) {
            // Stop if we've reached the last analyzed signature
            if (tx.signature === lastSignature) {
                foundLast = true;
                break;
            }

            // Check if this tx involves our mint
            if (tx.tokenTransfers) {
                for (const transfer of tx.tokenTransfers) {
                    if (transfer.mint === mint) {
                        newTxs.push({
                            signature: tx.signature,
                            amount: transfer.tokenAmount || 0,
                            isBuy: transfer.toUserAccount === wallet,
                            isSell: transfer.fromUserAccount === wallet
                        });
                    }
                }
            }
        }

        if (foundLast) break;
        before = txs[txs.length - 1]?.signature;
        if (!before || txs.length < 50) break;
    }

    return newTxs;
}

/**
 * Classify holder from buy/sell counts
 */
function classifyFromCounts(buyCount, sellCount, _netFlow) {
    if (buyCount === 0 && sellCount === 0) return 'holder';

    const ratio = sellCount > 0 ? buyCount / sellCount : buyCount > 0 ? 10 : 1;

    if (ratio >= 2) return 'accumulator';   // 2x more buys than sells
    if (ratio >= 0.8) return 'holder';       // Roughly balanced
    if (ratio >= 0.3) return 'reducer';      // More sells
    return 'extractor';                       // Heavy selling
}

/**
 * Delta analysis - update conviction from new transactions only
 * Returns null if snapshots too old or don't exist (triggers full analysis)
 */
async function deltaConvictionAnalysis(db, mint) {
    const snapshots = await loadHolderSnapshots(db, mint);

    if (snapshots.length === 0) {
        logger.info(`[Delta] ${mint.slice(0,8)}: No snapshots, need full analysis`);
        return null;
    }

    // Check if snapshots are fresh enough
    const newestSnapshot = Math.max(...snapshots.map(s => s.updated_at || 0));
    if (Date.now() - newestSnapshot > SNAPSHOT_TTL) {
        logger.info(`[Delta] ${mint.slice(0,8)}: Snapshots stale (${Math.round((Date.now() - newestSnapshot) / 60000)}min old), need full analysis`);
        return null;
    }

    logger.info(`[Delta] ${mint.slice(0,8)}: Using delta analysis (${snapshots.length} holders cached)`);

    let updated = 0;
    let accumulators = 0;
    let holders = 0;
    let reducers = 0;
    let extractors = 0;

    // TOP 20 for breakdown (sorted by balance DESC from loadHolderSnapshots)
    const TOP_20_FOR_BREAKDOWN = 20;
    const top20Addresses = new Set(snapshots.slice(0, TOP_20_FOR_BREAKDOWN).map(s => s.holder));

    // OPTIMIZATION: Process snapshots in parallel batches (5x faster)
    const BATCH_SIZE = 5;
    for (let i = 0; i < snapshots.length; i += BATCH_SIZE) {
        const batch = snapshots.slice(i, i + BATCH_SIZE);

        const results = await Promise.allSettled(
            batch.map(async (snap) => {
                // Get only NEW transactions since last check
                const newTxs = await getNewTransactions(snap.holder, snap.last_signature, mint, snap.updated_at);

                let convictionClass = snap.conviction_class;

                if (newTxs.length > 0) {
                    // Update counts incrementally
                    let buyCount = snap.buy_count || 0;
                    let sellCount = snap.sell_count || 0;
                    let netFlow = snap.net_flow || 0;
                    const newLastSig = newTxs[0].signature;

                    for (const tx of newTxs) {
                        if (tx.isBuy) { buyCount++; netFlow += tx.amount; }
                        if (tx.isSell) { sellCount++; netFlow -= tx.amount; }
                    }

                    convictionClass = classifyFromCounts(buyCount, sellCount, netFlow);

                    await db.run(`
                        UPDATE holder_snapshots
                        SET buy_count = $1, sell_count = $2, net_flow = $3,
                            conviction_class = $4, last_signature = $5, updated_at = $6
                        WHERE mint = $7 AND holder = $8
                    `, [buyCount, sellCount, netFlow, convictionClass, newLastSig, Date.now(), mint, snap.holder]);

                    return { updated: true, convictionClass, holder: snap.holder, txCount: newTxs.length };
                }

                // Always include holder for top20 breakdown counting
                return { updated: false, convictionClass, holder: snap.holder };
            })
        );

        for (const result of results) {
            if (result.status === 'fulfilled') {
                const { updated: wasUpdated, convictionClass, holder, txCount } = result.value;
                if (wasUpdated) {
                    updated++;
                    logger.debug(`[Delta] ${holder.slice(0,8)}: +${txCount} txs → ${convictionClass}`);
                }
                // Only count top 20 for breakdown (matches conviction score calculation)
                if (top20Addresses.has(holder)) {
                    switch (convictionClass) {
                        case 'accumulator': accumulators++; break;
                        case 'holder': holders++; break;
                        case 'reducer': reducers++; break;
                        case 'extractor': extractors++; break;
                    }
                }
            }
        }

        // Rate limit between batches
        if (i + BATCH_SIZE < snapshots.length) {
            await sleep(100);
        }
    }

    // Analyzed = top 20 only (matches conviction score calculation)
    const analyzed = Math.min(snapshots.length, TOP_20_FOR_BREAKDOWN);
    const score = analyzed > 0 ? Math.round(((accumulators + holders) / analyzed) * 100) : 0;

    logger.info(`[Delta] ${mint.slice(0,8)}: ${score}% (${updated} updated, ${accumulators} acc, ${holders} hold, top ${analyzed})`);

    return {
        score,
        analyzed,
        accumulators,
        holders,
        reducers,
        extractors,
        snapshotCount: snapshots.length,  // Total snapshots in cache
        isDelta: true
    };
}

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

// Pool cache - Redis-backed with in-memory fallback
// OPTIMIZED: Shared across instances, survives restarts
const POOL_CACHE_TTL_SECONDS = 3600; // 1 hour
const memoryPoolCache = new Map(); // Fallback if Redis unavailable

async function getPoolFromCache(address) {
    const redis = getRedisClient();
    if (redis) {
        try {
            const cached = await redis.get(`pool:${address}`);
            if (cached !== null) return cached === '1';
        } catch (_e) { /* fallback to memory */ }
    }
    const mem = memoryPoolCache.get(address);
    if (mem && Date.now() - mem.ts < POOL_CACHE_TTL_SECONDS * 1000) {
        return mem.isPool;
    }
    return undefined;
}

async function setPoolInCache(address, isPool) {
    const redis = getRedisClient();
    if (redis) {
        try {
            await redis.set(`pool:${address}`, isPool ? '1' : '0', 'EX', POOL_CACHE_TTL_SECONDS);
        } catch (_e) { /* fallback to memory */ }
    }
    memoryPoolCache.set(address, { isPool, ts: Date.now() });
}

async function getPoolsFromCache(addresses) {
    const results = new Map();
    const redis = getRedisClient();

    if (redis) {
        try {
            const keys = addresses.map(a => `pool:${a}`);
            const values = await redis.mget(...keys);
            for (let i = 0; i < addresses.length; i++) {
                if (values[i] !== null) {
                    results.set(addresses[i], values[i] === '1');
                }
            }
            return results;
        } catch (_e) { /* fallback to memory */ }
    }

    // Memory fallback
    const now = Date.now();
    for (const addr of addresses) {
        const mem = memoryPoolCache.get(addr);
        if (mem && now - mem.ts < POOL_CACHE_TTL_SECONDS * 1000) {
            results.set(addr, mem.isPool);
        }
    }
    return results;
}

async function setPoolsInCache(entries) {
    const redis = getRedisClient();
    if (redis && entries.length > 0) {
        try {
            const pipeline = redis.pipeline();
            for (const { address, isPool } of entries) {
                pipeline.set(`pool:${address}`, isPool ? '1' : '0', 'EX', POOL_CACHE_TTL_SECONDS);
            }
            await pipeline.exec();
        } catch (_e) { /* fallback to memory */ }
    }
    // Always update memory fallback
    const now = Date.now();
    for (const { address, isPool } of entries) {
        memoryPoolCache.set(address, { isPool, ts: now });
    }
}

// ============================================
// HELIUS API FUNCTIONS
// ============================================

async function rateLimitedFetch(url, options = {}) {
    // SECURITY: Check circuit breaker before making request
    if (!checkCircuitBreaker()) {
        throw new Error('Circuit breaker is open - API temporarily unavailable');
    }

    const now = Date.now();

    // Wait for rate limit reset if exhausted
    if (rateLimitRemaining <= 0 && rateLimitResetTime > now) {
        const waitTime = rateLimitResetTime - now + 100; // +100ms buffer
        logger.warn(`[RateLimit] Exhausted, waiting ${Math.round(waitTime / 1000)}s until reset`);
        await sleep(waitTime);
    }

    // Respect request interval
    const timeSince = now - lastRequestTime;
    if (timeSince < requestInterval) {
        await sleep(requestInterval - timeSince);
    }
    lastRequestTime = Date.now();

    // SECURITY: Add timeout to prevent hanging requests
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), API_TIMEOUT_MS);

    try {
        const response = await fetch(url, {
            ...options,
            signal: controller.signal
        });

        clearTimeout(timeoutId);

        // Parse rate limit headers for adaptive throttling
        parseRateLimitHeaders(response);

        // Record success for circuit breaker
        recordSuccess();

        return response;
    } catch (error) {
        clearTimeout(timeoutId);

        // Record failure for circuit breaker
        recordFailure();

        if (error.name === 'AbortError') {
            throw new Error(`Request timeout after ${API_TIMEOUT_MS}ms`);
        }
        throw error;
    }
}

async function heliusRpc(method, params) {
    if (!HELIUS_API_KEY) return null;

    try {
        const response = await rateLimitedFetch(HELIUS_RPC_URL, {
            method: 'POST',
            headers: HELIUS_HEADERS,
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

/**
 * Enhanced Transaction History API with new Helius features
 *
 * @param {string} address - Wallet address to query
 * @param {Object} options - Query options
 * @param {number} options.limit - Results per page (1-100, default: 10)
 * @param {string} options.before - Pagination: fetch before this signature (use with desc)
 * @param {string} options.after - Pagination: fetch after this signature (use with asc)
 * @param {string} options.sortOrder - 'asc' (oldest first) or 'desc' (newest first, default)
 * @param {number} options.gtTime - Filter: transactions after this unix timestamp
 * @param {number} options.gteTime - Filter: transactions at or after this unix timestamp
 * @param {number} options.ltTime - Filter: transactions before this unix timestamp
 * @param {number} options.lteTime - Filter: transactions at or before this unix timestamp
 * @param {string} options.type - Filter by tx type: SWAP, TRANSFER, etc.
 * @returns {Array} Enhanced transaction objects
 */
async function getEnhancedTransactions(address, options = {}) {
    if (!HELIUS_API_KEY) return [];

    // Note: Enhanced Transactions API (v0) requires API key in query string
    // Header auth returns 401 - this is a Helius API limitation
    const params = new URLSearchParams({ 'api-key': HELIUS_API_KEY });

    // Pagination
    if (options.limit) params.append('limit', options.limit.toString());
    if (options.before) params.append('before-signature', options.before);
    if (options.after) params.append('after-signature', options.after);

    // Sort order (new feature: oldest first)
    if (options.sortOrder) params.append('sort-order', options.sortOrder);

    // Time-based filtering (unix timestamps in seconds)
    if (options.gtTime) params.append('gt-time', options.gtTime.toString());
    if (options.gteTime) params.append('gte-time', options.gteTime.toString());
    if (options.ltTime) params.append('lt-time', options.ltTime.toString());
    if (options.lteTime) params.append('lte-time', options.lteTime.toString());

    // Transaction type filter
    if (options.type) params.append('type', options.type);

    const url = `https://api-mainnet.helius-rpc.com/v0/addresses/${address}/transactions?${params}`;

    try {
        const response = await rateLimitedFetch(url, { method: 'GET' });
        if (!response.ok) return [];
        return await response.json();
    } catch (_error) {
        return [];
    }
}

// ============================================
// POOL FILTERING
// ============================================

async function batchCheckPools(addresses) {
    const results = new Map();

    // 1. Check known pools first (instant)
    const toCheck = [];
    for (const addr of addresses) {
        if (KNOWN_POOL_WALLETS.has(addr)) {
            results.set(addr, true);
        } else {
            toCheck.push(addr);
        }
    }

    if (toCheck.length === 0) return results;

    // 2. Check Redis/memory cache (batch MGET)
    const cached = await getPoolsFromCache(toCheck);
    const uncached = [];
    for (const addr of toCheck) {
        if (cached.has(addr)) {
            results.set(addr, cached.get(addr));
        } else {
            uncached.push(addr);
        }
    }

    // 3. Fetch uncached from Helius and cache results
    if (uncached.length > 0) {
        try {
            const infos = await heliusRpc('getMultipleAccounts', [uncached, { encoding: 'base64' }]);
            const toCache = [];
            for (let i = 0; i < uncached.length; i++) {
                const addr = uncached[i];
                const info = infos?.value?.[i];
                const isPool = info ? DEX_PROGRAMS.has(info.owner) : false;
                results.set(addr, isPool);
                toCache.push({ address: addr, isPool });
            }
            // Batch cache update (Redis pipeline)
            await setPoolsInCache(toCache);
        } catch (_error) {
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

// SECURITY: Limit holder count to prevent memory exhaustion
const MAX_HOLDERS_PER_TOKEN = 10000; // 10k holders max
const MAX_HOLDER_PAGES = 10; // Max 10 pages of 1000 = 10k

async function fetchTokenHolders(mint) {
    const holders = [];
    let cursor = null;
    let pageCount = 0;

    while (pageCount < MAX_HOLDER_PAGES) {
        const params = { mint, limit: 1000 };
        if (cursor) params.cursor = cursor;

        const result = await heliusRpc('getTokenAccounts', params);
        if (!result?.token_accounts) break;

        for (const acc of result.token_accounts) {
            if (acc.amount > 0) {
                holders.push({ address: acc.owner, balance: acc.amount });
            }

            // SECURITY: Hard limit on total holders
            if (holders.length >= MAX_HOLDERS_PER_TOKEN) {
                logger.warn(`[Holders] ${mint.slice(0,8)}: Hit limit of ${MAX_HOLDERS_PER_TOKEN}`);
                break;
            }
        }

        if (holders.length >= MAX_HOLDERS_PER_TOKEN) break;

        cursor = result.cursor;
        pageCount++;
        if (!cursor) break;
    }

    holders.sort((a, b) => b.balance - a.balance);
    return holders;
}

/**
 * Analyze holder's retention and trading behavior
 * Optimized with Helius sort-order for efficient first transaction lookup
 */
async function getHolderRetention(wallet, mint) {
    let firstBuyAmount = 0;
    let currentBalance = 0;
    let before = null;
    let buyCount = 0;
    let sellCount = 0;
    let netFlow = 0;
    let lastSignature = null;

    // OPTIMIZATION: Get first transaction efficiently using sort-order=asc
    // This finds the holder's earliest activity in one call instead of paginating backwards
    const oldestTxs = await getEnhancedTransactions(wallet, {
        limit: 10,
        sortOrder: 'asc'  // Oldest first
    });

    if (oldestTxs && oldestTxs.length > 0) {
        // Find first buy for this mint
        for (const tx of oldestTxs) {
            if (!tx.tokenTransfers) continue;
            for (const transfer of tx.tokenTransfers) {
                if (transfer.mint === mint && transfer.toUserAccount === wallet) {
                    firstBuyAmount = transfer.tokenAmount || 0;
                    break;
                }
            }
            if (firstBuyAmount > 0) break;
        }
    }

    // Get recent transactions (newest first) for current state and signature
    for (let page = 0; page < 5; page++) {
        const txs = await getEnhancedTransactions(wallet, { limit: 100, before });
        if (!txs || txs.length === 0) break;

        // Capture the most recent signature (first tx on first page)
        if (page === 0 && txs.length > 0) {
            lastSignature = txs[0].signature;
        }

        for (const tx of txs) {
            if (!tx.tokenTransfers) continue;
            for (const transfer of tx.tokenTransfers) {
                if (transfer.mint !== mint) continue;

                const amount = transfer.tokenAmount || 0;
                if (transfer.toUserAccount === wallet) {
                    currentBalance += amount;
                    // Fallback: if asc query didn't find first buy, use last seen from desc
                    if (firstBuyAmount === 0) firstBuyAmount = amount;
                    buyCount++;
                    netFlow += amount;
                }
                if (transfer.fromUserAccount === wallet) {
                    currentBalance -= amount;
                    sellCount++;
                    netFlow -= amount;
                }
            }
        }

        before = txs[txs.length - 1]?.signature;
        if (!before || txs.length < 100) break;
    }

    if (currentBalance < 0) currentBalance = 0;
    const retention = firstBuyAmount > 0 ? currentBalance / firstBuyAmount : 0;

    return {
        retention,
        buyCount,
        sellCount,
        netFlow,
        lastSignature
    };
}

function classifyRetention(retentionData) {
    const retention = typeof retentionData === 'object' ? retentionData.retention : retentionData;
    if (retention >= 1.5) return 'accumulator';
    if (retention >= 1.0) return 'holder';
    if (retention >= 0.5) return 'reducer';
    return 'extractor';
}

/**
 * Calculate conviction score AND real holders count
 *
 * DUAL MODE:
 * - WEBHOOK MODE: If USE_WEBHOOKS=true and fresh snapshots exist, use cached data (0 API calls)
 * - POLLING MODE: Traditional API-based analysis (100+ API calls per token)
 *
 * @param {string} mint - Token mint address
 * @param {number} priceUsd - Current token price in USD
 * @param {number} decimals - Token decimals (default 9 for SPL)
 * @param {Object} db - Database connection (for saving snapshots)
 * @returns {Object} { score, analyzed, accumulators, holders, reducers, extractors, realHoldersCount, totalHolders }
 */
async function calculateConvictionAndHolders(mint, priceUsd = 0, decimals = 9, db = null) {
    const TOP_HOLDERS = 20;
    const CANDIDATES = 50;
    const MIN_USD_VALUE = 1; // $1 minimum to count as "real holder"

    try {
        // ============================================
        // WEBHOOK MODE: Use cached snapshots (0 API calls)
        // ============================================
        // When webhooks are active, holder_snapshots is constantly updated
        // by POST /webhook/transfers, so we just read from cache
        // Skip this mode during daily deep refresh (forceDeepRefreshMode)

        if (config.USE_WEBHOOKS && db && !forceDeepRefreshMode) {
            const snapshots = await db.all(
                'SELECT * FROM holder_snapshots WHERE mint = $1 ORDER BY balance DESC LIMIT 20',
                [mint]
            );

            // If we have cached data, use it (webhook mode)
            // OPTIMIZATION: In webhook mode, snapshots ARE the source of truth
            // Webhooks update them in real-time, so no staleness check needed
            // Even "stale" snapshots are valid - they just mean no activity
            if (snapshots && snapshots.length > 0) {
                const newestSnapshot = Math.max(...snapshots.map(s => s.updated_at || 0));
                const ageMinutes = (Date.now() - newestSnapshot) / 60000;

                // Always use cached data in webhook mode (0 API calls)
                // Staleness just means no new transfers happened - that's valid data
                {
                    let accumulators = 0, holders = 0, reducers = 0, extractors = 0;

                    // Only count TOP 20 for breakdown (sorted by balance DESC)
                    const TOP_20 = 20;
                    const top20Snapshots = snapshots.slice(0, TOP_20);

                    for (const snap of top20Snapshots) {
                        switch (snap.conviction_class) {
                            case 'accumulator': accumulators++; break;
                            case 'holder': holders++; break;
                            case 'reducer': reducers++; break;
                            case 'extractor': extractors++; break;
                        }
                    }

                    const analyzed = top20Snapshots.length;
                    const score = analyzed > 0 ? Math.round(((accumulators + holders) / analyzed) * 100) : 0;

                    // Use stored holder count from DB (0 RPC calls)
                    // This is updated periodically by deep refresh, not on every K-Score calc
                    const tokenData = await db.get(
                        'SELECT holders FROM tokens WHERE mint = $1',
                        [mint]
                    );
                    const storedHolderCount = tokenData?.holders || snapshots.length;

                    // Estimate real holders from snapshot count (holders with balance > dust)
                    // In webhook mode, snapshots only contain significant holders
                    const realHoldersCount = Math.max(snapshots.length, Math.floor(storedHolderCount * 0.7));

                    logger.info(`[Webhook Mode] ${mint.slice(0,8)}: ${score}% conviction from cache (${analyzed} analyzed, ${storedHolderCount} total, ${ageMinutes.toFixed(0)}m old) - 0 RPC`);

                    return {
                        score,
                        analyzed,
                        accumulators,
                        holders,
                        reducers,
                        extractors,
                        realHoldersCount,
                        totalHolders: storedHolderCount,
                        allHolders: null,  // Not fetched in webhook mode
                        isWebhookMode: true
                    };
                }
                // No snapshots in DB yet - will fall through to delta/polling mode
                logger.info(`[Webhook Mode] ${mint.slice(0,8)}: No snapshots in DB, need initial analysis`);
            }
        }

        // ============================================
        // POLLING MODE: Traditional API-based analysis
        // ============================================
        // Only runs when webhooks are OFF or no snapshots exist yet

        // 0. Try delta analysis first (if snapshots exist and are fresh)
        // OPTIMIZATION: Use cached holder count from DB - avoid expensive fetchTokenHolders
        // NOTE: Delta mode is for NON-WEBHOOK mode only (when we need to poll for updates)
        if (db && !config.USE_WEBHOOKS) {
            const deltaResult = await deltaConvictionAnalysis(db, mint);
            if (deltaResult) {
                // Delta succeeded - use cached holder count from tokens table
                // This is updated by: webhooks, periodic holder scan, or previous full analysis
                const tokenRow = await db.get(
                    'SELECT holders, priceusd FROM tokens WHERE mint = $1',
                    [mint]
                );
                const cachedHolders = tokenRow?.holders || 0;
                const cachedPrice = tokenRow?.priceusd || priceUsd;

                // Estimate real holders from snapshot data (no API call!)
                // Real holders = holders with $1+ value
                let realHoldersCount = cachedHolders;
                if (cachedPrice > 0 && deltaResult.snapshotCount > 0) {
                    // Use snapshot data to estimate % of real holders
                    // This is an approximation based on top holders having value
                    realHoldersCount = Math.max(
                        deltaResult.snapshotCount,
                        Math.floor(cachedHolders * 0.8) // Conservative: 80% are real
                    );
                }

                logger.info(`[Delta] ${mint.slice(0,8)}: Using cached holder count (${cachedHolders}) - 0 API calls`);

                return {
                    ...deltaResult,
                    realHoldersCount,
                    totalHolders: cachedHolders,
                    allHolders: null,  // Not fetched in delta mode
                    isDeltaMode: true
                };
            }
        }

        // 1. Fetch all holders (full analysis)
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
        const snapshotData = []; // For saving to DB

        // OPTIMIZATION: Process holders in parallel batches (5x faster)
        const BATCH_SIZE = 5;
        for (let i = 0; i < top20.length; i += BATCH_SIZE) {
            const batch = top20.slice(i, i + BATCH_SIZE);

            const results = await Promise.allSettled(
                batch.map(async (holder) => {
                    const retentionData = await getHolderRetention(holder.address, mint);
                    const classification = classifyRetention(retentionData);
                    return { holder, retentionData, classification };
                })
            );

            for (const result of results) {
                if (result.status === 'fulfilled') {
                    const { holder, retentionData, classification } = result.value;

                    if (classification === 'accumulator') accumulators++;
                    else if (classification === 'holder') holders++;
                    else if (classification === 'reducer') reducers++;
                    else if (classification === 'extractor') extractors++;

                    snapshotData.push({
                        address: holder.address,
                        balance: holder.balance,
                        buyCount: retentionData.buyCount,
                        sellCount: retentionData.sellCount,
                        netFlow: retentionData.netFlow,
                        lastSignature: retentionData.lastSignature,
                        convictionClass: classification
                    });

                    analyzed++;
                }
                // Failed holders are silently skipped
            }

            // Rate limit between batches (not per-holder)
            if (i + BATCH_SIZE < top20.length) {
                await sleep(200);
            }
        }

        if (analyzed === 0) {
            return { score: 0, analyzed: 0, realHoldersCount, totalHolders: allHolders.length };
        }

        // 5. Save snapshots for future delta analysis
        if (db && snapshotData.length > 0) {
            const { saved, failed } = await saveHolderSnapshots(db, mint, snapshotData);
            logger.info(`[Snapshot] ${mint.slice(0,8)}: Saved ${saved}/${snapshotData.length} holder snapshots${failed > 0 ? ` (${failed} failed)` : ''}`);

            // 6. Prune stale holders (keep only top 20 by balance)
            await pruneStaleHolders(db, mint);
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
            totalHolders: allHolders.length,
            allHolders,  // For burn calculation (avoid re-fetch)
            filteredTop50: realCandidates,  // For top10 calculation (pools removed)
            poolsFiltered
        };

    } catch (error) {
        logger.error(`[Conviction] Error for ${mint}: ${error.message}`);
        return {
            score: 0, analyzed: 0, accumulators: 0, holders: 0,
            reducers: 0, extractors: 0, realHoldersCount: 0, totalHolders: 0,
            allHolders: []
        };
    }
}

// Backwards compatibility alias
async function calculateConviction(mint) {
    return calculateConvictionAndHolders(mint, 0, 9);
}

// ============================================
// SECURITY CHECK (on-chain) - ELIMINATORY
// ============================================

// Known trusted program authorities (PumpFun, etc.)
// If mint/freeze authority belongs to a known program, it's OK
const TRUSTED_AUTHORITIES = new Set([
    // PumpFun
    '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',  // PumpFun program
    'Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1', // PumpFun bonding curve
    'TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM',  // PumpFun token authority
    '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg', // PumpFun AMM
    // Raydium
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', // Raydium v4
    'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK', // Raydium CLMM
    // Meteora
    'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',
]);

/**
 * Check token security: mint authority, freeze authority
 * These are ELIMINATORY - if not secure, score is capped
 *
 * RULES:
 * - Mint/Freeze revoked = SAFE
 * - Mint/Freeze = trusted program (PumpFun, etc.) = OK
 * - Mint/Freeze = random wallet = DANGER
 *
 * SPL Token Mint Account Layout (82 bytes):
 *   [0-4]   mintAuthorityOption (u32: 0=None, 1=Some)
 *   [4-36]  mintAuthority (Pubkey if Some)
 *   [36-44] supply (u64)
 *   [44]    decimals (u8)
 *   [45]    isInitialized (bool)
 *   [46-50] freezeAuthorityOption (u32: 0=None, 1=Some)
 *   [50-82] freezeAuthority (Pubkey if Some)
 */
async function checkTokenSecurity(mint) {
    const security = {
        mintAuthorityRevoked: false,
        mintAuthorityTrusted: false,
        mintAuthority: null,
        freezeAuthorityRevoked: false,
        freezeAuthorityTrusted: false,
        freezeAuthority: null,
        isSecure: false,
        maxScore: 100
    };

    try {
        const accountInfo = await heliusRpc('getAccountInfo', [mint, { encoding: 'base64' }]);

        if (!accountInfo?.value?.data?.[0]) {
            logger.warn(`[Security] ${mint.slice(0,8)}: Could not fetch mint account`);
            return security;
        }

        const data = Buffer.from(accountInfo.value.data[0], 'base64');

        if (data.length < 82) {
            logger.warn(`[Security] ${mint.slice(0,8)}: Invalid mint account size`);
            return security;
        }

        // Parse mint authority (offset 0-36)
        const mintAuthOption = data.readUInt32LE(0);
        if (mintAuthOption === 0) {
            security.mintAuthorityRevoked = true;
        } else {
            // Extract pubkey (32 bytes at offset 4) and convert to base58
            const mintAuthBytes = data.slice(4, 36);
            const mintAuthBase58 = bs58.encode(mintAuthBytes);
            security.mintAuthority = mintAuthBase58;

            // Check if authority is a known trusted program
            if (TRUSTED_AUTHORITIES.has(mintAuthBase58)) {
                security.mintAuthorityTrusted = true;
            }
        }

        // Parse freeze authority (offset 46-82)
        const freezeAuthOption = data.readUInt32LE(46);
        if (freezeAuthOption === 0) {
            security.freezeAuthorityRevoked = true;
        } else {
            // Extract pubkey (32 bytes at offset 50) and convert to base58
            const freezeAuthBytes = data.slice(50, 82);
            const freezeAuthBase58 = bs58.encode(freezeAuthBytes);
            security.freezeAuthority = freezeAuthBase58;

            // Check if authority is a known trusted program
            if (TRUSTED_AUTHORITIES.has(freezeAuthBase58)) {
                security.freezeAuthorityTrusted = true;
            }
        }

        // Determine security level
        // Safe = revoked OR trusted program (PumpFun, Raydium, etc.)
        const mintSafe = security.mintAuthorityRevoked || security.mintAuthorityTrusted;
        const freezeSafe = security.freezeAuthorityRevoked || security.freezeAuthorityTrusted;

        if (mintSafe && freezeSafe) {
            security.isSecure = true;
            security.maxScore = 100;
            const reason = security.mintAuthorityRevoked && security.freezeAuthorityRevoked
                ? 'both revoked'
                : 'trusted programs';
            logger.info(`[Security] ${mint.slice(0,8)}: ✓ FULLY SECURE (${reason})`);
        } else if (mintSafe && !freezeSafe) {
            // Freeze authority active and untrusted - moderate risk
            security.maxScore = 70;
            logger.info(`[Security] ${mint.slice(0,8)}: ⚠ Freeze active (${security.freezeAuthority?.slice(0,8)}) → cap 70`);
        } else if (!mintSafe && freezeSafe) {
            // Mint authority active and untrusted - could be unknown program
            security.maxScore = 80;
            logger.info(`[Security] ${mint.slice(0,8)}: ⚠ Mint active (${security.mintAuthority?.slice(0,8)}) → cap 80`);
        } else {
            // Both active and untrusted - higher risk
            security.maxScore = 50;
            logger.info(`[Security] ${mint.slice(0,8)}: ⚠ Both authorities active → cap 50`);
        }

        return security;

    } catch (e) {
        logger.error(`[Security] ${mint.slice(0,8)}: ${e.message}`);
        return security;
    }
}

// ============================================
// BURN CALCULATION (on-chain)
// ============================================

// Pump.fun tokens always start with 1 billion supply (with 6 decimals)
const PUMP_INITIAL_SUPPLY = 1_000_000_000_000_000n; // 1B * 10^6

// Known burn address patterns
const BURN_PATTERNS = [
    '1111111',           // Starts with 7+ ones
    '1nc1nerator',       // Incinerator
    'burn',              // Contains 'burn' (case insensitive check)
    'dead',              // Contains 'dead'
];

function isBurnAddress(address) {
    if (!address) return false;
    const lower = address.toLowerCase();
    return BURN_PATTERNS.some(p => lower.includes(p.toLowerCase()));
}

/**
 * Calculate burn percentage from on-chain data
 *
 * Two methods of burning on Solana:
 * 1. SPL Token `burn` instruction - removes tokens from supply
 * 2. Send to burn address - tokens still in supply but inaccessible
 *
 * Initial supply sources (priority order):
 * 1. Pump.fun tokens: always 1 billion (detected by mint suffix OR is_pump_fun flag)
 * 2. Stored initial_supply from DB (first indexing)
 * 3. First entry in supply_history (for Mayhem Mode)
 * 4. Fallback: current supply + burn addresses (no instruction burns detected)
 *
 * @param {string} mint - Token mint address
 * @param {Array} allHolders - Optional holders array
 * @param {Object} options - Optional: { storedInitialSupply, isPumpFunFlag, db }
 */
async function calculateBurn(mint, allHolders = null, options = {}) {
    try {
        const { storedInitialSupply, isPumpFunFlag, db } = options;

        // Get current supply from chain
        const supply = await heliusRpc('getTokenSupply', [mint]);
        if (!supply) return { burnPct: 0, burned: 0, totalSupply: 0, initialSupply: 0 };

        const currentSupply = BigInt(supply.value.amount);
        const decimals = supply.value.decimals;
        const divisor = Math.pow(10, decimals);

        // Use provided holders or fetch them
        const holders = allHolders || (await fetchTokenHolders(mint));

        // Find tokens held in burn addresses
        let burnedInAddresses = 0n;
        for (const h of holders) {
            if (isBurnAddress(h.address)) {
                burnedInAddresses += BigInt(Math.floor(h.balance));
            }
        }

        // Determine initial supply (priority order)
        let initialSupply;
        let supplySource = 'unknown';
        // Pump.fun detection: mint ends with 'pump' OR is_pump_fun flag from DB
        const isPumpFun = mint.endsWith('pump') || isPumpFunFlag === true;

        if (isPumpFun) {
            // 1. Pump.fun tokens always start with 1 billion
            initialSupply = PUMP_INITIAL_SUPPLY;
            supplySource = 'pump.fun';
        } else if (storedInitialSupply && storedInitialSupply > 0) {
            // 2. Use stored initial_supply from DB (converted to raw amount)
            initialSupply = BigInt(Math.floor(storedInitialSupply * divisor));
            supplySource = 'db';
        } else if (db) {
            // 3. Try to get first supply_history entry (for Mayhem Mode tokens)
            try {
                const firstHistory = await db.get(
                    'SELECT supply FROM supply_history WHERE mint = $1 ORDER BY timestamp ASC LIMIT 1',
                    [mint]
                );
                if (firstHistory?.supply) {
                    initialSupply = BigInt(firstHistory.supply);
                    supplySource = 'history';
                }
            } catch (_e) {
                // Ignore, fall through to default
            }
        }

        // 4. Fallback: can only detect burn addresses, not instruction burns
        if (!initialSupply) {
            initialSupply = currentSupply + burnedInAddresses;
            supplySource = 'fallback';
        }

        // Calculate total burned
        // burnedViaInstruction = initialSupply - currentSupply (already removed from supply)
        // burnedInAddresses = tokens still in supply but in burn addresses
        const burnedViaInstruction = initialSupply > currentSupply ? initialSupply - currentSupply : 0n;
        const totalBurned = burnedViaInstruction + burnedInAddresses;

        const burnPct = initialSupply > 0n
            ? (Number(totalBurned) / Number(initialSupply)) * 100
            : 0;

        const sourceLabel = supplySource === 'pump.fun' ? ' (pump.fun)' :
                           supplySource === 'db' ? ' (stored)' :
                           supplySource === 'history' ? ' (history)' : '';
        logger.info(`[Burn] ${mint.slice(0,8)}: ${burnPct.toFixed(2)}% burned${sourceLabel}`);

        return {
            burnPct,
            burned: Number(totalBurned) / divisor,
            burnedViaInstruction: Number(burnedViaInstruction) / divisor,
            burnedInAddresses: Number(burnedInAddresses) / divisor,
            totalSupply: Number(currentSupply) / divisor,
            initialSupply: Number(initialSupply) / divisor,
            decimals,
            isPumpFun,
            supplySource
        };
    } catch (e) {
        logger.error(`[Burn] Error for ${mint}: ${e.message}`);
        return { burnPct: 0, burned: 0, totalSupply: 0, initialSupply: 0 };
    }
}

// ============================================
// MAYHEM MODE - SUPPLY TRACKING
// ============================================

/**
 * Refresh and track supply changes for Mayhem Mode tokens
 *
 * Detects:
 * - Supply inflation (minting)
 * - Supply deflation (burning)
 * - Supply volatility over 24h
 *
 * @param {Object} db - Database connection
 * @param {string} mint - Token mint address
 * @param {number} decimals - Token decimals
 * @returns {Object} { currentSupply, previousSupply, changePercent, isMutable, supplyData }
 */
async function refreshSupply(db, mint, decimals = 9) {
    const result = {
        currentSupply: 0,
        previousSupply: 0,
        changePercent: 0,
        change24h: 0,
        isMutable: false,
        source: 'helius'
    };

    try {
        // 1. Fetch current supply from chain
        const supplyInfo = await heliusRpc('getTokenSupply', [mint]);
        if (!supplyInfo?.value?.amount) {
            return result;
        }

        const currentRaw = Number(supplyInfo.value.amount);
        const divisor = Math.pow(10, decimals);
        result.currentSupply = currentRaw / divisor;

        // 2. Get stored supply from DB
        const token = await db.get('SELECT supply, supply_last_check FROM tokens WHERE mint = $1', [mint]);
        const storedSupply = parseFloat(token?.supply || 0) / divisor;
        result.previousSupply = storedSupply;

        // 3. Calculate change percentage
        if (storedSupply > 0) {
            result.changePercent = ((result.currentSupply - storedSupply) / storedSupply) * 100;
        }

        // 4. Detect if supply is mutable (>0.1% change since last check)
        const MUTABLE_THRESHOLD = 0.1; // 0.1% change = mutable
        if (Math.abs(result.changePercent) > MUTABLE_THRESHOLD) {
            result.isMutable = true;
            logger.info(`[Supply] ${mint.slice(0,8)}: ${result.changePercent > 0 ? '📈' : '📉'} ${result.changePercent.toFixed(2)}% change (${storedSupply.toLocaleString()} → ${result.currentSupply.toLocaleString()})`);
        }

        // 5. Calculate 24h change from supply_history
        const history24h = await db.get(`
            SELECT supply FROM supply_history
            WHERE mint = $1 AND timestamp <= $2
            ORDER BY timestamp DESC LIMIT 1
        `, [mint, Date.now() - 86400000]);

        if (history24h?.supply) {
            const supply24hAgo = parseFloat(history24h.supply) / divisor;
            if (supply24hAgo > 0) {
                result.change24h = ((result.currentSupply - supply24hAgo) / supply24hAgo) * 100;
            }
        }

        // 6. Update tokens table with fresh supply
        if (Math.abs(result.changePercent) > 0.01 || !token?.supply_last_check) {
            await db.run(`
                UPDATE tokens
                SET supply = $1,
                    supply_last_check = $2,
                    supply_change_24h = $3,
                    is_mutable_supply = $4
                WHERE mint = $5
            `, [
                currentRaw.toString(),
                Date.now(),
                result.change24h,
                result.isMutable || Math.abs(result.change24h) > MUTABLE_THRESHOLD,
                mint
            ]);
        }

        // 7. Save to supply_history (max 1 entry per hour to avoid spam)
        const lastHistory = await db.get(`
            SELECT timestamp FROM supply_history
            WHERE mint = $1
            ORDER BY timestamp DESC LIMIT 1
        `, [mint]);

        const hourAgo = Date.now() - 3600000;
        if (!lastHistory || lastHistory.timestamp < hourAgo) {
            await db.run(`
                INSERT INTO supply_history (mint, supply, timestamp, source, change_percent)
                VALUES ($1, $2, $3, $4, $5)
            `, [mint, currentRaw.toString(), Date.now(), 'kscore', result.changePercent]);
        }

        return result;

    } catch (e) {
        logger.warn(`[Supply] ${mint.slice(0,8)}: Refresh failed - ${e.message}`);
        return result;
    }
}

/**
 * Get supply volatility score for K-Score penalty
 * High volatility = less trustworthy = lower score
 *
 * @param {Object} db - Database connection
 * @param {string} mint - Token mint address
 * @returns {Object} { volatility, penalty, dataPoints }
 */
async function getSupplyVolatility(db, mint) {
    try {
        // Get supply history for last 7 days
        const history = await db.all(`
            SELECT supply, change_percent, timestamp
            FROM supply_history
            WHERE mint = $1 AND timestamp >= $2
            ORDER BY timestamp ASC
        `, [mint, Date.now() - 7 * 86400000]);

        if (!history || history.length < 2) {
            return { volatility: 0, penalty: 0, dataPoints: 0 };
        }

        // Calculate standard deviation of changes
        const changes = history.map(h => Math.abs(h.change_percent || 0));
        const avgChange = changes.reduce((a, b) => a + b, 0) / changes.length;
        const variance = changes.reduce((sum, c) => sum + Math.pow(c - avgChange, 2), 0) / changes.length;
        const stdDev = Math.sqrt(variance);

        // Volatility score (0-100)
        // 0% stdDev = 0 volatility, 10% stdDev = 100 volatility
        const volatility = Math.min(100, stdDev * 10);

        // K-Score penalty based on volatility
        // 0-10 volatility: no penalty
        // 10-50 volatility: -5 to -15 points
        // 50-100 volatility: -15 to -30 points
        let penalty = 0;
        if (volatility > 10) {
            penalty = Math.min(30, Math.round((volatility - 10) * 0.33));
        }

        return {
            volatility: Math.round(volatility * 10) / 10,
            penalty,
            dataPoints: history.length,
            avgChange: Math.round(avgChange * 100) / 100
        };

    } catch (_e) {
        return { volatility: 0, penalty: 0, dataPoints: 0 };
    }
}

// ============================================
// LP BURN/LOCK CHECK (on-chain)
// ============================================

// Known burn addresses for LP tokens
const LP_BURN_ADDRESSES = new Set([
    '1111111111111111111111111111111111',
    '1nc1nerator11111111111111111111111',
    // PumpFun burn
    '1PUMPkr5FmyKcPZTPvGxJVa8P2LPtJNgxvUSSc1pump',
]);

// Known LP locker programs
const LP_LOCKER_PROGRAMS = new Set([
    // Streamflow
    'strmRqUCoQUgGUan5YhzUZa6KqdzwX5L6FpUxfmKg5m',
    // Uncx
    'LockKXdYQVMbhhckwH3BxoYJ9FYatcZjwNGVuNY1Po4',
    // Team Finance
    'TLoCKic2gGJm7VhZKumih4Lc35fUhYqVMgA4j389Buk',
]);

// Known launchpads with auto LP burn on graduation
const _LAUNCHPAD_DEXES = new Set([
    'pumpswap',
    'pump-fun',
    'moonshot',   // Moonshot also burns LP
]);

/**
 * Check if LP tokens are burned or locked
 * Returns lpBurnPct (0-100) and affects security cap
 *
 * @param {Object} db - Database connection
 * @param {string} mint - Token mint address
 * @returns {Object} { lpBurnPct, lpLockedPct, lpStatus, maxScoreModifier }
 */
async function checkLPStatus(db, mint) {
    const result = {
        lpBurnPct: 0,
        lpLockedPct: 0,
        lpStatus: 'unknown',
        maxScoreModifier: 0,  // 0 = no change, negative = penalty
        checkedPools: 0
    };

    try {
        // Check if token has ANY launchpad pool (indicates graduation with auto LP burn)
        const launchpadCheck = await db.get(`
            SELECT dex FROM pools
            WHERE mint = $1 AND dex IN ('pumpswap', 'pump-fun', 'moonshot')
            LIMIT 1
        `, [mint]);

        if (launchpadCheck) {
            // Launchpad graduated = LP burned by protocol
            result.lpBurnPct = 100;
            result.lpStatus = `burned (${launchpadCheck.dex})`;
            result.maxScoreModifier = 0;
            logger.info(`[LP] ${mint.slice(0,8)}: ✓ Graduated via ${launchpadCheck.dex} (LP auto-burned)`);
            return result;
        }

        // Don't trust, verify: Community verification is a hint, not proof
        // We still check LP status on-chain below, but reduce penalty for verified tokens
        const tokenCheck = await db.get(`
            SELECT hascommunityupdate FROM tokens WHERE mint = $1
        `, [mint]);
        const isCommunityVerified = tokenCheck?.hascommunityupdate || false;

        // Get main pools for other checks
        const pools = await db.all(`
            SELECT address, dex, liquidity_usd
            FROM pools
            WHERE mint = $1
            ORDER BY liquidity_usd DESC NULLS LAST
            LIMIT 3
        `, [mint]);

        if (!pools || pools.length === 0) {
            logger.info(`[LP] ${mint.slice(0,8)}: No pools found`);
            return result;
        }

        // For other DEXes, try to check LP token holders
        const mainPool = pools[0];
        result.checkedPools = 1;

        // Try to find LP mint from pool account (Raydium v4 layout)
        if (mainPool.dex === 'raydium' || mainPool.dex === 'raydium-clmm') {
            const lpStatus = await checkRaydiumLPBurn(mainPool.address);
            if (lpStatus) {
                result.lpBurnPct = lpStatus.burnPct;
                result.lpLockedPct = lpStatus.lockedPct;
                result.lpStatus = lpStatus.status;
            }
        }

        // Determine score modifier based on LP status (on-chain verification)
        const totalSecured = result.lpBurnPct + result.lpLockedPct;
        if (totalSecured >= 90) {
            result.lpStatus = 'secured';
            result.maxScoreModifier = 0;
        } else if (totalSecured >= 50) {
            result.lpStatus = 'partial';
            result.maxScoreModifier = -10;
        } else if (totalSecured > 0) {
            result.lpStatus = 'low';
            result.maxScoreModifier = -20;
        } else {
            result.lpStatus = 'unsecured';
            // Community verification reduces penalty but doesn't bypass on-chain check
            result.maxScoreModifier = isCommunityVerified ? -15 : -30;
        }

        const verifiedNote = isCommunityVerified ? ' (community verified, reduced penalty)' : '';
        logger.info(`[LP] ${mint.slice(0,8)}: ${result.lpBurnPct.toFixed(0)}% burn, ${result.lpLockedPct.toFixed(0)}% locked → ${result.lpStatus}${verifiedNote}`);

        return result;

    } catch (e) {
        logger.error(`[LP] ${mint.slice(0,8)}: ${e.message}`);
        return result;
    }
}

/**
 * Check Raydium pool LP burn status
 * Raydium AMM v4 pool layout:
 *   - lpMint: Pubkey at offset 432 (32 bytes)
 */
async function checkRaydiumLPBurn(poolAddress) {
    try {
        // Get pool account to find LP mint
        const poolInfo = await heliusRpc('getAccountInfo', [poolAddress, { encoding: 'base64' }]);

        if (!poolInfo?.value?.data?.[0]) {
            return null;
        }

        const data = Buffer.from(poolInfo.value.data[0], 'base64');

        // Raydium AMM v4: lpMint is at offset 432 (32 bytes)
        // Layout: status(8) + nonce(8) + ... + lpMint(32)
        const LP_MINT_OFFSET = 432;
        if (data.length < LP_MINT_OFFSET + 32) {
            return null;
        }

        // Extract LP mint pubkey and convert to base58
        const lpMintBytes = data.slice(LP_MINT_OFFSET, LP_MINT_OFFSET + 32);
        const lpMint = bs58.encode(lpMintBytes);

        // Validate it looks like a real pubkey (not all zeros)
        if (lpMint.startsWith('1111111')) {
            return null;
        }

        // Check LP holders for burn/lock status
        const lpStatus = await checkLPHolders(lpMint);
        if (lpStatus) {
            lpStatus.lpMint = lpMint;
        }

        return lpStatus;

    } catch (e) {
        logger.warn(`[LP] Raydium parse error: ${e.message}`);
        return null;
    }
}

/**
 * Check LP token holders for burn/lock status
 */
async function checkLPHolders(lpMint) {
    try {
        const holders = await fetchTokenHolders(lpMint);
        if (!holders || holders.length === 0) {
            return { burnPct: 0, lockedPct: 0, status: 'no-holders' };
        }

        const totalBalance = holders.reduce((sum, h) => sum + h.balance, 0);
        let burnedBalance = 0;
        let lockedBalance = 0;

        for (const h of holders) {
            // Check if holder is a burn address
            if (h.address.startsWith('1111111') || LP_BURN_ADDRESSES.has(h.address)) {
                burnedBalance += h.balance;
                continue;
            }

            // Check if holder is a locker program
            // Need to check the owner of the token account
            // For now, check against known locker addresses
            if (LP_LOCKER_PROGRAMS.has(h.address)) {
                lockedBalance += h.balance;
            }
        }

        const burnPct = totalBalance > 0 ? (burnedBalance / totalBalance) * 100 : 0;
        const lockedPct = totalBalance > 0 ? (lockedBalance / totalBalance) * 100 : 0;

        let status = 'unsecured';
        if (burnPct >= 90) status = 'burned';
        else if (burnPct + lockedPct >= 90) status = 'secured';
        else if (burnPct + lockedPct >= 50) status = 'partial';

        return { burnPct, lockedPct, status };

    } catch (_e) {
        return { burnPct: 0, lockedPct: 0, status: 'error' };
    }
}

// ============================================
// ON-CHAIN LIQUIDITY (via vault balances)
// ============================================

const SOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const USDT_MINT = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB';

/**
 * Get token account balance from RPC
 */
async function _getTokenAccountBalance(vaultAddress) {
    try {
        const result = await heliusRpc('getAccountInfo', [vaultAddress, { encoding: 'base64' }]);
        if (!result || !result.value || !result.value.data) return null;

        // SPL Token account: amount at offset 64 (8 bytes little-endian)
        const data = Buffer.from(result.value.data[0], 'base64');
        if (data.length < 72) return null;

        const amount = data.readBigUInt64LE(64);
        return Number(amount);
    } catch (_e) {
        return null;
    }
}

/**
 * Calculate on-chain liquidity for a token
 * Uses vault reserves from pools (reserve_a, reserve_b)
 * Falls back to cached liquidity_usd if reserves unavailable
 */
async function _calculateOnChainLiquidity(db, mint, solPrice) {
    try {
        // Get pools for this token paired with SOL/USDC/USDT (with reserves)
        const pools = await db.all(`
            SELECT address, dex, token_a, token_b, liquidity_usd, reserve_a, reserve_b
            FROM pools
            WHERE mint = $1
              AND (token_b = $2 OR token_b = $3 OR token_b = $4
                   OR token_a = $2 OR token_a = $3 OR token_a = $4)
            ORDER BY liquidity_usd DESC NULLS LAST
            LIMIT 5
        `, [mint, SOL_MINT, USDC_MINT, USDT_MINT]);

        if (!pools || pools.length === 0) {
            // Fallback to cached liquidity
            const token = await db.get('SELECT liquidity FROM tokens WHERE mint = $1', [mint]);
            return { liquidity: token?.liquidity || 0, source: 'db_cache' };
        }

        let totalLiquidity = 0;
        let source = 'pools_cached';

        for (const pool of pools) {
            // Try to calculate from reserves first
            if (pool.reserve_a && pool.reserve_b && solPrice > 0) {
                let poolLiquidity = 0;

                // Determine which side is the quote (SOL/USDC/USDT)
                const isQuoteB = [SOL_MINT, USDC_MINT, USDT_MINT].includes(pool.token_b);
                const isQuoteA = [SOL_MINT, USDC_MINT, USDT_MINT].includes(pool.token_a);

                if (isQuoteB) {
                    // token_b is quote (SOL/stable), use reserve_b
                    if (pool.token_b === SOL_MINT) {
                        poolLiquidity = pool.reserve_b * solPrice * 2;
                    } else {
                        // Stablecoin - reserve is in USD
                        poolLiquidity = pool.reserve_b * 2;
                    }
                    source = 'reserves';
                } else if (isQuoteA) {
                    // token_a is quote
                    if (pool.token_a === SOL_MINT) {
                        poolLiquidity = pool.reserve_a * solPrice * 2;
                    } else {
                        poolLiquidity = pool.reserve_a * 2;
                    }
                    source = 'reserves';
                }

                if (poolLiquidity > 0) {
                    totalLiquidity += poolLiquidity;
                    continue;
                }
            }

            // Fallback to cached liquidity_usd
            totalLiquidity += pool.liquidity_usd || 0;
        }

        logger.info(`[Liquidity] ${mint.slice(0,8)}: $${totalLiquidity.toFixed(0)} (${pools.length} pools, ${source})`);

        return {
            liquidity: totalLiquidity,
            source,
            poolCount: pools.length
        };
    } catch (e) {
        logger.error(`[Liquidity] Error for ${mint}: ${e.message}`);
        return { liquidity: 0, source: 'error' };
    }
}

// ============================================
// K-SCORE v8 - PURE MATHEMATICAL FORMULA
// ============================================
//
// DESIGN PRINCIPLES:
// 1. Asymptotic normalization (scales to infinity, never reaches 1)
// 2. No arbitrary min/max caps
// 3. Naturally bounded functions [0,1]
// 4. Weighted geometric mean (penalizes weaknesses)
// 5. Sum of weights = 1.00 exactly
//
// MATHEMATICAL PROPERTIES:
// - H(holders) = 1 - 1/(1 + ln(1 + h/κ))     κ=100 inflection
// - A(age)     = 1 - e^(-t/τ)                 τ=21 days half-life
// - T(top10)   = 1 - (concentration/100)      linear inverse
// - C(conviction) = score/100                 direct mapping
// - R(ratio)   = tanh(r/2)                    naturally bounded
//
// FORMULA:
// K = 100 × DiamondHands^0.50 × OrganicGrowth^0.35 × Longevity^0.15
//
// Where:
//   DiamondHands  = √(C × R)   [conviction × acc/ext ratio]
//   OrganicGrowth = √(H × T)   [holders × distribution]
//   Longevity     = A          [age]
//
// ============================================

/**
 * Asymptotic holder normalization
 * H(h) = 1 - 1/(1 + ln(1 + h/κ))
 *
 * Properties:
 * - h=0     → 0.00
 * - h=100   → 0.41 (inflection point κ)
 * - h=1000  → 0.70
 * - h=10000 → 0.82
 * - h=100k  → 0.88
 * - h→∞     → 1.00 (asymptote)
 */
function normalizeHolders(holders, kappa = 100) {
    if (holders <= 0) return 0;
    return 1 - 1 / (1 + Math.log(1 + holders / kappa));
}

/**
 * Exponential decay age normalization
 * A(t) = 1 - e^(-t/τ)
 *
 * Properties:
 * - t=0   → 0.00
 * - t=7   → 0.28 (1 week)
 * - t=21  → 0.63 (τ = half-life)
 * - t=60  → 0.94
 * - t=90  → 0.99
 * - t→∞   → 1.00 (asymptote)
 */
function normalizeAge(ageDays, tau = 21) {
    if (ageDays <= 0) return 0;
    return 1 - Math.exp(-ageDays / tau);
}

/**
 * Linear inverse concentration
 * T(c) = max(0, 1 - c/100)
 *
 * Properties:
 * - 0%   → 1.00 (perfect distribution)
 * - 20%  → 0.80
 * - 50%  → 0.50
 * - 80%  → 0.20
 * - 100% → 0.00 (single holder)
 */
function normalizeTop10(concentrationPct) {
    return Math.max(0, 1 - concentrationPct / 100);
}

/**
 * Hyperbolic tangent ratio normalization
 * R(r) = tanh(r/2)
 *
 * Properties:
 * - r=0   → 0.00 (no accumulators)
 * - r=0.5 → 0.24
 * - r=1   → 0.46 (balanced)
 * - r=2   → 0.76 (acc dominant)
 * - r=5   → 0.99
 * - r→∞   → 1.00 (asymptote)
 */
function normalizeAccExtRatio(ratio) {
    if (ratio <= 0) return 0;
    return Math.tanh(ratio / 2);
}

/**
 * Direct linear normalization for conviction [0-100] → [0-1]
 */
function normalizeConviction(score) {
    return Math.max(0, Math.min(1, score / 100));
}

/**
 * Safe geometric mean of two values
 * Returns √(a × b), with epsilon floor to avoid zero collapse
 */
function geometricMean2(a, b, epsilon = 0.001) {
    return Math.sqrt(Math.max(a, epsilon) * Math.max(b, epsilon));
}

// Pillar weights (MUST sum to 1.00)
const WEIGHTS = {
    DIAMOND_HANDS: 0.50,   // 50% - conviction behavior
    ORGANIC_GROWTH: 0.35,  // 35% - holder distribution
    LONGEVITY: 0.15        // 15% - survival/maturity
};

// Verify weights sum to 1
const WEIGHT_SUM = Object.values(WEIGHTS).reduce((a, b) => a + b, 0);
if (Math.abs(WEIGHT_SUM - 1.0) > 0.001) {
    throw new Error(`K-Score weights must sum to 1.0, got ${WEIGHT_SUM}`);
}

// EMA Smoothing config
// newScore = α × calculated + (1-α) × previous
// α = 0.3 means 30% new data, 70% historical (smooth but responsive)
const EMA_ALPHA = 0.3;

/**
 * Apply Exponential Moving Average smoothing to K-Score
 * Prevents wild swings from single data point changes
 *
 * @param {number} calculated - Newly calculated score
 * @param {number} previous - Previous stored score
 * @param {number} alpha - Smoothing factor (0-1), higher = more reactive
 * @returns {number} Smoothed score
 */
function applyEMA(calculated, previous, alpha = EMA_ALPHA) {
    // DATA VALIDATION: Ensure input is valid
    const safeCalculated = Math.max(0, Math.min(100, Math.round(calculated || 0)));

    // First calculation or no previous score
    if (!previous || previous <= 0) {
        return safeCalculated;
    }

    const smoothed = Math.round(alpha * safeCalculated + (1 - alpha) * previous);

    // DATA VALIDATION: Ensure output is always 0-100
    const validScore = Math.max(0, Math.min(100, smoothed));

    // Log significant smoothing adjustments
    const diff = Math.abs(safeCalculated - previous);
    if (diff >= 5) {
        logger.info(`[EMA] Smoothed ${safeCalculated} → ${validScore} (prev: ${previous}, Δ${diff})`);
    }

    return validScore;
}

async function computeScoreInternal(mint, dbData = null, skipConviction = false, db = null) {
    // Raw metrics
    let raw = {
        holders: 0,
        ageDays: 0,
        top10Pct: 50,  // default: assume 50% if unknown
        conviction: 0,
        accExtRatio: 0
    };

    // Normalized metrics [0-1]
    let normalized = {
        H: 0,  // holders
        A: 0,  // age
        T: 0,  // top10 (inverted)
        C: 0,  // conviction
        R: 0   // acc/ext ratio
    };

    // Pillars
    let pillars = {
        diamondHands: 0,
        organicGrowth: 0,
        longevity: 0
    };

    let convictionData = null;
    let burnData = null;
    let securityData = null;

    try {
        const decimals = dbData?.decimals || 9;

        // Get price for $1+ holder filtering
        let priceUsd = 0;
        if (db) {
            const priceData = await priceService.getPrice(db, mint, decimals);
            priceUsd = priceData.priceUsd;
        } else {
            priceUsd = dbData?.priceusd || dbData?.priceUsd || 0;
        }

        // ============================================
        // MAYHEM MODE: REFRESH SUPPLY
        // ============================================
        // OPTIMIZATION: Only refresh supply during deep refresh (24h) or first check
        // Supply changes are rare - no need to hit RPC every cycle

        let supplyData = null;
        let volatilityData = null;

        if (db && HELIUS_API_KEY) {
            const supplyLastCheck = parseInt(dbData?.supply_last_check || 0);
            const supplyNeedsRefresh = forceDeepRefreshMode ||
                                       supplyLastCheck === 0 ||
                                       (Date.now() - supplyLastCheck > 86400000); // 24h

            if (supplyNeedsRefresh) {
                supplyData = await refreshSupply(db, mint, decimals);
                logger.debug(`[Supply] ${mint.slice(0,8)}: Refreshed (deep mode)`);
            } else {
                // Use cached supply data (0 API calls)
                supplyData = {
                    currentSupply: parseFloat(dbData?.supply || 0) / Math.pow(10, decimals),
                    isMutable: !!dbData?.is_mutable_supply,
                    cached: true
                };
            }
            volatilityData = await getSupplyVolatility(db, mint);
        }

        // ============================================
        // SECURITY CHECK (ELIMINATORY)
        // ============================================
        // OPTIMIZATION: Use cached security data if available
        // Security status rarely changes - only check once per token, then use DB

        let lpData = null;

        const hasCachedSecurity = dbData?.mint_authority_revoked !== null ||
                                   dbData?.freeze_authority_revoked !== null;

        if (hasCachedSecurity) {
            // Use cached security data (0 API calls)
            securityData = {
                mintAuthorityRevoked: !!dbData.mint_authority_revoked,
                freezeAuthorityRevoked: !!dbData.freeze_authority_revoked,
                isSecure: !!dbData.mint_authority_revoked && !!dbData.freeze_authority_revoked,
                maxScore: (!!dbData.mint_authority_revoked && !!dbData.freeze_authority_revoked) ? 100 : 50,
                cached: true
            };
            logger.debug(`[Security] ${mint.slice(0,8)}: Using cached (mint=${securityData.mintAuthorityRevoked}, freeze=${securityData.freezeAuthorityRevoked})`);
        } else if (HELIUS_API_KEY) {
            // First time - fetch and cache
            securityData = await checkTokenSecurity(mint);
            // Cache in DB for future cycles
            if (db && securityData) {
                await db.run(`
                    UPDATE tokens SET
                        mint_authority_revoked = $1,
                        freeze_authority_revoked = $2
                    WHERE mint = $3
                `, [securityData.mintAuthorityRevoked, securityData.freezeAuthorityRevoked, mint]);
            }
        }

        // Check LP burn/lock status
        // OPTIMIZATION: LP status is cached - only check on deep refresh or first time
        // LP burn/lock is effectively permanent once done
        if (db) {
            // Check if we have cached LP data (stored in tokens table)
            // LP status rarely changes - only refresh during deep mode
            const hasCachedLP = dbData?.lp_burn_pct !== undefined && dbData?.lp_burn_pct !== null;

            if (hasCachedLP && !forceDeepRefreshMode) {
                // Use cached LP data (0 API calls)
                lpData = {
                    lpBurnPct: dbData.lp_burn_pct || 0,
                    lpLockedPct: dbData.lp_locked_pct || 0,
                    lpStatus: dbData.lp_status || 'unknown',
                    cached: true
                };
                logger.debug(`[LP] ${mint.slice(0,8)}: Using cached (${lpData.lpBurnPct}% burn)`);
            } else {
                // First time or deep refresh - check on-chain
                lpData = await checkLPStatus(db, mint);
                // Cache LP data in DB
                if (lpData) {
                    await db.run(`
                        UPDATE tokens SET
                            lp_burn_pct = $1,
                            lp_locked_pct = $2,
                            lp_status = $3
                        WHERE mint = $4
                    `, [lpData.lpBurnPct || 0, lpData.lpLockedPct || 0, lpData.lpStatus || 'unknown', mint]);
                }
            }
        }

        // ============================================
        // FETCH ON-CHAIN DATA
        // ============================================

        if (!skipConviction && HELIUS_API_KEY) {
            convictionData = await calculateConvictionAndHolders(mint, priceUsd, decimals, db);

            // In webhook/delta mode, skip burn recalculation - use stored values
            // Burns are immutable, no need to recalculate every cycle
            if (convictionData?.isWebhookMode || convictionData?.isDeltaMode) {
                // Use cached burn data from DB
                burnData = {
                    burnedPercent: dbData?.burned_percent || 0,
                    totalSupply: dbData?.supply || 0,
                    decimals: dbData?.decimals || 9
                };
                const mode = convictionData?.isWebhookMode ? 'Webhook' : 'Delta';
                logger.debug(`[${mode}] ${mint.slice(0,8)}: Using cached burn (${burnData.burnedPercent}%)`);
            } else {
                // Full analysis mode - calculate burn from holders
                burnData = await calculateBurn(mint, convictionData.allHolders || [], {
                    storedInitialSupply: dbData?.initial_supply,
                    isPumpFunFlag: dbData?.is_pump_fun,
                    db
                });
            }
        }

        // Get age from token timestamp (DexScreener creation date)
        if (db) {
            const tokenData = await db.get(`
                SELECT timestamp FROM tokens WHERE mint = $1
            `, [mint]);

            if (tokenData?.timestamp) {
                const ts = Number(tokenData.timestamp);
                const now = Date.now();
                const year2020 = 1577836800000; // Jan 1 2020

                // Validate timestamp is reasonable (between 2020 and now)
                if (ts > year2020 && ts < now) {
                    raw.ageDays = (now - ts) / 86400000;
                    logger.info(`[Age] ${mint.slice(0,8)}: ${raw.ageDays.toFixed(1)} days`);
                }
            }
        }

        // Fallback: minimum 1 day if no timestamp
        if (raw.ageDays === 0) {
            raw.ageDays = 1;
            logger.info(`[Age] ${mint.slice(0,8)}: defaulting to 1 day`);
        }

        // ============================================
        // EXTRACT RAW METRICS
        // ============================================

        if (convictionData) {
            raw.holders = convictionData.realHoldersCount || 0;
            raw.conviction = convictionData.score || 0;

            // Acc/Ext ratio
            const acc = convictionData.accumulators || 0;
            const ext = convictionData.extractors || 0;
            raw.accExtRatio = ext > 0 ? acc / ext : (acc > 0 ? 10 : 0);

            // Top10 concentration
            if (convictionData.isWebhookMode && db) {
                // Webhook mode: calculate from holder_snapshots (0 RPC)
                const top10Snapshots = await db.all(
                    'SELECT balance FROM holder_snapshots WHERE mint = $1 ORDER BY balance DESC LIMIT 10',
                    [mint]
                );
                if (top10Snapshots.length >= 10 && burnData?.totalSupply > 0) {
                    const top10Balance = top10Snapshots.reduce((s, h) => s + BigInt(h.balance || 0), 0n);
                    const totalSupplyRaw = BigInt(Math.floor(burnData.totalSupply * Math.pow(10, burnData.decimals || 9)));
                    raw.top10Pct = totalSupplyRaw > 0n ? Number((top10Balance * 100n) / totalSupplyRaw) : 50;
                }
            } else {
                // Full analysis mode: use filtered holders from RPC
                const filteredHolders = convictionData.filteredTop50 || convictionData.allHolders || [];
                if (filteredHolders.length >= 10 && burnData?.totalSupply > 0) {
                    const top10Balance = filteredHolders.slice(0, 10).reduce((s, h) => s + h.balance, 0);
                    const totalSupplyRaw = burnData.totalSupply * Math.pow(10, burnData.decimals || 9);
                    raw.top10Pct = (top10Balance / totalSupplyRaw) * 100;
                }
            }
        }

        // ============================================
        // NORMALIZE ALL METRICS [0-1]
        // ============================================

        normalized.H = normalizeHolders(raw.holders);
        normalized.A = normalizeAge(raw.ageDays);
        normalized.T = normalizeTop10(raw.top10Pct);
        normalized.C = normalizeConviction(raw.conviction);
        normalized.R = normalizeAccExtRatio(raw.accExtRatio);

        // ============================================
        // CALCULATE 3 PILLARS
        // ============================================

        // Diamond Hands = √(Conviction × AccExtRatio)
        pillars.diamondHands = geometricMean2(normalized.C, normalized.R);

        // Organic Growth = √(Holders × Top10Distribution)
        pillars.organicGrowth = geometricMean2(normalized.H, normalized.T);

        // Longevity = Age (direct)
        pillars.longevity = normalized.A;

        // ============================================
        // FINAL SCORE: Weighted Geometric Mean
        // K = 100 × D^0.50 × O^0.35 × L^0.15
        // ============================================

        const epsilon = 0.001;
        let score = Math.round(
            100 *
            Math.pow(Math.max(pillars.diamondHands, epsilon), WEIGHTS.DIAMOND_HANDS) *
            Math.pow(Math.max(pillars.organicGrowth, epsilon), WEIGHTS.ORGANIC_GROWTH) *
            Math.pow(Math.max(pillars.longevity, epsilon), WEIGHTS.LONGEVITY)
        );

        // ============================================
        // LEGACY FLOOR (harmonic, asymptotic)
        // ============================================
        // "The rock still shines even when under dirt" - Dior
        //
        // Reuses existing normalized values (scales to infinity):
        //   A = age normalization (asymptotic)
        //   H = holders normalization (asymptotic)
        //
        // LegacyStrength = sqrt(A * H)  [geometric mean of survival factors]
        // LegacyFloor = MAX_FLOOR * LegacyStrength
        //
        // Applied BEFORE security cap (security always wins)

        const LEGACY_MAX_FLOOR = 55;  // Asymptotic maximum floor

        const legacyStrength = Math.sqrt(normalized.A * normalized.H);
        const legacyFloor = Math.round(LEGACY_MAX_FLOOR * legacyStrength);

        const uncappedScore = score;

        if (score < legacyFloor) {
            logger.info(`[Legacy] ${mint.slice(0,8)}: ${score} -> ${legacyFloor} (strength: ${(legacyStrength * 100).toFixed(0)}%)`);
            score = legacyFloor;
        }

        // ============================================
        // APPLY SECURITY CAP (ELIMINATORY)
        // ============================================
        // Security ALWAYS wins - applied AFTER legacy floor

        // 1. Authority cap (mint/freeze)
        const authorityCap = securityData?.maxScore || 100;

        // 2. LP cap modifier (0 or negative)
        const lpModifier = lpData?.maxScoreModifier || 0;

        // 3. Supply volatility penalty (Mayhem Mode)
        const volatilityPenalty = volatilityData?.penalty || 0;

        // Final cap = authority cap + LP modifier - volatility penalty
        let finalCap = Math.max(0, authorityCap + lpModifier);

        // Apply volatility penalty (subtracts from score, not cap)
        if (volatilityPenalty > 0) {
            score = Math.max(0, score - volatilityPenalty);
            logger.info(`[Mayhem] ${mint.slice(0,8)}: -${volatilityPenalty} pts (volatility: ${volatilityData.volatility}%)`);
        }

        if (score > finalCap) {
            score = finalCap;
        }

        // Log breakdown
        const wasCapped = score < uncappedScore;
        logger.info(`[K-Score v8] ${mint.slice(0,8)}: ${score}${wasCapped ? ` (capped from ${uncappedScore})` : ''}`);
        logger.info(`  Diamond Hands: ${(pillars.diamondHands * 100).toFixed(0)}% (C:${(normalized.C * 100).toFixed(0)}% R:${(normalized.R * 100).toFixed(0)}%)`);
        logger.info(`  Organic Growth: ${(pillars.organicGrowth * 100).toFixed(0)}% (H:${(normalized.H * 100).toFixed(0)}% T:${(normalized.T * 100).toFixed(0)}%)`);
        logger.info(`  Longevity: ${(pillars.longevity * 100).toFixed(0)}% (${raw.ageDays.toFixed(1)}d)`);
        if (securityData) {
            const secStatus = securityData.isSecure ? '✓' : `cap=${authorityCap}`;
            logger.info(`  Security: ${secStatus}`);
        }
        if (lpData) {
            logger.info(`  LP: ${lpData.lpStatus} (${lpData.lpBurnPct.toFixed(0)}% burn)`);
        }
        if (supplyData?.isMutable) {
            logger.info(`  Supply: MUTABLE (${supplyData.changePercent.toFixed(2)}% change)`);
        }

        return {
            score: Math.min(100, Math.max(0, score)),
            uncappedScore,
            finalCap,
            conviction: convictionData,
            burn: burnData,
            security: securityData,
            lp: lpData,
            supply: supplyData,
            volatility: volatilityData,
            raw,
            normalized,
            pillars
        };

    } catch (e) {
        logger.error(`[K-Score] Calc error ${mint}: ${e.message}`);
        return { score: 0, conviction: null, burn: null, metrics: {}, breakdown: {} };
    }
}

// ============================================
// PUBLIC API
// ============================================

/**
 * Detect if token is from PumpFun and if bonding curve is complete
 */
async function detectTokenCategory(db, mint) {
    const category = {
        isPumpFun: false,
        bondingCurveComplete: false
    };

    // Check if mint ends with 'pump' (PumpFun convention)
    if (mint.endsWith('pump')) {
        category.isPumpFun = true;
    }

    // Check if token has pumpswap pool (= graduated from bonding curve)
    try {
        const pumpPool = await db.get(`
            SELECT dex FROM pools
            WHERE mint = $1 AND dex IN ('pumpswap', 'pump-fun')
            LIMIT 1
        `, [mint]);

        if (pumpPool) {
            category.isPumpFun = true;
            category.bondingCurveComplete = true;
        }
    } catch (_e) {
        // Ignore errors
    }

    return category;
}

/**
 * Update single token score immediately (for admin approval)
 * SECURITY: Only runs full analysis for verified tokens (hasCommunityUpdate = TRUE)
 * This prevents API abuse and saves Helius credits
 */
async function updateSingleToken(deps, mint) {
    const { db, broadcast } = deps;
    try {
        const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
        if (!token) return;

        // SECURITY: Only run deep analysis for verified tokens
        const isVerified = token.hasCommunityUpdate || token.hascommunityupdate;
        if (!isVerified) {
            logger.warn(`[K-Score] Skipping ${token.symbol || mint.slice(0,8)} - not verified (hasCommunityUpdate=false)`);
            return null;
        }

        logger.info(`[K-Score] Immediate calc for ${token.name || token.symbol} (verified)`);
        const result = await computeScoreInternal(mint, token, false, db);
        const conviction = result.conviction || {};
        const burn = result.burn || {};
        const security = result.security || {};
        const supply = result.supply || {};

        // Apply EMA smoothing to prevent wild swings
        const previousScore = token.k_score || 0;
        const smoothedScore = applyEMA(result.score, previousScore);

        // Detect token category (PumpFun, bonding curve)
        const category = await detectTokenCategory(db, mint);

        // Calculate initial supply (total supply before burns)
        const currentSupply = burn.totalSupply || 0;
        const burnedAmount = burn.burned || 0;
        const initialSupply = currentSupply + burnedAmount;

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
                last_holder_check = $10,
                burned_amount = $11,
                burned_percent = $12,
                initial_supply = $13,
                is_pump_fun = $14,
                bonding_curve_complete = $15,
                mint_authority_revoked = $16,
                freeze_authority_revoked = $17,
                is_mutable_supply = $18
            WHERE mint = $19
        `, [
            smoothedScore,
            Date.now().toString(),
            conviction.score || 0,
            conviction.accumulators || 0,
            conviction.holders || 0,
            conviction.reducers || 0,
            conviction.extractors || 0,
            conviction.analyzed || 0,
            conviction.realHoldersCount || 0,
            Date.now().toString(),
            burnedAmount,
            burn.burnPct || 0,
            initialSupply > 0 ? initialSupply.toString() : null,
            category.isPumpFun,
            category.bondingCurveComplete,
            security.mintAuthorityRevoked || false,
            security.freezeAuthorityRevoked || false,
            supply.isMutable || false,
            mint
        ]);

        // Save holder history snapshot (daily)
        await saveHolderHistory(db, mint, conviction.totalHolders || 0, conviction.realHoldersCount || 0);

        // Save K-Score history snapshot (daily) for credit rating trajectory
        await saveKScoreHistory(db, mint, smoothedScore, conviction.score || 0, conviction.realHoldersCount || 0);

        // Broadcast K-Score update via WebSocket (use smoothed score)
        if (broadcast) {
            broadcast.kscoreUpdate(mint, {
                kScore: smoothedScore,
                conviction: conviction.score || 0,
                accumulators: conviction.accumulators || 0,
                holders: conviction.realHoldersCount || 0,
                timestamp: Date.now()
            });
        }

        // Return with smoothed score
        result.score = smoothedScore;
        result.rawScore = result.uncappedScore; // Keep raw for debugging
        return result;
    } catch (e) {
        logger.error(`[K-Score] Failed single update for ${mint}:`, e);
        return 0;
    }
}

/**
 * Batch update K-Scores (scheduled task)
 */
// Flag to force deep refresh (bypass webhook mode)
let forceDeepRefreshMode = false;

async function updateKScores(deps) {
    const { db, broadcast, forceDeepRefresh } = deps;

    // Set global flag for deep refresh (will be read by calculateConvictionAndHolders)
    forceDeepRefreshMode = !!forceDeepRefresh;

    const modeLabel = forceDeepRefresh ? 'DEEP REFRESH' : (config.USE_WEBHOOKS ? 'LIGHT (webhook)' : 'POLLING');
    logger.info(`[K-Score v5] Starting cycle... Mode: ${modeLabel}`);

    try {
        // Only calculate K-Score for verified tokens (saves Helius API credits)
        // Tokens get verified via community submission or admin approval
        const tokens = await db.all(`
            SELECT * FROM tokens
            WHERE hascommunityupdate = TRUE
            ORDER BY volume24h DESC NULLS LAST
        `);

        if (!tokens || tokens.length === 0) {
            logger.info("[K-Score] No eligible tokens (none verified).");
            return;
        }

        logger.info(`[K-Score] Updating ${tokens.length} verified tokens...`);

        for (const t of tokens) {
            try {
                const result = await computeScoreInternal(t.mint, t, false, db);
                const conviction = result.conviction || {};
                const burn = result.burn || {};
                const security = result.security || {};
                const supply = result.supply || {};

                // Apply EMA smoothing to prevent wild swings
                const previousScore = t.k_score || 0;
                const smoothedScore = applyEMA(result.score, previousScore);

                // Detect token category (PumpFun, bonding curve)
                const category = await detectTokenCategory(db, t.mint);

                // Calculate initial supply (total supply before burns)
                const currentSupply = burn.totalSupply || 0;
                const burnedAmount = burn.burned || 0;
                const initialSupply = currentSupply + burnedAmount;

                // DATA VALIDATION: Ensure all values are safe before DB write
                const safeInt = (v, max = 1000000) => Math.max(0, Math.min(max, Math.round(v || 0)));
                const safeFloat = (v, max = 1e15) => Math.max(0, Math.min(max, v || 0));
                const safeBool = (v) => v === true;

                // Validate conviction counts don't exceed analyzed count
                const analyzedCount = safeInt(conviction.analyzed, 100);
                const validatedConviction = {
                    score: safeInt(conviction.score, 100),
                    accumulators: safeInt(conviction.accumulators, analyzedCount),
                    holders: safeInt(conviction.holders, analyzedCount),
                    reducers: safeInt(conviction.reducers, analyzedCount),
                    extractors: safeInt(conviction.extractors, analyzedCount),
                    analyzed: analyzedCount,
                    realHoldersCount: safeInt(conviction.realHoldersCount, 10000000)
                };

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
                        last_holder_check = $10,
                        burned_amount = $11,
                        burned_percent = $12,
                        initial_supply = $13,
                        is_pump_fun = $14,
                        bonding_curve_complete = $15,
                        mint_authority_revoked = $16,
                        freeze_authority_revoked = $17,
                        is_mutable_supply = $18
                    WHERE mint = $19
                `, [
                    smoothedScore,
                    Date.now().toString(),
                    validatedConviction.score,
                    validatedConviction.accumulators,
                    validatedConviction.holders,
                    validatedConviction.reducers,
                    validatedConviction.extractors,
                    validatedConviction.analyzed,
                    validatedConviction.realHoldersCount,
                    Date.now().toString(),
                    safeFloat(burnedAmount),
                    safeFloat(burn.burnPct, 100),
                    initialSupply > 0 ? initialSupply.toString() : null,
                    safeBool(category.isPumpFun),
                    safeBool(category.bondingCurveComplete),
                    safeBool(security.mintAuthorityRevoked),
                    safeBool(security.freezeAuthorityRevoked),
                    safeBool(supply.isMutable),
                    t.mint
                ]);

                // Save holder history snapshot (daily)
                await saveHolderHistory(db, t.mint, conviction.totalHolders || 0, conviction.realHoldersCount || 0);

                // Save K-Score history snapshot (daily) for credit rating trajectory
                await saveKScoreHistory(db, t.mint, smoothedScore, conviction.score || 0, conviction.realHoldersCount || 0);

                // Broadcast K-Score update via WebSocket (use smoothed score)
                if (broadcast) {
                    broadcast.kscoreUpdate(t.mint, {
                        kScore: smoothedScore,
                        symbol: t.symbol,
                        conviction: conviction.score || 0,
                        accumulators: conviction.accumulators || 0,
                        holders: conviction.realHoldersCount || 0,
                        timestamp: Date.now()
                    });
                }

            } catch (err) {
                logger.warn(`[K-Score] Failed for ${t.mint}: ${err.message}`);
            }

            await sleep(500); // Slower between tokens (conviction is heavy)
        }

        logger.info("[K-Score v5] Cycle complete.");

    } catch (e) {
        logger.error("[K-Score] Cycle error", e);
    } finally {
        // Reset deep refresh flag
        forceDeepRefreshMode = false;
    }
}

function start(deps) {
    if (!HELIUS_API_KEY) {
        logger.warn("[K-Score] No HELIUS_API_KEY - conviction analysis disabled");
    }

    // Determine interval based on webhook mode
    // Webhook mode: Data arrives in real-time, so we just need to aggregate periodically
    // Polling mode: We need to fetch data ourselves, but 10 min is overkill for conviction
    const LIGHT_INTERVAL = config.USE_WEBHOOKS ? 3600000 : 1800000; // 1h (webhook) or 30m (polling)
    const DEEP_INTERVAL = 86400000; // 24h for full holder refresh

    const modeLabel = config.USE_WEBHOOKS ? 'WEBHOOK (0 RPC)' : 'POLLING';
    const intervalMin = LIGHT_INTERVAL / 60000;

    logger.info(`🟢 K-Score Updater Started - Mode: ${modeLabel}, Interval: ${intervalMin}min`);

    // Light updates (uses cached data in webhook mode)
    setInterval(() => updateKScores(deps), LIGHT_INTERVAL);

    // Deep refresh once per day (full RPC analysis to update holder counts, burn, etc.)
    // Runs at a different offset to avoid overlap
    setInterval(() => {
        logger.info('[K-Score] Starting daily deep refresh (full RPC analysis)');
        // Force polling mode for deep refresh by temporarily clearing webhook flag
        updateKScores({ ...deps, forceDeepRefresh: true });
    }, DEEP_INTERVAL);

    // ============================================
    // STALENESS DETECTION & REFRESH
    // Check for stale tokens every 2 hours and trigger refresh
    // ============================================
    const STALENESS_CHECK_INTERVAL = 2 * 60 * 60 * 1000; // 2 hours

    setInterval(async () => {
        try {
            const staleTokens = await verification.getStaleTokens(deps.db);

            if (staleTokens.length > 0) {
                logger.warn(`[Staleness] Found ${staleTokens.length} stale tokens (>${verification.STALENESS_THRESHOLD_MS / (60*60*1000)}h old)`);

                // Refresh up to 5 stale tokens per cycle
                const tokensToRefresh = staleTokens.slice(0, 5);

                for (const token of tokensToRefresh) {
                    logger.info(`[Staleness] Refreshing stale token: ${token.symbol} (${token.ageHours}h old)`);
                    try {
                        await updateSingleToken(deps, token.mint);

                        // Log the refresh to audit trail
                        verification.logAudit(deps.db, {
                            action: 'staleness_refresh',
                            entity: 'token',
                            entityId: token.mint,
                            oldValue: { ageHours: token.ageHours, k_score: token.k_score },
                            source: 'staleness_detector',
                            metadata: { reason: 'No holder activity in staleness window' }
                        }).catch(_e => {});

                    } catch (err) {
                        logger.error(`[Staleness] Failed to refresh ${token.symbol}: ${err.message}`);
                    }
                }
            }
        } catch (err) {
            logger.error(`[Staleness] Check failed: ${err.message}`);
        }
    }, STALENESS_CHECK_INTERVAL);

    // Run once after startup (delay 30s to let other services init)
    setTimeout(() => updateKScores(deps), 30000);

    // Initialize audit table (async, non-blocking)
    setTimeout(() => {
        verification.ensureAuditTable(deps.db).catch(err => {
            logger.error(`[Audit] Table init failed: ${err.message}`);
        });
    }, 5000);
}

/**
 * Get Helius API health status for monitoring
 * Exposes circuit breaker state and rate limit info
 */
function getHealthStatus() {
    return {
        helius: {
            configured: !!HELIUS_API_KEY,
            circuitBreaker: {
                state: circuitBreaker.state,
                failures: circuitBreaker.failures,
                threshold: circuitBreaker.threshold,
                lastFailure: circuitBreaker.lastFailure > 0
                    ? new Date(circuitBreaker.lastFailure).toISOString()
                    : null,
                cooldownMs: circuitBreaker.cooldown
            },
            rateLimit: {
                remaining: rateLimitRemaining,
                limit: currentRateLimit,
                resetTime: rateLimitResetTime > 0
                    ? new Date(rateLimitResetTime).toISOString()
                    : null,
                requestIntervalMs: Math.round(requestInterval)
            }
        },
        caches: {
            poolCacheSize: memoryPoolCache.size,
            poolCacheTTLSeconds: POOL_CACHE_TTL_SECONDS,
            redisEnabled: !!getRedisClient()
        }
    };
}

module.exports = {
    start,
    updateSingleToken,
    calculateTokenScore: async (mint) => {
        const result = await computeScoreInternal(mint, null, true);
        return result.score;
    },
    calculateConviction, // Exposed for testing
    getHealthStatus,     // For /health endpoint
    checkTokenSecurity,  // Exposed for direct security checks
};
