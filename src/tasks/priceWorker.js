/**
 * Price Worker - Unified Price Service
 *
 * Philosophy: K-Score = Helius (our value), Price = DexScreener (free)
 *
 * Features:
 * - Batch API: 30 tokens per call (vs 1 token = 30x efficiency)
 * - Tiered refresh: Verified (1min), Active (5min), Dormant (30min)
 * - Zero Helius credits consumed
 * - WebSocket broadcast on price changes
 */

const { logger } = require('../services');
const { signMarket } = require('../utils/dataSignature');

// DexScreener batch endpoint: up to 30 tokens
const DEXSCREENER_BATCH_URL = 'https://api.dexscreener.com/tokens/v1/solana';
const DEXSCREENER_SINGLE_URL = 'https://api.dexscreener.com/latest/dex/tokens';

// Refresh tiers (in milliseconds)
const TIERS = {
    VERIFIED: 60 * 1000,      // 1 minute
    TOP_100: 60 * 1000,       // 1 minute
    ACTIVE: 5 * 60 * 1000,    // 5 minutes
    DORMANT: 30 * 60 * 1000   // 30 minutes
};

// Batch size limit
const BATCH_SIZE = 30;

// In-memory cache with TTL tracking
const priceCache = new Map();
const PRICE_CACHE_MAX_SIZE = 2000; // Max cached tokens (prevents memory leak)

/**
 * LRU-style eviction: remove oldest entries when cache is full
 */
function evictOldestPriceEntries() {
    if (priceCache.size <= PRICE_CACHE_MAX_SIZE) return;

    // Maps maintain insertion order - delete from the beginning
    const toDelete = priceCache.size - PRICE_CACHE_MAX_SIZE;
    let deleted = 0;
    for (const key of priceCache.keys()) {
        if (deleted >= toDelete) break;
        priceCache.delete(key);
        deleted++;
    }
}

// Periodic cleanup of stale entries (every 10 minutes)
setInterval(() => {
    const now = Date.now();
    const staleThreshold = 30 * 60 * 1000; // 30 minutes

    for (const [mint, cached] of priceCache) {
        if (now - cached.timestamp > staleThreshold) {
            priceCache.delete(mint);
        }
    }

    logger.debug(`[PriceWorker] Cache cleanup: ${priceCache.size} entries`);
}, 10 * 60 * 1000);

// ============================================
// BOUNDS VALIDATION - Protect against malicious/corrupted external data
// ============================================
const PRICE_BOUNDS = {
    MIN_PRICE: 0,
    MAX_PRICE: 1e15,           // $1 quadrillion max (absurd but safe)
    MIN_MCAP: 0,
    MAX_MCAP: 1e15,            // $1 quadrillion max
    MIN_LIQUIDITY: 0,
    MAX_LIQUIDITY: 1e12,       // $1 trillion max
    MIN_VOLUME: 0,
    MAX_VOLUME: 1e12,          // $1 trillion max
    MIN_CHANGE_PCT: -100,      // Can't lose more than 100%
    MAX_CHANGE_PCT: 100000,    // 100,000% max gain (100x)
};

/**
 * Validate and clamp a numeric value within bounds
 * @param {number} value - Value to validate
 * @param {number} min - Minimum allowed
 * @param {number} max - Maximum allowed
 * @param {number} fallback - Fallback if invalid
 * @returns {number} Validated value
 */
function clampValue(value, min, max, fallback = 0) {
    const num = parseFloat(value);
    if (!Number.isFinite(num)) return fallback;
    if (num < min) return min;
    if (num > max) return max;
    return num;
}

/**
 * Validate external price data - reject if critically malformed
 * @param {Object} raw - Raw price data from external API
 * @returns {Object|null} Validated data or null if rejected
 */
function validatePriceData(raw) {
    if (!raw || typeof raw !== 'object') return null;

    const priceUsd = clampValue(raw.priceUsd, PRICE_BOUNDS.MIN_PRICE, PRICE_BOUNDS.MAX_PRICE);
    const mcap = clampValue(raw.mcap, PRICE_BOUNDS.MIN_MCAP, PRICE_BOUNDS.MAX_MCAP);
    const liquidity = clampValue(raw.liquidity, PRICE_BOUNDS.MIN_LIQUIDITY, PRICE_BOUNDS.MAX_LIQUIDITY);
    const volume24h = clampValue(raw.volume24h, PRICE_BOUNDS.MIN_VOLUME, PRICE_BOUNDS.MAX_VOLUME);
    const change24h = clampValue(raw.change24h, PRICE_BOUNDS.MIN_CHANGE_PCT, PRICE_BOUNDS.MAX_CHANGE_PCT);
    const change1h = clampValue(raw.change1h, PRICE_BOUNDS.MIN_CHANGE_PCT, PRICE_BOUNDS.MAX_CHANGE_PCT);
    const change5m = clampValue(raw.change5m, PRICE_BOUNDS.MIN_CHANGE_PCT, PRICE_BOUNDS.MAX_CHANGE_PCT);

    // Reject if price is 0 but mcap is huge (inconsistent data)
    if (priceUsd === 0 && mcap > 1e9) {
        logger.warn(`[PriceWorker] Rejected inconsistent data: price=0 but mcap=${mcap}`);
        return null;
    }

    return {
        priceUsd,
        mcap,
        liquidity,
        volume24h,
        change24h,
        change1h,
        change5m,
        pairAddress: raw.pairAddress,
        dex: raw.dex,
        source: raw.source || 'dexscreener',
        timestamp: raw.timestamp || Date.now()
    };
}

/**
 * Fetch prices for multiple tokens in one API call
 * @param {string[]} mints - Array of token mint addresses (max 30)
 * @returns {Map<string, Object>} Map of mint -> price data
 */
async function fetchBatchPrices(mints, retryCount = 0) {
    if (!mints || mints.length === 0) return new Map();

    const MAX_RETRIES = 3;
    const BASE_DELAY = 1000; // 1 second base delay for backoff

    // DexScreener batch: comma-separated addresses
    const batchMints = mints.slice(0, BATCH_SIZE);
    const url = `${DEXSCREENER_BATCH_URL}/${batchMints.join(',')}`;

    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10000);

        const response = await fetch(url, { signal: controller.signal });
        clearTimeout(timeout);

        // Handle rate limiting with exponential backoff
        if (response.status === 429) {
            if (retryCount < MAX_RETRIES) {
                const delay = BASE_DELAY * Math.pow(2, retryCount); // 1s, 2s, 4s
                logger.debug(`[PriceWorker] Rate limited, retry ${retryCount + 1}/${MAX_RETRIES} in ${delay}ms`);
                await new Promise(r => setTimeout(r, delay));
                return fetchBatchPrices(mints, retryCount + 1);
            }
            logger.warn(`[PriceWorker] Rate limited, max retries exceeded`);
            return new Map();
        }

        if (!response.ok) {
            logger.warn(`[PriceWorker] Batch fetch failed: ${response.status}`);
            return new Map();
        }

        const data = await response.json();
        const results = new Map();
        const now = Date.now();

        // Process each token's pairs
        for (const pair of (data || [])) {
            const mint = pair.baseToken?.address;
            if (!mint) continue;

            // Aggregate all pairs for this token
            if (!results.has(mint)) {
                // Validate external data before storing
                const rawData = {
                    priceUsd: parseFloat(pair.priceUsd) || 0,
                    mcap: pair.marketCap || pair.fdv || 0,
                    liquidity: pair.liquidity?.usd || 0,
                    volume24h: parseFloat(pair.volume?.h24) || 0,
                    change24h: parseFloat(pair.priceChange?.h24) || 0,
                    change1h: parseFloat(pair.priceChange?.h1) || 0,
                    change5m: parseFloat(pair.priceChange?.m5) || 0,
                    pairAddress: pair.pairAddress,
                    dex: pair.dexId,
                    source: 'dexscreener',
                    timestamp: now
                };
                const validated = validatePriceData(rawData);
                if (validated) {
                    results.set(mint, validated);
                }
            } else {
                // Add liquidity from additional pairs (with bounds check)
                const existing = results.get(mint);
                const additionalLiq = clampValue(pair.liquidity?.usd, 0, PRICE_BOUNDS.MAX_LIQUIDITY);
                existing.liquidity = clampValue(existing.liquidity + additionalLiq, 0, PRICE_BOUNDS.MAX_LIQUIDITY);
            }
        }

        // Update cache (with LRU eviction)
        evictOldestPriceEntries();
        for (const [mint, data] of results) {
            priceCache.set(mint, { data, timestamp: now });
        }

        logger.debug(`[PriceWorker] Batch fetched ${results.size}/${batchMints.length} tokens`);
        return results;

    } catch (e) {
        if (e.name === 'AbortError') {
            logger.warn('[PriceWorker] Batch request timeout');
        } else {
            logger.error('[PriceWorker] Batch fetch error:', e.message);
        }
        return new Map();
    }
}

/**
 * Get cached price or fetch if stale
 * @param {string} mint - Token mint address
 * @param {number} maxAge - Max cache age in ms
 * @returns {Object|null} Price data or null
 */
function getCachedPrice(mint, maxAge = TIERS.ACTIVE) {
    const cached = priceCache.get(mint);
    if (cached && Date.now() - cached.timestamp < maxAge) {
        return cached.data;
    }
    return null;
}

/**
 * Main price update cycle
 * Fetches prices for all tokens based on their tier
 */
async function runPriceUpdateCycle(db, broadcast) {
    const startTime = Date.now();

    try {
        // Get tokens grouped by tier
        const tokens = await db.all(`
            SELECT
                mint,
                symbol,
                hasCommunityUpdate,
                volume24h,
                price_timestamp
            FROM tokens
            WHERE mint != 'So11111111111111111111111111111111111111112'
            ORDER BY
                CASE WHEN hasCommunityUpdate = TRUE THEN 0 ELSE 1 END,
                volume24h DESC NULLS LAST
            LIMIT 1000
        `);

        const now = Date.now();
        const toUpdate = {
            verified: [],
            top100: [],
            active: [],
            dormant: []
        };

        // Classify tokens by tier and staleness
        for (let i = 0; i < tokens.length; i++) {
            const t = tokens[i];
            const lastUpdate = parseInt(t.price_timestamp || 0);
            const age = now - lastUpdate;

            if (t.hascommunityupdate || t.hasCommunityUpdate) {
                if (age > TIERS.VERIFIED) toUpdate.verified.push(t.mint);
            } else if (i < 100) {
                if (age > TIERS.TOP_100) toUpdate.top100.push(t.mint);
            } else if (t.volume24h > 1000) {
                if (age > TIERS.ACTIVE) toUpdate.active.push(t.mint);
            } else {
                if (age > TIERS.DORMANT) toUpdate.dormant.push(t.mint);
            }
        }

        // Combine all tokens needing update (prioritized)
        const allToUpdate = [
            ...toUpdate.verified,
            ...toUpdate.top100,
            ...toUpdate.active.slice(0, 100), // Limit active per cycle
            ...toUpdate.dormant.slice(0, 30)   // Limit dormant per cycle
        ];

        if (allToUpdate.length === 0) {
            logger.debug('[PriceWorker] All prices fresh');
            return { updated: 0, duration: Date.now() - startTime };
        }

        // Batch fetch (30 tokens per API call)
        let totalUpdated = 0;
        const batches = [];

        for (let i = 0; i < allToUpdate.length; i += BATCH_SIZE) {
            batches.push(allToUpdate.slice(i, i + BATCH_SIZE));
        }

        logger.info(`[PriceWorker] Updating ${allToUpdate.length} tokens in ${batches.length} batches`);

        for (const batch of batches) {
            const prices = await fetchBatchPrices(batch);

            // Update database
            for (const [mint, priceData] of prices) {
                try {
                    // Update prices and return fields needed for sig_market
                    const result = await db.get(`
                        UPDATE tokens SET
                            priceusd = $1,
                            marketcap = $2,
                            liquidity = $3,
                            volume24h = $4,
                            change24h = $5,
                            change1h = $6,
                            change5m = $7,
                            price_source = $8,
                            price_timestamp = $9,
                            price_pool = $10,
                            liquidity_source = $8,
                            liquidity_timestamp = $9
                        WHERE mint = $11
                        RETURNING mint, priceusd, marketcap, liquidity,
                                  price_source, price_timestamp, price_pool,
                                  liquidity_source, liquidity_timestamp,
                                  mcap_calculated, holders_source, holders_timestamp, age_days
                    `, [
                        priceData.priceUsd,
                        priceData.mcap,
                        priceData.liquidity,
                        priceData.volume24h,
                        priceData.change24h,
                        priceData.change1h,
                        priceData.change5m,
                        'dexscreener',
                        priceData.timestamp.toString(),
                        priceData.pairAddress,
                        mint
                    ]);

                    // Re-sign market data to maintain integrity
                    if (result) {
                        const sig_market = signMarket(result);
                        await db.run(
                            `UPDATE tokens SET sig_market = $1 WHERE mint = $2`,
                            [sig_market, mint]
                        );
                    }

                    totalUpdated++;

                    // Broadcast price update via WebSocket
                    if (broadcast) {
                        broadcast.priceUpdate(mint, priceData);
                    }
                } catch (e) {
                    logger.error(`[PriceWorker] DB update failed for ${mint}:`, e.message);
                }
            }

            // Delay between batches to respect DexScreener rate limits
            if (batches.length > 1) {
                await new Promise(r => setTimeout(r, 500));
            }
        }

        const duration = Date.now() - startTime;
        logger.info(`[PriceWorker] Updated ${totalUpdated} prices in ${duration}ms (${batches.length} API calls)`);

        return { updated: totalUpdated, duration, batches: batches.length };

    } catch (e) {
        logger.error('[PriceWorker] Cycle failed:', e.message);
        return { updated: 0, error: e.message };
    }
}

/**
 * Get price for a single token (with cache)
 * Used by other services that need current price
 */
async function getPrice(mint) {
    // Check cache first
    const cached = getCachedPrice(mint, 60000); // 1 min cache
    if (cached) return cached;

    // Fetch single token
    try {
        const response = await fetch(`${DEXSCREENER_SINGLE_URL}/${mint}`);
        if (!response.ok) return null;

        const data = await response.json();
        const pairs = data.pairs || [];
        if (pairs.length === 0) return null;

        const bestPair = pairs.reduce((best, p) =>
            (p.liquidity?.usd || 0) > (best.liquidity?.usd || 0) ? p : best
        , pairs[0]);

        const totalLiquidity = pairs.reduce((sum, p) => sum + (p.liquidity?.usd || 0), 0);
        const now = Date.now();

        // Validate external data before storing
        const rawData = {
            priceUsd: parseFloat(bestPair.priceUsd) || 0,
            mcap: bestPair.marketCap || bestPair.fdv || 0,
            liquidity: totalLiquidity,
            volume24h: parseFloat(bestPair.volume?.h24) || 0,
            change24h: parseFloat(bestPair.priceChange?.h24) || 0,
            change1h: parseFloat(bestPair.priceChange?.h1) || 0,
            change5m: parseFloat(bestPair.priceChange?.m5) || 0,
            pairAddress: bestPair.pairAddress,
            source: 'dexscreener',
            timestamp: now
        };

        const priceData = validatePriceData(rawData);
        if (!priceData) {
            logger.warn(`[PriceWorker] Rejected invalid price data for ${mint}`);
            return null;
        }

        // Cache it (with LRU eviction)
        evictOldestPriceEntries();
        priceCache.set(mint, { data: priceData, timestamp: now });

        return priceData;
    } catch (e) {
        logger.debug(`[PriceWorker] Single fetch failed for ${mint}: ${e.message}`);
        return null;
    }
}

/**
 * Initialize and start the price worker
 */
function startPriceWorker(deps) {
    const { db, broadcast } = deps;

    logger.info('[PriceWorker] Starting unified price service');

    // Initial run after 5 seconds
    setTimeout(() => runPriceUpdateCycle(db, broadcast), 5000);

    // Main cycle: every 30 seconds
    // (Individual tokens refresh based on their tier within this cycle)
    const interval = setInterval(() => {
        runPriceUpdateCycle(db, broadcast);
    }, 30 * 1000);

    return {
        stop: () => clearInterval(interval),
        getPrice,
        getCachedPrice,
        fetchBatchPrices,
        runNow: () => runPriceUpdateCycle(db, broadcast)
    };
}

module.exports = {
    startPriceWorker,
    getPrice,
    getCachedPrice,
    fetchBatchPrices,
    runPriceUpdateCycle,
    TIERS
};
