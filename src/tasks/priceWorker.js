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
const { getClient: getRedisClient } = require('../services/redis');

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

/**
 * Fetch prices for multiple tokens in one API call
 * @param {string[]} mints - Array of token mint addresses (max 30)
 * @returns {Map<string, Object>} Map of mint -> price data
 */
async function fetchBatchPrices(mints) {
    if (!mints || mints.length === 0) return new Map();

    // DexScreener batch: comma-separated addresses
    const batchMints = mints.slice(0, BATCH_SIZE);
    const url = `${DEXSCREENER_BATCH_URL}/${batchMints.join(',')}`;

    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10000);

        const response = await fetch(url, { signal: controller.signal });
        clearTimeout(timeout);

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
                results.set(mint, {
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
                });
            } else {
                // Add liquidity from additional pairs
                const existing = results.get(mint);
                existing.liquidity += pair.liquidity?.usd || 0;
            }
        }

        // Update cache
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
                    await db.run(`
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

                    totalUpdated++;

                    // Broadcast price update via WebSocket
                    if (broadcast) {
                        broadcast.priceUpdate(mint, priceData);
                    }
                } catch (e) {
                    logger.error(`[PriceWorker] DB update failed for ${mint}:`, e.message);
                }
            }

            // Small delay between batches to be nice to DexScreener
            if (batches.length > 1) {
                await new Promise(r => setTimeout(r, 200));
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

        const priceData = {
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

        // Cache it
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
