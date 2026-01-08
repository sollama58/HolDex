/**
 * Price Worker - Unified Price Service
 *
 * Philosophy: Independent data sources, no DexScreener dependency
 * - Jupiter Price API V3 for token prices (free tier)
 * - Raydium API for pool/liquidity data (free)
 * - On-chain calculations via Helius RPC
 *
 * Features:
 * - Batch API: 100 tokens per Jupiter call
 * - Tiered refresh: Verified (1min), Active (5min), Dormant (30min)
 * - Zero external paid API dependencies
 * - WebSocket broadcast on price changes
 */

const { logger } = require('../services');
const { signMarket, signFull } = require('../utils/dataSignature');
const priceProvider = require('../services/priceProvider');

// Refresh tiers (in milliseconds)
const TIERS = {
    VERIFIED: 60 * 1000,      // 1 minute
    TOP_100: 60 * 1000,       // 1 minute
    ACTIVE: 5 * 60 * 1000,    // 5 minutes
    DORMANT: 30 * 60 * 1000   // 30 minutes
};

// Batch size limit (Jupiter allows 100)
const BATCH_SIZE = 30; // Keep at 30 to match Raydium rate limits

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

        // Batch fetch (30 tokens per API call to respect rate limits)
        let totalUpdated = 0;
        const batches = [];

        for (let i = 0; i < allToUpdate.length; i += BATCH_SIZE) {
            batches.push(allToUpdate.slice(i, i + BATCH_SIZE));
        }

        logger.info(`[PriceWorker] Updating ${allToUpdate.length} tokens in ${batches.length} batches`);

        for (const batch of batches) {
            // Use new price provider instead of DexScreener
            const prices = await priceProvider.fetchBatchPrices(batch);

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
                        priceData.source, // 'jupiter' or 'raydium'
                        priceData.timestamp.toString(),
                        priceData.pairAddress,
                        mint
                    ]);

                    // Re-sign market data AND sig_full to maintain integrity
                    if (result) {
                        const sig_market = signMarket(result);

                        // Fetch all signatures to recompute sig_full
                        const sigs = await db.get(`
                            SELECT sig_identity, sig_security, sig_lp, sig_supply,
                                   sig_kscore, sig_origin, chaos_nonce
                            FROM tokens WHERE mint = $1
                        `, [mint]);

                        if (sigs && sigs.chaos_nonce) {
                            const sig_full = signFull({
                                sig_identity: sigs.sig_identity,
                                sig_security: sigs.sig_security,
                                sig_lp: sigs.sig_lp,
                                sig_supply: sigs.sig_supply,
                                sig_kscore: sigs.sig_kscore,
                                sig_market: sig_market,
                                sig_origin: sigs.sig_origin
                            }, sigs.chaos_nonce);

                            await db.run(
                                `UPDATE tokens SET sig_market = $1, sig_full = $2 WHERE mint = $3`,
                                [sig_market, sig_full, mint]
                            );
                        } else {
                            // Fallback: just update sig_market
                            await db.run(
                                `UPDATE tokens SET sig_market = $1 WHERE mint = $2`,
                                [sig_market, mint]
                            );
                        }
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

            // Delay between batches to respect rate limits
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
    // Check cache first (from priceProvider)
    const cached = priceProvider.getCachedPrice(mint, 60000);
    if (cached) return cached;

    // Fetch single token price
    return await priceProvider.getPrice(mint);
}

/**
 * Get cached price (quick lookup, no API call)
 */
function getCachedPrice(mint, maxAge = 60000) {
    return priceProvider.getCachedPrice(mint, maxAge);
}

/**
 * Fetch batch prices (delegated to priceProvider)
 */
async function fetchBatchPrices(mints) {
    return await priceProvider.fetchBatchPrices(mints);
}

/**
 * Initialize and start the price worker
 */
function startPriceWorker(deps) {
    const { db, broadcast } = deps;

    logger.info('[PriceWorker] Starting unified price service (Jupiter + Raydium)');

    // Initial run after 5 seconds
    setTimeout(() => runPriceUpdateCycle(db, broadcast), 5000);

    // Main cycle: every 30 seconds
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
