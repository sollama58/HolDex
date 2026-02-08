const axios = require('axios');
const logger = require('../services/logger');
const { broadcastTokenUpdate } = require('../services/socket');
const { getHolderCountFromRPC } = require('../services/solana');
const config = require('../config/env');
const ignitionService = require('../services/ignitionService');

let isRunning = false;

// Ignition check interval (24 hours)
const IGNITION_CHECK_INTERVAL = 24 * 60 * 60 * 1000;

// GeckoTerminal rate limiting (~30 req/min free tier)
// With 1 call/token for most tokens, we can process ~28 tokens/min
let geckoRateLimitedUntil = 0;
const GECKO_CALL_DELAY_MS = 2200; // ~27 req/min, safely under limit

// ============================================
// GECKO TERMINAL API
// ============================================

async function fetchGeckoTerminalData(mintAddress) {
    if (Date.now() < geckoRateLimitedUntil) return null;
    try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mintAddress}/pools?page=1`;
        const response = await axios.get(url, { timeout: 5000 });
        if (!response.data || !response.data.data) return null;
        return response.data.data;
    } catch (e) {
        if (e.response && e.response.status === 429) {
            const retryAfter = parseInt(e.response.headers['retry-after'] || '10', 10);
            geckoRateLimitedUntil = Date.now() + (retryAfter + 2) * 1000;
            logger.debug(`[MetadataUpdater] GeckoTerminal 429 (pools), backing off ${retryAfter}s`);
        }
        return null;
    }
}

async function fetchTokenDetails(mintAddress) {
    if (Date.now() < geckoRateLimitedUntil) return null;
    try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mintAddress}`;
        const response = await axios.get(url, { timeout: 5000 });
        if (!response.data || !response.data.data) return null;
        return response.data.data;
    } catch (e) {
        if (e.response && e.response.status === 429) {
            const retryAfter = parseInt(e.response.headers['retry-after'] || '10', 10);
            geckoRateLimitedUntil = Date.now() + (retryAfter + 2) * 1000;
            logger.debug(`[MetadataUpdater] GeckoTerminal 429 (details), backing off ${retryAfter}s`);
        }
        return null;
    }
}

// ============================================
// TOKEN PROCESSING
// ============================================

async function processSingleToken(db, t, now) {
    try {
        // Smart API call selection:
        // - fetchTokenDetails (1 call): holder count, FDV, metadata — the core value
        // - fetchGeckoTerminalData (1 call): pool timestamps, pool table upserts
        //   Only needed when token has no creation timestamp (first-time pool discovery).
        //   PriceWorker already handles price/volume/liquidity from Jupiter/Raydium.
        const needsPoolDiscovery = !t.timestamp || parseInt(t.timestamp) === 0;

        let poolsData = null;
        let tokenDetails = null;
        let apiCallCount = 0;

        if (needsPoolDiscovery) {
            // New token: need both calls (parallel)
            [poolsData, tokenDetails] = await Promise.all([
                fetchGeckoTerminalData(t.mint),
                fetchTokenDetails(t.mint)
            ]);
            apiCallCount = 2;
        } else {
            // Existing token: only need details for holder count + FDV
            tokenDetails = await fetchTokenDetails(t.mint);
            apiCallCount = 1;
        }

        // Track whether we received any API data (for updated_at logic)
        const gotApiData = poolsData !== null || tokenDetails !== null;

        // --- 0. CHECK FOR PLACEHOLDER METADATA AND UPDATE IF POSSIBLE ---
        const placeholderNames = ['Unknown', 'New Discovery', '', null, undefined];
        const placeholderSymbols = ['UNK', 'UNKNOWN', 'NEW', '', null, undefined];
        const hasPlaceholderName = placeholderNames.includes(t.name);
        const hasPlaceholderSymbol = placeholderSymbols.includes(t.symbol);
        const hasNoImage = !t.image || t.image === '';

        if ((hasPlaceholderName || hasPlaceholderSymbol || hasNoImage) && tokenDetails && tokenDetails.attributes) {
            const attr = tokenDetails.attributes;
            const newName = attr.name;
            const newSymbol = attr.symbol;
            const newImage = attr.image_url;

            const hasRealName = newName && !placeholderNames.includes(newName);
            const hasRealSymbol = newSymbol && !placeholderSymbols.includes(newSymbol);

            if (hasRealName || hasRealSymbol || newImage) {
                const metaUpdates = [];
                const metaParams = [];
                let metaIdx = 1;

                if (hasPlaceholderName && hasRealName) {
                    metaUpdates.push(`name = $${metaIdx++}`);
                    metaParams.push(newName);
                }
                if (hasPlaceholderSymbol && hasRealSymbol) {
                    metaUpdates.push(`symbol = $${metaIdx++}`);
                    metaParams.push(newSymbol);
                }
                if (hasNoImage && newImage) {
                    metaUpdates.push(`image = $${metaIdx++}`);
                    metaParams.push(newImage);
                }

                if (metaUpdates.length > 0) {
                    metaParams.push(t.mint);
                    await db.run(
                        `UPDATE tokens SET ${metaUpdates.join(', ')}, updated_at = NOW() WHERE mint = $${metaIdx}`,
                        metaParams
                    );
                    logger.info(`🔄 [MetadataUpdater] Fixed placeholder metadata for ${t.mint.slice(0, 8)}: ${newName || t.name} (${newSymbol || t.symbol})`);
                }
            }
        }

        // --- 1. HOLDER COUNT LOGIC ---
        let holderCount = t.holders || 0;
        let foundNewData = false;
        let didCheckRpc = false;

        // Strategy A: GeckoTerminal (free)
        if (tokenDetails && tokenDetails.attributes) {
            if (tokenDetails.attributes.holder_count || tokenDetails.attributes.holders_count) {
                const geckoHolders = parseInt(tokenDetails.attributes.holder_count || tokenDetails.attributes.holders_count);
                if (geckoHolders > 0) {
                    holderCount = geckoHolders;
                    foundNewData = true;
                }
            }
        }

        // Strategy B: RPC Direct Check (expensive - limited to once per 24h, env-gated)
        const lastCheck = parseInt(t.last_holder_check || 0);
        const msSinceCheck = now - lastCheck;
        const ONE_DAY_MS = 24 * 60 * 60 * 1000;

        if (config.ENABLE_RPC_HOLDER_CHECK && !foundNewData && msSinceCheck > ONE_DAY_MS) {
            try {
                const rpcHolders = await getHolderCountFromRPC(t.mint);
                if (rpcHolders > 0) {
                    holderCount = rpcHolders;
                }
                didCheckRpc = true;
            } catch (_e) {
                // Ignore RPC errors, try again next cycle
            }
        }

        // --- 2. POOL DATA (only when pool discovery needed) ---
        let totalVolume24h = 0;
        let totalLiquidity = 0;
        let bestPrice = 0;
        let bestChange24h = null;
        let bestChange1h = null;
        let bestChange5m = null;
        let maxLiquidity = -1;
        let earliestPoolTime = null;

        if (poolsData && poolsData.length > 0) {
            for (const poolData of poolsData) {
                const attr = poolData.attributes;
                const rel = poolData.relationships;

                if (attr.pool_created_at) {
                    const createdAt = new Date(attr.pool_created_at).getTime();
                    if (!earliestPoolTime || createdAt < earliestPoolTime) {
                        earliestPoolTime = createdAt;
                    }
                }

                const address = attr.address;
                const dexId = rel?.dex?.data?.id || 'unknown';
                const price = parseFloat(attr.base_token_price_usd || 0);
                const liqUsd = Math.floor(parseFloat(attr.reserve_in_usd || 0));
                const vol24h = Math.floor(parseFloat(attr.volume_usd?.h24 || 0));

                let tokenA = rel?.base_token?.data?.id || null;
                let tokenB = rel?.quote_token?.data?.id || null;

                if (tokenA && tokenA.includes('solana_')) tokenA = tokenA.replace('solana_', '');
                if (tokenB && tokenB.includes('solana_')) tokenB = tokenB.replace('solana_', '');

                if (!tokenA) tokenA = t.mint;
                if (!tokenB) tokenB = 'So11111111111111111111111111111111111111112';

                totalVolume24h += vol24h;
                totalLiquidity += liqUsd;

                if (liqUsd > maxLiquidity) {
                    maxLiquidity = liqUsd;
                    bestPrice = price;

                    const parseChange = (val) => (val !== undefined && val !== null) ? parseFloat(val) : null;
                    bestChange24h = parseChange(attr.price_change_percentage?.h24);
                    bestChange1h = parseChange(attr.price_change_percentage?.h1);
                    bestChange5m = parseChange(attr.price_change_percentage?.m5);
                }

                try {
                    await db.run(`
                        INSERT INTO pools (
                            address, mint, dex, price_usd, liquidity_usd, volume_24h, created_at, token_a, token_b
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT(address) DO UPDATE SET
                            price_usd = CASE WHEN EXCLUDED.price_usd > 0 THEN EXCLUDED.price_usd ELSE pools.price_usd END,
                            liquidity_usd = CASE WHEN EXCLUDED.liquidity_usd > 0 THEN EXCLUDED.liquidity_usd ELSE pools.liquidity_usd END,
                            volume_24h = CASE WHEN EXCLUDED.volume_24h > 0 THEN EXCLUDED.volume_24h ELSE pools.volume_24h END
                    `, [address, t.mint, dexId, price, liqUsd, vol24h, now, tokenA, tokenB]);
                } catch (poolErr) {
                    if (!poolErr.message.includes('foreign key')) {
                        throw poolErr;
                    }
                }
            }
        }

        // --- 3. MARKET CAP LOGIC ---
        let marketCap = 0;

        // A. Try direct FDV from Gecko
        if (tokenDetails && tokenDetails.attributes) {
            marketCap = Math.floor(parseFloat(tokenDetails.attributes.fdv_usd || tokenDetails.attributes.market_cap_usd || 0));
        }

        // B. Fallback: Manual Calculation (only if we had pool data with a price)
        if (marketCap === 0 && bestPrice > 0) {
            const decimals = t.decimals || 9;
            let rawSupply = parseFloat(t.supply || '0');

            if (rawSupply === 0 && tokenDetails?.attributes?.total_supply) {
                rawSupply = parseFloat(tokenDetails.attributes.total_supply);
            }
            if (rawSupply === 0) rawSupply = 1000000000 * Math.pow(10, decimals);

            const divisor = Math.pow(10, decimals);
            const supply = rawSupply / divisor;
            marketCap = Math.floor(supply * bestPrice);
        }

        // Clear large objects
        poolsData = null;
        tokenDetails = null;

        // --- 4. CONSTRUCT QUERY ---
        const finalParams = [];
        const updateParts = [];
        let idx = 1;

        if (totalVolume24h > 0) {
            updateParts.push(`volume24h = $${idx++}`); finalParams.push(Math.floor(totalVolume24h));
        }
        if (marketCap > 0) {
            updateParts.push(`marketCap = $${idx++}`); finalParams.push(Math.floor(marketCap));
        }
        if (bestPrice > 0) {
            updateParts.push(`priceUsd = $${idx++}`); finalParams.push(bestPrice);
        }
        if (totalLiquidity > 0) {
            updateParts.push(`liquidity = $${idx++}`); finalParams.push(Math.floor(totalLiquidity));
        }

        if (bestPrice > 0) {
            if (bestChange24h !== null) { updateParts.push(`change24h = $${idx++}`); finalParams.push(bestChange24h); }
            if (bestChange1h !== null) { updateParts.push(`change1h = $${idx++}`); finalParams.push(bestChange1h); }
            if (bestChange5m !== null) { updateParts.push(`change5m = $${idx++}`); finalParams.push(bestChange5m); }
        }

        if (earliestPoolTime && earliestPoolTime > 0) {
            const currentTs = parseInt(t.timestamp) || 0;
            if (currentTs === 0 || earliestPoolTime < currentTs) {
                updateParts.push(`timestamp = $${idx++}`); finalParams.push(earliestPoolTime);
            }
        }

        if ((foundNewData || didCheckRpc) && holderCount > 0) {
            updateParts.push(`holders = $${idx++}`);
            finalParams.push(Math.floor(holderCount));

            updateParts.push(`last_holder_check = $${idx++}`);
            finalParams.push(now);

            const today = Math.floor(now / (24 * 60 * 60 * 1000)) * (24 * 60 * 60 * 1000);
            await db.run(`
                INSERT INTO holders_history (mint, count, timestamp)
                VALUES ($1, $2, $3)
                ON CONFLICT(mint, timestamp) DO UPDATE SET count = EXCLUDED.count
            `, [t.mint, Math.floor(holderCount), today]);
        } else if (foundNewData || didCheckRpc) {
            updateParts.push(`last_holder_check = $${idx++}`);
            finalParams.push(now);
        }

        // Only bump updated_at if we actually received API data.
        // If Gecko was rate-limited (returned null), leave updated_at alone
        // so this token stays near the front of the queue for retry.
        if (gotApiData) {
            updateParts.push(`updated_at = CURRENT_TIMESTAMP`);
        }

        if (updateParts.length > 0) {
            const finalQuery = `UPDATE tokens SET ${updateParts.join(', ')} WHERE mint = $${idx}`;
            finalParams.push(t.mint);
            await db.run(finalQuery, finalParams);
        }

        // --- IGNITION INTEGRATION ---
        const ignitionLastCheck = parseInt(t.ignition_last_check || 0);
        if (ignitionService.isConfigured() && (now - ignitionLastCheck > IGNITION_CHECK_INTERVAL)) {
            try {
                const ignitionData = await ignitionService.lookupToken(t.mint);
                if (ignitionData) {
                    await db.run(`
                        UPDATE tokens SET
                            ignition_registered = $1,
                            ignition_type = $2,
                            ignition_fee_share_pct = $3,
                            ignition_active = $4,
                            ignition_last_check = $5
                        WHERE mint = $6
                    `, [
                        ignitionData.registered,
                        ignitionData.type || null,
                        ignitionData.feeSharePercent || null,
                        ignitionData.active !== undefined ? ignitionData.active : null,
                        now,
                        t.mint
                    ]);

                    if (ignitionData.registered) {
                        logger.info(`🔥 [Ignition] ${t.mint.slice(0, 8)} registered as ${ignitionData.type}${ignitionData.feeSharePercent ? ` (${ignitionData.feeSharePercent}% share)` : ''}`);
                    }
                }
            } catch (ignitionErr) {
                logger.debug(`[Ignition] Check failed for ${t.mint.slice(0, 8)}: ${ignitionErr.message}`);
            }
        }

        // Broadcast current state
        if (totalLiquidity > 0) {
            broadcastTokenUpdate(t.mint, {
                priceUsd: bestPrice,
                marketCap: marketCap,
                volume24h: totalVolume24h,
                change1h: bestChange1h,
                change24h: bestChange24h,
                holders: holderCount,
                updatedAt: now
            });
        } else if (holderCount > 0) {
            broadcastTokenUpdate(t.mint, {
                holders: holderCount,
                updatedAt: now
            });
        }

        return apiCallCount;

    } catch (err) {
        logger.error(`Token Update Failed [${t.mint}]: ${err.message}`);
        return 0;
    }
}

// ============================================
// MAIN CYCLE
// ============================================

async function updateMetadata(deps) {
    if (isRunning) return;
    isRunning = true;
    const { db } = deps;
    const now = Date.now();

    try {
        // Tiered token selection:
        //   Verified tokens (hascommunityupdate): refresh every 2 minutes
        //   Active tokens (volume > 1000):        refresh every 10 minutes
        //   All other tokens:                     refresh every 30 minutes
        //
        // Verified tokens get priority in ordering so they're always processed first.
        // With ~28 tokens/min throughput (1 Gecko call per token):
        //   - 50 verified tokens cycle every ~2-4 min
        //   - 100 active tokens cycle every ~10-15 min
        //   - 350 dormant tokens cycle every ~30-45 min
        let tokens = await db.all(`
            SELECT mint, name, symbol, image, supply, decimals, holders,
                   last_holder_check, timestamp, ignition_last_check
            FROM tokens
            WHERE updated_at IS NULL
               OR (hascommunityupdate = TRUE AND updated_at < NOW() - INTERVAL '2 minutes')
               OR (COALESCE(volume24h, 0) > 1000 AND updated_at < NOW() - INTERVAL '10 minutes')
               OR updated_at < NOW() - INTERVAL '30 minutes'
            ORDER BY
                CASE WHEN hascommunityupdate = TRUE THEN 0 ELSE 1 END,
                updated_at ASC NULLS FIRST
            LIMIT 50
        `);

        if (tokens && tokens.length > 0) {
            logger.info(`[MetadataUpdater] Processing ${tokens.length} due tokens`);

            let totalCalls = 0;
            let processed = 0;

            for (const t of tokens) {
                // If GeckoTerminal is rate limited, wait for the lockout to expire
                // rather than burning through tokens with no data
                if (Date.now() < geckoRateLimitedUntil) {
                    const waitTime = geckoRateLimitedUntil - Date.now();
                    logger.debug(`[MetadataUpdater] Rate limited, waiting ${Math.ceil(waitTime / 1000)}s`);
                    await new Promise(r => setTimeout(r, waitTime));
                }

                const callsMade = await processSingleToken(db, t, now);
                totalCalls += callsMade;
                processed++;

                // Rate limit: delay proportional to API calls made
                // 1 call → 2.2s, 2 calls → 4.4s, 0 calls (rate limited) → no delay
                if (callsMade > 0) {
                    await new Promise(r => setTimeout(r, callsMade * GECKO_CALL_DELAY_MS));
                }
            }

            logger.info(`[MetadataUpdater] Done: ${processed} tokens, ${totalCalls} API calls`);
        }

        tokens = null;
        if (global.gc) {
            try { global.gc(); } catch (_e) { /* ignore */ }
        }

    } catch (_e) {
        logger.error(`Metadata Cycle Error: ${_e.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    // Check every 5 seconds for due tokens; isRunning mutex prevents overlap.
    // Cycles run back-to-back when there are tokens to process, with only
    // a 5s gap between batches of 50.
    setInterval(() => updateMetadata(deps), 5000);
    setTimeout(() => updateMetadata(deps), 5000);
}

module.exports = { start };
