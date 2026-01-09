const { PublicKey } = require('@solana/web3.js');
const { getDB, enableIndexing, aggregateAndSaveToken } = require('./database');
const { findPoolsOnChain } = require('./pool_finder');
const { fetchTokenMetadata } = require('../utils/metaplex');
const { getSolanaConnection } = require('./solana');
const { enqueueTokenUpdate } = require('./queue');
const { snapshotPools } = require('../indexer/tasks/snapshotter');
const logger = require('./logger');
const axios = require('axios');
const tokenSearch = require('./tokenSearch');

const solanaConnection = getSolanaConnection();

/**
 * Fetch initial market data for a token
 * Uses Jupiter + Raydium (primary) with GeckoTerminal fallback
 */
async function fetchInitialMarketData(mint) {
    // Try Jupiter + Raydium first (scalable, no rate limits)
    const data = await tokenSearch.fetchInitialMarketData(mint);
    if (data && data.priceUsd > 0) {
        return data;
    }

    // Fallback to GeckoTerminal if Jupiter fails
    try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mint}`;
        const res = await axios.get(url, { timeout: 3000 });
        const attrs = res.data.data.attributes;
        return {
            priceUsd: parseFloat(attrs.price_usd || 0),
            volume24h: Math.floor(parseFloat(attrs.volume_usd?.h24 || 0)),
            change24h: parseFloat(attrs.price_change_percentage?.h24 || 0),
            change1h: parseFloat(attrs.price_change_percentage?.h1 || 0),
            change5m: parseFloat(attrs.price_change_percentage?.m5 || 0),
            marketCap: Math.floor(parseFloat(attrs.fdv_usd || attrs.market_cap_usd || 0)),
            liquidity: Math.floor(parseFloat(attrs.total_reserve_in_usd || 0))
        };
    } catch (_e) {
        return null;
    }
}

/**
 * Search for tokens matching a query string
 * Uses Jupiter token list (primary) with GeckoTerminal fallback
 * Scalable solution - no rate limit issues
 *
 * @param {string} query - Search term (name or symbol)
 * @param {number} limit - Max results to return
 * @returns {Promise<Array<{mint: string, name: string, symbol: string, image: string, priceUsd: number, volume24h: number, marketCap: number}>>}
 */
async function searchGeckoTerminal(query, limit = 10) {
    // Try Jupiter/Helius search first (scalable)
    try {
        const results = await tokenSearch.searchTokens(query, limit);
        if (results.length > 0) {
            logger.info(`🔍 [TokenSearch] Found ${results.length} results for "${query}" via Jupiter`);
            return results.map(r => ({
                mint: r.mint,
                name: r.name,
                symbol: r.symbol,
                image: r.image,
                priceUsd: r.priceUsd || 0,
                volume24h: 0, // Will be fetched during indexing
                marketCap: 0, // Will be fetched during indexing
                liquidity: 0  // Will be fetched during indexing
            }));
        }
    } catch (e) {
        logger.debug(`[TokenSearch] Jupiter search failed, trying GeckoTerminal: ${e.message}`);
    }

    // Fallback to GeckoTerminal
    try {
        const url = `https://api.geckoterminal.com/api/v2/search/pools?query=${encodeURIComponent(query)}&network=solana&page=1`;
        const res = await axios.get(url, { timeout: 5000 });

        const results = [];
        const seenMints = new Set();
        const queryLower = query.toLowerCase();

        if (res.data?.data && Array.isArray(res.data.data)) {
            for (const pool of res.data.data) {
                if (results.length >= limit) break;

                const attrs = pool.attributes;
                const relationships = pool.relationships;

                // Get base token info (the non-SOL/USDC token)
                let baseTokenId = relationships?.base_token?.data?.id;
                if (!baseTokenId) continue;

                // Extract mint from token ID (format: "solana_<mint>")
                const mint = baseTokenId.replace('solana_', '');

                // Skip if already seen or invalid
                if (seenMints.has(mint) || mint.length < 30) continue;
                seenMints.add(mint);

                // Get token details from included data
                const includedTokens = res.data.included || [];
                const tokenData = includedTokens.find(t => t.id === baseTokenId);

                if (tokenData) {
                    const tokenAttrs = tokenData.attributes;
                    const tokenName = tokenAttrs.name;
                    const tokenSymbol = tokenAttrs.symbol;

                    // Skip tokens without real metadata
                    if (!tokenName || !tokenSymbol || tokenName === 'Unknown' || tokenSymbol === 'UNK') {
                        continue;
                    }

                    // Prioritize tokens that actually match the query in name or symbol
                    const nameLower = tokenName.toLowerCase();
                    const symbolLower = tokenSymbol.toLowerCase();
                    const matchesQuery = nameLower.includes(queryLower) || symbolLower.includes(queryLower);

                    const tokenResult = {
                        mint,
                        name: tokenName,
                        symbol: tokenSymbol,
                        image: tokenAttrs.image_url || null,
                        priceUsd: parseFloat(tokenAttrs.price_usd || attrs.base_token_price_usd || 0),
                        volume24h: Math.floor(parseFloat(attrs.volume_usd?.h24 || 0)),
                        marketCap: Math.floor(parseFloat(tokenAttrs.fdv_usd || attrs.fdv_usd || 0)),
                        liquidity: Math.floor(parseFloat(attrs.reserve_in_usd || 0)),
                        _matchScore: matchesQuery ? 1 : 0
                    };

                    results.push(tokenResult);
                }
            }
        }

        // Sort by match score then by volume
        results.sort((a, b) => {
            if (a._matchScore !== b._matchScore) return b._matchScore - a._matchScore;
            return (b.volume24h || 0) - (a.volume24h || 0);
        });

        results.forEach(r => delete r._matchScore);

        logger.info(`🔍 [GeckoSearch] Found ${results.length} results for "${query}" (fallback)`);
        return results;
    } catch (e) {
        logger.warn(`⚠️ [TokenSearch] All search methods failed for "${query}": ${e.message}`);
        return [];
    }
}

/**
 * Quick index a token from GeckoTerminal search results (lighter than full indexTokenOnChain)
 * Used when backfilling search results - skips pool discovery for speed
 * @param {object} tokenData - Token data from GeckoTerminal search
 * @returns {Promise<boolean>} - Success status
 */
async function quickIndexFromGecko(tokenData) {
    const db = getDB();
    const { mint, name, symbol, image, priceUsd, volume24h, marketCap, liquidity } = tokenData;

    try {
        // Validate mint address - REQUIRED
        if (!mint || typeof mint !== 'string' || mint.length < 32) {
            logger.warn(`⚠️ [QuickIndex] Skipping token - invalid mint address: ${mint}`);
            return false;
        }

        // Validate metadata - only add tokens with real names/symbols
        const hasRealName = name && name !== 'Unknown' && name !== 'New Discovery' && !name.startsWith('Token ') && name.length > 0;
        const hasRealSymbol = symbol && symbol !== 'UNK' && symbol !== 'UNKNOWN' && symbol !== 'NEW' && symbol.length > 0;

        if (!hasRealName || !hasRealSymbol) {
            logger.warn(`⚠️ [QuickIndex] Skipping ${mint.slice(0, 8)} - invalid metadata (name: ${name}, symbol: ${symbol})`);
            return false;
        }

        // Check if already exists
        const existing = await db.get(
            'SELECT mint, priceUsd, volume24h, marketCap, liquidity FROM tokens WHERE mint = $1',
            [mint]
        );

        if (existing) {
            // Token exists - update any missing market data fields
            const hasNewData = priceUsd > 0 || volume24h > 0 || marketCap > 0 || liquidity > 0;

            // Check which fields need updating (are currently blank/zero)
            const needsPriceUpdate = !existing.priceusd || existing.priceusd === 0;
            const needsVolumeUpdate = !existing.volume24h || existing.volume24h === 0;
            const needsMcapUpdate = !existing.marketcap || existing.marketcap === 0;
            const needsLiquidityUpdate = !existing.liquidity || existing.liquidity === 0;
            const needsAnyUpdate = (needsPriceUpdate || needsVolumeUpdate || needsMcapUpdate || needsLiquidityUpdate) && hasNewData;

            if (needsAnyUpdate) {
                // Update existing token with market data from GeckoTerminal
                // Only update fields that are currently blank/zero
                await db.run(`
                    UPDATE tokens SET
                        priceUsd = CASE WHEN COALESCE(priceUsd, 0) = 0 AND $2 > 0 THEN $2 ELSE priceUsd END,
                        volume24h = CASE WHEN COALESCE(volume24h, 0) = 0 AND $3 > 0 THEN $3 ELSE volume24h END,
                        marketCap = CASE WHEN COALESCE(marketCap, 0) = 0 AND $4 > 0 THEN $4 ELSE marketCap END,
                        liquidity = CASE WHEN COALESCE(liquidity, 0) = 0 AND $5 > 0 THEN $5 ELSE liquidity END,
                        image = COALESCE(NULLIF($6, ''), image),
                        updated_at = NOW()
                    WHERE mint = $1
                `, [mint, priceUsd, volume24h, marketCap, liquidity || 0, image]);

                logger.info(`🔄 [QuickIndex] Updated ${symbol} (${mint.slice(0, 8)}) with market data from GeckoTerminal`);

                // Queue for full indexing
                enqueueTokenUpdate(mint).catch(() => {});
                return true;
            }

            logger.debug(`[QuickIndex] Token ${symbol} (${mint.slice(0, 8)}) already has market data`);
            return true; // Token exists, that's success for search purposes
        }

        // Insert new token with data from GeckoTerminal
        // Use DO UPDATE to fill in market data if token exists but has blanks
        await db.run(`
            INSERT INTO tokens (mint, name, symbol, image, priceUsd, volume24h, marketCap, liquidity, timestamp, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
            ON CONFLICT(mint) DO UPDATE SET
                name = CASE WHEN tokens.name IS NULL OR tokens.name = '' OR tokens.name = 'Unknown' THEN EXCLUDED.name ELSE tokens.name END,
                symbol = CASE WHEN tokens.symbol IS NULL OR tokens.symbol = '' OR tokens.symbol = 'UNK' THEN EXCLUDED.symbol ELSE tokens.symbol END,
                image = CASE WHEN tokens.image IS NULL OR tokens.image = '' THEN EXCLUDED.image ELSE tokens.image END,
                priceUsd = CASE WHEN COALESCE(tokens.priceUsd, 0) = 0 AND EXCLUDED.priceUsd > 0 THEN EXCLUDED.priceUsd ELSE tokens.priceUsd END,
                volume24h = CASE WHEN COALESCE(tokens.volume24h, 0) = 0 AND EXCLUDED.volume24h > 0 THEN EXCLUDED.volume24h ELSE tokens.volume24h END,
                marketCap = CASE WHEN COALESCE(tokens.marketCap, 0) = 0 AND EXCLUDED.marketCap > 0 THEN EXCLUDED.marketCap ELSE tokens.marketCap END,
                liquidity = CASE WHEN COALESCE(tokens.liquidity, 0) = 0 AND EXCLUDED.liquidity > 0 THEN EXCLUDED.liquidity ELSE tokens.liquidity END,
                updated_at = NOW()
        `, [mint, name, symbol, image, priceUsd, volume24h, marketCap, liquidity || 0, Date.now()]);

        logger.info(`⚡ [QuickIndex] Added/Updated ${symbol} (${mint.slice(0, 8)}) from GeckoTerminal`);

        // Queue for full indexing in background (pools, supply, etc.)
        enqueueTokenUpdate(mint).catch(() => {});

        return true;
    } catch (e) {
        logger.warn(`⚠️ [QuickIndex] Failed for ${mint.slice(0, 8)}: ${e.message}`);
        return false;
    }
}

async function indexTokenOnChain(mint, retryCount = 0) {
    try {
        // Validate mint address - REQUIRED
        if (!mint || typeof mint !== 'string' || mint.length < 32) {
            logger.warn(`⚠️ [Indexer] Invalid mint address: ${mint}`);
            return { name: null, ticker: null, pairs: [], skipped: true, reason: 'invalid_mint' };
        }

        const db = getDB();
        logger.info(`🔍 [Indexer] Starting indexing for ${mint.slice(0, 8)}...${retryCount > 0 ? ` (retry ${retryCount})` : ''}`);

        let meta = await fetchTokenMetadata(mint);

        // For brand new tokens, metadata might not be indexed yet - retry with delay
        const MAX_RETRIES = 3;
        const RETRY_DELAY_MS = 2000;

        // Check if we got real metadata (not placeholder)
        const hasRealName = meta && meta.name && meta.name !== 'Unknown' && meta.name !== 'New Discovery' && meta.name.length > 0;
        const hasRealSymbol = meta && meta.symbol && meta.symbol !== 'UNK' && meta.symbol !== 'UNKNOWN';

        if ((!hasRealName || !hasRealSymbol) && retryCount < MAX_RETRIES) {
            logger.info(`⏳ [Indexer] Metadata not ready for ${mint.slice(0, 8)} (name: ${meta?.name}, symbol: ${meta?.symbol}), will retry in ${RETRY_DELAY_MS}ms...`);
            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
            return indexTokenOnChain(mint, retryCount + 1);
        }

        // If still no valid metadata after retries, DO NOT add token to database
        // We only want tokens with real metadata - no placeholders like "New Discovery"
        if (!hasRealName || !hasRealSymbol) {
            logger.warn(`⚠️ [Indexer] Skipping ${mint.slice(0, 8)} - no valid metadata after ${MAX_RETRIES} retries (name: ${meta?.name}, symbol: ${meta?.symbol})`);
            return { name: null, ticker: null, pairs: [], skipped: true, reason: 'no_metadata' };
        }

        logger.info(`📝 [Indexer] Metadata: ${meta.name} (${meta.symbol})`);

        let supply = '1000000000';
        let decimals = 9;
        try {
            const supplyInfo = await solanaConnection.getTokenSupply(new PublicKey(mint));
            supply = supplyInfo.value.amount;
            decimals = supplyInfo.value.decimals;
        } catch (e) {
            logger.warn(`⚠️ [Indexer] Failed to fetch supply for ${mint.slice(0, 8)}: ${e.message}`);
        }

        const marketData = await fetchInitialMarketData(mint);
        if (marketData) {
            logger.info(`💹 [Indexer] Market data: $${marketData.priceUsd} | Vol: $${marketData.volume24h}`);
        } else {
            logger.warn(`⚠️ [Indexer] No market data from GeckoTerminal for ${mint.slice(0, 8)}`);
        }

        const baseData = { name: meta.name, ticker: meta.symbol, image: meta?.image || null };
        const initialPrice = marketData?.priceUsd || 0;
        const initialVol = marketData?.volume24h || 0;
        const initialChange = marketData?.change24h || 0;
        const initialChange1h = marketData?.change1h || 0;
        const initialChange5m = marketData?.change5m || 0;
        const initialMcap = marketData?.marketCap || 0;
        const initialLiquidity = marketData?.liquidity || 0;

        // 1. CREATE TOKEN RECORD
        // FIX: Update market data on conflict, but only update identity if it's still "Unknown"
        // Identity fields (name, symbol, image) are signed with sig_identity
        // If already indexed with real data, preserve it. If still "Unknown", update it.
        // FIX: Only update market data if we have valid values (> 0), otherwise preserve existing
        try {
            await db.run(`
                INSERT INTO tokens (mint, name, symbol, image, supply, decimals, priceUsd, liquidity, marketCap, volume24h, change24h, change1h, change5m, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT(mint) DO UPDATE SET
                name = CASE
                    WHEN tokens.name IN ('Unknown', 'New Discovery', '') OR tokens.name IS NULL
                    THEN EXCLUDED.name
                    ELSE tokens.name
                END,
                symbol = CASE
                    WHEN tokens.symbol IN ('UNKNOWN', 'UNK', 'NEW', '') OR tokens.symbol IS NULL
                    THEN EXCLUDED.symbol
                    ELSE tokens.symbol
                END,
                image = CASE
                    WHEN tokens.image IS NULL OR tokens.image = ''
                    THEN EXCLUDED.image
                    ELSE tokens.image
                END,
                priceUsd = CASE WHEN EXCLUDED.priceUsd > 0 THEN EXCLUDED.priceUsd ELSE tokens.priceUsd END,
                liquidity = CASE WHEN EXCLUDED.liquidity > 0 THEN EXCLUDED.liquidity ELSE tokens.liquidity END,
                marketCap = CASE WHEN EXCLUDED.marketCap > 0 THEN EXCLUDED.marketCap ELSE tokens.marketCap END,
                volume24h = CASE WHEN EXCLUDED.volume24h > 0 THEN EXCLUDED.volume24h ELSE tokens.volume24h END,
                change24h = CASE WHEN EXCLUDED.priceUsd > 0 THEN EXCLUDED.change24h ELSE tokens.change24h END,
                change1h = CASE WHEN EXCLUDED.priceUsd > 0 THEN EXCLUDED.change1h ELSE tokens.change1h END,
                change5m = CASE WHEN EXCLUDED.priceUsd > 0 THEN EXCLUDED.change5m ELSE tokens.change5m END,
                updated_at = NOW()
            `, [
                mint, baseData.name, baseData.ticker, baseData.image, supply, decimals,
                initialPrice, initialLiquidity, initialMcap, initialVol, initialChange,
                initialChange1h, initialChange5m, Date.now()
            ]);
            logger.info(`💾 [Indexer] Token record created/updated for ${mint.slice(0, 8)}`);
        } catch (tokenErr) {
            logger.error(`❌ [Indexer] Failed to create token record for ${mint}: ${tokenErr.message}`);
            throw tokenErr; // Can't continue without token record
        }

        // Verify token exists before adding pools (foreign key constraint)
        const tokenExists = await db.get('SELECT mint FROM tokens WHERE mint = $1', [mint]);
        if (!tokenExists) {
            logger.error(`❌ [Indexer] Token record not found after insert for ${mint.slice(0, 8)}`);
            throw new Error('Token insert verification failed');
        }

        // 2. FIND POOLS
        const pools = await findPoolsOnChain(mint);
        const poolAddresses = [];

        logger.info(`🏊 [Indexer] Found ${pools.length} pool(s) for ${mint.slice(0, 8)}`);

        for (const pool of pools) {
            poolAddresses.push(pool.pairAddress);
            await enableIndexing(db, mint, {
                pairAddress: pool.pairAddress,
                dexId: pool.dexId,
                liquidity: pool.liquidity || { usd: 0 },
                volume: pool.volume || { h24: 0 },
                priceUsd: pool.priceUsd || 0,
                baseToken: pool.baseToken,
                quoteToken: pool.quoteToken,
                reserve_a: pool.reserve_a,
                reserve_b: pool.reserve_b
            });
        }

        await enqueueTokenUpdate(mint);
        if (poolAddresses.length > 0) {
            await snapshotPools(poolAddresses).catch(e => logger.error(`❌ [Indexer] Snapshot error: ${e.message}`));
            await aggregateAndSaveToken(db, mint);
            logger.info(`✅ [Indexer] Successfully indexed ${baseData.name} (${mint.slice(0, 8)})`);
        } else {
            logger.warn(`⚠️ [Indexer] No pools found for ${mint.slice(0, 8)} - token added but no price data`);
        }

        return { ...baseData, pairs: pools };
    } catch (error) {
        logger.error(`❌ [Indexer] CRITICAL ERROR indexing ${mint}: ${error.message}`);
        logger.error(error.stack);
        throw error; // Re-throw to let caller handle
    }
}

module.exports = { indexTokenOnChain, searchGeckoTerminal, quickIndexFromGecko, fetchInitialMarketData };
