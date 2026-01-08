/**
 * Unified Price Provider - DexScreener-Free Implementation
 *
 * Data Sources (Priority Order):
 * 1. Jupiter Price API V3 - Primary price source (free tier available)
 * 2. Helius RPC - On-chain pool data, token accounts
 * 3. On-chain calculation - Vault balance ratios for liquidity
 * 4. CoinGecko - SOL/USD price (cached)
 *
 * Philosophy: Independent, verifiable, on-chain data
 */

const config = require('../config/env');
const logger = require('./logger');
const { getRedis } = require('./redis');

// ============================================
// CONFIGURATION
// ============================================

// Jupiter Price API V3 - uses api.jup.ag with API key
// Set JUPITER_API_KEY in environment (required for api.jup.ag)
// Get your API key at: https://portal.jup.ag/
const JUPITER_API_KEY = config.JUPITER_API_KEY || process.env.JUPITER_API_KEY;
const JUPITER_PRICE_URL = 'https://api.jup.ag/price/v3';
const JUPITER_BATCH_SIZE = 50; // Jupiter V3 allows up to 50 tokens per request

// Helius RPC for on-chain data
const HELIUS_RPC_URL = 'https://mainnet.helius-rpc.com';

// Known token addresses
const SOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const USDT_MINT = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB';

// Raydium AMM Program IDs for pool identification
const RAYDIUM_AMM_V4 = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8';
const RAYDIUM_CLMM = 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK';
const ORCA_WHIRLPOOL = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc';

// Cache settings
const PRICE_CACHE_TTL = 60000;      // 1 minute for prices
const LIQUIDITY_CACHE_TTL = 300000; // 5 minutes for liquidity

// In-memory cache
const priceCache = new Map();
const liquidityCache = new Map();

// SOL price cache (shared with priceService.js pattern)
let solPriceCache = { price: 0, timestamp: 0, lastAttempt: 0 };
const SOL_CACHE_DURATION = 300000; // 5 minutes
const SOL_RETRY_COOLDOWN = 60000;  // 1 minute

// ============================================
// BOUNDS VALIDATION
// ============================================
const PRICE_BOUNDS = {
    MIN_PRICE: 0,
    MAX_PRICE: 1e15,
    MIN_MCAP: 0,
    MAX_MCAP: 1e15,
    MIN_LIQUIDITY: 0,
    MAX_LIQUIDITY: 1e12,
    MIN_VOLUME: 0,
    MAX_VOLUME: 1e12,
    MIN_CHANGE_PCT: -100,
    MAX_CHANGE_PCT: 100000,
};

function clampValue(value, min, max, fallback = 0) {
    const num = parseFloat(value);
    if (!Number.isFinite(num)) return fallback;
    if (num < min) return min;
    if (num > max) return max;
    return num;
}

// ============================================
// SOL PRICE (CoinGecko - free, reliable)
// ============================================

async function getSolPrice() {
    const now = Date.now();

    // Return cached if fresh
    if (solPriceCache.price > 0 && (now - solPriceCache.timestamp) < SOL_CACHE_DURATION) {
        return solPriceCache.price;
    }

    // Cooldown after recent failure
    if (solPriceCache.lastAttempt > 0 && (now - solPriceCache.lastAttempt) < SOL_RETRY_COOLDOWN) {
        return solPriceCache.price || 190;
    }

    solPriceCache.lastAttempt = now;

    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 5000);

        const response = await fetch(
            'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd',
            { signal: controller.signal }
        );
        clearTimeout(timeout);

        if (!response.ok) throw new Error(`CoinGecko: ${response.status}`);

        const data = await response.json();
        const price = data?.solana?.usd || 0;

        if (price > 0) {
            solPriceCache = { price, timestamp: now, lastAttempt: now };
            logger.debug(`[PriceProvider] SOL: $${price}`);
        }

        return price || solPriceCache.price || 190;
    } catch (e) {
        logger.debug(`[PriceProvider] SOL price error: ${e.message}`);
        return solPriceCache.price || 190;
    }
}

// ============================================
// JUPITER PRICE API V3
// ============================================

/**
 * Fetch prices from Jupiter Price API
 * @param {string[]} mints - Array of token mint addresses
 * @returns {Map<string, Object>} Map of mint -> price data
 */
async function fetchJupiterPrices(mints, retryCount = 0) {
    if (!mints || mints.length === 0) return new Map();

    const MAX_RETRIES = 3;
    const results = new Map();

    // Jupiter batch: comma-separated, max 100
    const batchMints = mints.slice(0, JUPITER_BATCH_SIZE);
    const ids = batchMints.join(',');
    const url = `${JUPITER_PRICE_URL}?ids=${ids}&showExtraInfo=true`;

    // Build headers - include API key if available
    const headers = {};
    if (JUPITER_API_KEY) {
        headers['x-api-key'] = JUPITER_API_KEY;
    }

    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 15000);

        const response = await fetch(url, {
            signal: controller.signal,
            headers: Object.keys(headers).length > 0 ? headers : undefined
        });
        clearTimeout(timeout);

        // Handle rate limiting
        if (response.status === 429) {
            if (retryCount < MAX_RETRIES) {
                const delay = 1000 * Math.pow(2, retryCount);
                logger.debug(`[Jupiter] Rate limited, retry ${retryCount + 1} in ${delay}ms`);
                await new Promise(r => setTimeout(r, delay));
                return fetchJupiterPrices(mints, retryCount + 1);
            }
            logger.warn('[Jupiter] Rate limit exceeded, max retries');
            return results;
        }

        if (!response.ok) {
            logger.warn(`[Jupiter] API error: ${response.status}`);
            return results;
        }

        const data = await response.json();
        const now = Date.now();

        // Process Jupiter V3 response
        // V3 format: { data: { [mint]: { usdPrice, decimals, blockId, priceChange24h } } }
        const priceData = data?.data || data;

        for (const mint of batchMints) {
            const tokenData = priceData[mint];
            if (!tokenData) continue;

            // Jupiter V3 returns usdPrice (not price)
            const priceUsd = parseFloat(tokenData.usdPrice) || parseFloat(tokenData.price) || 0;
            if (priceUsd <= 0) continue;

            results.set(mint, {
                priceUsd: clampValue(priceUsd, PRICE_BOUNDS.MIN_PRICE, PRICE_BOUNDS.MAX_PRICE),
                // Jupiter V3 provides these fields
                decimals: tokenData.decimals || 9,
                blockId: tokenData.blockId || null,
                // 24h change available in V3
                change24h: clampValue(tokenData.priceChange24h, PRICE_BOUNDS.MIN_CHANGE_PCT, PRICE_BOUNDS.MAX_CHANGE_PCT),
                // Volume not available in V3 - will get from Raydium
                volume24h: 0,
                // Metadata
                source: 'jupiter',
                timestamp: now,
                confidence: 'high'
            });
        }

        logger.debug(`[Jupiter] Fetched ${results.size}/${batchMints.length} prices`);
        return results;

    } catch (e) {
        if (e.name === 'AbortError') {
            logger.warn('[Jupiter] Request timeout');
        } else {
            logger.error('[Jupiter] Fetch error:', e.message);
        }
        return results;
    }
}

/**
 * Get single token price from Jupiter
 */
async function getJupiterPrice(mint) {
    // Check cache first
    const cached = priceCache.get(mint);
    if (cached && Date.now() - cached.timestamp < PRICE_CACHE_TTL) {
        return cached.data;
    }

    const prices = await fetchJupiterPrices([mint]);
    const priceData = prices.get(mint);

    if (priceData) {
        priceCache.set(mint, { data: priceData, timestamp: Date.now() });
    }

    return priceData || null;
}

// ============================================
// HELIUS ON-CHAIN DATA
// ============================================

/**
 * Get token account balances using Helius RPC
 */
async function getTokenAccountBalance(connection, vaultAddress) {
    try {
        const { PublicKey } = require('@solana/web3.js');
        const pubkey = new PublicKey(vaultAddress);
        const info = await connection.getAccountInfo(pubkey);

        if (!info || info.data.length < 72) return null;

        // SPL Token account layout: amount at offset 64 (8 bytes)
        const amount = info.data.readBigUInt64LE(64);
        return Number(amount);
    } catch (e) {
        logger.debug(`[Helius] Balance fetch error: ${e.message}`);
        return null;
    }
}

/**
 * Calculate liquidity from on-chain pool vault balances
 * Works for Raydium AMM pools
 */
async function calculateOnChainLiquidity(connection, poolAddress, baseDecimals = 9, quoteDecimals = 9) {
    try {
        const { PublicKey } = require('@solana/web3.js');

        // Check cache first
        const cacheKey = `liq:${poolAddress}`;
        const cached = liquidityCache.get(cacheKey);
        if (cached && Date.now() - cached.timestamp < LIQUIDITY_CACHE_TTL) {
            return cached.data;
        }

        // Get pool account to extract vault addresses
        const poolPubkey = new PublicKey(poolAddress);
        const poolInfo = await connection.getAccountInfo(poolPubkey);

        if (!poolInfo || !poolInfo.data) {
            return null;
        }

        // Raydium AMM V4 layout - vault addresses at specific offsets
        // This is simplified - real implementation needs proper deserialization
        const data = poolInfo.data;

        // For Raydium AMM V4, approximate offsets:
        // baseVault: offset 336 (32 bytes)
        // quoteVault: offset 368 (32 bytes)
        if (data.length < 400) return null;

        const baseVault = new PublicKey(data.slice(336, 368)).toBase58();
        const quoteVault = new PublicKey(data.slice(368, 400)).toBase58();

        // Fetch vault balances
        const [baseBalance, quoteBalance] = await Promise.all([
            getTokenAccountBalance(connection, baseVault),
            getTokenAccountBalance(connection, quoteVault)
        ]);

        if (baseBalance === null || quoteBalance === null) return null;

        // Get SOL price for USD conversion
        const solPrice = await getSolPrice();

        // Calculate liquidity in USD
        // Assuming quote is SOL or stablecoin
        const baseAmount = baseBalance / Math.pow(10, baseDecimals);
        const quoteAmount = quoteBalance / Math.pow(10, quoteDecimals);

        // Total liquidity = 2 * quote side (standard AMM)
        const liquidityUsd = quoteAmount * solPrice * 2;

        const result = {
            liquidity: clampValue(liquidityUsd, PRICE_BOUNDS.MIN_LIQUIDITY, PRICE_BOUNDS.MAX_LIQUIDITY),
            baseReserve: baseAmount,
            quoteReserve: quoteAmount,
            poolAddress,
            source: 'on_chain',
            timestamp: Date.now()
        };

        // Cache result
        liquidityCache.set(cacheKey, { data: result, timestamp: Date.now() });

        return result;

    } catch (e) {
        logger.debug(`[OnChain] Liquidity calc error: ${e.message}`);
        return null;
    }
}

/**
 * Discover pools for a token using Helius DAS API
 */
async function discoverPools(mint) {
    if (!config.HELIUS_API_KEY) return [];

    try {
        const response = await fetch(HELIUS_RPC_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${config.HELIUS_API_KEY}`
            },
            body: JSON.stringify({
                jsonrpc: '2.0',
                id: 'pool-search',
                method: 'searchAssets',
                params: {
                    ownerAddress: mint,
                    tokenType: 'fungible',
                    displayOptions: {
                        showCollectionMetadata: false
                    }
                }
            })
        });

        const data = await response.json();
        // Process and return pool addresses
        return data?.result?.items || [];

    } catch (e) {
        logger.debug(`[Helius] Pool discovery error: ${e.message}`);
        return [];
    }
}

// ============================================
// RAYDIUM API (for pool discovery - free)
// ============================================

/**
 * Get pool info from Raydium API (free, no key required)
 */
async function getRaydiumPoolInfo(mint) {
    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10000);

        // Raydium V3 API - get pools by mint
        const response = await fetch(
            `https://api-v3.raydium.io/pools/info/mint?mint1=${mint}&poolType=all&poolSortField=liquidity&sortType=desc&pageSize=5&page=1`,
            { signal: controller.signal }
        );
        clearTimeout(timeout);

        if (!response.ok) return null;

        const data = await response.json();
        if (!data.success || !data.data?.data?.length) return null;

        const pools = data.data.data;
        const bestPool = pools[0];

        return {
            poolAddress: bestPool.id,
            dex: bestPool.type || 'raydium',
            liquidity: clampValue(bestPool.tvl, PRICE_BOUNDS.MIN_LIQUIDITY, PRICE_BOUNDS.MAX_LIQUIDITY),
            volume24h: clampValue(bestPool.day?.volume, PRICE_BOUNDS.MIN_VOLUME, PRICE_BOUNDS.MAX_VOLUME),
            price: clampValue(bestPool.price, PRICE_BOUNDS.MIN_PRICE, PRICE_BOUNDS.MAX_PRICE),
            lpMint: bestPool.lpMint?.address,
            source: 'raydium',
            timestamp: Date.now()
        };

    } catch (e) {
        logger.debug(`[Raydium] Pool info error: ${e.message}`);
        return null;
    }
}

// ============================================
// UNIFIED PRICE FETCHING
// ============================================

/**
 * Get comprehensive price data for a token
 * Combines Jupiter price + on-chain liquidity
 *
 * @param {string} mint - Token mint address
 * @param {Object} options - Optional: db connection, decimals
 * @returns {Object} Complete price data
 */
async function getPrice(mint, options = {}) {
    const timestamp = Date.now();

    // 1. Get price from Jupiter (primary source)
    const jupiterData = await getJupiterPrice(mint);

    // 2. Get pool/liquidity info from Raydium API
    const raydiumData = await getRaydiumPoolInfo(mint);

    // 3. Combine data sources
    const priceUsd = jupiterData?.priceUsd || raydiumData?.price || 0;
    const liquidity = raydiumData?.liquidity || 0;
    const volume24h = jupiterData?.volume24h || raydiumData?.volume24h || 0;

    // 4. Calculate market cap if we have price and supply
    let mcap = 0;
    if (options.supply && priceUsd > 0) {
        mcap = clampValue(options.supply * priceUsd, PRICE_BOUNDS.MIN_MCAP, PRICE_BOUNDS.MAX_MCAP);
    }

    return {
        priceUsd: clampValue(priceUsd, PRICE_BOUNDS.MIN_PRICE, PRICE_BOUNDS.MAX_PRICE),
        mcap,
        liquidity,
        volume24h: clampValue(volume24h, PRICE_BOUNDS.MIN_VOLUME, PRICE_BOUNDS.MAX_VOLUME),
        change24h: clampValue(jupiterData?.change24h, PRICE_BOUNDS.MIN_CHANGE_PCT, PRICE_BOUNDS.MAX_CHANGE_PCT),
        change1h: 0, // Not available from Jupiter free tier
        change5m: 0, // Not available from Jupiter free tier
        pairAddress: raydiumData?.poolAddress || null,
        dex: raydiumData?.dex || 'unknown',
        source: jupiterData ? 'jupiter' : (raydiumData ? 'raydium' : 'none'),
        timestamp,
        confidence: jupiterData?.confidence || 'low'
    };
}

/**
 * Batch fetch prices for multiple tokens
 * Optimized for efficiency - single Jupiter call + parallel Raydium calls
 *
 * @param {string[]} mints - Array of token mint addresses
 * @returns {Map<string, Object>} Map of mint -> price data
 */
async function fetchBatchPrices(mints) {
    if (!mints || mints.length === 0) return new Map();

    const results = new Map();
    const now = Date.now();

    // 1. Batch fetch from Jupiter (up to 100 at once)
    const jupiterPrices = await fetchJupiterPrices(mints);

    // 2. For tokens with Jupiter prices, fetch Raydium liquidity in parallel
    const raydiumPromises = [];
    for (const mint of mints) {
        raydiumPromises.push(
            getRaydiumPoolInfo(mint)
                .then(data => ({ mint, data }))
                .catch(() => ({ mint, data: null }))
        );
    }

    // Limit concurrent Raydium requests
    const RAYDIUM_CONCURRENCY = 5;
    const raydiumResults = new Map();

    for (let i = 0; i < raydiumPromises.length; i += RAYDIUM_CONCURRENCY) {
        const batch = raydiumPromises.slice(i, i + RAYDIUM_CONCURRENCY);
        const batchResults = await Promise.all(batch);
        for (const { mint, data } of batchResults) {
            if (data) raydiumResults.set(mint, data);
        }
        // Small delay between batches to be respectful
        if (i + RAYDIUM_CONCURRENCY < raydiumPromises.length) {
            await new Promise(r => setTimeout(r, 200));
        }
    }

    // 3. Combine results
    for (const mint of mints) {
        const jupiter = jupiterPrices.get(mint);
        const raydium = raydiumResults.get(mint);

        if (!jupiter && !raydium) continue;

        results.set(mint, {
            priceUsd: clampValue(jupiter?.priceUsd || raydium?.price || 0, PRICE_BOUNDS.MIN_PRICE, PRICE_BOUNDS.MAX_PRICE),
            mcap: 0, // Calculated separately with supply
            liquidity: clampValue(raydium?.liquidity || 0, PRICE_BOUNDS.MIN_LIQUIDITY, PRICE_BOUNDS.MAX_LIQUIDITY),
            volume24h: clampValue(jupiter?.volume24h || raydium?.volume24h || 0, PRICE_BOUNDS.MIN_VOLUME, PRICE_BOUNDS.MAX_VOLUME),
            change24h: clampValue(jupiter?.change24h || 0, PRICE_BOUNDS.MIN_CHANGE_PCT, PRICE_BOUNDS.MAX_CHANGE_PCT),
            change1h: 0,
            change5m: 0,
            pairAddress: raydium?.poolAddress || null,
            dex: raydium?.dex || 'unknown',
            source: jupiter ? 'jupiter' : 'raydium',
            timestamp: now,
            confidence: jupiter?.confidence || 'low'
        });
    }

    logger.debug(`[PriceProvider] Batch: ${results.size}/${mints.length} tokens`);
    return results;
}

/**
 * Get cached price (for quick lookups without API call)
 */
function getCachedPrice(mint, maxAge = PRICE_CACHE_TTL) {
    const cached = priceCache.get(mint);
    if (cached && Date.now() - cached.timestamp < maxAge) {
        return cached.data;
    }
    return null;
}

// ============================================
// CACHE MANAGEMENT
// ============================================

const PRICE_CACHE_MAX_SIZE = 2000;

function evictOldestEntries() {
    if (priceCache.size <= PRICE_CACHE_MAX_SIZE) return;

    const toDelete = priceCache.size - PRICE_CACHE_MAX_SIZE;
    let deleted = 0;
    for (const key of priceCache.keys()) {
        if (deleted >= toDelete) break;
        priceCache.delete(key);
        deleted++;
    }
}

// Periodic cache cleanup
setInterval(() => {
    const now = Date.now();
    const staleThreshold = 30 * 60 * 1000;

    for (const [mint, cached] of priceCache) {
        if (now - cached.timestamp > staleThreshold) {
            priceCache.delete(mint);
        }
    }

    for (const [key, cached] of liquidityCache) {
        if (now - cached.timestamp > staleThreshold) {
            liquidityCache.delete(key);
        }
    }

    logger.debug(`[PriceProvider] Cache: ${priceCache.size} prices, ${liquidityCache.size} liquidity`);
}, 10 * 60 * 1000);

// ============================================
// EXPORTS
// ============================================

module.exports = {
    // Core functions
    getPrice,
    fetchBatchPrices,
    getCachedPrice,
    getSolPrice,

    // Individual source functions
    fetchJupiterPrices,
    getJupiterPrice,
    getRaydiumPoolInfo,
    calculateOnChainLiquidity,

    // Utilities
    clampValue,
    evictOldestEntries,

    // Constants
    PRICE_BOUNDS,
    SOL_MINT,
    USDC_MINT,
    USDT_MINT
};
