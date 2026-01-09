/**
 * Token Search Service - Helius DAS API + Jupiter
 *
 * Scalable token search without GeckoTerminal/CoinGecko rate limits
 * Uses Helius DAS API for metadata and Jupiter for pricing
 *
 * Features:
 * - Search by name/symbol using Helius searchAssets
 * - Search by mint address using Helius getAsset
 * - Get pricing from Jupiter (already integrated)
 * - Redis caching for search results
 * - Fallback chain: Helius → Jupiter Token List → Local DB
 */

const axios = require('axios');
const config = require('../config/env');
const logger = require('./logger');
const { getClient } = require('./redis');

// API Configuration
const HELIUS_API_KEY = config.HELIUS_API_KEY;
const HELIUS_RPC_URL = 'https://mainnet.helius-rpc.com';
// Jupiter Token API V2 - https://dev.jup.ag/docs/tokens/v2/token-information
const JUPITER_TOKEN_API = 'https://api.jup.ag/tokens/v2';
const JUPITER_PRICE_URL = 'https://api.jup.ag/price/v3';
const JUPITER_API_KEY = config.JUPITER_API_KEY;

// Cache settings
const SEARCH_CACHE_TTL = 300; // 5 minutes for search results
const TOKEN_CACHE_TTL = 3600; // 1 hour for token metadata
const JUPITER_LIST_CACHE_TTL = 86400; // 24 hours for Jupiter token list

// In-memory cache for Jupiter token list (loaded once)
let jupiterTokenList = null;
let jupiterListTimestamp = 0;

/**
 * Get Helius API headers
 */
function getHeliusHeaders() {
    return HELIUS_API_KEY
        ? { 'Content-Type': 'application/json', 'Authorization': `Bearer ${HELIUS_API_KEY}` }
        : { 'Content-Type': 'application/json' };
}

/**
 * Get Jupiter API headers (API key required for V2)
 */
function getJupiterHeaders() {
    const headers = { 'Content-Type': 'application/json' };
    if (JUPITER_API_KEY) {
        headers['x-api-key'] = JUPITER_API_KEY;
    }
    return headers;
}

/**
 * Load Jupiter token list (cached for 24h)
 * Uses Jupiter Token API V2 - fetches verified tokens
 * https://dev.jup.ag/docs/tokens/v2/token-information
 */
async function loadJupiterTokenList() {
    const now = Date.now();

    // Return cached if fresh
    if (jupiterTokenList && jupiterTokenList.length > 0 && (now - jupiterListTimestamp) < JUPITER_LIST_CACHE_TTL * 1000) {
        return jupiterTokenList;
    }

    try {
        // Try Redis cache first
        const redis = getClient();
        if (redis) {
            const cached = await redis.get('jupiter:token_list_v2');
            if (cached) {
                jupiterTokenList = JSON.parse(cached);
                jupiterListTimestamp = now;
                logger.info(`[TokenSearch] Loaded ${jupiterTokenList.length} tokens from Redis cache`);
                return jupiterTokenList;
            }
        }

        // Fetch from Jupiter Token API V2 - /tag endpoint for verified tokens
        logger.info('[TokenSearch] Fetching Jupiter verified token list (V2 API)...');
        const response = await axios.get(`${JUPITER_TOKEN_API}/tag?query=verified`, {
            headers: getJupiterHeaders(),
            timeout: 30000
        });

        // V2 returns array of tokens
        let tokens = response.data?.tokens || response.data;
        if (tokens && Array.isArray(tokens)) {
            // Normalize V2 format to match expected structure
            jupiterTokenList = tokens.map(t => ({
                address: t.mint || t.address,
                name: t.name || 'Unknown',
                symbol: t.symbol || 'UNK',
                logoURI: t.logo || t.logoURI || t.image || null,
                decimals: t.decimals || 9,
                tags: t.tags || ['verified']
            }));
            jupiterListTimestamp = now;

            // Cache in Redis
            if (redis) {
                await redis.setex('jupiter:token_list_v2', JUPITER_LIST_CACHE_TTL, JSON.stringify(jupiterTokenList));
            }

            logger.info(`[TokenSearch] Loaded ${jupiterTokenList.length} tokens from Jupiter V2 API`);
            return jupiterTokenList;
        }

        return jupiterTokenList || [];
    } catch (e) {
        logger.warn(`[TokenSearch] Failed to load Jupiter token list: ${e.message}`);
        return jupiterTokenList || [];
    }
}

/**
 * Search Jupiter token list by name/symbol
 * Fast local search - no API calls
 */
async function searchJupiterList(query, limit = 10) {
    const tokens = await loadJupiterTokenList();
    if (!tokens.length) return [];

    const queryLower = query.toLowerCase();
    const results = [];

    for (const token of tokens) {
        if (results.length >= limit) break;

        const nameLower = (token.name || '').toLowerCase();
        const symbolLower = (token.symbol || '').toLowerCase();

        // Exact symbol match gets priority
        if (symbolLower === queryLower) {
            results.unshift({
                mint: token.address,
                name: token.name,
                symbol: token.symbol,
                image: token.logoURI || null,
                decimals: token.decimals || 9,
                verified: token.tags?.includes('verified') || false,
                source: 'jupiter_list',
                _matchScore: 3
            });
        }
        // Symbol starts with query
        else if (symbolLower.startsWith(queryLower)) {
            results.push({
                mint: token.address,
                name: token.name,
                symbol: token.symbol,
                image: token.logoURI || null,
                decimals: token.decimals || 9,
                verified: token.tags?.includes('verified') || false,
                source: 'jupiter_list',
                _matchScore: 2
            });
        }
        // Name contains query
        else if (nameLower.includes(queryLower) || symbolLower.includes(queryLower)) {
            results.push({
                mint: token.address,
                name: token.name,
                symbol: token.symbol,
                image: token.logoURI || null,
                decimals: token.decimals || 9,
                verified: token.tags?.includes('verified') || false,
                source: 'jupiter_list',
                _matchScore: 1
            });
        }
    }

    // Sort by match score
    results.sort((a, b) => b._matchScore - a._matchScore);
    results.forEach(r => delete r._matchScore);

    return results.slice(0, limit);
}

/**
 * Get token metadata from Helius DAS API
 * Works for any Solana token mint address
 */
async function getTokenFromHelius(mint) {
    if (!HELIUS_API_KEY) {
        logger.debug('[TokenSearch] Helius API key not configured');
        return null;
    }

    try {
        const response = await axios.post(HELIUS_RPC_URL, {
            jsonrpc: '2.0',
            id: 'get-asset',
            method: 'getAsset',
            params: { id: mint }
        }, {
            headers: getHeliusHeaders(),
            timeout: 10000
        });

        const asset = response.data?.result;
        if (!asset) return null;

        // Extract metadata from Helius response
        const content = asset.content || {};
        const metadata = content.metadata || {};
        const links = content.links || {};

        return {
            mint: asset.id,
            name: metadata.name || content.json_uri?.split('/').pop() || 'Unknown',
            symbol: metadata.symbol || 'UNK',
            image: links.image || content.json_uri || null,
            decimals: asset.token_info?.decimals || 9,
            supply: asset.token_info?.supply || null,
            verified: asset.authorities?.length > 0,
            source: 'helius'
        };
    } catch (e) {
        logger.debug(`[TokenSearch] Helius getAsset failed for ${mint}: ${e.message}`);
        return null;
    }
}

/**
 * Search tokens using Helius DAS searchAssets
 * Note: searchAssets is primarily for NFTs, may have limited fungible token support
 */
async function searchHeliusAssets(query, limit = 10) {
    if (!HELIUS_API_KEY) return [];

    try {
        // Helius searchAssets - search by name
        const response = await axios.post(HELIUS_RPC_URL, {
            jsonrpc: '2.0',
            id: 'search-assets',
            method: 'searchAssets',
            params: {
                page: 1,
                limit: limit,
                displayOptions: {
                    showFungible: true,
                    showNativeBalance: false
                },
                // Search in token names
                // Note: This is experimental - Helius searchAssets is primarily for NFTs
                conditionType: 'all',
                interface: 'FungibleToken'
            }
        }, {
            headers: getHeliusHeaders(),
            timeout: 15000
        });

        const items = response.data?.result?.items || [];
        return items.map(item => ({
            mint: item.id,
            name: item.content?.metadata?.name || 'Unknown',
            symbol: item.content?.metadata?.symbol || 'UNK',
            image: item.content?.links?.image || null,
            decimals: item.token_info?.decimals || 9,
            source: 'helius_search'
        }));
    } catch (e) {
        logger.debug(`[TokenSearch] Helius searchAssets failed: ${e.message}`);
        return [];
    }
}

/**
 * Get token price from Jupiter
 */
async function getJupiterPrice(mint) {
    try {
        const headers = {};
        if (JUPITER_API_KEY) {
            headers['x-api-key'] = JUPITER_API_KEY;
        }

        const response = await axios.get(`${JUPITER_PRICE_URL}?ids=${mint}`, {
            headers: Object.keys(headers).length > 0 ? headers : undefined,
            timeout: 5000
        });

        const priceData = response.data?.data?.[mint] || response.data?.[mint];
        if (!priceData) return null;

        return {
            priceUsd: parseFloat(priceData.usdPrice || priceData.price) || 0,
            change24h: parseFloat(priceData.priceChange24h) || 0
        };
    } catch (e) {
        logger.debug(`[TokenSearch] Jupiter price failed for ${mint}: ${e.message}`);
        return null;
    }
}

/**
 * Get batch prices from Jupiter (up to 100 tokens)
 */
async function getJupiterPricesBatch(mints) {
    if (!mints.length) return new Map();

    try {
        const headers = {};
        if (JUPITER_API_KEY) {
            headers['x-api-key'] = JUPITER_API_KEY;
        }

        const ids = mints.slice(0, 100).join(',');
        const response = await axios.get(`${JUPITER_PRICE_URL}?ids=${ids}`, {
            headers: Object.keys(headers).length > 0 ? headers : undefined,
            timeout: 10000
        });

        const prices = new Map();
        const data = response.data?.data || response.data || {};

        for (const mint of mints) {
            const priceData = data[mint];
            if (priceData) {
                prices.set(mint, {
                    priceUsd: parseFloat(priceData.usdPrice || priceData.price) || 0,
                    change24h: parseFloat(priceData.priceChange24h) || 0
                });
            }
        }

        return prices;
    } catch (e) {
        logger.debug(`[TokenSearch] Jupiter batch price failed: ${e.message}`);
        return new Map();
    }
}

/**
 * Search using Jupiter Token API V2 /search endpoint
 * Supports searching by mint address, symbol, or name
 * https://dev.jup.ag/docs/tokens/v2/token-information
 */
async function searchJupiterV2(query, limit = 10) {
    try {
        const response = await axios.get(`${JUPITER_TOKEN_API}/search?query=${encodeURIComponent(query)}`, {
            headers: getJupiterHeaders(),
            timeout: 10000
        });

        // V2 search returns array of tokens
        let tokens = response.data?.tokens || response.data;
        if (!tokens || !Array.isArray(tokens)) return [];

        return tokens.slice(0, limit).map(t => ({
            mint: t.mint || t.address,
            name: t.name || 'Unknown',
            symbol: t.symbol || 'UNK',
            image: t.logo || t.logoURI || t.image || null,
            decimals: t.decimals || 9,
            verified: t.tags?.includes('verified') || t.verified || false,
            source: 'jupiter_v2_search'
        }));
    } catch (e) {
        logger.debug(`[TokenSearch] Jupiter V2 search failed: ${e.message}`);
        return [];
    }
}

/**
 * Main search function - combines all sources
 *
 * Search priority:
 * 1. Check Redis cache
 * 2. Jupiter V2 /search API (primary - supports mint, symbol, name)
 * 3. Jupiter token list (fallback - local search)
 * 4. Helius DAS API (fallback for mint addresses)
 * 5. Enrich with Jupiter prices
 *
 * @param {string} query - Search term (name, symbol, or mint address)
 * @param {number} limit - Max results
 * @returns {Promise<Array>} Search results with prices
 */
async function searchTokens(query, limit = 10) {
    if (!query || query.length < 2) return [];

    const cacheKey = `search:${query.toLowerCase()}:${limit}`;
    const redis = getClient();

    // Check cache
    if (redis) {
        try {
            const cached = await redis.get(cacheKey);
            if (cached) {
                logger.debug(`[TokenSearch] Cache hit for "${query}"`);
                return JSON.parse(cached);
            }
        } catch (_e) { /* ignore */ }
    }

    let results = [];
    const isAddressSearch = query.length >= 32 && query.length <= 44;

    // Try Jupiter V2 /search API first (works for both addresses and names)
    if (JUPITER_API_KEY) {
        results = await searchJupiterV2(query, limit);
    }

    // Fallback strategies if Jupiter V2 search returned nothing
    if (results.length === 0) {
        if (isAddressSearch) {
            // Mint address lookup via Helius
            const token = await getTokenFromHelius(query);
            if (token && token.name !== 'Unknown') {
                results = [token];
            }
        } else {
            // Name/symbol search - use cached Jupiter list
            results = await searchJupiterList(query, limit);
        }
    }

    // Enrich with prices if we have results
    if (results.length > 0) {
        const mints = results.map(r => r.mint);
        const prices = await getJupiterPricesBatch(mints);

        results = results.map(token => {
            const price = prices.get(token.mint);
            return {
                ...token,
                priceUsd: price?.priceUsd || 0,
                change24h: price?.change24h || 0
            };
        });
    }

    // Cache results
    if (redis && results.length > 0) {
        try {
            await redis.setex(cacheKey, SEARCH_CACHE_TTL, JSON.stringify(results));
        } catch (_e) { /* ignore */ }
    }

    logger.info(`[TokenSearch] Found ${results.length} results for "${query}"`);
    return results;
}

/**
 * Get single token by mint address
 * Combines Helius metadata + Jupiter price
 */
async function getTokenByMint(mint) {
    const cacheKey = `token:${mint}`;
    const redis = getClient();

    // Check cache
    if (redis) {
        try {
            const cached = await redis.get(cacheKey);
            if (cached) {
                return JSON.parse(cached);
            }
        } catch (_e) { /* ignore */ }
    }

    // Get metadata from Helius
    let token = await getTokenFromHelius(mint);

    // Fallback to Jupiter list
    if (!token) {
        const tokens = await loadJupiterTokenList();
        const found = tokens.find(t => t.address === mint);
        if (found) {
            token = {
                mint: found.address,
                name: found.name,
                symbol: found.symbol,
                image: found.logoURI || null,
                decimals: found.decimals || 9,
                verified: found.tags?.includes('verified') || false,
                source: 'jupiter_list'
            };
        }
    }

    if (!token) return null;

    // Get price
    const price = await getJupiterPrice(mint);
    if (price) {
        token.priceUsd = price.priceUsd;
        token.change24h = price.change24h;
    }

    // Cache
    if (redis) {
        try {
            await redis.setex(cacheKey, TOKEN_CACHE_TTL, JSON.stringify(token));
        } catch (_e) { /* ignore */ }
    }

    return token;
}

/**
 * Fetch initial market data for a token
 * Replacement for GeckoTerminal fetchInitialMarketData
 */
async function fetchInitialMarketData(mint) {
    try {
        // Get price from Jupiter
        const headers = {};
        if (JUPITER_API_KEY) {
            headers['x-api-key'] = JUPITER_API_KEY;
        }

        const [priceResponse, raydiumResponse] = await Promise.all([
            axios.get(`${JUPITER_PRICE_URL}?ids=${mint}&showExtraInfo=true`, {
                headers: Object.keys(headers).length > 0 ? headers : undefined,
                timeout: 5000
            }).catch(() => null),
            // Get volume/liquidity from Raydium (free API)
            axios.get(`https://api-v3.raydium.io/pools/info/mint?mint1=${mint}&poolType=all&poolSortField=liquidity&sortType=desc&pageSize=1`, {
                timeout: 5000
            }).catch(() => null)
        ]);

        const jupiterData = priceResponse?.data?.data?.[mint] || priceResponse?.data?.[mint];
        const raydiumPool = raydiumResponse?.data?.data?.data?.[0];

        const priceUsd = parseFloat(jupiterData?.usdPrice || jupiterData?.price) || 0;
        const change24h = parseFloat(jupiterData?.priceChange24h) || 0;

        // Raydium provides better liquidity/volume data
        const volume24h = Math.floor(parseFloat(raydiumPool?.day?.volume) || 0);
        const liquidity = Math.floor(parseFloat(raydiumPool?.tvl) || 0);
        const marketCap = Math.floor(parseFloat(raydiumPool?.marketCap) || 0);

        return {
            priceUsd,
            volume24h,
            change24h,
            change1h: 0,  // Not available from free APIs
            change5m: 0,  // Not available from free APIs
            marketCap,
            liquidity
        };
    } catch (e) {
        logger.debug(`[TokenSearch] fetchInitialMarketData failed for ${mint}: ${e.message}`);
        return null;
    }
}

module.exports = {
    searchTokens,
    getTokenByMint,
    getTokenFromHelius,
    searchJupiterList,
    getJupiterPrice,
    getJupiterPricesBatch,
    fetchInitialMarketData,
    loadJupiterTokenList
};
