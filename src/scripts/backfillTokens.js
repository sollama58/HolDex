/**
 * Auto Seeder Task (Jupiter + Raydium Version)
 * Discovers and indexes top volume Solana tokens using independent data sources.
 * No DexScreener dependency - uses Jupiter Price API + Raydium pools.
 */
const { logger } = require('../services');
const { saveTokenData } = require('../services/database');
const priceProvider = require('../services/priceProvider');

// Configuration
const MIN_VOLUME_24H = 5000; // Only index tokens with > $5k daily volume
const BATCH_SIZE = 30;
let isRunning = false;

/**
 * Fetch top tokens from Raydium pools
 * Returns tokens sorted by volume/liquidity
 */
async function fetchTopRaydiumTokens() {
    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 15000);

        // Raydium V3 API - get top pools by volume
        const response = await fetch(
            'https://api-v3.raydium.io/pools/info/list?poolType=all&poolSortField=volume24h&sortType=desc&pageSize=100&page=1',
            { signal: controller.signal }
        );
        clearTimeout(timeout);

        if (!response.ok) {
            logger.warn(`[AutoSeeder] Raydium API error: ${response.status}`);
            return [];
        }

        const data = await response.json();
        if (!data.success || !data.data?.data?.length) {
            return [];
        }

        const tokens = [];
        const seenMints = new Set();

        for (const pool of data.data.data) {
            // Extract base token (non-SOL/USDC/USDT)
            const baseMint = pool.mintA?.address;
            const quoteMint = pool.mintB?.address;

            // Skip if we've seen this token
            if (!baseMint || seenMints.has(baseMint)) continue;

            // Skip wrapped SOL and stablecoins
            const skipMints = [
                'So11111111111111111111111111111111111111112', // SOL
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
                'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'  // USDT
            ];
            if (skipMints.includes(baseMint)) continue;

            // Volume check
            const volume = pool.day?.volume || 0;
            if (volume < MIN_VOLUME_24H) continue;

            seenMints.add(baseMint);
            tokens.push({
                mint: baseMint,
                symbol: pool.mintA?.symbol || 'UNKNOWN',
                name: pool.mintA?.name || pool.mintA?.symbol || 'Unknown Token',
                decimals: pool.mintA?.decimals || 9,
                poolAddress: pool.id,
                dex: pool.type || 'raydium',
                price: pool.price || 0,
                liquidity: pool.tvl || 0,
                volume24h: volume,
                image: pool.mintA?.logoURI || null
            });
        }

        return tokens;

    } catch (e) {
        logger.error(`[AutoSeeder] Raydium fetch error: ${e.message}`);
        return [];
    }
}

/**
 * Fetch token metadata from Helius DAS API
 */
async function fetchTokenMetadata(mint) {
    const config = require('../config/env');
    if (!config.HELIUS_API_KEY) return null;

    try {
        const response = await fetch('https://mainnet.helius-rpc.com', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${config.HELIUS_API_KEY}`
            },
            body: JSON.stringify({
                jsonrpc: '2.0',
                id: 'metadata',
                method: 'getAsset',
                params: { id: mint }
            })
        });

        const data = await response.json();
        if (data.error || !data.result) return null;

        const asset = data.result;
        const content = asset.content || {};
        const metadata = content.metadata || {};
        const links = content.links || {};

        return {
            name: metadata.name || asset.name,
            symbol: metadata.symbol || asset.symbol,
            image: content.files?.[0]?.uri || links.image,
            description: metadata.description,
            website: links.external_url,
            twitter: null, // Not in DAS response
            telegram: null
        };

    } catch (e) {
        logger.debug(`[AutoSeeder] Metadata fetch error for ${mint}: ${e.message}`);
        return null;
    }
}

async function syncTopTokens(_deps) {
    if (isRunning) return;
    isRunning = true;

    logger.info('[AutoSeeder] Scanning Raydium for top volume tokens...');

    try {
        // 1. Get top tokens from Raydium
        const tokens = await fetchTopRaydiumTokens();

        if (tokens.length === 0) {
            logger.info('[AutoSeeder] No tokens found');
            isRunning = false;
            return;
        }

        logger.info(`[AutoSeeder] Found ${tokens.length} candidate tokens`);

        // 2. Enrich with Jupiter prices in batches
        let addedCount = 0;
        const mints = tokens.map(t => t.mint);

        for (let i = 0; i < mints.length; i += BATCH_SIZE) {
            const batch = mints.slice(i, i + BATCH_SIZE);
            const prices = await priceProvider.fetchBatchPrices(batch);

            for (const token of tokens.filter(t => batch.includes(t.mint))) {
                const jupiterData = prices.get(token.mint);

                // Get additional metadata from Helius
                const metadata = await fetchTokenMetadata(token.mint);

                const tokenData = {
                    ticker: metadata?.symbol || token.symbol,
                    name: metadata?.name || token.name,
                    description: metadata?.description || `Discovered via AutoSeeder (${token.dex})`,
                    twitter: metadata?.twitter,
                    website: metadata?.website,
                    telegram: metadata?.telegram,
                    metadataUri: null,
                    image: metadata?.image || token.image,
                    isMayhemMode: false,
                    marketCap: jupiterData?.mcap || 0,
                    volume24h: jupiterData?.volume24h || token.volume24h,
                    priceUsd: jupiterData?.priceUsd || token.price,
                    liquidity: jupiterData?.liquidity || token.liquidity
                };

                // Use current time as creation (we don't have exact creation time)
                const createdAt = Date.now();

                // Upsert into DB
                await saveTokenData(null, token.mint, tokenData, createdAt);
                addedCount++;
            }

            // Small delay between batches
            if (i + BATCH_SIZE < mints.length) {
                await new Promise(r => setTimeout(r, 500));
            }
        }

        logger.info(`[AutoSeeder] Synced ${addedCount} tokens from Raydium`);

    } catch (e) {
        logger.error(`[AutoSeeder] Error: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    // Run immediately
    setTimeout(() => syncTopTokens(deps), 5000);

    // Run every 60 seconds to keep refreshing/finding new tops
    setInterval(() => syncTopTokens(deps), 60000);
}

module.exports = { start, syncTopTokens, fetchTopRaydiumTokens };
