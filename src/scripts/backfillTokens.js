/**
 * Auto Seeder Task (DexScreener Top Volume Version)
 * Replaces the blocked Pump.fun API with DexScreener to fetch top volume tokens.
 * Focuses on 'pump' DEX pairs to find tokens relevant to the PumpFun ecosystem.
 */
const axios = require('axios');
const { logger } = require('../services');
const { saveTokenData } = require('../services/database');

// Configuration
const MIN_VOLUME_24H = 5000; // Only index tokens with > $5k daily volume
const BATCH_SIZE = 50; // DexScreener usually returns 30-50 pairs per search
let isRunning = false;

// Search terms to find active Pump tokens on DexScreener
// We rotate these to find "Top" tokens in different clusters
const SEARCH_TERMS = ['pump', 'solana', 'meme', 'coin', 'moon', 'pepe', 'doge'];

async function syncTopTokens(deps) {
    if (isRunning) return;
    isRunning = true;

    // Rotate search term to diversify "Top" finding
    const term = SEARCH_TERMS[Math.floor(Math.random() * SEARCH_TERMS.length)];
    logger.info(`🏆 AutoSeeder: Scanning DexScreener for Top Volume '${term}' tokens...`);

    try {
        // DexScreener Search API
        // This returns pairs sorted by relevance/volume usually
        const response = await axios.get(`https://api.dexscreener.com/latest/dex/search?q=${term}`, {
            timeout: 10000
        });

        const pairs = response.data.pairs;
        if (!pairs || pairs.length === 0) {
            isRunning = false;
            return;
        }

        let addedCount = 0;

        for (const pair of pairs) {
            // 1. Solana Only
            if (pair.chainId !== 'solana') continue;

            // 2. Strict Filter: PumpSwap LP (Bonding Curve) OR Raydium (Graduated)
            // If you ONLY want pre-bonded, check for dexId === 'pump'
            // If you want ALL successful pump tokens, checking for 'pump' in labels or dexId helps.
            // Note: DexScreener often labels the DEX as 'raydium' if it graduated.
            // To find "Only PumpSwap LP", we look for dexId 'pump'.
            if (pair.dexId !== 'pump') continue;

            // 3. Volume Check (Ensure it's a "Top" token)
            const volume = pair.volume?.h24 || 0;
            if (volume < MIN_VOLUME_24H) continue;

            // 4. Map & Save
            await processPair(pair);
            addedCount++;
        }

        logger.info(`🏆 AutoSeeder: Synced ${addedCount} active PumpSwap pairs from '${term}'.`);

    } catch (e) {
        logger.error(`🏆 AutoSeeder Error: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

async function processPair(pair) {
    const mcap = pair.fdv || pair.marketCap || 0;
    
    const metadata = {
        ticker: pair.baseToken.symbol,
        name: pair.baseToken.name,
        description: `Discovered via AutoSeeder (${pair.dexId})`,
        twitter: pair.info?.socials?.find(s => s.type === 'twitter')?.url,
        website: pair.info?.websites?.[0]?.url,
        telegram: pair.info?.socials?.find(s => s.type === 'telegram')?.url,
        metadataUri: null,
        image: pair.info?.imageUrl,
        isMayhemMode: false,
        marketCap: mcap,
        volume24h: pair.volume?.h24 || 0,
        priceUsd: pair.priceUsd
    };

    // Use pair creation time or current time
    const createdAt = pair.pairCreatedAt || Date.now();
    
    // Upsert into DB
    await saveTokenData(null, pair.baseToken.address, metadata, createdAt);
}

function start(deps) {
    // Run immediately
    setTimeout(() => syncTopTokens(deps), 5000);

    // Run every 60 seconds to keep refreshing/finding new tops
    setInterval(() => syncTopTokens(deps), 60000);
}

module.exports = { start };
