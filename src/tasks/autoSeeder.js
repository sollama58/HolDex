/**
 * Auto Seeder Task (DexScreener Version)
 * Populates the DB with high-value tokens (> $10k) using DexScreener Search.
 * Bypasses Pump.fun Cloudflare blocks.
 */
const axios = require('axios');
const { logger } = require('../services');
const { saveTokenData } = require('../services/database');

const MIN_MARKET_CAP = 10000;
const SEARCH_TERMS = ['pump.fun', 'pump', 'solana']; // Rotate these to cast a wide net

function getSocialLink(pair, type) {
    if (!pair.info || !pair.info.socials) return null;
    const social = pair.info.socials.find(s => s.type === type);
    return social ? social.url : null;
}

function getWebsite(pair) {
    if (!pair.info || !pair.info.websites) return null;
    return pair.info.websites.length > 0 ? pair.info.websites[0].url : null;
}

async function seedHighValueTokens(deps) {
    // Pick a random search term each cycle to diversify results
    const term = SEARCH_TERMS[Math.floor(Math.random() * SEARCH_TERMS.length)];
    logger.info(`🌱 AutoSeeder: Scanning DexScreener for "${term}" tokens > $10k...`);

    try {
        const response = await axios.get(`https://api.dexscreener.com/latest/dex/search?q=${term}`, {
            timeout: 10000
        });

        const pairs = response.data.pairs;
        if (!pairs || pairs.length === 0) {
            logger.info("🌱 AutoSeeder: No pairs found.");
            return;
        }

        let addedCount = 0;

        for (const pair of pairs) {
            // 1. Must be Solana
            if (pair.chainId !== 'solana') continue;

            // 2. Must be > $10k MC
            // DexScreener uses 'fdv' (Fully Diluted Valuation) or 'marketCap'
            const mcap = pair.fdv || pair.marketCap || 0;
            if (mcap < MIN_MARKET_CAP) continue;

            // 3. Map Data
            const metadata = {
                ticker: pair.baseToken.symbol,
                name: pair.baseToken.name,
                description: `Discovered via DexScreener (${pair.dexId})`, // DexScreener doesn't provide desc in search
                twitter: getSocialLink(pair, 'twitter'),
                website: getWebsite(pair),
                telegram: getSocialLink(pair, 'telegram'),
                metadataUri: null, // Not available via search, but optional
                image: pair.info ? pair.info.imageUrl : null,
                isMayhemMode: false,
                marketCap: mcap,
                volume24h: pair.volume ? pair.volume.h24 : 0,
                priceUsd: pair.priceUsd
            };

            // 4. Save
            // Use baseToken.address as the mint
            await saveTokenData(null, pair.baseToken.address, metadata);
            addedCount++;
        }

        if (addedCount > 0) {
            logger.info(`🌱 AutoSeeder: Synced ${addedCount} tokens from DexScreener.`);
        }

    } catch (e) {
        logger.error(`🌱 AutoSeeder Error: ${e.message}`);
    }
}

function start(deps) {
    // Run 5 seconds after boot
    setTimeout(() => seedHighValueTokens(deps), 5000);

    // Run every 2 minutes
    setInterval(() => seedHighValueTokens(deps), 120000);
}

module.exports = { start };
