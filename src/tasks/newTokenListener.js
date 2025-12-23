/**
 * New Token Listener (DexScreener Version)
 * Fix: Allows tracking of tokens that grow in Market Cap.
 */
const axios = require('axios');
const { logger } = require('../services');
const { saveTokenData } = require('../services/database');

const MIN_MARKET_CAP = 10000;
const knownMints = new Set();
const MAX_HISTORY = 2000;

function getSocialLink(pair, type) {
    if (!pair.info || !pair.info.socials) return null;
    const social = pair.info.socials.find(s => s.type === type);
    return social ? social.url : null;
}

async function checkNewTokens(deps) {
    try {
        const response = await axios.get('https://api.dexscreener.com/latest/dex/search?q=pump', {
            timeout: 5000
        });

        const pairs = response.data.pairs;
        if (!pairs) return;

        // Sort newest first
        pairs.sort((a, b) => (b.pairCreatedAt || 0) - (a.pairCreatedAt || 0));

        let addedCount = 0;

        for (const pair of pairs) {
            const mint = pair.baseToken.address;

            // 1. Skip non-Solana
            if (pair.chainId !== 'solana') continue;

            // 2. Skip if we have already SAVED this token (in memory cache)
            if (knownMints.has(mint)) continue;

            const mcap = pair.fdv || pair.marketCap || 0;

            // 3. CRITICAL FIX: Do NOT mark as known if we skip due to low mcap.
            // This allows us to catch it on the next loop if it pumps.
            if (mcap < MIN_MARKET_CAP) {
                continue; 
            }

            const metadata = {
                ticker: pair.baseToken.symbol,
                name: pair.baseToken.name,
                description: 'Pump.fun Token',
                twitter: getSocialLink(pair, 'twitter'),
                website: pair.info?.websites?.[0]?.url || null,
                telegram: getSocialLink(pair, 'telegram'),
                metadataUri: null,
                image: pair.info?.imageUrl,
                isMayhemMode: false,
                marketCap: mcap,
                volume24h: pair.volume?.h24 || 0,
                priceUsd: pair.priceUsd
            };

            // 4. Pass Creation Time for accurate "Age"
            const createdAt = pair.pairCreatedAt || Date.now();
            await saveTokenData(null, mint, metadata, createdAt);
            
            // 5. NOW we mark it as known/processed
            knownMints.add(mint);
            addedCount++;
            logger.info(`💎 NEW FIND: ${pair.baseToken.symbol} ($${Math.floor(mcap)})`);
        }

        // Memory Cleanup
        if (knownMints.size > MAX_HISTORY) {
            const it = knownMints.values();
            for (let i = 0; i < 500; i++) {
                knownMints.delete(it.next().value);
            }
        }

    } catch (e) {
        logger.error('NewTokenListener Error:', { error: e.message });
    }
}

function start(deps) {
    logger.info("🚀 New Token Listener started (DexScreener Mode)");
    checkNewTokens(deps);
    setInterval(() => checkNewTokens(deps), 60000);
}

module.exports = { start };
