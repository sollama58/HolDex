/**
 * New Token Listener (DexScreener Version)
 * Optimized memory handling.
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

            // Skip if seen or not Solana
            if (knownMints.has(mint) || pair.chainId !== 'solana') continue;

            const mcap = pair.fdv || pair.marketCap || 0;

            if (mcap < MIN_MARKET_CAP) {
                knownMints.add(mint); // Still track it so we don't process it again next loop
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

            await saveTokenData(null, mint, metadata);
            
            knownMints.add(mint);
            addedCount++;
            logger.info(`💎 DEXSCREENER DETECT: ${pair.baseToken.symbol} ($${Math.floor(mcap)})`);
        }

        // Cleanup memory (FIFO approach - delete oldest is hard with Set, 
        // so we just clear half if it gets too big to prevent total amnesia)
        if (knownMints.size > MAX_HISTORY) {
            const it = knownMints.values();
            // Remove first 500 items (oldest inserted)
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
