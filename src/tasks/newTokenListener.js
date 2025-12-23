/**
 * New Token Listener (Bonded Only)
 * Updated: Filters out 'pump' DEX pairs. Only indexes tokens once they reach Raydium (Bonded).
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
        // Search for 'pump' to find Pump.fun tokens that have made it to DexScreener
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

            // 2. EXCLUDE PRE-BONDED (Pump.fun internal DEX)
            // We only want to add tokens when they "Bond" (graduate to Raydium/etc)
            if (pair.dexId === 'pump') continue;

            // 3. Skip if we have already processed this session
            if (knownMints.has(mint)) continue;

            const mcap = pair.fdv || pair.marketCap || 0;

            if (mcap < MIN_MARKET_CAP) continue;

            const metadata = {
                ticker: pair.baseToken.symbol,
                name: pair.baseToken.name,
                description: 'Pump.fun Token (Bonded)',
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

            // Pass Creation Time
            const createdAt = pair.pairCreatedAt || Date.now();
            await saveTokenData(null, mint, metadata, createdAt);
            
            knownMints.add(mint);
            addedCount++;
            logger.info(`🎓 BONDED DETECT: ${pair.baseToken.symbol} on ${pair.dexId} ($${Math.floor(mcap)})`);
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
    logger.info("🚀 New Token Listener started (Bonded Mode)");
    checkNewTokens(deps);
    setInterval(() => checkNewTokens(deps), 60000);
}

module.exports = { start };
