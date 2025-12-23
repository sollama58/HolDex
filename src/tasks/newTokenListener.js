/**
 * New Token Listener (DexScreener Version)
 * Watches for active Pump.fun tokens via DexScreener.
 * FILTER: Only allows tokens > $10k Market Cap.
 * INTERVAL: Checks every 60 seconds.
 */
const axios = require('axios');
const { logger } = require('../services');
const { saveTokenData } = require('../services/database');

const MIN_MARKET_CAP = 10000;
const knownMints = new Set();
const MAX_HISTORY = 1000;

function getSocialLink(pair, type) {
    if (!pair.info || !pair.info.socials) return null;
    const social = pair.info.socials.find(s => s.type === type);
    return social ? social.url : null;
}

async function checkNewTokens(deps) {
    try {
        // Specifically search for "pump" to get tokens on the bonding curve or graduated
        const response = await axios.get('https://api.dexscreener.com/latest/dex/search?q=pump', {
            timeout: 5000
        });

        const pairs = response.data.pairs;
        if (!pairs) return;

        // Sort by creation time (DexScreener sends pairCreatedAt)
        // We want the newest first
        pairs.sort((a, b) => (b.pairCreatedAt || 0) - (a.pairCreatedAt || 0));

        let addedCount = 0;
        let skippedCount = 0;

        for (const pair of pairs) {
            const mint = pair.baseToken.address;

            // Skip if seen or not Solana
            if (knownMints.has(mint) || pair.chainId !== 'solana') continue;

            const mcap = pair.fdv || pair.marketCap || 0;

            if (mcap < MIN_MARKET_CAP) {
                knownMints.add(mint);
                skippedCount++;
                continue;
            }

            // Extract Creator (Not provided by DexScreener Search directly, send null)
            // Ideally we fetch this from RPC, but for speed we leave it null for now.
            const creator = null; 

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

            await saveTokenData(creator, mint, metadata);
            
            knownMints.add(mint);
            addedCount++;
            logger.info(`💎 DEXSCREENER DETECT: ${pair.baseToken.symbol} ($${Math.floor(mcap)})`);
        }

        if (addedCount > 0) {
            logger.info(`Listener: Added ${addedCount} tokens. Skipped ${skippedCount} low-cap.`);
        }

        // Cleanup memory
        if (knownMints.size > MAX_HISTORY) {
            knownMints.clear();
        }

    } catch (e) {
        logger.error('NewTokenListener Error:', { error: e.message });
    }
}

function start(deps) {
    logger.info("🚀 New Token Listener started (DexScreener Mode)");
    
    checkNewTokens(deps);
    
    // Poll every 60 seconds (DexScreener allows 300 req/min, so this is safe)
    setInterval(() => checkNewTokens(deps), 60000);
}

module.exports = { start };
