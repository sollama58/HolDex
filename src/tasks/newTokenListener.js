/**
 * New Token Listener (High Value Filter)
 * Watches for ANY new pairs on Solana via DexScreener.
 * STRICT FILTER: Market Cap > $25k AND Liquidity > $5k.
 */
const axios = require('axios');
const { logger } = require('../services');
const { saveTokenData } = require('../services/database');

// Configuration Thresholds
const MIN_MARKET_CAP = 25000;
const MIN_LIQUIDITY = 5000;

const knownMints = new Set();
const MAX_HISTORY = 2000;

function getSocialLink(pair, type) {
    if (!pair.info || !pair.info.socials) return null;
    const social = pair.info.socials.find(s => s.type === type);
    return social ? social.url : null;
}

async function checkNewTokens(deps) {
    try {
        // Search generically for "pump" related or just latest profiles to cast a wide net
        // Note: DexScreener "latest" endpoint isn't fully public/documented for broad scanning without filters.
        // We continue using a broad search term or specific chain filter if possible. 
        // Using "pump" search is still effective for finding meme tokens, but we filter purely by stats now.
        const response = await axios.get('https://api.dexscreener.com/latest/dex/search?q=pump', {
            timeout: 5000
        });

        const pairs = response.data.pairs;
        if (!pairs) return;

        // Sort by creation time (Newest first)
        pairs.sort((a, b) => (b.pairCreatedAt || 0) - (a.pairCreatedAt || 0));

        let addedCount = 0;

        for (const pair of pairs) {
            const mint = pair.baseToken.address;

            // 1. Skip non-Solana
            if (pair.chainId !== 'solana') continue;

            // 2. Skip if already tracked
            if (knownMints.has(mint)) continue;

            // 3. STATS FILTERS (Strict)
            const liquidity = pair.liquidity?.usd || 0;
            const mcap = pair.fdv || pair.marketCap || 0;

            if (mcap < MIN_MARKET_CAP || liquidity < MIN_LIQUIDITY) {
                continue; // Skip silently if below thresholds
            }

            // 4. Map Data
            const metadata = {
                ticker: pair.baseToken.symbol,
                name: pair.baseToken.name,
                description: `Discovered via Listener (${pair.dexId})`,
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

            const createdAt = pair.pairCreatedAt || Date.now();
            
            await saveTokenData(null, mint, metadata, createdAt);
            
            knownMints.add(mint);
            addedCount++;
            
            logger.info(`💎 HIGH VALUE DETECT: ${pair.baseToken.symbol} on ${pair.dexId} | MC: $${Math.floor(mcap)} | Liq: $${Math.floor(liquidity)}`);
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
    logger.info("🚀 New Token Listener started (Threshold Mode: MC > 25k, Liq > 5k)");
    checkNewTokens(deps);
    setInterval(() => checkNewTokens(deps), 30000); // Check every 30s
}

module.exports = { start };
