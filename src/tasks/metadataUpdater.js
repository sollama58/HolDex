/**
 * Metadata Updater (Smart Priority Version)
 * Optimizes API usage by prioritizing active tokens over dead ones.
 */
const axios = require('axios');
const config = require('../config/env');
const { logger } = require('../services');

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

function chunkArray(array, size) {
    const result = [];
    for (let i = 0; i < array.length; i += size) {
        result.push(array.slice(i, i + size));
    }
    return result;
}

async function updateMetadata(deps) {
    const { db, globalState } = deps;

    // --- SCALABILITY FIX ---
    // Instead of selecting ALL tokens, we select:
    // 1. New launches ( < 24h old)
    // 2. Active tokens (Volume > 0 in the last check)
    // 3. High Market Cap tokens (> $5k)
    // Dead tokens are ignored to prevent the loop from taking hours.
    
    const ONE_DAY_MS = 24 * 60 * 60 * 1000;
    const yesterday = Date.now() - ONE_DAY_MS;

    let tokens = [];
    try {
        // Postgres syntax
        tokens = await db.all(`
            SELECT mint FROM tokens 
            WHERE timestamp > $1 
            OR volume24h > 1000 
            OR marketCap > 5000
        `, [yesterday]);
    } catch (e) {
        logger.error("DB Error fetching update list", { error: e.message });
        return;
    }

    if (!tokens || tokens.length === 0) return;

    logger.info(`🔄 Updater: Refreshing ${tokens.length} active tokens...`);

    // DexScreener supports up to 30 mints per call
    const chunks = chunkArray(tokens, 30);
    
    for (const chunk of chunks) {
        const mints = chunk.map(t => t.mint).join(',');
        
        try {
            const dexRes = await axios.get(
                `https://api.dexscreener.com/latest/dex/tokens/${mints}`,
                { timeout: 8000 }
            );

            const pairs = dexRes.data?.pairs || [];
            
            // DexScreener returns multiple pairs per token (Raydium, Orca, etc).
            // We want the most liquid pair for each mint.
            const updates = new Map();
            
            for (const pair of pairs) {
                const mint = pair.baseToken.address;
                const existing = updates.get(mint);
                
                // If we haven't seen this mint in this batch, OR this pair has higher liquidity
                if (!existing || (pair.liquidity?.usd > existing.liquidity)) {
                    updates.set(mint, {
                        marketCap: pair.fdv || pair.marketCap || 0,
                        volume24h: pair.volume?.h24 || 0,
                        priceUsd: pair.priceUsd || 0,
                        liquidity: pair.liquidity?.usd || 0,
                        imageUrl: pair.info?.imageUrl,
                        change5m: pair.priceChange?.m5 || 0,
                        change1h: pair.priceChange?.h1 || 0,
                        change24h: pair.priceChange?.h24 || 0
                    });
                }
            }

            // Execute Updates
            for (const t of chunk) {
                const data = updates.get(t.mint);
                
                if (data) {
                    const updateQuery = `
                        UPDATE tokens SET 
                        volume24h = $1, 
                        marketCap = $2, 
                        priceUsd = $3, 
                        change5m = $4, 
                        change1h = $5, 
                        change24h = $6, 
                        lastUpdated = $7 
                        ${data.imageUrl ? ', image = $9' : ''} 
                        WHERE mint = $8
                    `;

                    const params = [
                        data.volume24h, 
                        data.marketCap, 
                        data.priceUsd,
                        data.change5m,
                        data.change1h,
                        data.change24h,
                        Date.now(),
                        t.mint
                    ];

                    if (data.imageUrl) params.push(data.imageUrl); // $9

                    await db.run(updateQuery, params);
                } 
                // Note: We removed the "Pump Fallback" here. 
                // If DexScreener doesn't have it, it likely has no volume, so we skip to save time.
            }
            
            // Rate Limit Protection
            // 300 requests per minute = 1 request every 200ms.
            // We are safer at 1 request every 1000ms.
            await delay(1000);

        } catch (e) {
            if (e.response && e.response.status === 429) {
                logger.warn(`DexScreener Rate Limit (429). Pausing...`);
                await delay(20000);
            } else {
                logger.warn(`DexScreener Batch Error: ${e.message}`);
            }
        }
    }

    globalState.lastBackendUpdate = Date.now();
}

function start(deps) {
    // Run 5 seconds after start
    setTimeout(() => updateMetadata(deps), 5000);
    // Then every interval (default 60s)
    setInterval(() => updateMetadata(deps), config.METADATA_UPDATE_INTERVAL);
    logger.info("Metadata updater started (Priority Mode)");
}

module.exports = { updateMetadata, start };
