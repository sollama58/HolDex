/**
 * Metadata Updater Task (Postgres Syntax)
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

    // 1. Fetch tokens
    const tokens = await db.all('SELECT mint FROM tokens');
    if (!tokens || tokens.length === 0) return;

    // 2. BATCH FETCH MARKET DATA
    const chunks = chunkArray(tokens, 30);
    
    for (const chunk of chunks) {
        const mints = chunk.map(t => t.mint).join(',');
        
        try {
            const dexRes = await axios.get(
                `https://api.dexscreener.com/latest/dex/tokens/${mints}`,
                { timeout: 8000 }
            );

            const pairs = dexRes.data?.pairs || [];
            const updates = new Map();
            
            for (const pair of pairs) {
                const mint = pair.baseToken.address;
                const existing = updates.get(mint);
                
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

            for (const t of chunk) {
                const data = updates.get(t.mint);
                
                if (data) {
                    // Update - Postgres Syntax ($1, $2...)
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
                } else {
                    // Pump.fun Fallback
                    try {
                        await delay(300); 
                        const pumpRes = await axios.get(
                            `https://frontend-api.pump.fun/coins/${t.mint}`,
                            { timeout: 3000 }
                        );
                        if (pumpRes.data) {
                            const mcap = pumpRes.data.usd_market_cap || 0;
                            await db.run(
                                `UPDATE tokens SET marketCap = $1, lastUpdated = $2 WHERE mint = $3`,
                                [mcap, Date.now(), t.mint]
                            );
                        }
                    } catch (pumpErr) { /* Silent fail */ }
                }
            }
            await delay(1500);

        } catch (e) {
            if (e.response && e.response.status === 429) {
                logger.warn(`DexScreener Rate Limit (429). Pausing 30 seconds...`);
                await delay(30000);
            } else {
                logger.warn(`DexScreener Batch Error: ${e.message}`);
            }
        }
    }

    globalState.lastBackendUpdate = Date.now();
    logger.info(`Metadata update complete. Tokens scanned: ${tokens.length}`);
}

function start(deps) {
    setTimeout(() => updateMetadata(deps), 5000);
    setInterval(() => updateMetadata(deps), config.METADATA_UPDATE_INTERVAL);
    logger.info("Metadata updater started (Postgres + Redis)");
}

module.exports = { updateMetadata, start };
