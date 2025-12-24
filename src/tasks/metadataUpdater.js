/**
 * Metadata Updater (Smart Priority Version)
 * Optimizes API usage by prioritizing active tokens over dead ones.
 * Updates: Prices, Volume, Market Cap, AND Creation Timestamp (Age).
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

    // 1. Select candidates (New or Active or High Cap)
    const ONE_DAY_MS = 24 * 60 * 60 * 1000;
    const yesterday = Date.now() - ONE_DAY_MS;

    let tokens = [];
    try {
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
                
                // Safe values (default to 0 to prevent comparison errors)
                const pairLiq = pair.liquidity?.usd || 0;
                const pairVol = pair.volume?.h24 || 0;
                
                if (!existing) {
                    updates.set(mint, {
                        marketCap: pair.fdv || pair.marketCap || 0,
                        volume24h: pairVol, // Start with this pair's volume
                        priceUsd: pair.priceUsd || 0,
                        liquidity: pairLiq,
                        imageUrl: pair.info?.imageUrl,
                        change5m: pair.priceChange?.m5 || 0,
                        change1h: pair.priceChange?.h1 || 0,
                        change24h: pair.priceChange?.h24 || 0,
                        pairCreatedAt: pair.pairCreatedAt 
                    });
                } else {
                    // 1. Aggregate Volume (Sum volume from all pools)
                    existing.volume24h += pairVol;

                    // 2. Select Largest Liquidity Pool for Price/Stats
                    // We check strictly if this pair is bigger than the stored one
                    if (pairLiq > existing.liquidity) {
                        existing.liquidity = pairLiq;
                        existing.marketCap = pair.fdv || pair.marketCap || 0;
                        existing.priceUsd = pair.priceUsd || 0;
                        existing.imageUrl = pair.info?.imageUrl || existing.imageUrl;
                        existing.change5m = pair.priceChange?.m5 || 0;
                        existing.change1h = pair.priceChange?.h1 || 0;
                        existing.change24h = pair.priceChange?.h24 || 0;
                        existing.pairCreatedAt = pair.pairCreatedAt;
                        // Note: We do NOT overwrite volume24h here, we aggregated it above.
                    }
                }
            }

            // OPTIMIZATION: Execute updates in parallel for this chunk
            const updatePromises = chunk.map(t => {
                const data = updates.get(t.mint);
                if (!data) return Promise.resolve(); // Skip if no data

                // FIX: Dynamic Query Construction using array push for robustness
                const setClauses = [];
                const queryParams = [];

                // Helper to add param
                const addParam = (val) => {
                    queryParams.push(val);
                    return `$${queryParams.length}`;
                };

                setClauses.push(`volume24h = ${addParam(data.volume24h)}`);
                setClauses.push(`marketCap = ${addParam(data.marketCap)}`);
                setClauses.push(`priceUsd = ${addParam(data.priceUsd)}`);
                setClauses.push(`change5m = ${addParam(data.change5m)}`);
                setClauses.push(`change1h = ${addParam(data.change1h)}`);
                setClauses.push(`change24h = ${addParam(data.change24h)}`);
                setClauses.push(`lastUpdated = ${addParam(Date.now())}`);

                // Conditional updates
                if (data.imageUrl) {
                    setClauses.push(`image = ${addParam(data.imageUrl)}`);
                }
                
                if (data.pairCreatedAt) {
                    // Update timestamp (Age) if we have data from DexScreener
                    setClauses.push(`timestamp = ${addParam(data.pairCreatedAt)}`);
                }

                // WHERE clause
                const whereClause = `WHERE mint = ${addParam(t.mint)}`;
                
                const fullQuery = `UPDATE tokens SET ${setClauses.join(', ')} ${whereClause}`;

                return db.run(fullQuery, queryParams);
            });

            await Promise.all(updatePromises);
            
            await delay(1100);

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
    setTimeout(() => updateMetadata(deps), 5000);
    setInterval(() => updateMetadata(deps), config.METADATA_UPDATE_INTERVAL);
    logger.info("Metadata updater started (Global Volume + Best Pool Mode)");
}

module.exports = { updateMetadata, start };
