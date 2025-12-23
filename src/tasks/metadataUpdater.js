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
                
                // Get most liquid pair
                if (!existing || (pair.liquidity?.usd > existing.liquidity)) {
                    updates.set(mint, {
                        marketCap: pair.fdv || pair.marketCap || 0,
                        volume24h: pair.volume?.h24 || 0,
                        priceUsd: pair.priceUsd || 0,
                        liquidity: pair.liquidity?.usd || 0,
                        imageUrl: pair.info?.imageUrl,
                        change5m: pair.priceChange?.m5 || 0,
                        change1h: pair.priceChange?.h1 || 0,
                        change24h: pair.priceChange?.h24 || 0,
                        // Fix Age: Use pairCreatedAt from DexScreener
                        pairCreatedAt: pair.pairCreatedAt 
                    });
                }
            }

            // OPTIMIZATION: Execute updates in parallel for this chunk
            const updatePromises = chunk.map(t => {
                const data = updates.get(t.mint);
                if (!data) return Promise.resolve(); // Skip if no data

                // We update 'timestamp' with 'pairCreatedAt' if available to ensure "Age" is accurate
                const updateQuery = `
                    UPDATE tokens SET 
                    volume24h = $1, 
                    marketCap = $2, 
                    priceUsd = $3, 
                    change5m = $4, 
                    change1h = $5, 
                    change24h = $6, 
                    lastUpdated = $7,
                    timestamp = COALESCE($10, timestamp)
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
                else params.push(null); // Placeholder if we need to keep index alignment, but here we construct query dynamic-ish.
                // Wait, simplified:
                // If data.imageUrl is present, params has 9 items. $10 is the timestamp.
                // If NOT present, params has 8 items. timestamp is $9.
                // Let's rewrite the query construction to be safer.
                
                // RE-WRITING QUERY CONSTRUCTION FOR SAFETY
                let queryParts = [
                    "volume24h = $1", "marketCap = $2", "priceUsd = $3", 
                    "change5m = $4", "change1h = $5", "change24h = $6", 
                    "lastUpdated = $7"
                ];
                let queryParams = [
                    data.volume24h, data.marketCap, data.priceUsd, 
                    data.change5m, data.change1h, data.change24h, Date.now()
                ];
                let paramIdx = 8;

                if (data.imageUrl) {
                    queryParts.push(`image = $${paramIdx++}`);
                    queryParams.push(data.imageUrl);
                }

                if (data.pairCreatedAt) {
                    queryParts.push(`timestamp = $${paramIdx++}`);
                    queryParams.push(data.pairCreatedAt);
                }

                queryParams.push(t.mint); // The WHERE clause param

                const safeQuery = `UPDATE tokens SET ${queryParts.join(', ')} WHERE mint = $${paramIdx}`;

                return db.run(safeQuery, queryParams);
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
    logger.info("Metadata updater started (Age Fix Mode)");
}

module.exports = { updateMetadata, start };
