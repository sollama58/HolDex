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

// Helper to determine the best pair based on user rules
function getBestPair(pairs, mint) {
    if (!pairs || pairs.length === 0) return null;

    // 1. Sort all pairs by liquidity first (descending)
    // This ensures that if we select a specific DEX, we get the best pool on that DEX.
    const sortedPairs = [...pairs].sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0));

    // 2. Identify specific pools
    const pumpPair = sortedPairs.find(p => p.dexId === 'pump');
    const raydiumPair = sortedPairs.find(p => p.dexId === 'raydium');

    // --- RULE 1: BONK Token Exception ---
    // If Mint is BONK, always use Raydium
    if (mint === 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' && raydiumPair) {
        return raydiumPair;
    }

    // --- RULE 2: PumpFun Tokens ---
    // "For PumpFun tokens, always consider the PumpSwap pool."
    // If a PumpSwap pool exists, we prioritize it regardless of liquidity size compared to others.
    if (pumpPair) {
        return pumpPair;
    }

    // --- RULE 3: Default / Other Tokens ---
    // "For other tokens, consider the largest liquidity pool."
    return sortedPairs[0];
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

            const pairsData = dexRes.data?.pairs || [];
            
            // Group pairs by Mint ID
            const pairsByMint = {};
            for (const pair of pairsData) {
                const mint = pair.baseToken.address;
                if (!pairsByMint[mint]) pairsByMint[mint] = [];
                pairsByMint[mint].push(pair);
            }

            // Process each token in the chunk
            const updatePromises = chunk.map(t => {
                const pairs = pairsByMint[t.mint] || [];
                
                if (pairs.length === 0) return Promise.resolve(); // No data found

                // A. Calculate Aggregate Volume (Sum of ALL pools)
                const totalVolume = pairs.reduce((sum, p) => sum + (p.volume?.h24 || 0), 0);

                // B. Select "Best Pair" for Price & Changes
                const bestPair = getBestPair(pairs, t.mint);
                
                if (!bestPair) return Promise.resolve();

                // Prepare Data
                const marketCap = bestPair.fdv || bestPair.marketCap || 0;
                const priceUsd = bestPair.priceUsd || 0;
                const change5m = bestPair.priceChange?.m5 || 0;
                const change1h = bestPair.priceChange?.h1 || 0;
                const change24h = bestPair.priceChange?.h24 || 0;
                const pairCreatedAt = bestPair.pairCreatedAt;
                const imageUrl = bestPair.info?.imageUrl;

                // C. Construct Query
                const setClauses = [];
                const queryParams = [];
                const addParam = (val) => { queryParams.push(val); return `$${queryParams.length}`; };

                setClauses.push(`volume24h = ${addParam(totalVolume)}`); // Using Aggregate
                setClauses.push(`marketCap = ${addParam(marketCap)}`);    // Using Best Pair
                setClauses.push(`priceUsd = ${addParam(priceUsd)}`);      // Using Best Pair
                setClauses.push(`change5m = ${addParam(change5m)}`);      // Using Best Pair
                setClauses.push(`change1h = ${addParam(change1h)}`);      // Using Best Pair
                setClauses.push(`change24h = ${addParam(change24h)}`);    // Using Best Pair
                setClauses.push(`lastUpdated = ${addParam(Date.now())}`);

                if (imageUrl) setClauses.push(`image = ${addParam(imageUrl)}`);
                if (pairCreatedAt) setClauses.push(`timestamp = ${addParam(pairCreatedAt)}`);

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
    logger.info("Metadata updater started (Prioritized Mode: Bonk > Pump > Liq)");
}

module.exports = { updateMetadata, start };
