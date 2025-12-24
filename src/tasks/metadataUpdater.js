/**
 * Metadata Updater (Global Version)
 * Runs on ALL tokens in the database to ensure nothing is missed.
 * Updates: Prices, Volume, Market Cap, Change, and Timestamp.
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
    const sortedPairs = [...pairs].sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0));

    // 2. Identify specific pools
    const pumpPair = sortedPairs.find(p => p.dexId === 'pump');
    const raydiumPair = sortedPairs.find(p => p.dexId === 'raydium');

    // --- RULE 3: Default / Other Tokens ---
    return sortedPairs[0];
}

// Robust Fetch with Retry
async function fetchWithRetry(url, retries = 3) {
    for (let i = 0; i < retries; i++) {
        try {
            return await axios.get(url, { timeout: 10000 });
        } catch (e) {
            const isRateLimit = e.response && e.response.status === 429;
            const isLastAttempt = i === retries - 1;

            if (isRateLimit) {
                logger.warn(`⚠️ DexScreener Rate Limit (429). Retrying in 5s... (Attempt ${i + 1}/${retries})`);
                await delay(5000 * (i + 1));
            } else if (e.code === 'ECONNABORTED') {
                logger.warn(`⚠️ DexScreener Timeout. Retrying... (Attempt ${i + 1}/${retries})`);
                await delay(2000);
            } else {
                if (isLastAttempt) throw e;
                await delay(1000);
            }
            
            if (isLastAttempt) throw e;
        }
    }
}

/**
 * Shared logic to update a single token's data in the DB
 */
async function syncTokenData(deps, mint, pairs) {
    const { db } = deps;
    if (!pairs || pairs.length === 0) return;

    // A. Calculate Aggregate Volume (Sum of ALL pools)
    const totalVolume = pairs.reduce((sum, p) => sum + (Number(p.volume?.h24) || 0), 0);

    // B. Select "Best Pair" for Price & Changes
    const bestPair = getBestPair(pairs, mint);
    if (!bestPair) return;

    // Prepare Data
    const marketCap = Number(bestPair.fdv || bestPair.marketCap || 0);
    const priceUsd = Number(bestPair.priceUsd || 0);
    const change5m = Number(bestPair.priceChange?.m5 || 0);
    const change1h = Number(bestPair.priceChange?.h1 || 0);
    const change24h = Number(bestPair.priceChange?.h24 || 0);
    const pairCreatedAt = bestPair.pairCreatedAt;
    const imageUrl = bestPair.info?.imageUrl;

    // C. Construct Query
    const setClauses = [];
    const queryParams = [];
    const addParam = (val) => { queryParams.push(val); return `$${queryParams.length}`; };

    setClauses.push(`volume24h = ${addParam(totalVolume)}`);
    setClauses.push(`marketCap = ${addParam(marketCap)}`);
    setClauses.push(`priceUsd = ${addParam(priceUsd)}`);
    setClauses.push(`change5m = ${addParam(change5m)}`);
    setClauses.push(`change1h = ${addParam(change1h)}`);
    setClauses.push(`change24h = ${addParam(change24h)}`);
    setClauses.push(`lastUpdated = ${addParam(Date.now())}`);

    if (imageUrl) setClauses.push(`image = ${addParam(imageUrl)}`);
    
    // Only update timestamp if it looks valid
    if (pairCreatedAt && pairCreatedAt < Date.now() + 86400000) {
        setClauses.push(`timestamp = ${addParam(pairCreatedAt)}`);
    }

    const whereClause = `WHERE mint = ${addParam(mint)}`;
    const fullQuery = `UPDATE tokens SET ${setClauses.join(', ')} ${whereClause}`;

    await db.run(fullQuery, queryParams);
}

async function updateMetadata(deps) {
    const { db, globalState } = deps;
    
    // 1. Select ALL tokens (Removed all filters)
    let tokens = [];
    try {
        tokens = await db.all(`SELECT mint FROM tokens`);
    } catch (e) {
        logger.error("❌ DB Error fetching token list", { error: e.message });
        return;
    }

    if (!tokens || tokens.length === 0) {
        logger.info("ℹ️ Metadata Updater: No tokens in database to update.");
        return;
    }

    logger.info(`🔄 Metadata Updater: Starting update for ALL ${tokens.length} tokens...`);

    const chunks = chunkArray(tokens, 30); // 30 is safe for DexScreener
    let processed = 0;

    for (const chunk of chunks) {
        const mints = chunk.map(t => t.mint).join(',');
        
        try {
            const dexRes = await fetchWithRetry(`https://api.dexscreener.com/latest/dex/tokens/${mints}`);
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
                return syncTokenData(deps, t.mint, pairs);
            });

            await Promise.all(updatePromises);
            
            processed += chunk.length;
            // Respect Rate Limits (approx 50-60 requests per minute)
            await delay(1100);

        } catch (e) {
            logger.error(`❌ DexScreener Batch Failed (Skipping ${chunk.length} tokens): ${e.message}`);
        }
    }

    globalState.lastBackendUpdate = Date.now();
    logger.info(`✅ Metadata Updater: Completed. Updated ${processed}/${tokens.length} tokens.`);
}

function start(deps) {
    // Run immediately after 5s
    setTimeout(() => updateMetadata(deps), 5000);
    // Then run on interval
    setInterval(() => updateMetadata(deps), config.METADATA_UPDATE_INTERVAL);
    logger.info(`🚀 Metadata Updater started on ALL tokens (Interval: ${config.METADATA_UPDATE_INTERVAL / 60000}m)`);
}

module.exports = { updateMetadata, start, syncTokenData };
