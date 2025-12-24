/**
 * Metadata Updater (Stabilized Version)
 * Implements Pagination & Locking to handle high concurrency.
 * UPDATED: Strict logic to pull price/mcap from Largest Liquidity Pool.
 * UPDATED: Reduced Batch Size to prevent 400 Errors from DexScreener.
 */
const axios = require('axios');
const config = require('../config/env');
const { logger } = require('../services');

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// --- LOCKING MECHANISM ---
let isRunning = false;

// Helper: Batch fetch with retry
async function fetchWithRetry(url, retries = 3) {
    for (let i = 0; i < retries; i++) {
        try {
            return await axios.get(url, { timeout: 10000 });
        } catch (e) {
            // If 400, do not retry, it's a bad request (too many mints or invalid format)
            if (e.response && e.response.status === 400) throw e;
            
            if (i === retries - 1) throw e;
            await delay(1000 * (i + 1));
        }
    }
}

// Logic to determine the best pair for Price/MCap data
function getBestPair(pairs, mint) {
    if (!pairs || pairs.length === 0) return null;

    // 1. Sort all pairs by LIQUIDITY (USD) Descending
    // This is the most reliable way to get the "real" price.
    // We filter out pairs with very low liquidity to avoid noise.
    const validPairs = pairs.filter(p => (p.liquidity?.usd || 0) > 100);
    
    // Fallback to all pairs if everything is low liquidity (e.g. new launch)
    const candidates = validPairs.length > 0 ? validPairs : pairs;

    const sortedPairs = candidates.sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0));

    // --- RULE 1: BONK Token Exception (Legacy) ---
    // Keep specific rule if requested, otherwise rely on liquidity.
    const raydiumPair = sortedPairs.find(p => p.dexId === 'raydium');
    if (mint === 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' && raydiumPair) {
        return raydiumPair;
    }
    
    return sortedPairs[0];
}

async function syncTokenData(deps, mint, pairs) {
    const { db } = deps;
    if (!pairs || pairs.length === 0) return;

    // A. Calculate Aggregate Volume (Sum of ALL pools)
    // Volume is global, so we sum it up.
    const totalVolume = pairs.reduce((sum, p) => sum + (Number(p.volume?.h24) || 0), 0);

    // B. Select "Best Pair" for Price & Market Cap
    const bestPair = getBestPair(pairs, mint);
    
    if (!bestPair) return;

    // Prepare Data
    // Use FDV (Fully Diluted Valuation) as Market Cap standard for Solana tokens
    // Fallback to marketCap property if FDV is missing.
    const marketCap = Number(bestPair.fdv || bestPair.marketCap || 0);
    const priceUsd = Number(bestPair.priceUsd || 0);
    
    const change5m = Number(bestPair.priceChange?.m5 || 0);
    const change1h = Number(bestPair.priceChange?.h1 || 0);
    const change24h = Number(bestPair.priceChange?.h24 || 0);

    const query = `
        UPDATE tokens SET 
        volume24h = $1, marketCap = $2, priceUsd = $3, 
        change5m = $4, change1h = $5, change24h = $6, 
        lastUpdated = $7 
        WHERE mint = $8
    `;

    const params = [
        totalVolume,
        marketCap,
        priceUsd,
        change5m,
        change1h,
        change24h,
        Date.now(),
        mint
    ];

    await db.run(query, params);
}

/**
 * Main Update Function (Paged & Locked)
 */
async function updateMetadata(deps) {
    if (isRunning) {
        logger.warn("⚠️ Metadata Updater: Previous cycle still active. Skipping this run.");
        return;
    }
    isRunning = true;

    const { db, globalState } = deps;
    
    // FIX: Reduced from 50 to 30. DexScreener API limits the /tokens/ endpoint to 30 addresses.
    const BATCH_SIZE = 30; 
    
    let offset = 0;
    let hasMore = true;
    let totalProcessed = 0;

    logger.info("🔄 Metadata Updater: Starting paginated cycle...");

    try {
        while (hasMore) {
            const tokens = await db.all(`SELECT mint FROM tokens LIMIT $1 OFFSET $2`, [BATCH_SIZE, offset]);

            if (!tokens || tokens.length === 0) {
                hasMore = false;
                break;
            }

            const mints = tokens.map(t => t.mint).join(',');
            
            try {
                const dexRes = await fetchWithRetry(`https://api.dexscreener.com/latest/dex/tokens/${mints}`);
                const pairsData = dexRes.data?.pairs || [];
                
                const pairsByMint = {};
                for (const pair of pairsData) {
                    const m = pair.baseToken.address;
                    if (!pairsByMint[m]) pairsByMint[m] = [];
                    pairsByMint[m].push(pair);
                }

                const updatePromises = tokens.map(t => {
                    return syncTokenData(deps, t.mint, pairsByMint[t.mint] || []);
                });

                await Promise.all(updatePromises);

            } catch (e) {
                logger.error(`❌ Batch Error (Offset ${offset}): ${e.message}`, {
                    status: e.response?.status,
                    data: e.response?.data
                });
            }

            totalProcessed += tokens.length;
            offset += BATCH_SIZE;
            
            await delay(1100);
        }

        globalState.lastBackendUpdate = Date.now();
        logger.info(`✅ Metadata Updater: Cycle complete. Processed ${totalProcessed} tokens.`);

    } catch (fatalError) {
        logger.error(`🔥 Metadata Updater FATAL: ${fatalError.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    setTimeout(() => updateMetadata(deps), 5000);
    setInterval(() => updateMetadata(deps), config.METADATA_UPDATE_INTERVAL);
    logger.info(`🚀 Metadata Updater started (Interval: ${config.METADATA_UPDATE_INTERVAL / 60000}m)`);
}

module.exports = { updateMetadata, start, syncTokenData };
