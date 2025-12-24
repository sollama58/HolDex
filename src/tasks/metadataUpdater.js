/**
 * Metadata Updater (Stabilized Version)
 * Implements Pagination & Locking to handle high concurrency.
 */
const axios = require('axios');
const config = require('../config/env');
const { logger } = require('../services');

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// --- LOCKING MECHANISM ---
// Prevents multiple update cycles from overlapping and crashing memory
let isRunning = false;

// Helper: Batch fetch with retry
async function fetchWithRetry(url, retries = 3) {
    for (let i = 0; i < retries; i++) {
        try {
            return await axios.get(url, { timeout: 10000 });
        } catch (e) {
            if (i === retries - 1) throw e;
            await delay(1000 * (i + 1));
        }
    }
}

// Logic to determine best pair (kept from previous version)
function getBestPair(pairs, mint) {
    if (!pairs || pairs.length === 0) return null;
    const sortedPairs = [...pairs].sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0));
    const pumpPair = sortedPairs.find(p => p.dexId === 'pump');
    const raydiumPair = sortedPairs.find(p => p.dexId === 'raydium');
    
    if (mint === 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' && raydiumPair) return raydiumPair;
    if (pumpPair) return pumpPair;
    return sortedPairs[0];
}

async function syncTokenData(deps, mint, pairs) {
    const { db } = deps;
    if (!pairs || pairs.length === 0) return;

    const totalVolume = pairs.reduce((sum, p) => sum + (Number(p.volume?.h24) || 0), 0);
    const bestPair = getBestPair(pairs, mint);
    if (!bestPair) return;

    const query = `
        UPDATE tokens SET 
        volume24h = $1, marketCap = $2, priceUsd = $3, 
        change5m = $4, change1h = $5, change24h = $6, 
        lastUpdated = $7 
        WHERE mint = $8
    `;

    const params = [
        totalVolume,
        Number(bestPair.fdv || bestPair.marketCap || 0),
        Number(bestPair.priceUsd || 0),
        Number(bestPair.priceChange?.m5 || 0),
        Number(bestPair.priceChange?.h1 || 0),
        Number(bestPair.priceChange?.h24 || 0),
        Date.now(),
        mint
    ];

    await db.run(query, params);
}

/**
 * Main Update Function (Paged & Locked)
 */
async function updateMetadata(deps) {
    // 1. Check Lock
    if (isRunning) {
        logger.warn("⚠️ Metadata Updater: Previous cycle still active. Skipping this run.");
        return;
    }
    isRunning = true;

    const { db, globalState } = deps;
    const BATCH_SIZE = 50; // DexScreener supports up to 30 officially, but 50 usually works or we can split
    let offset = 0;
    let hasMore = true;
    let totalProcessed = 0;

    logger.info("🔄 Metadata Updater: Starting paginated cycle...");

    try {
        while (hasMore) {
            // 2. Fetch Batch from DB (Pagination)
            // This prevents loading 10,000 tokens into memory at once
            const tokens = await db.all(`SELECT mint FROM tokens LIMIT $1 OFFSET $2`, [BATCH_SIZE, offset]);

            if (!tokens || tokens.length === 0) {
                hasMore = false;
                break;
            }

            // 3. Process Batch
            const mints = tokens.map(t => t.mint).join(',');
            
            try {
                // We request 50, but DexScreener might want fewer. 
                // If this fails often, reduce BATCH_SIZE to 30.
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
                logger.error(`❌ Batch Error (Offset ${offset}): ${e.message}`);
            }

            // 4. Next Batch & Rate Limit
            totalProcessed += tokens.length;
            offset += BATCH_SIZE;
            
            // Wait 1.1s to respect DexScreener rate limits (approx 50-60 calls/min)
            await delay(1100);
        }

        globalState.lastBackendUpdate = Date.now();
        logger.info(`✅ Metadata Updater: Cycle complete. Processed ${totalProcessed} tokens.`);

    } catch (fatalError) {
        logger.error(`🔥 Metadata Updater FATAL: ${fatalError.message}`);
    } finally {
        // 5. Release Lock
        isRunning = false;
    }
}

function start(deps) {
    setTimeout(() => updateMetadata(deps), 5000);
    setInterval(() => updateMetadata(deps), config.METADATA_UPDATE_INTERVAL);
    logger.info(`🚀 Metadata Updater started (Interval: ${config.METADATA_UPDATE_INTERVAL / 60000}m)`);
}

module.exports = { updateMetadata, start, syncTokenData };
