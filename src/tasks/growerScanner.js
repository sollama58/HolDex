/**
 * Grower Scanner Task
 * -------------------
 * Scans the 'pending_growers' Redis set for tokens that were initially skipped
 * due to low market cap. If they have grown above the threshold, they are promoted
 * to the main index.
 */
const axios = require('axios');
const { getClient } = require('../services/redis');
const { getDB } = require('../services/database');
const { indexTokenOnChain } = require('../services/indexer');
const logger = require('../services/logger');

const PENDING_KEY = 'pending_growers';
const MIN_MCAP_USD = 20000;
const MAX_AGE_MS = 24 * 60 * 60 * 1000; // Stop tracking after 24 hours
const BATCH_SIZE = 10; // Check 10 tokens per cycle to respect rate limits

let isRunning = false;

async function checkMarketCap(mint) {
    try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mint}`;
        const res = await axios.get(url, { timeout: 5000 });
        const attrs = res.data?.data?.attributes;
        return parseFloat(attrs?.fdv_usd || attrs?.market_cap_usd || 0);
    } catch (e) {
        if (e.response && e.response.status === 429) {
            throw new Error('RATE_LIMIT');
        }
        return 0;
    }
}

async function scanGrowers(deps) {
    if (isRunning) return;
    isRunning = true;

    const redis = getClient();
    const db = deps.db || getDB();

    if (!redis) {
        logger.warn("⚠️ Grower Scanner: Redis unavailable.");
        isRunning = false;
        return;
    }

    try {
        // 1. Get random batch of pending tokens
        // SRANDMEMBER gets random members without removing them
        const members = await redis.srandmember(PENDING_KEY, BATCH_SIZE);
        
        if (!members || members.length === 0) {
            isRunning = false;
            return;
        }

        logger.info(`🌱 Scanner: Checking ${members.length} pending growers...`);

        for (const memberStr of members) {
            let data;
            try {
                data = JSON.parse(memberStr);
            } catch (e) {
                // Invalid JSON, remove it
                await redis.srem(PENDING_KEY, memberStr);
                continue;
            }

            const { mint, addedAt } = data;
            const now = Date.now();

            // 2. Prune if too old
            if (now - addedAt > MAX_AGE_MS) {
                await redis.srem(PENDING_KEY, memberStr);
                // logger.info(`🗑️ Pruned stale grower: ${mint}`);
                continue;
            }

            // 3. Check MCAP
            try {
                const mcap = await checkMarketCap(mint);

                if (mcap >= MIN_MCAP_USD) {
                    logger.info(`🚀 GROWER PROMOTED: ${mint} (MCAP: $${mcap.toFixed(0)})`);
                    
                    // Promote to DB
                    await db.run(`
                        INSERT INTO tokens (mint, name, symbol, timestamp, k_score, marketCap) 
                        VALUES ($1, 'Growth Discovery', 'GROW', $2, 60, $3) 
                        ON CONFLICT (mint) DO NOTHING
                    `, [mint, Date.now(), mcap]);

                    // Trigger Indexing
                    await indexTokenOnChain(mint);

                    // Remove from pending list
                    await redis.srem(PENDING_KEY, memberStr);
                } else {
                    // Still too small, keep in list
                    // logger.debug(`... ${mint} still small ($${mcap})`);
                }
            } catch (err) {
                if (err.message === 'RATE_LIMIT') {
                    logger.warn("⚠️ Grower Scanner Rate Limited. Pausing.");
                    break; // Stop batch processing
                }
            }
            
            // Nice delay between items
            await new Promise(r => setTimeout(r, 2000));
        }

    } catch (e) {
        logger.error(`Grower Scanner Error: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    logger.info("🟢 Grower Scanner Started (5m interval).");
    // Run every 5 minutes
    setInterval(() => scanGrowers(deps), 5 * 60 * 1000);
    // Run once on start
    setTimeout(() => scanGrowers(deps), 10000);
}

module.exports = { start };
