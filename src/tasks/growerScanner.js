/**
 * Grower Scanner Task (Optimized)
 * -------------------
 * Scans 'pending_growers' Redis set for tokens that grew > $10k MCAP.
 * Pruning Rule: If > 10 minutes old AND < $10k mcap, drop it.
 */
const axios = require('axios');
const { getClient } = require('../services/redis');
const { getDB } = require('../services/database');
const { indexTokenOnChain } = require('../services/indexer');
const logger = require('../services/logger');

const PENDING_KEY = 'pending_growers';
const MIN_MCAP_USD = 10000; // Target Threshold
const PRUNE_AGE_MS = 10 * 60 * 1000; // 10 Minutes
const MAX_AGE_MS = 24 * 60 * 60 * 1000; // Hard Stop: 24 hours (Safety net)

// Capacity: 50 tokens * 12 runs/hour = 600 checks/hour
const BATCH_SIZE = 50; 

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
                await redis.srem(PENDING_KEY, memberStr);
                continue;
            }

            const { mint, addedAt } = data;
            const now = Date.now();
            const age = now - addedAt;

            // Safety Net: Hard Prune (24h)
            if (age > MAX_AGE_MS) {
                await redis.srem(PENDING_KEY, memberStr);
                continue;
            }

            try {
                // Throttle: 200ms wait before request to smooth load
                await new Promise(r => setTimeout(r, 200));

                const mcap = await checkMarketCap(mint);

                // 1. Promotion Logic (Grown enough)
                if (mcap >= MIN_MCAP_USD) {
                    logger.info(`🚀 GROWER PROMOTED: ${mint} (MCAP: $${mcap.toFixed(0)})`);
                    
                    await db.run(`
                        INSERT INTO tokens (mint, name, symbol, timestamp, k_score, marketCap) 
                        VALUES ($1, 'Growth Discovery', 'GROW', $2, 60, $3) 
                        ON CONFLICT (mint) DO NOTHING
                    `, [mint, Date.now(), mcap]);

                    await indexTokenOnChain(mint);
                    await redis.srem(PENDING_KEY, memberStr);
                } 
                // 2. Prune Logic (Too old & too small)
                // If token is older than 10 mins AND hasn't reached $10k, drop it.
                else if (age > PRUNE_AGE_MS && mcap < MIN_MCAP_USD) {
                    // logger.debug(`🗑️ Pruned failed grower: ${mint} (Age: ${(age/60000).toFixed(1)}m, Mcap: $${mcap})`);
                    await redis.srem(PENDING_KEY, memberStr);
                }
                
            } catch (err) {
                if (err.message === 'RATE_LIMIT') {
                    logger.warn("⚠️ Grower Scanner Rate Limited. Pausing batch.");
                    break; // Stop immediate batch, resume next interval
                }
            }
        }

    } catch (e) {
        logger.error(`Grower Scanner Error: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    logger.info("🟢 Grower Scanner Started (5m interval).");
    setInterval(() => scanGrowers(deps), 5 * 60 * 1000);
    // Initial delay to let server settle
    setTimeout(() => scanGrowers(deps), 15000);
}

module.exports = { start };
