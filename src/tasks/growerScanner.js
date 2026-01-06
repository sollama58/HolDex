/**
 * Grower Scanner Task
 * -------------------
 * Scans 'pending_growers' Redis set.
 * - Checks MCAP via GeckoTerminal.
 * - Promotion: If MCAP >= $10,000 -> INSERT into DB.
 */
const axios = require('axios');
const { getClient } = require('../services/redis');
const { getDB } = require('../services/database');
const { indexTokenOnChain } = require('../services/indexer');
const logger = require('../services/logger');

const PENDING_KEY = 'pending_growers';
const MIN_MCAP_USD = 10000;         
const PRUNE_AGE_MS = 10 * 60 * 1000; 
const MAX_AGE_MS = 24 * 60 * 60 * 1000; 

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

        logger.info(`🌱 Scanner: Evaluating ${members.length} pending candidates...`);

        let promotedCount = 0;

        for (const memberStr of members) {
            let data;
            try {
                data = JSON.parse(memberStr);
            } catch (_e) {
                await redis.srem(PENDING_KEY, memberStr);
                continue;
            }

            const { mint, addedAt } = data;
            const now = Date.now();
            const age = now - addedAt;
            const _ageMins = (age / 60000).toFixed(1);

            // 1. Prune Very Old Tokens (24h+)
            if (age > MAX_AGE_MS) {
                logger.info(`🗑️ Pruning stale grower candidate: ${mint}`);
                await redis.srem(PENDING_KEY, memberStr);
                continue;
            }

            try {
                await new Promise(r => setTimeout(r, 200));

                const mcap = await checkMarketCap(mint);

                // 2. CHECK PROMOTION CONDITION
                if (mcap >= MIN_MCAP_USD) {
                    console.log(`\n==================================================`);
                    logger.info(`🚀 [GROWER PROMOTION] Token Qualified!`);
                    logger.info(`   🎯 Mint:   ${mint}`);
                    logger.info(`   CX Mcap:   $${mcap.toLocaleString()}`);
                    
                    // INSERT NEW RECORD (Score 15)
                    await db.run(`
                        INSERT INTO tokens (mint, name, symbol, timestamp, updated_at, k_score, marketCap, hasCommunityUpdate) 
                        VALUES ($1, 'Growth Discovery', 'GROW', $2, NOW(), 15, $3, FALSE) 
                        ON CONFLICT (mint) DO UPDATE SET
                            k_score = GREATEST(tokens.k_score, 15),
                            marketCap = $3,
                            updated_at = NOW()
                    `, [mint, Date.now(), mcap]);
                    
                    logger.info(`   ✅ Database: Inserted successfully`);

                    await indexTokenOnChain(mint);
                    logger.info(`   ✅ Indexer:  Triggered deep scan`);

                    await redis.srem(PENDING_KEY, memberStr);
                    console.log(`==================================================\n`);

                    promotedCount++;
                } 
                // 3. Early Prune (if waiting > 10 mins and still tiny mcap)
                else if (age > PRUNE_AGE_MS && mcap < (MIN_MCAP_USD * 0.1)) {
                    // Silent Prune
                    await redis.srem(PENDING_KEY, memberStr);
                }
                
            } catch (err) {
                if (err.message === 'RATE_LIMIT') {
                    logger.warn("⚠️ Scanner Rate Limited. Pausing.");
                    break; 
                }
            }
        }

        if (promotedCount > 0) {
            logger.info(`📈 Promoted ${promotedCount} tokens.`);
        }

    } catch (e) {
        logger.error(`Grower Scanner Error: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    logger.info("🟢 Grower Scanner Started (5m interval).");
    setTimeout(() => scanGrowers(deps), 5000);
    setInterval(() => scanGrowers(deps), 5 * 60 * 1000);
}

module.exports = { start };
