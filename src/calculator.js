#!/usr/bin/env node
/**
 * HolDex Calculator - The Brain
 *
 * Standalone service that runs ALL data processing tasks:
 * 1. K-Score calculation (conviction scoring)
 * 2. Metadata updates (price/volume/liquidity/holders)
 * 3. Grower scanning (promote tokens reaching $10k mcap)
 *
 * Philosophy: $asdfasdfa - just ship it, do the work.
 *
 * Usage:
 *   DATABASE_URL=... HELIUS_API_KEY=... node src/calculator.js
 */

require('dotenv').config();
const { Pool } = require('pg');
const axios = require('axios');
const logger = require('./services/logger');
const config = require('./config/env');

// ============================================
// DATABASE CONNECTION
// ============================================

const pool = new Pool({
    connectionString: config.DATABASE_URL,
    ssl: config.DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false },
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000
});

const db = {
    get: (q, p) => pool.query(q, p).then(r => r.rows[0]),
    all: (q, p) => pool.query(q, p).then(r => r.rows),
    run: (q, p) => pool.query(q, p)
};

// ============================================
// TASK MODULES
// ============================================

const kScoreUpdater = require('./tasks/kScoreUpdater');

// ============================================
// STATE
// ============================================

let isRunning = true;
let stats = {
    kscoreUpdates: 0,
    metadataUpdates: 0,
    growersPromoted: 0,
    errors: 0,
    cycles: 0
};

// ============================================
// TASK 1: K-SCORE UPDATER
// ============================================

function logMemory() {
    const used = process.memoryUsage();
    const mb = (bytes) => (bytes / 1024 / 1024).toFixed(1);
    logger.info(`[Memory] RSS: ${mb(used.rss)}MB | Heap: ${mb(used.heapUsed)}/${mb(used.heapTotal)}MB`);
}

async function runKScoreTask() {
    logger.info('🧮 [K-Score] Starting calculation cycle...');
    logMemory();

    try {
        // Reduced batch size for memory efficiency
        const tokens = await db.all(`
            SELECT mint, symbol, k_score, holders
            FROM tokens
            WHERE hascommunityupdate = true
            ORDER BY volume24h DESC NULLS LAST
            LIMIT 20
        `);

        logger.info(`[K-Score] Processing ${tokens.length} verified tokens...`);

        for (const token of tokens) {
            if (!isRunning) break;

            try {
                const result = await kScoreUpdater.updateSingleToken({ db }, token.mint);
                if (result) {
                    stats.kscoreUpdates++;
                    logger.info(`  ✓ K-Score: ${token.symbol} ${token.k_score || '?'} → ${result.score}`);
                }
            } catch (err) {
                stats.errors++;
                logger.warn(`  ✗ K-Score ${token.symbol}: ${err.message}`);
            }

            await sleep(2000);

            // Help GC between tokens
            if (global.gc) try { global.gc(); } catch (_) {}
        }

        logMemory();
    } catch (err) {
        logger.error('[K-Score] Task error:', err.message);
    }
}

// ============================================
// TASK 2: METADATA UPDATER (Price/Volume/Liquidity)
// ============================================

async function fetchGeckoData(mint) {
    try {
        const [poolsRes, tokenRes] = await Promise.all([
            axios.get(`https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mint}/pools?page=1`, { timeout: 5000 }).catch(() => null),
            axios.get(`https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mint}`, { timeout: 5000 }).catch(() => null)
        ]);
        return {
            pools: poolsRes?.data?.data || [],
            token: tokenRes?.data?.data || null
        };
    } catch (e) {
        if (e.response?.status === 429) await sleep(10000);
        return { pools: [], token: null };
    }
}

async function updateTokenMetadata(token) {
    const now = Date.now();
    const { pools, token: tokenDetails } = await fetchGeckoData(token.mint);

    let totalVolume24h = 0;
    let totalLiquidity = 0;
    let bestPrice = 0;
    let bestChange24h = null;
    let bestChange1h = null;
    let bestChange5m = null;
    let maxLiquidity = -1;
    let earliestPoolTime = null;

    // Process pools
    for (const poolData of pools) {
        const attr = poolData.attributes;
        const rel = poolData.relationships;

        if (attr.pool_created_at) {
            const createdAt = new Date(attr.pool_created_at).getTime();
            if (!earliestPoolTime || createdAt < earliestPoolTime) {
                earliestPoolTime = createdAt;
            }
        }

        const address = attr.address;
        const dexId = rel?.dex?.data?.id || 'unknown';
        const price = parseFloat(attr.base_token_price_usd || 0);
        const liqUsd = parseFloat(attr.reserve_in_usd || 0);
        const vol24h = parseFloat(attr.volume_usd?.h24 || 0);

        let tokenA = rel?.base_token?.data?.id?.replace('solana_', '') || token.mint;
        let tokenB = rel?.quote_token?.data?.id?.replace('solana_', '') || 'So11111111111111111111111111111111111111112';

        totalVolume24h += vol24h;
        totalLiquidity += liqUsd;

        if (liqUsd > maxLiquidity) {
            maxLiquidity = liqUsd;
            bestPrice = price;
            bestChange24h = attr.price_change_percentage?.h24 ?? null;
            bestChange1h = attr.price_change_percentage?.h1 ?? null;
            bestChange5m = attr.price_change_percentage?.m5 ?? null;
        }

        // Upsert pool
        await db.run(`
            INSERT INTO pools (address, mint, dex, price_usd, liquidity_usd, volume_24h, created_at, token_a, token_b)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT(address) DO UPDATE SET
                price_usd = EXCLUDED.price_usd,
                liquidity_usd = EXCLUDED.liquidity_usd,
                volume_24h = EXCLUDED.volume_24h
        `, [address, token.mint, dexId, price, liqUsd, vol24h, now, tokenA, tokenB]);
    }

    // Market Cap
    let marketCap = 0;
    if (tokenDetails?.attributes) {
        marketCap = parseFloat(tokenDetails.attributes.fdv_usd || tokenDetails.attributes.market_cap_usd || 0);

        // Holder count from Gecko
        const geckoHolders = parseInt(tokenDetails.attributes.holder_count || tokenDetails.attributes.holders_count || 0);
        if (geckoHolders > 0) {
            await db.run(`UPDATE tokens SET holders = $1, last_holder_check = $2 WHERE mint = $3`, [geckoHolders, now, token.mint]);
        }
    }

    if (marketCap === 0 && bestPrice > 0) {
        const decimals = token.decimals || 9;
        let rawSupply = parseFloat(token.supply || '0') || (1000000000 * Math.pow(10, decimals));
        marketCap = (rawSupply / Math.pow(10, decimals)) * bestPrice;
    }

    // Build update query
    if (totalVolume24h > 0 || totalLiquidity > 0) {
        const updateParts = [];
        const params = [];
        let idx = 1;

        updateParts.push(`volume24h = $${idx++}`); params.push(totalVolume24h);
        updateParts.push(`marketCap = $${idx++}`); params.push(marketCap);
        updateParts.push(`priceUsd = $${idx++}`); params.push(bestPrice);
        updateParts.push(`liquidity = $${idx++}`); params.push(totalLiquidity);

        if (bestChange24h !== null) { updateParts.push(`change24h = $${idx++}`); params.push(parseFloat(bestChange24h)); }
        if (bestChange1h !== null) { updateParts.push(`change1h = $${idx++}`); params.push(parseFloat(bestChange1h)); }
        if (bestChange5m !== null) { updateParts.push(`change5m = $${idx++}`); params.push(parseFloat(bestChange5m)); }

        if (earliestPoolTime) {
            const currentTs = parseInt(token.timestamp) || 0;
            if (currentTs === 0 || earliestPoolTime < currentTs) {
                updateParts.push(`timestamp = $${idx++}`); params.push(earliestPoolTime);
            }
        }

        updateParts.push(`updated_at = CURRENT_TIMESTAMP`);
        params.push(token.mint);

        await db.run(`UPDATE tokens SET ${updateParts.join(', ')} WHERE mint = $${idx}`, params);
        return true;
    }
    return false;
}

async function runMetadataTask() {
    logger.info('📊 [Metadata] Starting update cycle...');

    try {
        // Reduced batch for memory
        const tokens = await db.all(`
            SELECT mint, supply, decimals, holders, timestamp
            FROM tokens
            ORDER BY liquidity DESC NULLS LAST, updated_at ASC
            LIMIT 25
        `);

        logger.info(`[Metadata] Processing ${tokens.length} tokens...`);

        for (const token of tokens) {
            if (!isRunning) break;

            try {
                const updated = await updateTokenMetadata(token);
                if (updated) {
                    stats.metadataUpdates++;
                }
            } catch (err) {
                stats.errors++;
                logger.warn(`  ✗ Metadata ${token.mint.slice(0, 8)}: ${err.message}`);
            }

            await sleep(1200);
        }
    } catch (err) {
        logger.error('[Metadata] Task error:', err.message);
    }
}

// ============================================
// TASK 3: GROWER SCANNER (Promotion)
// ============================================

const MIN_MCAP_USD = 10000;

async function checkMarketCap(mint) {
    try {
        const res = await axios.get(`https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mint}`, { timeout: 5000 });
        const attrs = res.data?.data?.attributes;
        return parseFloat(attrs?.fdv_usd || attrs?.market_cap_usd || 0);
    } catch (e) {
        if (e.response?.status === 429) throw new Error('RATE_LIMIT');
        return 0;
    }
}

async function runGrowerTask() {
    logger.info('🌱 [Growers] Scanning for promotions...');

    try {
        // Get unverified tokens that might qualify
        const candidates = await db.all(`
            SELECT mint, symbol, marketcap, timestamp
            FROM tokens
            WHERE hascommunityupdate = false
            AND timestamp > $1
            ORDER BY marketcap DESC NULLS LAST
            LIMIT 30
        `, [Date.now() - 24 * 60 * 60 * 1000]); // Last 24h only

        logger.info(`[Growers] Checking ${candidates.length} candidates...`);

        for (const token of candidates) {
            if (!isRunning) break;

            try {
                await sleep(300);
                const mcap = await checkMarketCap(token.mint);

                if (mcap >= MIN_MCAP_USD) {
                    logger.info(`🚀 [PROMOTION] ${token.symbol || token.mint.slice(0, 8)} reached $${mcap.toLocaleString()}`);

                    await db.run(`
                        UPDATE tokens SET
                            k_score = GREATEST(COALESCE(k_score, 0), 15),
                            marketcap = $1,
                            hascommunityupdate = false,
                            updated_at = NOW()
                        WHERE mint = $2
                    `, [mcap, token.mint]);

                    stats.growersPromoted++;
                }
            } catch (err) {
                if (err.message === 'RATE_LIMIT') {
                    logger.warn('⚠️ Grower rate limited, pausing...');
                    await sleep(10000);
                    break;
                }
            }
        }
    } catch (err) {
        logger.error('[Growers] Task error:', err.message);
    }
}

// ============================================
// TASK 4: DEEP REFRESH (Daily)
// ============================================

async function runDeepRefresh() {
    logger.info('🔬 [DeepRefresh] Starting daily deep refresh...');

    try {
        const tokens = await db.all(`
            SELECT mint, symbol
            FROM tokens
            WHERE hascommunityupdate = true
            LIMIT 50
        `);

        for (const token of tokens) {
            if (!isRunning) break;

            try {
                await kScoreUpdater.updateSingleToken({ db, forceDeepRefresh: true }, token.mint);
                logger.info(`  🔬 ${token.symbol}: Deep refresh done`);
            } catch (err) {
                logger.warn(`  ✗ ${token.symbol}: ${err.message}`);
            }

            await sleep(2500);
        }
    } catch (err) {
        logger.error('[DeepRefresh] Task error:', err.message);
    }
}

// ============================================
// ORCHESTRATOR
// ============================================

const CYCLE_INTERVAL = 60 * 60 * 1000;  // 1 hour between full cycles
const METADATA_INTERVAL = 5 * 60 * 1000; // 5 min for metadata
const GROWER_INTERVAL = 10 * 60 * 1000;  // 10 min for growers

async function runFullCycle() {
    stats.cycles++;
    logger.info(`\n${'='.repeat(60)}`);
    logger.info(`🧠 [BRAIN] Starting cycle #${stats.cycles}`);
    logger.info(`${'='.repeat(60)}\n`);

    // DIAGNOSTIC: Just test basic tasks first
    try {
        const count = await db.get('SELECT COUNT(*) as c FROM tokens');
        logger.info(`[DB Test] Token count: ${count.c}`);
    } catch (err) {
        logger.error('[DB Test] Failed:', err.message);
    }

    // Only run lightweight metadata for now
    await runMetadataTask();

    logMemory();
    logger.info(`\n📈 Cycle #${stats.cycles} complete | M:${stats.metadataUpdates} E:${stats.errors}\n`);
}

// ============================================
// MAIN
// ============================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
    console.log(`
╔═══════════════════════════════════════════════════════════════╗
║           HOLDEX CALCULATOR - THE BRAIN                       ║
║           Philosophy: $asdfasdfa                              ║
╠═══════════════════════════════════════════════════════════════╣
║  Tasks:                                                       ║
║    1. K-Score     - Conviction scoring (hourly)               ║
║    2. Metadata    - Price/Volume/Liquidity (5min)             ║
║    3. Growers     - Token promotion (10min)                   ║
║    4. DeepRefresh - Full RPC refresh (daily)                  ║
╠═══════════════════════════════════════════════════════════════╣
║  DB: ${(config.DATABASE_URL.split('@')[1]?.split('/')[0] || 'localhost').padEnd(52)}║
║  Helius: ${(config.HELIUS_API_KEY ? '✓ configured' : '✗ missing').padEnd(48)}║
╚═══════════════════════════════════════════════════════════════╝
    `);

    // Verify DB
    try {
        const result = await db.get('SELECT COUNT(*) as total, COUNT(CASE WHEN hascommunityupdate = true THEN 1 END) as verified FROM tokens');
        logger.info(`✅ Connected to DB. ${result.verified}/${result.total} tokens (${result.verified} verified).`);
    } catch (err) {
        logger.error('❌ DB connection failed:', err.message);
        process.exit(1);
    }

    // Graceful shutdown
    const shutdown = () => {
        logger.info('🛑 [Brain] Shutting down...');
        isRunning = false;
        setTimeout(() => process.exit(0), 2000);
    };
    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);

    // Wrap async tasks to prevent unhandled rejections
    const safeRun = (fn, name) => async () => {
        if (!isRunning) return;
        try {
            await fn();
        } catch (err) {
            logger.error(`[${name}] Unhandled error:`, err.message);
            stats.errors++;
        }
    };

    // Initial run after 10s
    setTimeout(() => safeRun(runFullCycle, 'FullCycle')(), 10000);

    // DIAGNOSTIC: Only run the full cycle, no parallel tasks
    setInterval(() => safeRun(runFullCycle, 'FullCycle')(), CYCLE_INTERVAL);

    // Disabled for diagnostics
    // setInterval(() => safeRun(runMetadataTask, 'Metadata')(), METADATA_INTERVAL);
    // setInterval(() => safeRun(runGrowerTask, 'Growers')(), GROWER_INTERVAL);
    // setTimeout(() => {
    //     setInterval(() => safeRun(runDeepRefresh, 'DeepRefresh')(), 24 * 60 * 60 * 1000);
    // }, 6 * 60 * 60 * 1000);

    logger.info('🧠 [Brain] Calculator started. First cycle in 10 seconds...');
}

// Global error handlers to prevent crashes
process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled Rejection:', reason);
});

process.on('uncaughtException', (err) => {
    logger.error('Uncaught Exception:', err.message);
    // Don't exit - keep running
});

main().catch(err => {
    logger.error('Fatal error:', err);
    process.exit(1);
});
