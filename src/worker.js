require('dotenv').config();
const { initDB, getDB, enableIndexing, aggregateAndSaveToken } = require('./services/database');
const { getClient, connectRedis } = require('./services/redis');
const { findPoolsOnChain } = require('./services/pool_finder');
const { fetchTokenMetadata } = require('./utils/metaplex');
const { getSolanaConnection } = require('./services/solana');
const { PublicKey } = require('@solana/web3.js');
const logger = require('./services/logger');
const metadataUpdater = require('./tasks/metadataUpdater'); 

const QUEUE_KEY = 'token_queue';
const RETRY_KEY = 'token_queue:retry';
const MAX_RETRIES = 5;
let isRunning = false;

// OPTIMIZATION: Explicitly nullify large objects to help GC
async function processToken(mint, retryCount = 0) {
    const db = getDB();
    const connection = getSolanaConnection();

    logger.info(`⚙️ Worker: Processing ${mint}...`);

    try {
        let meta = await fetchTokenMetadata(mint);

        // Check if we got real metadata (not placeholder)
        const hasRealName = meta && meta.name && meta.name !== 'Unknown' && meta.name !== 'New Discovery' && meta.name.length > 0;
        const hasRealSymbol = meta && meta.symbol && meta.symbol !== 'UNK' && meta.symbol !== 'UNKNOWN';

        if (!hasRealName || !hasRealSymbol) {
            // Metadata not ready - re-queue for retry if under max retries
            if (retryCount < MAX_RETRIES) {
                const redis = getClient();
                if (redis) {
                    // Store retry data with incremented count
                    const retryData = JSON.stringify({ mint, retryCount: retryCount + 1 });
                    await redis.lpush(RETRY_KEY, retryData);
                    logger.info(`⏳ Worker: Re-queued ${mint.slice(0, 8)} for retry (${retryCount + 1}/${MAX_RETRIES}) - metadata not ready`);
                }
            } else {
                logger.warn(`⚠️ Worker: Giving up on ${mint.slice(0, 8)} after ${MAX_RETRIES} retries - metadata unavailable (name: ${meta?.name}, symbol: ${meta?.symbol})`);
            }
            return { success: false, reason: 'metadata_not_ready' };
        }

        let supply = '1000000000';
        let decimals = 9;
        try {
            const supplyInfo = await connection.getTokenSupply(new PublicKey(mint));
            supply = supplyInfo.value.amount;
            decimals = supplyInfo.value.decimals;
        } catch (_e) { /* ignore */ }

        const baseData = {
            name: meta.name,
            ticker: meta.symbol,
            image: meta?.image || null,
        };

        // Release meta object
        meta = null;

        const finalSupply = supply || '0';
        const finalDecimals = decimals || 9;

        // Only insert if we have real metadata - prevents placeholder tokens
        await db.run(`
            INSERT INTO tokens (mint, name, symbol, image, supply, decimals, priceUsd, liquidity, marketCap, volume24h, change24h, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT(mint) DO UPDATE SET
                name = CASE WHEN tokens.name IN ('Unknown', 'New Discovery', '') OR tokens.name IS NULL THEN EXCLUDED.name ELSE tokens.name END,
                symbol = CASE WHEN tokens.symbol IN ('UNKNOWN', 'UNK', 'NEW', '') OR tokens.symbol IS NULL THEN EXCLUDED.symbol ELSE tokens.symbol END,
                image = CASE WHEN tokens.image IS NULL OR tokens.image = '' THEN EXCLUDED.image ELSE tokens.image END,
                updated_at = NOW()
        `, [
            mint,
            baseData.name,
            baseData.ticker,
            baseData.image,
            finalSupply,
            finalDecimals,
            0, 0, 0, 0, 0,
            Date.now()
        ]);

        let pools = await findPoolsOnChain(mint);

        if (pools && pools.length > 0) {
            for (const pool of pools) {
                await enableIndexing(db, mint, {
                    pairAddress: pool.pairAddress,
                    dexId: pool.dexId,
                    liquidity: pool.liquidity || { usd: 0 },
                    volume: pool.volume || { h24: 0 },
                    priceUsd: pool.priceUsd || 0,
                    baseToken: pool.baseToken, 
                    quoteToken: pool.quoteToken, 
                    reserve_a: pool.reserve_a,
                    reserve_b: pool.reserve_b
                });
            }
        }
        
        // Release pools array
        pools = null;

        await aggregateAndSaveToken(db, mint);
        logger.info(`✅ Worker: Finished ${mint}`);

    } catch (e) {
        logger.error(`❌ Worker Error [${mint}]: ${e.message}`);
    }
}

async function startWorker() {
    if (isRunning) return;
    isRunning = true;

    try {
        await initDB();
        await connectRedis();
        
        const redis = getClient();
        if (!redis) {
            logger.warn("⚠️ Worker: Redis not available. Retrying in 5s...");
            isRunning = false;
            setTimeout(startWorker, 5000);
            return;
        }

        const db = getDB();

        logger.info("🛠️ Worker: Starting Services...");

        // Start Metadata Updater
        if (metadataUpdater && typeof metadataUpdater.start === 'function') {
            metadataUpdater.start({ db });
            logger.info("🛠️ Worker: Metadata Updater Started.");
        }

        logger.info("🛠️ Worker: Listening for token queue jobs...");

        const runLoop = async () => {
            if (!isRunning) return;
            try {
                // OPTIMIZATION: Process one item at a time to control memory pressure
                // First check main queue
                let item = await redis.rpop(QUEUE_KEY);
                let retryCount = 0;

                // If main queue empty, check retry queue (with delay for metadata to become available)
                if (!item) {
                    const retryItem = await redis.rpop(RETRY_KEY);
                    if (retryItem) {
                        try {
                            const retryData = JSON.parse(retryItem);
                            item = retryData.mint;
                            retryCount = retryData.retryCount || 0;
                            // Add delay before retry to give metadata time to propagate
                            await new Promise(r => setTimeout(r, 3000));
                        } catch (_e) {
                            // If parse fails, treat as plain mint address
                            item = retryItem;
                        }
                    }
                }

                if (item) {
                    await processToken(item, retryCount);

                    // Small delay to allow GC to run if needed
                    if (global.gc) {
                        try { global.gc(); } catch (_e) { /* ignore */ }
                    }
                    setTimeout(runLoop, 50);
                } else {
                    // Backoff when empty
                    setTimeout(runLoop, 2000);
                }
            } catch (err) {
                logger.error(`Worker Loop Error: ${err.message}`);
                setTimeout(runLoop, 5000);
            }
        };

        runLoop();

    } catch (e) {
        logger.error(`Worker Fatal Error: ${e.message}`);
        isRunning = false;
        setTimeout(startWorker, 5000); // Retry logic
    }
}

if (require.main === module) {
    startWorker();
}

module.exports = { startWorker };
