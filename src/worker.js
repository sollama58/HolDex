require('dotenv').config();
const { initDB, getDB, enableIndexing, aggregateAndSaveToken } = require('./services/database');
const { getClient, connectRedis } = require('./services/redis');
const { findPoolsOnChain } = require('./services/pool_finder');
const { fetchTokenMetadata } = require('./utils/metaplex');
const { getSolanaConnection } = require('./services/solana');
const { PublicKey } = require('@solana/web3.js');
const logger = require('./services/logger');

const QUEUE_KEY = 'token_queue';

async function processToken(mint) {
    const db = getDB();
    const connection = getSolanaConnection();

    logger.info(`⚙️ Worker: Processing ${mint}...`);

    try {
        // 1. Fetch Metadata (Off-chain)
        const meta = await fetchTokenMetadata(mint);
        
        // 2. Fetch Supply (On-chain)
        let supply = '1000000000';
        let decimals = 9;
        try {
            const supplyInfo = await connection.getTokenSupply(new PublicKey(mint));
            supply = supplyInfo.value.amount;
            decimals = supplyInfo.value.decimals;
        } catch (e) {
            logger.warn(`Failed to fetch supply for ${mint}: ${e.message}`);
        }

        // 3. Find Pools (On-chain & API)
        const pools = await findPoolsOnChain(mint);

        // 4. Index Pools
        for (const pool of pools) {
            // enableIndexing now handles token extraction robustly (object vs string)
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

        // 5. Update Token Record
        const baseData = {
            name: meta?.name || 'Unknown',
            ticker: meta?.symbol || 'UNKNOWN',
            image: meta?.image || null,
        };

        // Fallback checks for critical fields
        const finalSupply = supply || '0';
        const finalDecimals = decimals || 9;

        await db.run(`
            INSERT INTO tokens (mint, name, symbol, image, supply, decimals, priceUsd, liquidity, marketCap, volume24h, change24h, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT(mint) DO UPDATE SET
            name = EXCLUDED.name,
            symbol = EXCLUDED.symbol,
            image = EXCLUDED.image,
            decimals = EXCLUDED.decimals
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

        // 6. Aggregate Stats
        await aggregateAndSaveToken(db, mint);
        logger.info(`✅ Worker: Finished ${mint}`);

    } catch (e) {
        logger.error(`❌ Worker Error [${mint}]: ${e.message}`);
    }
}

async function startWorker() {
    try {
        logger.info("🛠️ Worker: Starting...");
        
        // Initialize services
        await initDB();
        await connectRedis();
        
        const redis = getClient();
        if (!redis) {
            throw new Error("Redis client failed to initialize.");
        }

        logger.info("🛠️ Worker: Services Ready. Waiting for jobs...");

        // Job Loop
        while (true) {
            try {
                // RPOP is non-blocking in ioredis, so we poll
                const item = await redis.rpop(QUEUE_KEY);
                
                if (item) {
                    await processToken(item);
                } else {
                    // Sleep 2 seconds if queue empty
                    await new Promise(r => setTimeout(r, 2000));
                }
            } catch (err) {
                logger.error(`Worker Loop Error: ${err.message}`);
                await new Promise(r => setTimeout(r, 5000));
            }
        }

    } catch (e) {
        logger.error(`Worker Fatal Error: ${e.message}`);
        process.exit(1);
    }
}

startWorker();
