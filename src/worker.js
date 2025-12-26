require('dotenv').config();
const { initDB, getDB, enableIndexing, aggregateAndSaveToken } = require('./services/database');
const { getClient, connectRedis } = require('./services/redis');
const { findPoolsOnChain } = require('./services/pool_finder');
const { fetchTokenMetadata } = require('./utils/metaplex');
const { getSolanaConnection } = require('./services/solana');
const { PublicKey } = require('@solana/web3.js');
const logger = require('./services/logger');

const QUEUE_KEY = 'token_queue';
let isRunning = false;

async function processToken(mint) {
    const db = getDB();
    const connection = getSolanaConnection();

    logger.info(`⚙️ Worker: Processing ${mint}...`);

    try {
        const meta = await fetchTokenMetadata(mint);
        
        let supply = '1000000000';
        let decimals = 9;
        try {
            const supplyInfo = await connection.getTokenSupply(new PublicKey(mint));
            supply = supplyInfo.value.amount;
            decimals = supplyInfo.value.decimals;
        } catch (e) {}

        const pools = await findPoolsOnChain(mint);

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

        const baseData = {
            name: meta?.name || 'Unknown',
            ticker: meta?.symbol || 'UNKNOWN',
            image: meta?.image || null,
        };

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
            logger.warn("⚠️ Worker: Redis not available. Worker disabled.");
            isRunning = false;
            return;
        }

        logger.info("🛠️ Worker: Listening for jobs...");

        const runLoop = async () => {
            if (!isRunning) return;
            try {
                const item = await redis.rpop(QUEUE_KEY);
                if (item) {
                    await processToken(item);
                    setTimeout(runLoop, 100); 
                } else {
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
    }
}

// AUTO-START if run directly (node src/worker.js)
// But do NOT auto-start if imported (require('./worker'))
if (require.main === module) {
    startWorker();
}

module.exports = { startWorker };
