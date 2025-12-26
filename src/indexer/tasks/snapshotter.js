const { PublicKey } = require('@solana/web3.js');
const { getConnection } = require('../services/helius');
const { getDB } = require('../../services/database');
const logger = require('../../services/logger');

// RELIABILITY FIX: Execution Lock
let isSnapshotRunning = false;

async function runSnapshotCycle() {
    if (isSnapshotRunning) {
        logger.warn("📸 Snapshot Cycle Skipped: Previous cycle still running.");
        return;
    }
    isSnapshotRunning = true;

    const db = getDB();
    const connection = getConnection();
    const BATCH_SIZE = 100;
    
    // Timestamp for this entire cycle (aligned to minute)
    const timestamp = Math.floor(Date.now() / 60000) * 60000;
    
    logger.info(`📸 Snapshot Cycle Starting [${new Date(timestamp).toISOString()}]`);

    let offset = 0;
    let processed = 0;
    let keepFetching = true;

    try {
        while (keepFetching) {
            // 1. Fetch Batch using Pagination & JOIN
            // FIX: We join 'active_trackers' with 'pools' to get mint & dex info
            const pools = await db.all(`
                SELECT t.pool_address, p.mint, p.dex 
                FROM active_trackers t
                JOIN pools p ON t.pool_address = p.address
                ORDER BY t.priority DESC, t.pool_address ASC 
                LIMIT ${BATCH_SIZE} OFFSET ${offset}
            `);

            if (pools.length === 0) {
                keepFetching = false;
                break;
            }

            const poolKeys = pools.map(p => new PublicKey(p.pool_address));

            // 2. RPC Call
            const accounts = await connection.getMultipleAccountsInfo(poolKeys);

            // 3. Process Accounts
            const updates = [];
            
            for (let i = 0; i < pools.length; i++) {
                const pool = pools[i];
                const account = accounts[i];
                if (!account) continue;

                let price = 0;
                try {
                    if (pool.dex === 'pump') {
                        const data = account.data;
                        const virtualTokenReserves = data.readBigUInt64LE(8);
                        const virtualSolReserves = data.readBigUInt64LE(16);
                        
                        if (Number(virtualTokenReserves) > 0) {
                            price = Number(virtualSolReserves) / Number(virtualTokenReserves);
                        }
                    } else if (pool.dex === 'raydium') {
                        // Basic Raydium AMM support (future proofing)
                        // This usually requires layout parsing, skipping for now to prevent crashes
                    }

                    if (price > 0) {
                        updates.push(db.run(`
                            INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close, volume)
                            VALUES ($1, $2, $3, $3, $3, $3, 0)
                            ON CONFLICT(pool_address, timestamp) 
                            DO UPDATE SET close = $3, high = GREATEST(candles_1m.high, $3), low = LEAST(candles_1m.low, $3)
                        `, [pool.pool_address, timestamp, price]));
                    }
                } catch (err) {}
            }

            await Promise.all(updates);
            
            processed += pools.length;
            offset += BATCH_SIZE;

            await new Promise(r => setTimeout(r, 100));
        }
    } catch (err) {
        logger.error(`Snapshot Batch Fatal Error: ${err.message}`);
    } finally {
        isSnapshotRunning = false;
        logger.info(`📸 Snapshot Cycle Complete. Processed ${processed} pools.`);
    }
}

function startSnapshotter() {
    const now = new Date();
    // Align with the clock (run at :00 seconds)
    const delay = 60000 - (now.getSeconds() * 1000 + now.getMilliseconds());
    
    logger.info(`📸 Snapshot Engine scheduling first run in ${delay}ms`);

    setTimeout(() => {
        runSnapshotCycle();
        setInterval(runSnapshotCycle, 60000);
    }, delay);
}

module.exports = { startSnapshotter };
