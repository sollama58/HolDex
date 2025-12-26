const { PublicKey } = require('@solana/web3.js');
const { getConnection } = require('../services/helius');
const { getDB } = require('../../services/database');
const logger = require('../../services/logger');

async function runSnapshotCycle() {
    const db = getDB();
    const connection = getConnection();
    const BATCH_SIZE = 100;
    
    // Timestamp for this entire cycle (aligned to minute)
    const timestamp = Math.floor(Date.now() / 60000) * 60000;
    
    logger.info(`📸 Snapshot Cycle Starting [${new Date(timestamp).toISOString()}]`);

    let offset = 0;
    let processed = 0;
    let keepFetching = true;

    while (keepFetching) {
        try {
            // 1. Fetch Batch using Pagination
            // We order by priority first to ensure trending tokens get updated first in the cycle
            const pools = await db.all(`
                SELECT pool_address, mint, dex 
                FROM active_trackers 
                ORDER BY priority DESC, pool_address ASC 
                LIMIT ${BATCH_SIZE} OFFSET ${offset}
            `);

            if (pools.length === 0) {
                keepFetching = false;
                break;
            }

            const poolKeys = pools.map(p => new PublicKey(p.pool_address));

            // 2. RPC Call (Batch of 100 max)
            // Note: Helius/Solana limits getMultipleAccounts to 100 keys
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
                        // Pump.fun Curve Layout: 8 (disc) + 8 (vToken) + 8 (vSol)
                        // Read BigUInt64LE
                        const virtualTokenReserves = data.readBigUInt64LE(8);
                        const virtualSolReserves = data.readBigUInt64LE(16);
                        
                        if (Number(virtualTokenReserves) > 0) {
                            price = Number(virtualSolReserves) / Number(virtualTokenReserves);
                        }
                    }

                    if (price > 0) {
                        // Optimistic Snapshot: 
                        // We assume "close" is the current price. 
                        // The aggregation logic (in routes) will determine Open/High/Low from these 1m snapshots.
                        updates.push(db.run(`
                            INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close, volume)
                            VALUES ($1, $2, $3, $3, $3, $3, 0)
                            ON CONFLICT(pool_address, timestamp) 
                            DO UPDATE SET close = $3, high = GREATEST(candles_1m.high, $3), low = LEAST(candles_1m.low, $3)
                        `, [pool.pool_address, timestamp, price]));
                    }
                } catch (err) {
                    // Fail silently for individual pool errors to not block batch
                }
            }

            // Execute batch writes in parallel
            await Promise.all(updates);
            
            processed += pools.length;
            offset += BATCH_SIZE;

            // Rate limit protection: Sleep 100ms between batches to be nice to RPC
            await new Promise(r => setTimeout(r, 100));

        } catch (err) {
            logger.error(`Snapshot Batch Error (Offset ${offset}): ${err.message}`);
            // Break loop on fatal DB/RPC error to prevent infinite loops, but allows retry next minute
            keepFetching = false; 
        }
    }
    
    logger.info(`📸 Snapshot Cycle Complete. Processed ${processed} pools.`);
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
