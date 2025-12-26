const { PublicKey } = require('@solana/web3.js');
const { getConnection } = require('../services/helius');
const { getDB } = require('../../services/database');
const logger = require('../../services/logger');

async function runSnapshotCycle() {
    const db = getDB();
    const connection = getConnection();

    // 1. Get Active Pools (Limit 100 for batch 1)
    const pools = await db.all("SELECT pool_address, mint, dex FROM active_trackers LIMIT 100");
    if (pools.length === 0) return;

    logger.info(`📸 Snapshot: Updating ${pools.length} active pools...`);

    const poolKeys = pools.map(p => new PublicKey(p.pool_address));

    // 2. Fetch All Accounts efficiently
    const accounts = await connection.getMultipleAccountsInfo(poolKeys);

    // Round to nearest minute for clean charting
    const timestamp = Math.floor(Date.now() / 60000) * 60000;

    for (let i = 0; i < pools.length; i++) {
        const pool = pools[i];
        const account = accounts[i];
        
        if (!account) continue;

        let price = 0;

        try {
            if (pool.dex === 'pump') {
                const data = account.data;
                // Pump.fun Curve Layout: 8 (disc) + 8 (vToken) + 8 (vSol) + 8 (rToken) + 8 (rSol)
                const virtualTokenReserves = data.readBigUInt64LE(8);
                const virtualSolReserves = data.readBigUInt64LE(16);
                
                // Price in SOL
                if (Number(virtualTokenReserves) > 0) {
                    price = Number(virtualSolReserves) / Number(virtualTokenReserves);
                }
            }

            if (price > 0) {
                // Get previous close to determine 'Open' for this minute
                const prev = await db.get(
                    `SELECT close FROM candles_1m WHERE pool_address = $1 ORDER BY timestamp DESC LIMIT 1`, 
                    [pool.pool_address]
                );
                
                // If no previous candle, Open = Current Price. Else Open = Prev Close.
                const open = prev ? prev.close : price;

                // For a 1-minute snapshot, High/Low are approximate to the Open/Close range
                // In Phase 2, we would listen to every trade to get true High/Low
                await db.run(`
                    INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT(pool_address, timestamp) 
                    DO UPDATE SET close = excluded.close, high = GREATEST(candles_1m.high, excluded.close), low = LEAST(candles_1m.low, excluded.close)
                `, [
                    pool.pool_address, 
                    timestamp, 
                    open, 
                    Math.max(open, price), 
                    Math.min(open, price), 
                    price
                ]);
            }
        } catch (err) {
            logger.error(`Error snapshotting pool ${pool.pool_address}: ${err.message}`);
        }
    }
}

function startSnapshotter() {
    // Align with the clock (run at :00 seconds)
    const now = new Date();
    const delay = 60000 - (now.getSeconds() * 1000 + now.getMilliseconds());
    
    setTimeout(() => {
        runSnapshotCycle();
        setInterval(runSnapshotCycle, 60000);
    }, delay);
    
    logger.info("📸 Snapshot Engine Started (60s Interval)");
}

module.exports = { startSnapshotter };
