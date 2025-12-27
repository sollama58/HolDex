const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection, retryRPC } = require('../../services/solana'); 
const { getDB, aggregateAndSaveToken } = require('../../services/database');
const { getClient } = require('../../services/redis');
const logger = require('../../services/logger');
const { getRealVolume } = require('../services/volume_tracker');
const { enrichPoolsWithReserves } = require('../../services/pool_finder');

const stateCache = new Map();

// Known Quote Tokens
const QUOTE_TOKENS = {
    'So11111111111111111111111111111111111111112': { decimals: 9, symbol: 'SOL' },
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': { decimals: 6, symbol: 'USDC' },
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': { decimals: 6, symbol: 'USDT' },
};

let solPriceCache = 0; 
let isCycleRunning = false; // LOCKING MECHANISM

async function updateSolPrice(db) {
    try {
        const pool = await db.get(`
            SELECT price_usd FROM pools 
            WHERE token_a = 'So11111111111111111111111111111111111111112' 
            AND liquidity_usd > 10000 
            ORDER BY liquidity_usd DESC LIMIT 1
        `);
        if (pool && pool.price_usd > 0) {
            solPriceCache = pool.price_usd;
        } else {
            if(solPriceCache === 0) solPriceCache = 0; 
        }
    } catch(e) {
        logger.warn("Failed to update SOL price cache");
    }
}

async function processPoolBatch(db, connection, pools, redis) {
    const timestamp = Math.floor(Date.now() / 60000) * 60000;
    const now = Date.now();
    
    // --- SELF-HEAL: FETCH MISSING RESERVES ---
    const poolsNeedingReserves = pools.filter(p => p.dex !== 'pumpfun' && (!p.reserve_a || !p.reserve_b));
    if (poolsNeedingReserves.length > 0) {
        await enrichPoolsWithReserves(poolsNeedingReserves);
        const fixPromises = poolsNeedingReserves.map(p => {
            if (p.reserve_a && p.reserve_b) {
                return db.query(`UPDATE pools SET reserve_a = $1, reserve_b = $2 WHERE address = $3`, [p.reserve_a, p.reserve_b, p.address]);
            }
        });
        await Promise.allSettled(fixPromises);
    }

    const keysToFetch = [];
    const poolMap = new Map();
    const affectedMints = new Set();

    pools.forEach(p => {
        try {
            if (p.dex === 'pumpfun' && p.reserve_b) {
                 keysToFetch.push(new PublicKey(p.reserve_b)); 
                 poolMap.set(p.address, { type: 'pumpfun', idx: keysToFetch.length - 1, pool: p });
            } else if (p.reserve_a && p.reserve_b) {
                 keysToFetch.push(new PublicKey(p.reserve_a));
                 keysToFetch.push(new PublicKey(p.reserve_b));
                 poolMap.set(p.address, { type: 'standard', idxA: keysToFetch.length - 2, idxB: keysToFetch.length - 1, pool: p });
            }
        } catch(e) {}
    });

    if (keysToFetch.length === 0) {
        // Just update last_check so we don't get stuck
        for (const p of pools) await db.query(`UPDATE active_trackers SET last_check = $1 WHERE pool_address = $2`, [now, p.address]);
        return;
    }

    let accounts = [];
    try { 
        if (!connection) throw new Error("Connection object is undefined in processPoolBatch");
        accounts = await retryRPC(() => connection.getMultipleAccountsInfo(keysToFetch));
    } catch (e) {
        logger.warn(`Batch RPC Failed for ${pools.length} pools: ${e.message}`);
        return;
    }

    const updates = [];
    const trackerUpdates = [];
    
    for (const p of pools) {
        // Update last_check to NOW so it won't be picked up again for 5 minutes
        trackerUpdates.push(db.query(`UPDATE active_trackers SET last_check = $1 WHERE pool_address = $2`, [now, p.address]));

        const task = poolMap.get(p.address);
        if (!task || !accounts) continue;

        let priceUsd = 0;
        let liquidityUsd = 0;
        let success = false;
        
        try {
            let reserveA = 0;
            let reserveB = 0;
            let decA = 6; 
            let decB = 6; 

            const readAmount = (buffer) => {
                if (!buffer || buffer.length < 72) return 0;
                return Number(buffer.readBigUInt64LE(64));
            };

            if (task.type === 'pumpfun') {
                const acc = accounts[task.idx];
                if (acc) {
                    reserveA = Number(acc.data.readBigUInt64LE(8));  // Virtual Token
                    reserveB = Number(acc.data.readBigUInt64LE(16)); // Virtual SOL
                    decA = 6; 
                    decB = 9; 
                }
            } else {
                const accA = accounts[task.idxA];
                const accB = accounts[task.idxB];
                
                if (accA && accB) {
                    reserveA = readAmount(accA.data);
                    reserveB = readAmount(accB.data);
                    
                    if (QUOTE_TOKENS[p.token_a]) decA = QUOTE_TOKENS[p.token_a].decimals;
                    else decA = p.token_decimals || 9;

                    if (QUOTE_TOKENS[p.token_b]) decB = QUOTE_TOKENS[p.token_b].decimals;
                    else decB = 6;
                }
            }

            if (reserveA > 0 && reserveB > 0) {
                const rawA = reserveA / Math.pow(10, decA);
                const rawB = reserveB / Math.pow(10, decB);
                
                let priceInB = rawB / rawA;
                let quotePrice = 0;
                
                if (QUOTE_TOKENS[p.token_b] || p.token_b === 'So11111111111111111111111111111111111111112') {
                    if (p.token_b.includes('So111')) quotePrice = solPriceCache; 
                    else quotePrice = 1;

                    priceUsd = priceInB * quotePrice;
                    liquidityUsd = rawB * quotePrice * 2;
                } else if (QUOTE_TOKENS[p.token_a]) {
                    if (p.token_a.includes('So111')) quotePrice = solPriceCache; 
                    else quotePrice = 1;

                    priceUsd = (1 / priceInB) * quotePrice;
                    liquidityUsd = rawA * quotePrice * 2;
                } else {
                    if (task.type === 'pumpfun') {
                         quotePrice = solPriceCache;
                         priceUsd = priceInB * quotePrice;
                         const realSol = Number(accounts[task.idx].data.readBigUInt64LE(32)); 
                         liquidityUsd = (realSol / 1e9) * quotePrice * 2;
                    }
                }
                
                if (Number.isFinite(priceUsd) && priceUsd > 0) {
                    success = true;
                } else {
                    priceUsd = 0;
                    success = false;
                }
            }
        } catch (err) {
            // logger.error(`Math Error ${p.address}: ${err.message}`);
        }

        // --- VOLUME TRACKING (ASYNC) ---
        // Keeps checking volume even if price is stale, but respects the loop batching
        const volKey = `vol_last_check:${p.address}`;
        const lastCheck = stateCache.get(volKey) || 0;
        
        // Only run volume check if > 2 mins passed (separate from price check)
        if (now - lastCheck > 120000) { 
            stateCache.set(volKey, now);
            const sigKey = `vol_sig:${p.address}`;
            const lastSig = stateCache.get(sigKey);
            
            getRealVolume(p.address, lastSig, solPriceCache).then(volData => {
                if (volData.txCount > 0) {
                    stateCache.set(sigKey, volData.latestSignature);
                    
                    if (volData.volumeUsd > 0) {
                        db.query(`UPDATE pools SET volume_24h = volume_24h + $1 WHERE address = $2`, [volData.volumeUsd, p.address]).catch(() => {});

                        const bucket = Math.floor(Date.now() / 60000) * 60000;
                        db.query(`
                            INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close, volume) 
                            VALUES ($1, $2, $3, $3, $3, $3, $4) 
                            ON CONFLICT(pool_address, timestamp) 
                            DO UPDATE SET volume = candles_1m.volume + $4
                        `, [p.address, bucket, priceUsd, volData.volumeUsd]).catch(() => {});
                    }
                }
            }).catch(() => {});
        }

        if (success && priceUsd > 0) { 
            affectedMints.add(p.mint);
            
            updates.push(db.query(`UPDATE pools SET price_usd = $1, liquidity_usd = $2 WHERE address = $3`, [priceUsd, liquidityUsd, p.address]));
            
            updates.push(db.query(`
                INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close, volume) 
                VALUES ($1, $2, $3, $3, $3, $3, 0) 
                ON CONFLICT(pool_address, timestamp) 
                DO UPDATE SET 
                    close = $3, 
                    high = GREATEST(candles_1m.high, $3), 
                    low = LEAST(candles_1m.low, $3)
            `, [p.address, timestamp, priceUsd]));
        }
    }
    
    await Promise.allSettled([...updates, ...trackerUpdates]);

    if (affectedMints.size > 0) {
        for (const mint of affectedMints) await aggregateAndSaveToken(db, mint);
    }
}

async function runSnapshotCycle() {
    // PREVENT OVERLAP
    if (isCycleRunning) {
        logger.warn("⚠️ Snapshotter: Previous cycle still running. Skipping.");
        return;
    }
    isCycleRunning = true;

    try {
        const db = getDB();
        const connection = getSolanaConnection(); 
        
        await updateSolPrice(db);
        
        // SMART POLLING: Only fetch pools that haven't been checked in 5 minutes (300000ms)
        // We do NOT filter by volume, so 0 volume pools are included.
        const staleThreshold = Date.now() - 300000;

        const res = await db.query(`
            SELECT tr.pool_address, tr.last_check, p.*, t.decimals as token_decimals 
            FROM active_trackers tr 
            JOIN pools p ON tr.pool_address = p.address 
            LEFT JOIN tokens t ON p.mint = t.mint
            WHERE tr.last_check < $1
            ORDER BY tr.priority DESC, tr.last_check ASC 
            LIMIT 200
        `, [staleThreshold]);
        
        const pools = res.rows;

        if (pools.length > 0) {
            // logger.info(`⏱️ Snapshotter: Refreshing ${pools.length} stale pools...`);
            for (let i = 0; i < pools.length; i += 50) {
                await processPoolBatch(db, connection, pools.slice(i, i + 50), null);
                await new Promise(r => setTimeout(r, 200)); 
            }
        }
    } catch (e) {
        logger.error(`Snapshot Cycle Error: ${e.message}`);
    } finally {
        isCycleRunning = false;
    }
}

function startSnapshotter() {
    setTimeout(() => {
        logger.info("🟢 Snapshotter Engine Started (Smart Polling 5m)");
        runSnapshotCycle();
        setInterval(runSnapshotCycle, 30000); // Run cycle every 30s to pick up next batch
    }, 5000);
}

async function snapshotPools(poolAddresses) {
    if (!poolAddresses.length) return;
    const db = getDB();
    const connection = getSolanaConnection();
    await updateSolPrice(db);
    const res = await db.query(`
        SELECT p.*, t.decimals as token_decimals 
        FROM pools p 
        LEFT JOIN tokens t ON p.mint = t.mint
        WHERE p.address = ANY($1)
    `, [poolAddresses]); 
    await processPoolBatch(db, connection, res.rows, null);
}

module.exports = { startSnapshotter, snapshotPools };
