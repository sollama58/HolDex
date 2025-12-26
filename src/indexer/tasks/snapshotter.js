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

async function updateSolPrice(db) {
    try {
        // Try to get SOL price from a high-liquidity USDC/SOL pool
        const pool = await db.get(`
            SELECT price_usd FROM pools 
            WHERE token_a = 'So11111111111111111111111111111111111111112' 
            AND liquidity_usd > 10000 
            ORDER BY liquidity_usd DESC LIMIT 1
        `);
        if (pool && pool.price_usd > 0) {
            solPriceCache = pool.price_usd;
        } else {
            // Fallback if no pool is indexed yet (prevents division by zero)
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
                 // PumpFun: Reserve A is Vault (unused for price), Reserve B is Bonding Curve
                 // We need the Bonding Curve account data.
                 keysToFetch.push(new PublicKey(p.reserve_b)); 
                 poolMap.set(p.address, { type: 'pumpfun', idx: keysToFetch.length - 1, pool: p });
            } else if (p.reserve_a && p.reserve_b) {
                 // Standard AMM: We need both Vault Accounts
                 keysToFetch.push(new PublicKey(p.reserve_a));
                 keysToFetch.push(new PublicKey(p.reserve_b));
                 poolMap.set(p.address, { type: 'standard', idxA: keysToFetch.length - 2, idxB: keysToFetch.length - 1, pool: p });
            }
        } catch(e) {
            // logger.warn(`Invalid PubKey for pool ${p.address}`);
        }
    });

    if (keysToFetch.length === 0) {
        // Just update last_check so we don't get stuck
        for (const p of pools) await db.query(`UPDATE active_trackers SET last_check = $1 WHERE pool_address = $2`, [now, p.address]);
        return;
    }

    let accounts = [];
    try { 
        accounts = await retryRPC((conn) => conn.getMultipleAccountsInfo(keysToFetch));
    } catch (e) {
        logger.warn(`Batch RPC Failed for ${pools.length} pools: ${e.message}`);
        return;
    }

    const updates = [];
    const trackerUpdates = [];
    
    for (const p of pools) {
        trackerUpdates.push(db.query(`UPDATE active_trackers SET last_check = $1 WHERE pool_address = $2`, [now, p.address]));

        const task = poolMap.get(p.address);
        if (!task || !accounts) continue;

        let priceUsd = 0;
        let liquidityUsd = 0;
        let success = false;
        
        // --- DATA DECODING ---
        try {
            let reserveA = 0;
            let reserveB = 0;
            let decA = 6; 
            let decB = 6; 

            // Helper to read Little Endian u64 (standard SPL token amount)
            const readAmount = (buffer) => {
                if (!buffer || buffer.length < 72) return 0;
                return Number(buffer.readBigUInt64LE(64));
            };

            if (task.type === 'pumpfun') {
                const acc = accounts[task.idx];
                if (acc) {
                    // PUMPFUN BONDING CURVE LAYOUT
                    // 0-8: Discriminator
                    // 8-16: Virtual Token Reserves
                    // 16-24: Virtual SOL Reserves
                    reserveA = Number(acc.data.readBigUInt64LE(8));  // Virtual Token
                    reserveB = Number(acc.data.readBigUInt64LE(16)); // Virtual SOL
                    decA = 6; // PumpFun Tokens are 6 decimals
                    decB = 9; // SOL is 9 decimals
                }
            } else {
                const accA = accounts[task.idxA];
                const accB = accounts[task.idxB];
                
                if (accA && accB) {
                    reserveA = readAmount(accA.data);
                    reserveB = readAmount(accB.data);
                    
                    // --- CRITICAL FIX: DYNAMIC DECIMALS ---
                    // Use the token decimals we fetched in the JOIN query
                    // Fallback to 9 for SOL, 6 for USDC/USDT, or 9 for unknown (safest default for SPL)
                    
                    // Determine Decimals for Token A
                    if (QUOTE_TOKENS[p.token_a]) decA = QUOTE_TOKENS[p.token_a].decimals;
                    else decA = p.token_decimals || 9; // Use DB value or default to 9

                    // Determine Decimals for Token B
                    if (QUOTE_TOKENS[p.token_b]) decB = QUOTE_TOKENS[p.token_b].decimals;
                    else decB = 6; // Usually USDC/USDT quote
                }
            }

            // --- PRICE CALCULATION ---
            if (reserveA > 0 && reserveB > 0) {
                const rawA = reserveA / Math.pow(10, decA);
                const rawB = reserveB / Math.pow(10, decB);
                
                // Calculate Price relative to Token B
                let priceInB = rawB / rawA;
                
                let quotePrice = 0;
                
                // Identify Quote Token to convert to USD
                if (QUOTE_TOKENS[p.token_b] || p.token_b === 'So11111111111111111111111111111111111111112') {
                    // Case 1: Token B is the Quote (e.g. SOL or USDC)
                    if (p.token_b.includes('So111')) quotePrice = solPriceCache; 
                    else quotePrice = 1; // USDC/USDT assumed peg

                    priceUsd = priceInB * quotePrice;
                    liquidityUsd = rawB * quotePrice * 2; // Simple 50/50 liquidity est
                } else if (QUOTE_TOKENS[p.token_a]) {
                    // Case 2: Token A is the Quote (inverted pair)
                    if (p.token_a.includes('So111')) quotePrice = solPriceCache; 
                    else quotePrice = 1;

                    priceUsd = (1 / priceInB) * quotePrice;
                    liquidityUsd = rawA * quotePrice * 2;
                } else {
                    // PumpFun Case (B is Virtual SOL)
                    if (task.type === 'pumpfun') {
                         quotePrice = solPriceCache;
                         priceUsd = priceInB * quotePrice;
                         // PumpFun Liquidity is virtual, but we can estimate real Sol side
                         const realSol = Number(accounts[task.idx].data.readBigUInt64LE(32)); // Offset 32 is RealSolReserves
                         liquidityUsd = (realSol / 1e9) * quotePrice * 2;
                    }
                }

                if (priceUsd > 0) {
                    success = true;
                }
            }
        } catch (err) {
            // logger.error(`Math Error ${p.address}: ${err.message}`);
        }

        // --- VOLUME TRACKING (ASYNC) ---
        const volKey = `vol_last_check:${p.address}`;
        const lastCheck = stateCache.get(volKey) || 0;
        
        // Check volume every 2 minutes
        if (now - lastCheck > 120000) { 
            stateCache.set(volKey, now);
            const sigKey = `vol_sig:${p.address}`;
            const lastSig = stateCache.get(sigKey);
            
            // Fire and forget volume update
            getRealVolume(p.address, lastSig, solPriceCache).then(volData => {
                if (volData.txCount > 0) {
                    stateCache.set(sigKey, volData.latestSignature);
                    
                    // --- CRITICAL FIX: ACTUALLY SAVE VOLUME ---
                    if (volData.volumeUsd > 0) {
                        db.query(`
                            UPDATE pools SET volume_24h = volume_24h + $1 WHERE address = $2
                        `, [volData.volumeUsd, p.address]).catch(() => {});

                        // Also update the candle volume specifically
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

        if (success) {
            affectedMints.add(p.mint);
            
            // Update Pool Stats
            updates.push(db.query(`UPDATE pools SET price_usd = $1, liquidity_usd = $2 WHERE address = $3`, [priceUsd, liquidityUsd, p.address]));
            
            // Upsert Candle (Price Only here, volume handled async above)
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

    // Aggregate stats for affected tokens
    if (affectedMints.size > 0) {
        for (const mint of affectedMints) await aggregateAndSaveToken(db, mint);
    }
}

async function runSnapshotCycle() {
    try {
        const db = getDB();
        const connection = getSolanaConnection(); 
        await updateSolPrice(db);
        
        // --- CRITICAL FIX: JOIN TOKENS TO GET DECIMALS ---
        // We fetch 't.decimals' so we don't assume 6 for standard 9-decimal tokens
        const res = await db.query(`
            SELECT tr.pool_address, tr.last_check, p.*, t.decimals as token_decimals 
            FROM active_trackers tr 
            JOIN pools p ON tr.pool_address = p.address 
            LEFT JOIN tokens t ON p.mint = t.mint
            ORDER BY tr.priority DESC, tr.last_check ASC 
            LIMIT 200
        `);
        const pools = res.rows;

        logger.info(`⏱️ Snapshotter: Checking ${pools.length} pools (SOL: $${solPriceCache.toFixed(2)})`);

        for (let i = 0; i < pools.length; i += 50) {
            await processPoolBatch(db, connection, pools.slice(i, i + 50), null);
            await new Promise(r => setTimeout(r, 200)); 
        }
    } catch (e) {
        logger.error(`Snapshot Cycle Error: ${e.message}`);
    }
}

function startSnapshotter() {
    setTimeout(() => {
        logger.info("🟢 Snapshotter Engine Started");
        runSnapshotCycle();
        setInterval(runSnapshotCycle, 30000); 
    }, 5000);
}

async function snapshotPools(poolAddresses) {
    if (!poolAddresses.length) return;
    const db = getDB();
    const connection = getSolanaConnection();
    await updateSolPrice(db);
    // Include decimals in manual snapshot too
    const res = await db.query(`
        SELECT p.*, t.decimals as token_decimals 
        FROM pools p 
        LEFT JOIN tokens t ON p.mint = t.mint
        WHERE p.address = ANY($1)
    `, [poolAddresses]); 
    await processPoolBatch(db, connection, res.rows, null);
}

module.exports = { startSnapshotter, snapshotPools };
