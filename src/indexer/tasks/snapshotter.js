const { PublicKey } = require('@solana/web3.js');
const { getConnection } = require('../services/helius');
const { getDB } = require('../../services/database');
const { getClient } = require('../../services/redis');
const logger = require('../../services/logger');

let isSnapshotRunning = false;
let solPriceCache = 200; 

// FIX: Correct Raydium V4 Offsets for VAULTS (not Mints)
const RAY_COIN_VAULT_OFFSET = 320; 
const RAY_PC_VAULT_OFFSET = 352;
const ORCA_VAULT_A_OFFSET = 101;
const ORCA_VAULT_B_OFFSET = 133;

const QUOTE_MINTS = new Set([
    'So11111111111111111111111111111111111111112', // SOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
]);

// Helper: Refresh SOL Price
async function updateSolPrice(db) {
    try {
        const pool = await db.get(`SELECT price_usd FROM pools WHERE (token_a = 'So11111111111111111111111111111111111111112' AND token_b = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v') ORDER BY liquidity_usd DESC LIMIT 1`);
        if (pool && pool.price_usd > 0) solPriceCache = pool.price_usd;
    } catch (e) {}
}

/**
 * Sync the best pool price to the main tokens table.
 * This ensures the frontend list view (which reads from 'tokens') is populated.
 */
async function syncTokenPrice(db, mint, priceUsd, liquidityUsd, marketCap) {
    if (!mint || priceUsd <= 0) return;
    try {
        // We update the token record.
        // In a more complex system, we'd check if this pool is the *best* pool.
        // For now, any non-zero update from our tracked pools is better than 0.
        await db.run(`
            UPDATE tokens 
            SET priceUsd = $1, liquidity = $2, marketCap = $3
            WHERE mint = $4
        `, [priceUsd, liquidityUsd, marketCap, mint]);
    } catch (e) {
        // Ignore concurrency errors
    }
}

/**
 * EXPORTED: Snapshot specific pools immediately.
 * Used when a user adds a new token to populate data instantly.
 */
async function snapshotPools(poolAddresses) {
    if (!poolAddresses || poolAddresses.length === 0) return;
    const db = getDB();
    const connection = getConnection();
    const redis = getClient();
    await updateSolPrice(db);

    logger.info(`📸 Immediate Snapshot for ${poolAddresses.length} pools...`);

    const pools = await db.all(`
        SELECT p.address, p.mint, p.dex, p.reserve_a, p.reserve_b, p.token_a, p.token_b
        FROM pools p WHERE p.address IN (${poolAddresses.map(p => `'${p}'`).join(',')})
    `);

    // Reuse the processing logic
    await processPoolBatch(db, connection, pools, redis, true);
}

/**
 * Core processing logic for a batch of pools.
 * Handles: Pump, Raydium/Orca Discovery, and Direct Balance Checks.
 */
async function processPoolBatch(db, connection, pools, redis, isImmediate = false) {
    const keysToFetch = [];
    const poolMap = new Map();
    const timestamp = Math.floor(Date.now() / 60000) * 60000;

    pools.forEach((p) => {
        if (p.dex === 'pump') {
            keysToFetch.push(new PublicKey(p.address));
            poolMap.set(p.address, { type: 'pump', index: keysToFetch.length - 1 });
        } else if (['raydium', 'orca', 'meteora'].includes(p.dex)) {
             if (p.reserve_a && p.reserve_b) {
                keysToFetch.push(new PublicKey(p.reserve_a));
                keysToFetch.push(new PublicKey(p.reserve_b));
                poolMap.set(p.address, { type: 'direct', idxA: keysToFetch.length - 2, idxB: keysToFetch.length - 1 });
            } else {
                keysToFetch.push(new PublicKey(p.address));
                poolMap.set(p.address, { type: 'discovery', dex: p.dex, index: keysToFetch.length - 1 });
            }
        }
    });

    if (keysToFetch.length === 0) return;

    try {
        const accounts = await connection.getMultipleAccountsInfo(keysToFetch);
        
        // If background job, use transaction. If immediate, just run queries (simpler/faster for single updates).
        const client = isImmediate ? null : await db.pool.connect();

        try {
            if (client) await client.query('BEGIN');

            for (const p of pools) {
                const task = poolMap.get(p.address);
                if (!task) continue;

                let nativePrice = 0, liquidityUsd = 0, quoteIsSol = false;
                let currentQuoteReserve = 0;
                let tokenSupply = 1000000000; // Default 1B for MCAP calc

                // 1. DATA EXTRACTION
                if (task.type === 'pump') {
                    const data = accounts[task.index]?.data;
                    if (data && data.length >= 40) {
                        const vToken = data.readBigUInt64LE(8);
                        const vSol = data.readBigUInt64LE(16);
                        const realSol = data.readBigUInt64LE(32);
                        if (Number(vToken) > 0) {
                            nativePrice = Number(vSol) / Number(vToken);
                            liquidityUsd = (Number(realSol) / 1e9) * solPriceCache * 2; // *2 for Total Liquidity (approx)
                            quoteIsSol = true;
                            currentQuoteReserve = Number(realSol) / 1e9;
                        }
                    }
                } else if (task.type === 'discovery') {
                    const data = accounts[task.index]?.data;
                    if (data) {
                        let vaultA, vaultB;
                        if (task.dex === 'raydium' && data.length >= 500) {
                            vaultA = new PublicKey(data.subarray(RAY_COIN_VAULT_OFFSET, RAY_COIN_VAULT_OFFSET + 32));
                            vaultB = new PublicKey(data.subarray(RAY_PC_VAULT_OFFSET, RAY_PC_VAULT_OFFSET + 32));
                        } else if (task.dex === 'orca' && data.length >= 165) {
                            vaultA = new PublicKey(data.subarray(ORCA_VAULT_A_OFFSET, ORCA_VAULT_A_OFFSET + 32));
                            vaultB = new PublicKey(data.subarray(ORCA_VAULT_B_OFFSET, ORCA_VAULT_B_OFFSET + 32));
                        }

                        if (vaultA && vaultB) {
                            const q = `UPDATE pools SET reserve_a = '${vaultA.toBase58()}', reserve_b = '${vaultB.toBase58()}' WHERE address = '${p.address}'`;
                            if (client) await client.query(q); else await db.run(q);
                        }
                    }
                } else if (task.type === 'direct') {
                    const accA = accounts[task.idxA];
                    const accB = accounts[task.idxB];
                    if (accA && accB && accA.data.length >= 72) {
                        const balA = Number(accA.data.readBigUInt64LE(64));
                        const balB = Number(accB.data.readBigUInt64LE(64));
                        const isAQuote = QUOTE_MINTS.has(p.token_a);
                        
                        // NOTE: p.token_a/b checks for Sol11... fallback for identifying SOL
                        if (isAQuote) {
                            if (balB > 0) nativePrice = balA / balB;
                            liquidityUsd = (balA / 1e9) * solPriceCache * 2;
                            if (p.token_a.includes('So11')) quoteIsSol = true;
                            currentQuoteReserve = balA / 1e9;
                        } else {
                            if (balA > 0) nativePrice = balB / balA;
                            liquidityUsd = (balB / 1e9) * solPriceCache * 2;
                            if (p.token_b.includes('So11')) quoteIsSol = true;
                            currentQuoteReserve = balB / 1e9;
                        }
                    }
                }

                // 2. SAVING DATA
                if (nativePrice > 0) {
                    const finalPriceUsd = quoteIsSol ? (nativePrice * solPriceCache) : nativePrice;
                    const marketCap = finalPriceUsd * tokenSupply;
                    
                    // Volume Calculation (Persistent via Redis)
                    let approxVolume = 0;
                    if (redis) {
                        const volKey = `vol:${p.address}`;
                        const prev = await redis.get(volKey);
                        if (prev) {
                            const delta = Math.abs(currentQuoteReserve - parseFloat(prev));
                            // Filter noise: change must be significant but not a massive liquidity add/remove
                            if (delta > 0.000001 && delta < (currentQuoteReserve * 0.5)) {
                                approxVolume = delta * solPriceCache;
                            }
                        }
                        await redis.set(volKey, currentQuoteReserve, 'EX', 300);
                    }

                    // Database Updates
                    if (client) {
                        // Batch Mode
                        await client.query(`UPDATE pools SET price_usd = $1, liquidity_usd = $2 WHERE address = $3`, [finalPriceUsd, liquidityUsd, p.address]);
                        await client.query(`INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close, volume) VALUES ($1, $2, $3, $3, $3, $3, $4) ON CONFLICT(pool_address, timestamp) DO UPDATE SET close = $3, volume = candles_1m.volume + $4, high = GREATEST(candles_1m.high, $3), low = LEAST(candles_1m.low, $3)`, [p.address, timestamp, finalPriceUsd, approxVolume]);
                        
                        // CRITICAL: SYNC PARENT TOKEN
                        await client.query(`UPDATE tokens SET priceUsd = $1, marketCap = $2, liquidity = $3 WHERE mint = $4`, [finalPriceUsd, marketCap, liquidityUsd, p.mint]);
                    } else {
                        // Immediate Mode
                        await db.run(`UPDATE pools SET price_usd = $1, liquidity_usd = $2 WHERE address = $3`, [finalPriceUsd, liquidityUsd, p.address]);
                        await db.run(`INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close, volume) VALUES ($1, $2, $3, $3, $3, $3, $4) ON CONFLICT(pool_address, timestamp) DO UPDATE SET close = $3, volume = candles_1m.volume + $4, high = GREATEST(candles_1m.high, $3), low = LEAST(candles_1m.low, $3)`, [p.address, timestamp, finalPriceUsd, approxVolume]);
                        await syncTokenPrice(db, p.mint, finalPriceUsd, liquidityUsd, marketCap);
                    }
                }
            }

            if (client) await client.query('COMMIT');
        } catch (e) {
            if (client) await client.query('ROLLBACK');
            logger.error(`Snapshot Batch Error: ${e.message}`);
        } finally {
            if (client) client.release();
        }

    } catch (e) { logger.error(`Immediate Snapshot failed: ${e.message}`); }
}

async function runSnapshotCycle() {
    if (isSnapshotRunning) return;
    isSnapshotRunning = true;
    const db = getDB();
    const redis = getClient();
    const connection = getConnection();
    await updateSolPrice(db);

    const BATCH_SIZE = 100;
    let offset = 0;
    let keepFetching = true;

    try {
        while (keepFetching) {
            const pools = await db.all(`SELECT t.pool_address as address, p.mint, p.dex, p.reserve_a, p.reserve_b, p.token_a, p.token_b FROM active_trackers t JOIN pools p ON t.pool_address = p.address ORDER BY t.priority DESC, t.pool_address ASC LIMIT ${BATCH_SIZE} OFFSET ${offset}`);
            
            if (pools.length === 0) break;
            
            await processPoolBatch(db, connection, pools, redis, false);
            
            if (pools.length < BATCH_SIZE) keepFetching = false;
            offset += BATCH_SIZE;
        }
    } catch (err) { logger.error(err.message); } finally { isSnapshotRunning = false; }
}

function startSnapshotter() {
    setTimeout(() => {
        runSnapshotCycle();
        setInterval(runSnapshotCycle, 30000); // 30s Loop
    }, 5000);
}

module.exports = { startSnapshotter, snapshotPools };
