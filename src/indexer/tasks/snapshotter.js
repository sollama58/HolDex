const { PublicKey } = require('@solana/web3.js');
const { getConnection } = require('../services/helius');
const { getDB } = require('../../services/database');
const logger = require('../../services/logger');

let isSnapshotRunning = false;
let solPriceCache = 200; 

// Memory Cache for Reserve Deltas (To calculate Volume)
const reserveCache = new Map(); // poolAddress -> { quoteReserve: number }

// FIX: Correct Raydium V4 Offsets for VAULTS (not Mints)
const RAY_COIN_VAULT_OFFSET = 320; 
const RAY_PC_VAULT_OFFSET = 352;
const ORCA_VAULT_A_OFFSET = 101;
const ORCA_VAULT_B_OFFSET = 133;
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');

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
 * EXPORTED: Snapshot specific pools immediately.
 * Used when a user adds a new token to populate data instantly.
 */
async function snapshotPools(poolAddresses) {
    if (!poolAddresses || poolAddresses.length === 0) return;
    const db = getDB();
    const connection = getConnection();
    await updateSolPrice(db);

    logger.info(`📸 Immediate Snapshot for ${poolAddresses.length} pools...`);

    const pools = await db.all(`
        SELECT p.address as pool_address, p.mint, p.dex, p.reserve_a, p.reserve_b, p.token_a, p.token_b
        FROM pools p WHERE p.address IN (${poolAddresses.map(p => `'${p}'`).join(',')})
    `);

    const keysToFetch = [];
    const poolMap = new Map();

    pools.forEach((p, i) => {
        if (p.dex === 'pump') {
            keysToFetch.push(new PublicKey(p.pool_address));
            poolMap.set(p.pool_address, { type: 'pump', index: keysToFetch.length - 1, pool: p });
        } else if (['raydium', 'orca', 'meteora'].includes(p.dex)) {
             if (p.reserve_a && p.reserve_b) {
                keysToFetch.push(new PublicKey(p.reserve_a));
                keysToFetch.push(new PublicKey(p.reserve_b));
                poolMap.set(p.pool_address, { type: 'direct', idxA: keysToFetch.length - 2, idxB: keysToFetch.length - 1, pool: p });
            } else {
                // DISCOVERY NEEDED
                keysToFetch.push(new PublicKey(p.pool_address));
                poolMap.set(p.pool_address, { type: 'discovery', dex: p.dex, index: keysToFetch.length - 1, pool: p });
            }
        }
    });

    if (keysToFetch.length === 0) return;

    try {
        const accounts = await connection.getMultipleAccountsInfo(keysToFetch);
        const timestamp = Date.now();
        
        // Secondary Fetch List (For newly discovered Vaults)
        const secondaryKeys = [];
        const secondaryMap = new Map(); // key -> { pool, type: 'A' or 'B' }

        // Pass 1: Handle Pump & Discovery
        for (const p of pools) {
            const task = poolMap.get(p.pool_address);
            if (!task) continue;

            if (task.type === 'pump') {
                const data = accounts[task.index]?.data;
                if (data && data.length >= 40) {
                     const vToken = data.readBigUInt64LE(8);
                     const vSol = data.readBigUInt64LE(16);
                     const realSol = data.readBigUInt64LE(32);
                     if (Number(vToken) > 0) {
                         const nativePrice = Number(vSol) / Number(vToken);
                         const liquidityUsd = (Number(realSol) / 1e9) * solPriceCache;
                         const finalPrice = nativePrice * solPriceCache;
                         
                         await db.run(`UPDATE pools SET price_usd = $1, liquidity_usd = $2 WHERE address = $3`, [finalPrice, liquidityUsd, p.pool_address]);
                     }
                }
            } else if (task.type === 'discovery') {
                // FIX: DECODE VAULTS AND PREPARE SECONDARY FETCH
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
                        // 1. Save to DB so next time it is 'direct'
                        await db.run(`UPDATE pools SET reserve_a = $1, reserve_b = $2 WHERE address = $3`, [vaultA.toString(), vaultB.toString(), p.pool_address]);
                        
                        // 2. Queue for Immediate Balance Fetch
                        secondaryKeys.push(vaultA);
                        secondaryKeys.push(vaultB);
                        secondaryMap.set(p.pool_address, { pool: p, idxA: secondaryKeys.length - 2, idxB: secondaryKeys.length - 1 });
                    }
                }
            }
        }

        // Pass 2: Fetch Newly Discovered Vaults & Process Direct
        if (secondaryKeys.length > 0) {
            const secondaryAccounts = await connection.getMultipleAccountsInfo(secondaryKeys);
            
            // Process secondary results
            for (const [poolAddr, task] of secondaryMap.entries()) {
                const accA = secondaryAccounts[task.idxA];
                const accB = secondaryAccounts[task.idxB];
                await processBalanceLogic(db, task.pool, accA, accB, timestamp);
            }
        }

        // Process Direct (already had keys)
        for (const p of pools) {
            const task = poolMap.get(p.pool_address);
            if (task && task.type === 'direct') {
                const accA = accounts[task.idxA];
                const accB = accounts[task.idxB];
                await processBalanceLogic(db, p, accA, accB, timestamp);
            }
        }

    } catch (e) { logger.error(`Immediate Snapshot failed: ${e.message}`); }
}

async function processBalanceLogic(db, p, accA, accB, timestamp) {
    if (accA && accB) {
         const balA = Number(accA.data.readBigUInt64LE(64));
         const balB = Number(accB.data.readBigUInt64LE(64));
         let nativePrice = 0, liquidityUsd = 0, quoteIsSol = false;
         let currentQuoteReserve = 0;

         const isAQuote = QUOTE_MINTS.has(p.token_a);
         const isBQuote = QUOTE_MINTS.has(p.token_b);

         if (isAQuote && !isBQuote) {
             if (balB > 0) nativePrice = balA/balB;
             liquidityUsd = (balA/1e9) * solPriceCache; // Approx
             if (p.token_a.includes('So11')) quoteIsSol = true;
             currentQuoteReserve = balA;
         } else if (isBQuote && !isAQuote) {
             if (balA > 0) nativePrice = balB/balA;
             liquidityUsd = (balB/1e9) * solPriceCache; 
             if (p.token_b.includes('So11')) quoteIsSol = true;
             currentQuoteReserve = balB;
         } else {
             // Fallback
             if (balA > 0) nativePrice = balB/balA;
         }

         if (nativePrice > 0) {
             const finalPrice = quoteIsSol ? nativePrice * solPriceCache : nativePrice;
             // Cache Volume Logic
             let approxVolume = 0;
             if (reserveCache.has(p.pool_address)) {
                const prev = reserveCache.get(p.pool_address);
                const delta = Math.abs(currentQuoteReserve - prev.quoteReserve);
                // Adjust threshold based on decimals (raw units here)
                if (delta > 1000) approxVolume = delta / 1e9; 
             }
             reserveCache.set(p.pool_address, { quoteReserve: currentQuoteReserve });

             await db.run(`UPDATE pools SET price_usd = $1, liquidity_usd = $2 WHERE address = $3`, [finalPrice, liquidityUsd, p.pool_address]);
             await db.run(`INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close, volume) VALUES ($1, $2, $3, $3, $3, $3, $4) ON CONFLICT(pool_address, timestamp) DO UPDATE SET close = $3, volume = candles_1m.volume + $4`, [p.pool_address, timestamp, finalPrice, approxVolume]);
         }
    }
}

async function runSnapshotCycle() {
    if (isSnapshotRunning) return;
    isSnapshotRunning = true;
    const db = getDB();
    const connection = getConnection();
    await updateSolPrice(db);

    const BATCH_SIZE = 100;
    const timestamp = Math.floor(Date.now() / 60000) * 60000;
    let offset = 0;
    let keepFetching = true;

    try {
        while (keepFetching) {
            const pools = await db.all(`SELECT t.pool_address, p.mint, p.dex, p.reserve_a, p.reserve_b, p.token_a, p.token_b FROM active_trackers t JOIN pools p ON t.pool_address = p.address ORDER BY t.priority DESC, t.pool_address ASC LIMIT ${BATCH_SIZE} OFFSET ${offset}`);
            if (pools.length === 0) break;
            const keysToFetch = [];
            const poolMap = new Map();
            for (let i = 0; i < pools.length; i++) {
                const p = pools[i];
                const pid = p.pool_address;
                if (p.dex === 'pump') {
                    keysToFetch.push(new PublicKey(pid));
                    poolMap.set(pid, { type: 'pump', index: keysToFetch.length - 1 });
                } else if (['raydium', 'orca', 'meteora'].includes(p.dex)) {
                    if (p.reserve_a && p.reserve_b) {
                        keysToFetch.push(new PublicKey(p.reserve_a));
                        keysToFetch.push(new PublicKey(p.reserve_b));
                        poolMap.set(pid, { type: 'direct_balance', idxA: keysToFetch.length - 2, idxB: keysToFetch.length - 1 });
                    } else {
                        keysToFetch.push(new PublicKey(pid));
                        poolMap.set(pid, { type: 'discovery', dex: p.dex, index: keysToFetch.length - 1 });
                    }
                }
            }
            if (keysToFetch.length === 0) { offset += BATCH_SIZE; continue; }
            const accounts = await connection.getMultipleAccountsInfo(keysToFetch);
            const updates = [];
            for (const p of pools) {
                const task = poolMap.get(p.pool_address);
                if (!task) continue;
                let nativePrice = 0, liquidityUsd = 0, quoteIsSol = false;
                let currentQuoteReserve = 0;

                try {
                    if (task.type === 'pump') {
                        const data = accounts[task.index]?.data;
                        if (data && data.length >= 40) {
                            const vToken = data.readBigUInt64LE(8);
                            const vSol = data.readBigUInt64LE(16);
                            const realSol = data.readBigUInt64LE(32);
                            if (Number(vToken) > 0) {
                                nativePrice = Number(vSol) / Number(vToken);
                                liquidityUsd = (Number(realSol) / 1e9) * solPriceCache;
                                quoteIsSol = true;
                                currentQuoteReserve = Number(realSol) / 1e9;
                            }
                        }
                    } else if (task.type === 'discovery') {
                        const data = accounts[task.index]?.data;
                        if (data) {
                            let vaultA, vaultB;
                            if (task.dex === 'raydium' && data.length >= 500) {
                                // FIX: Use Correct Offsets Here too
                                vaultA = new PublicKey(data.subarray(RAY_COIN_VAULT_OFFSET, RAY_COIN_VAULT_OFFSET + 32));
                                vaultB = new PublicKey(data.subarray(RAY_PC_VAULT_OFFSET, RAY_PC_VAULT_OFFSET + 32));
                            } else if (task.dex === 'orca' && data.length >= 165) {
                                vaultA = new PublicKey(data.subarray(ORCA_VAULT_A_OFFSET, ORCA_VAULT_A_OFFSET + 32));
                                vaultB = new PublicKey(data.subarray(ORCA_VAULT_B_OFFSET, ORCA_VAULT_B_OFFSET + 32));
                            }
                            if (vaultA && vaultB) updates.push(db.run(`UPDATE pools SET reserve_a = $1, reserve_b = $2 WHERE address = $3`, [vaultA.toString(), vaultB.toString(), p.pool_address]));
                        }
                    } else if (task.type === 'direct_balance') {
                        const accA = accounts[task.idxA];
                        const accB = accounts[task.idxB];
                        if (accA && accB && accA.data.length >= 72) {
                            const balA = Number(accA.data.readBigUInt64LE(64));
                            const balB = Number(accB.data.readBigUInt64LE(64));
                            const isAQuote = QUOTE_MINTS.has(p.token_a);
                            const isBQuote = QUOTE_MINTS.has(p.token_b);
                            
                            // Simplified Balance Logic similar to immediate
                            if (isAQuote && !isBQuote) {
                                if (balB > 0) nativePrice = balA / balB; 
                                liquidityUsd = (balA / (p.token_a.includes('So11') ? 1e9 : 1e6)) * (p.token_a.includes('So11') ? solPriceCache : 1);
                                if (p.token_a.includes('So11')) quoteIsSol = true;
                                currentQuoteReserve = balA / (p.token_a.includes('So11') ? 1e9 : 1e6);
                            } else if (isBQuote && !isAQuote) {
                                if (balA > 0) nativePrice = balB / balA;
                                liquidityUsd = (balB / (p.token_b.includes('So11') ? 1e9 : 1e6)) * (p.token_b.includes('So11') ? solPriceCache : 1);
                                if (p.token_b.includes('So11')) quoteIsSol = true;
                                currentQuoteReserve = balB / (p.token_b.includes('So11') ? 1e9 : 1e6);
                            } else {
                                if (balA > 0) nativePrice = balB / balA;
                            }
                        }
                    }
                    if (nativePrice > 0) {
                        const finalPriceUsd = quoteIsSol ? (nativePrice * solPriceCache) : nativePrice;
                        
                        // --- VOLUME LOGIC START ---
                        let approxVolume = 0;
                        if (reserveCache.has(p.pool_address)) {
                            const prev = reserveCache.get(p.pool_address);
                            const delta = Math.abs(currentQuoteReserve - prev.quoteReserve);
                            if (delta > 0.000001) {
                                approxVolume = delta;
                            }
                        }
                        reserveCache.set(p.pool_address, { quoteReserve: currentQuoteReserve });
                        // --- VOLUME LOGIC END ---

                        updates.push(db.run(`INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close, volume) VALUES ($1, $2, $3, $3, $3, $3, $4) ON CONFLICT(pool_address, timestamp) DO UPDATE SET close = $3, volume = candles_1m.volume + $4`, [p.pool_address, timestamp, finalPriceUsd, approxVolume]));
                        updates.push(db.run(`UPDATE pools SET price_usd = $1, liquidity_usd = $2 WHERE address = $3`, [finalPriceUsd, liquidityUsd, p.pool_address]));
                    }
                } catch (e) {}
            }
            await Promise.all(updates);
            offset += BATCH_SIZE;
        }
    } catch (err) { logger.error(err.message); } finally { isSnapshotRunning = false; }
}

function startSnapshotter() {
    setTimeout(() => {
        runSnapshotCycle();
        setInterval(runSnapshotCycle, 60000);
    }, 5000);
}

module.exports = { startSnapshotter, snapshotPools };
