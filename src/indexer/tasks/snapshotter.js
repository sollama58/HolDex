const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection } = require('../../services/solana'); 
const { getDB } = require('../../services/database');
const { getClient } = require('../../services/redis');
const { dequeueBatch } = require('../../services/queue');
const logger = require('../../services/logger');
const axios = require('axios'); // Added for fallback price fetching

// Distributed Lock Configuration
const LOCK_KEY = 'lock:snapshotter_cycle';
const LOCK_TTL = 30; // Seconds

// Data Offsets
const POOL_OFFSETS = {
    RAY_COIN: 320,
    RAY_PC: 352,
    ORCA_A: 101,
    ORCA_B: 133
};

// Quote Token Configuration
const QUOTE_TOKENS = {
    'So11111111111111111111111111111111111111112': { decimals: 9, isStable: false, symbol: 'SOL' }, // SOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': { decimals: 6, isStable: true, symbol: 'USDC' },  // USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': { decimals: 6, isStable: true, symbol: 'USDT' },  // USDT
};

const TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');

// Default safe fallback if DB lookup fails
let solPriceCache = 0; 

// Cache for decimals to avoid repeated RPC calls for the same token
const decimalCache = new Map();

/**
 * Robust SOL Price Updater
 * 1. Tries internal DB (USDC/SOL pool)
 * 2. Tries External API (CoinGecko/DexScreener)
 * 3. Falls back to last known or hardcoded safety
 */
async function updateSolPrice(db) {
    try {
        // 1. Try Internal DB (Fastest)
        const pool = await db.get(`
            SELECT price_usd FROM pools 
            WHERE token_a = 'So11111111111111111111111111111111111111112' 
            AND token_b = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' 
            AND liquidity_usd > 1000
            ORDER BY liquidity_usd DESC LIMIT 1
        `);
        
        if (pool && pool.price_usd > 0) {
            solPriceCache = pool.price_usd;
            return;
        }

        // 2. External Fallback (If internal pool missing/low liq)
        // We use a simple fetch to ensure we don't calculate everything based on $200
        const response = await axios.get('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd', { timeout: 2000 });
        if (response.data && response.data.solana && response.data.solana.usd) {
            solPriceCache = response.data.solana.usd;
            // logger.info(`Updated SOL Price from API: $${solPriceCache}`);
        }

    } catch (e) {
        // If everything fails, keep previous value. If 0, warn.
        if (solPriceCache === 0) {
            logger.warn("CRITICAL: Failed to fetch SOL price. Using default $150.");
            solPriceCache = 150; 
        }
    }
}

async function syncTokenPrice(db, mint, priceUsd, liquidityUsd, marketCap) {
    if (!mint || priceUsd <= 0) return;
    try {
        await db.run(`
            UPDATE tokens 
            SET priceUsd = $1, liquidity = $2, marketCap = $3 
            WHERE mint = $4
        `, [priceUsd, liquidityUsd, marketCap, mint]);
    } catch (e) {}
}

/**
 * Calculates correct price accounting for decimals
 */
function calculatePrice(rawQuote, rawBase, quoteDecimals, baseDecimals) {
    if (rawBase === 0) return 0;
    
    // Normalize to units
    const quoteAmount = rawQuote / Math.pow(10, quoteDecimals);
    const baseAmount = rawBase / Math.pow(10, baseDecimals);
    
    return quoteAmount / baseAmount;
}

/**
 * Batch fetches mint decimals from RPC
 */
async function fetchMintDecimals(connection, mints) {
    const missingMints = mints.filter(m => !decimalCache.has(m) && !QUOTE_TOKENS[m]);
    if (missingMints.length === 0) return;

    try {
        const publicKeys = missingMints.map(m => new PublicKey(m));
        // Split into chunks of 100 to avoid RPC limits
        const chunkSize = 100;
        for (let i = 0; i < publicKeys.length; i += chunkSize) {
            const chunk = publicKeys.slice(i, i + chunkSize);
            const infos = await connection.getMultipleAccountsInfo(chunk);
            
            infos.forEach((info, idx) => {
                const actualIdx = i + idx;
                if (info) {
                    // SPL Token Mint Layout offset 44 is decimals
                    const decimals = info.data[44];
                    decimalCache.set(missingMints[actualIdx], decimals);
                } else {
                    decimalCache.set(missingMints[actualIdx], 6); // Default
                }
            });
        }
    } catch (e) {
        logger.warn(`Failed to fetch decimals batch: ${e.message}`);
    }
}

async function processPoolBatch(db, connection, pools, redis) {
    const keysToFetch = [];
    const poolMap = new Map();
    const uniqueMints = new Set();
    const timestamp = Math.floor(Date.now() / 60000) * 60000;

    // 1. Prepare Batch
    pools.forEach((p) => {
        try {
            uniqueMints.add(p.mint);
            
            if (p.dex === 'pump') {
                // Pump Bonding Curve Address
                keysToFetch.push(new PublicKey(p.address));
                poolMap.set(p.address, { type: 'pump', index: keysToFetch.length - 1 });
            } else if (p.reserve_a && p.reserve_b) {
                // Direct AMM (Raydium/Orca)
                keysToFetch.push(new PublicKey(p.reserve_a));
                keysToFetch.push(new PublicKey(p.reserve_b));
                poolMap.set(p.address, { type: 'direct', idxA: keysToFetch.length - 2, idxB: keysToFetch.length - 1 });
            } else {
                // Discovery Mode (Find Reserves)
                // We handle Discovery separately to prevent slowing down the direct batch
                poolMap.set(p.address, { type: 'discovery', dex: p.dex });
            }
        } catch(e) { }
    });

    if (uniqueMints.size > 0) {
        await fetchMintDecimals(connection, Array.from(uniqueMints));
    }

    // 2. Fetch Data (Reserves)
    let accounts = [];
    if (keysToFetch.length > 0) {
        try {
            accounts = await connection.getMultipleAccountsInfo(keysToFetch);
        } catch (e) {
            logger.error(`Snapshot RPC Error: ${e.message}`);
            return; // Abort batch on RPC fail
        }
    }

    const updates = []; 

    for (const p of pools) {
        const task = poolMap.get(p.address);
        if (!task) continue;

        // Resolve Decimals
        const baseDecimals = decimalCache.get(p.mint) || p.decimals || 9; // Default 9 for most SPL

        // Identify Quote Token
        let quoteDecimals = 9;
        let quotePrice = 0;
        let isKnownQuote = false;

        // Strict Quote Checking
        if (QUOTE_TOKENS[p.token_a]) {
            quoteDecimals = QUOTE_TOKENS[p.token_a].decimals;
            quotePrice = p.token_a === 'So11111111111111111111111111111111111111112' ? solPriceCache : 1.0;
            isKnownQuote = true;
        } else if (QUOTE_TOKENS[p.token_b]) {
            quoteDecimals = QUOTE_TOKENS[p.token_b].decimals;
            quotePrice = p.token_b === 'So11111111111111111111111111111111111111112' ? solPriceCache : 1.0;
            isKnownQuote = true;
        }

        // Supply for Market Cap
        const tokenSupply = p.supply ? (parseFloat(p.supply) / Math.pow(10, baseDecimals)) : 1000000000;

        let nativePrice = 0;
        let liquidityUsd = 0;
        let currentQuoteReserve = 0;

        try {
            // --- PUMP.FUN ---
            if (task.type === 'pump') {
                const data = accounts[task.index]?.data;
                // Pump Layout: 8 (discriminator) + 8 (virtualToken) + 8 (virtualSol) + 8 (realToken) + 8 (realSol)
                if (data && data.length >= 40) {
                    const vToken = Number(data.readBigUInt64LE(8));
                    const vSol = Number(data.readBigUInt64LE(16));
                    const realSol = Number(data.readBigUInt64LE(32)); // Offset 32 is realSolReserves

                    if (vToken > 0) {
                        nativePrice = calculatePrice(vSol, vToken, 9, 6); // Pump is always 6 dec
                        liquidityUsd = (realSol / 1e9) * solPriceCache * 2; // Real Sol * Price * 2 (Symmetric)
                        currentQuoteReserve = realSol / 1e9;
                        quotePrice = solPriceCache;
                    }
                }
            } 
            // --- DISCOVERY (The "No Data" Fix) ---
            else if (task.type === 'discovery') {
                // We perform discovery HERE, individually, to catch errors per pool
                // This prevents one bad pool from killing the batch
                try {
                    const poolAddr = new PublicKey(p.address);
                    let vaultA, vaultB;

                    if (p.dex === 'raydium') {
                         // Fallback for Raydium
                         const info = await connection.getAccountInfo(poolAddr);
                         if (info && info.data.length === 752) {
                             vaultA = new PublicKey(info.data.subarray(POOL_OFFSETS.RAY_COIN, POOL_OFFSETS.RAY_COIN + 32));
                             vaultB = new PublicKey(info.data.subarray(POOL_OFFSETS.RAY_PC, POOL_OFFSETS.RAY_PC + 32));
                         }
                    }

                    // Generic Discovery (Parsed Accounts)
                    if (!vaultA) {
                        const tokenAccounts = await connection.getParsedTokenAccountsByOwner(poolAddr, { programId: TOKEN_PROGRAM_ID });
                        if (tokenAccounts.value.length >= 2) {
                            // Sort by amount to find the two largest (the vaults)
                            const sorted = tokenAccounts.value.sort((a, b) => 
                                b.account.data.parsed.info.tokenAmount.uiAmount - a.account.data.parsed.info.tokenAmount.uiAmount
                            );
                            vaultA = new PublicKey(sorted[0].pubkey);
                            vaultB = new PublicKey(sorted[1].pubkey);
                        }
                    }

                    if (vaultA && vaultB) {
                        // Found them! Save to DB so next run uses 'direct' path (Fast)
                        updates.push(db.run(`UPDATE pools SET reserve_a = $1, reserve_b = $2 WHERE address = $3`, [vaultA.toString(), vaultB.toString(), p.address]));
                        logger.info(`✅ Discovered Vaults for ${p.address}`);
                    }
                } catch (discoveryErr) {
                    // logger.warn(`Discovery failed for ${p.address}: ${discoveryErr.message}`);
                }
            } 
            // --- DIRECT BALANCE ---
            else if (task.type === 'direct') {
                const accA = accounts[task.idxA];
                const accB = accounts[task.idxB];
                
                if (accA && accB && accA.data.length >= 64) {
                    const balA = Number(accA.data.readBigUInt64LE(64));
                    const balB = Number(accB.data.readBigUInt64LE(64));
                    
                    let rawQuote = 0, rawBase = 0;
                    
                    if (QUOTE_TOKENS[p.token_a]) {
                        // Token A is Quote (e.g. USDC/BONK)
                        rawQuote = balA; rawBase = balB;
                    } else if (QUOTE_TOKENS[p.token_b]) {
                        // Token B is Quote (e.g. BONK/SOL)
                        rawQuote = balB; rawBase = balA;
                    } else {
                        // UNKNOWN QUOTE PAIR (e.g. MEME1/MEME2)
                        // We skip price calc to avoid garbage data, but we track the raw reserves
                        // Or we could try to see if one is wrapped SOL
                        continue; 
                    }

                    // Calculate Price
                    nativePrice = calculatePrice(rawQuote, rawBase, quoteDecimals, baseDecimals);
                    
                    const normQuote = rawQuote / Math.pow(10, quoteDecimals);
                    liquidityUsd = normQuote * quotePrice * 2;
                    currentQuoteReserve = normQuote;
                }
            }

            // --- UPDATE DB ---
            if (nativePrice > 0 && liquidityUsd > 0) {
                const finalPriceUsd = nativePrice * quotePrice;
                
                // Sanity check: If price is infinity or NaN, skip
                if (!isFinite(finalPriceUsd)) continue;

                const marketCap = finalPriceUsd * tokenSupply;

                // Volume Calculation (Approximate based on reserve changes)
                let approxVolume = 0;
                if (redis) {
                    const volKey = `vol:${p.address}`;
                    const prev = await redis.get(volKey);
                    if (prev) {
                        const delta = Math.abs(currentQuoteReserve - parseFloat(prev));
                        // Ignore massive jumps (new liquidity add) or tiny noise
                        if (delta > 0.0001 && delta < (currentQuoteReserve * 0.5)) {
                            approxVolume = delta * quotePrice;
                        }
                    }
                    await redis.set(volKey, currentQuoteReserve, 'EX', 300);
                }

                updates.push(db.run(`UPDATE pools SET price_usd = $1, liquidity_usd = $2 WHERE address = $3`, [finalPriceUsd, liquidityUsd, p.address]));
                
                // Update Candles
                updates.push(db.run(`
                    INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close, volume) 
                    VALUES ($1, $2, $3, $3, $3, $3, $4) 
                    ON CONFLICT(pool_address, timestamp) 
                    DO UPDATE SET 
                        close = $3, 
                        volume = candles_1m.volume + $4, 
                        high = GREATEST(candles_1m.high, $3), 
                        low = LEAST(candles_1m.low, $3)
                `, [p.address, timestamp, finalPriceUsd, approxVolume]));
                
                // Update Main Token Stats
                updates.push(syncTokenPrice(db, p.mint, finalPriceUsd, liquidityUsd, marketCap));
            }

        } catch (inner) {
             // Swallow individual pool errors
        }
    }
    await Promise.all(updates);
}

async function runSnapshotCycle() {
    const redis = getClient();
    if (redis) {
        // Distributed Lock to prevent overlapping runs
        const acquired = await redis.set(LOCK_KEY, '1', 'NX', 'EX', LOCK_TTL);
        if (!acquired) return; 
    } else {
        if (global.isSnapshotRunning) return;
        global.isSnapshotRunning = true;
    }

    const db = getDB();
    const connection = getSolanaConnection();
    
    // Ensure SOL Price is valid before processing
    await updateSolPrice(db);

    try {
        // 1. Process Queued Items (High Priority)
        const queuedMints = await dequeueBatch(20);
        if (queuedMints.length > 0) {
            const poolRes = await db.all(`
                SELECT p.address, p.mint, p.dex, p.reserve_a, p.reserve_b, p.token_a, p.token_b, t.decimals, t.supply 
                FROM pools p 
                LEFT JOIN tokens t ON p.mint = t.mint 
                WHERE p.mint IN (${queuedMints.map(m => `'${m}'`).join(',')})
            `);
            if (poolRes.length > 0) await processPoolBatch(db, connection, poolRes, redis);
        }

        // 2. Process All Active Trackers
        const BATCH_SIZE = 50; // Smaller batch size for stability
        let offset = 0;
        let keepFetching = true;

        while (keepFetching) {
            const pools = await db.all(`
                SELECT p.address, p.mint, p.dex, p.reserve_a, p.reserve_b, p.token_a, p.token_b, t.decimals, t.supply 
                FROM active_trackers tr
                JOIN pools p ON tr.pool_address = p.address
                LEFT JOIN tokens t ON p.mint = t.mint
                ORDER BY tr.priority DESC, tr.pool_address ASC 
                LIMIT ${BATCH_SIZE} OFFSET ${offset}
            `);
            
            if (pools.length === 0) break;
            
            await processPoolBatch(db, connection, pools, redis);
            
            if (pools.length < BATCH_SIZE) keepFetching = false;
            offset += BATCH_SIZE;
            
            // Tiny sleep to yield event loop
            await new Promise(r => setTimeout(r, 100));
        }
    } catch (err) { 
        logger.error(`Snapshot Cycle Error: ${err.message}`); 
    } finally {
        if (!redis) global.isSnapshotRunning = false;
        if (redis) await redis.del(LOCK_KEY);
    }
}

function startSnapshotter() {
    setTimeout(() => {
        logger.info("🟢 Snapshotter Started");
        runSnapshotCycle();
        setInterval(runSnapshotCycle, 20000); // 20s interval
    }, 5000);
}

async function snapshotPools(poolAddresses) {
    if (!poolAddresses.length) return;
    const db = getDB();
    const redis = getClient();
    const connection = getSolanaConnection();
    
    // Quick SOL price check
    await updateSolPrice(db);

    const pools = await db.all(`
        SELECT p.address, p.mint, p.dex, p.reserve_a, p.reserve_b, p.token_a, p.token_b, t.decimals, t.supply 
        FROM pools p 
        LEFT JOIN tokens t ON p.mint = t.mint 
        WHERE p.address IN (${poolAddresses.map(p => `'${p}'`).join(',')})
    `);
    await processPoolBatch(db, connection, pools, redis);
}

module.exports = { startSnapshotter, snapshotPools };
