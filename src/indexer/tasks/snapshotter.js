const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection } = require('../../services/solana'); 
const { getDB } = require('../../services/database');
const { getClient } = require('../../services/redis');
const { dequeueBatch } = require('../../services/queue');
const logger = require('../../services/logger');

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
    'So11111111111111111111111111111111111111112': { decimals: 9, isStable: false }, // SOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': { decimals: 6, isStable: true },  // USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': { decimals: 6, isStable: true },  // USDT
};

const TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');

let solPriceCache = 200;

// Cache for decimals to avoid repeated RPC calls for the same token
const decimalCache = new Map();

async function updateSolPrice(db) {
    try {
        const pool = await db.get(`
            SELECT price_usd FROM pools 
            WHERE token_a = 'So11111111111111111111111111111111111111112' 
            AND token_b = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' 
            ORDER BY liquidity_usd DESC LIMIT 1
        `);
        if (pool && pool.price_usd > 0) solPriceCache = pool.price_usd;
    } catch (e) {}
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
    // Price = (RawQuote / 10^QuoteDecimals) / (RawBase / 10^BaseDecimals)
    //       = (RawQuote * 10^BaseDecimals) / (RawBase * 10^QuoteDecimals)
    
    // We use a simpler ratio multiplication to avoid massive numbers
    const rawRatio = rawQuote / rawBase;
    const decimalAdjustment = Math.pow(10, baseDecimals - quoteDecimals);
    return rawRatio * decimalAdjustment;
}

/**
 * Batch fetches mint decimals from RPC
 */
async function fetchMintDecimals(connection, mints) {
    const missingMints = mints.filter(m => !decimalCache.has(m) && !QUOTE_TOKENS[m]);
    if (missingMints.length === 0) return;

    try {
        const publicKeys = missingMints.map(m => new PublicKey(m));
        const infos = await connection.getMultipleAccountsInfo(publicKeys);
        
        infos.forEach((info, i) => {
            if (info) {
                // SPL Token Mint Layout:
                // 0-4: Mint Authority Option
                // 4-36: Mint Authority
                // 36-44: Supply
                // 44: Decimals (1 byte)
                const decimals = info.data[44];
                decimalCache.set(missingMints[i], decimals);
            } else {
                // Default if not found (safer than crashing)
                decimalCache.set(missingMints[i], 6); 
            }
        });
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
                keysToFetch.push(new PublicKey(p.address));
                poolMap.set(p.address, { type: 'pump', index: keysToFetch.length - 1 });
            } else if (p.reserve_a && p.reserve_b) {
                keysToFetch.push(new PublicKey(p.reserve_a));
                keysToFetch.push(new PublicKey(p.reserve_b));
                poolMap.set(p.address, { type: 'direct', idxA: keysToFetch.length - 2, idxB: keysToFetch.length - 1 });
            } else {
                keysToFetch.push(new PublicKey(p.address));
                poolMap.set(p.address, { type: 'discovery', dex: p.dex, index: keysToFetch.length - 1 });
            }
        } catch(e) { }
    });

    if (keysToFetch.length === 0) return;

    // 2. Fetch Data (Reserves + Decimals)
    // Run in parallel for speed
    const [accounts] = await Promise.all([
        connection.getMultipleAccountsInfo(keysToFetch),
        fetchMintDecimals(connection, Array.from(uniqueMints))
    ]);

    const updates = []; 

    for (const p of pools) {
        const task = poolMap.get(p.address);
        if (!task) continue;

        let nativePrice = 0, liquidityUsd = 0;
        let currentQuoteReserve = 0;
        
        // Resolve Decimals
        const baseDecimals = decimalCache.get(p.mint) || p.decimals || 6;
        
        // Identify Quote Token & Decimals
        let quoteDecimals = 9;
        let isQuoteSol = true; 
        let quotePrice = solPriceCache;

        if (QUOTE_TOKENS[p.token_a]) {
            quoteDecimals = QUOTE_TOKENS[p.token_a].decimals;
            isQuoteSol = p.token_a === 'So11111111111111111111111111111111111111112';
            quotePrice = isQuoteSol ? solPriceCache : 1.0; 
        } else if (QUOTE_TOKENS[p.token_b]) {
            quoteDecimals = QUOTE_TOKENS[p.token_b].decimals;
            isQuoteSol = p.token_b === 'So11111111111111111111111111111111111111112';
            quotePrice = isQuoteSol ? solPriceCache : 1.0;
        }

        // Use Supply from DB if available, else calc via RPC later (omitted for speed here)
        // Default 1B for quick estimation if missing
        const tokenSupply = p.supply ? (p.supply / Math.pow(10, baseDecimals)) : 1000000000;

        try {
            // --- PUMP.FUN ---
            if (task.type === 'pump') {
                const data = accounts[task.index]?.data;
                if (data && data.length >= 40) {
                    const vToken = Number(data.readBigUInt64LE(8));
                    const vSol = Number(data.readBigUInt64LE(16));
                    const realSol = Number(data.readBigUInt64LE(32));
                    const complete = data.length >= 49 ? (data[48] === 1) : false;

                    if (!complete && vToken > 0) {
                        // Pump is always 6 decimals for token, 9 for SOL
                        nativePrice = calculatePrice(vSol, vToken, 9, 6);
                        liquidityUsd = (realSol / 1e9) * solPriceCache * 2;
                        currentQuoteReserve = realSol / 1e9;
                    }
                }
            } 
            // --- DISCOVERY ---
            else if (task.type === 'discovery') {
                const data = accounts[task.index]?.data;
                const poolAddr = new PublicKey(p.address);
                let vaultA, vaultB;

                if (data && p.dex === 'raydium' && data.length === 752) {
                    vaultA = new PublicKey(data.subarray(POOL_OFFSETS.RAY_COIN, POOL_OFFSETS.RAY_COIN + 32));
                    vaultB = new PublicKey(data.subarray(POOL_OFFSETS.RAY_PC, POOL_OFFSETS.RAY_PC + 32));
                } else {
                    const tokenAccounts = await connection.getParsedTokenAccountsByOwner(poolAddr, { programId: TOKEN_PROGRAM_ID });
                    if (tokenAccounts.value.length >= 2) {
                        const sorted = tokenAccounts.value.sort((a, b) => b.account.data.parsed.info.tokenAmount.uiAmount - a.account.data.parsed.info.tokenAmount.uiAmount);
                        vaultA = new PublicKey(sorted[0].pubkey);
                        vaultB = new PublicKey(sorted[1].pubkey);
                    }
                }

                if (vaultA && vaultB) {
                    updates.push(db.run(`UPDATE pools SET reserve_a = $1, reserve_b = $2 WHERE address = $3`, [vaultA.toString(), vaultB.toString(), p.address]));
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
                        rawQuote = balA; rawBase = balB;
                    } else {
                        rawQuote = balB; rawBase = balA;
                    }

                    // Calculate Price with DYNAMIC Base Decimals
                    nativePrice = calculatePrice(rawQuote, rawBase, quoteDecimals, baseDecimals);
                    
                    const normQuote = rawQuote / Math.pow(10, quoteDecimals);
                    liquidityUsd = normQuote * quotePrice * 2;
                    currentQuoteReserve = normQuote;
                }
            }

            // --- UPDATE DB ---
            if (nativePrice > 0) {
                const finalPriceUsd = nativePrice * quotePrice;
                const marketCap = finalPriceUsd * tokenSupply;

                let approxVolume = 0;
                if (redis) {
                    const volKey = `vol:${p.address}`;
                    const prev = await redis.get(volKey);
                    if (prev) {
                        const delta = Math.abs(currentQuoteReserve - parseFloat(prev));
                        if (delta > 0.000001 && delta < (currentQuoteReserve * 0.5)) {
                            approxVolume = delta * quotePrice;
                        }
                    }
                    await redis.set(volKey, currentQuoteReserve, 'EX', 300);
                }

                updates.push(db.run(`UPDATE pools SET price_usd = $1, liquidity_usd = $2 WHERE address = $3`, [finalPriceUsd, liquidityUsd, p.address]));
                updates.push(db.run(`INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close, volume) VALUES ($1, $2, $3, $3, $3, $3, $4) ON CONFLICT(pool_address, timestamp) DO UPDATE SET close = $3, volume = candles_1m.volume + $4, high = GREATEST(candles_1m.high, $3), low = LEAST(candles_1m.low, $3)`, [p.address, timestamp, finalPriceUsd, approxVolume]));
                updates.push(syncTokenPrice(db, p.mint, finalPriceUsd, liquidityUsd, marketCap));
            }

        } catch (inner) {}
    }
    await Promise.all(updates);
}

async function runSnapshotCycle() {
    const redis = getClient();
    if (redis) {
        const acquired = await redis.set(LOCK_KEY, '1', 'NX', 'EX', LOCK_TTL);
        if (!acquired) return; 
    } else {
        if (global.isSnapshotRunning) return;
        global.isSnapshotRunning = true;
    }

    const db = getDB();
    const connection = getSolanaConnection();
    await updateSolPrice(db);

    try {
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

        const BATCH_SIZE = 100;
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
        runSnapshotCycle();
        setInterval(runSnapshotCycle, 30000);
    }, 5000);
}

async function snapshotPools(poolAddresses) {
    if (!poolAddresses.length) return;
    const db = getDB();
    const redis = getClient();
    const connection = getSolanaConnection();
    const pools = await db.all(`
        SELECT p.address, p.mint, p.dex, p.reserve_a, p.reserve_b, p.token_a, p.token_b, t.decimals, t.supply 
        FROM pools p 
        LEFT JOIN tokens t ON p.mint = t.mint 
        WHERE p.address IN (${poolAddresses.map(p => `'${p}'`).join(',')})
    `);
    await processPoolBatch(db, connection, pools, redis);
}

module.exports = { startSnapshotter, snapshotPools };
