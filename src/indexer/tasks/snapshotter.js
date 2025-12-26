const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection, retryRPC } = require('../../services/solana'); 
const { getDB, aggregateAndSaveToken } = require('../../services/database');
const { getClient } = require('../../services/redis');
const logger = require('../../services/logger');
const { getRealVolume } = require('../services/volume_tracker');

const stateCache = new Map();
const QUOTE_TOKENS = {
    'So11111111111111111111111111111111111111112': { decimals: 9, symbol: 'SOL' },
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': { decimals: 6, symbol: 'USDC' },
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': { decimals: 6, symbol: 'USDT' },
};

let solPriceCache = 200; 

// Update SOL Price every minute (simple cache)
async function updateSolPrice(db) {
    try {
        const pool = await db.get(`SELECT price_usd FROM pools WHERE token_a = 'So11111111111111111111111111111111111111112' AND liquidity_usd > 10000 ORDER BY liquidity_usd DESC LIMIT 1`);
        if (pool && pool.price_usd > 0) solPriceCache = pool.price_usd;
    } catch(e) {}
}

async function processPoolBatch(db, connection, pools, redis) {
    const keysToFetch = [];
    const poolMap = new Map();
    const affectedMints = new Set();
    const timestamp = Math.floor(Date.now() / 60000) * 60000;
    const now = Date.now();

    // 1. Prepare Batch Request for Reserves
    pools.forEach(p => {
        try {
            if (p.dex === 'pumpfun' && p.reserve_a) {
                 // PumpFun bonding curve logic is different, often reading the curve state itself
                 // But typically reserve_a is the Token Vault, reserve_b is the Curve Account (SOL virtual reserves)
                 keysToFetch.push(new PublicKey(p.reserve_a)); // Token Vault
                 keysToFetch.push(new PublicKey(p.reserve_b)); // Bonding Curve State
                 poolMap.set(p.address, { type: 'pumpfun', idxA: keysToFetch.length - 2, idxB: keysToFetch.length - 1, pool: p });
            } else if (p.reserve_a && p.reserve_b) {
                 // Standard AMM (Raydium, Meteora, Orca)
                 keysToFetch.push(new PublicKey(p.reserve_a));
                 keysToFetch.push(new PublicKey(p.reserve_b));
                 poolMap.set(p.address, { type: 'standard', idxA: keysToFetch.length - 2, idxB: keysToFetch.length - 1, pool: p });
            }
        } catch(e) {}
    });

    if (keysToFetch.length === 0) return;

    let accounts = [];
    try { 
        accounts = await retryRPC((conn) => conn.getMultipleAccountsInfo(keysToFetch));
    } catch (e) { return; }

    const updates = [];
    const trackerUpdates = [];
    
    // 2. Process Price & Liquidity
    for (const p of pools) {
        const task = poolMap.get(p.address);
        if (!task) continue;

        let priceUsd = 0;
        let liquidityUsd = 0;
        let success = false;
        let volumeUsd = 0;

        const accA = accounts[task.idxA];
        const accB = accounts[task.idxB];

        if (accA && accB) {
            try {
                let reserveA = 0;
                let reserveB = 0;
                let decA = 6; // Default/Guess
                let decB = 9; // Default SOL

                // Helper to read U64 LE from Token Account
                const readAmount = (buffer) => {
                    if (buffer.length < 72) return 0; // Invalid Token Account
                    // Amount is at offset 64
                    return Number(buffer.readBigUInt64LE(64));
                };

                if (task.type === 'pumpfun') {
                    // PumpFun: A is Token Vault (Amount), B is Curve State (Virtual SOL)
                    // Curve State Offset 8 is Virtual Token Reserves, Offset 16 is Virtual SOL Reserves
                    reserveA = Number(accB.data.readBigUInt64LE(8)); // Virtual Token Res
                    reserveB = Number(accB.data.readBigUInt64LE(16)); // Virtual Sol Res
                    decA = 6;
                    decB = 9;
                } else {
                    // Standard AMM: Both are Token Accounts
                    reserveA = readAmount(accA.data);
                    reserveB = readAmount(accB.data);
                    
                    // Determine decimals (Heuristic: Quote is usually SOL/USDC)
                    const isBQuote = QUOTE_TOKENS[p.token_b];
                    const isAQuote = QUOTE_TOKENS[p.token_a];
                    
                    if (isBQuote) {
                        decB = isBQuote.decimals;
                        // Fetch mint A decimals from DB if possible, or assume 6/9
                        // For speed, we assume 6 if unknown, or rely on ratio
                        decA = 6; // ToDo: Fetch from tokens table
                    } else if (isAQuote) {
                        decA = isAQuote.decimals;
                        decB = 6;
                    }
                }

                if (reserveA > 0 && reserveB > 0) {
                    const rawA = reserveA / Math.pow(10, decA);
                    const rawB = reserveB / Math.pow(10, decB);
                    
                    // Price of A in terms of B
                    let priceInB = rawB / rawA;
                    
                    // Convert to USD
                    let quotePrice = 0;
                    if (QUOTE_TOKENS[p.token_b] || p.token_b === 'So11111111111111111111111111111111111111112') {
                        // B is Quote
                        if (p.token_b.includes('So111')) quotePrice = solPriceCache;
                        else quotePrice = 1; // USDC/USDT
                        priceUsd = priceInB * quotePrice;
                        liquidityUsd = rawB * quotePrice * 2; 
                    } else if (QUOTE_TOKENS[p.token_a]) {
                        // A is Quote (Inverse)
                        if (p.token_a.includes('So111')) quotePrice = solPriceCache;
                        else quotePrice = 1;
                        priceUsd = (1 / priceInB) * quotePrice;
                        liquidityUsd = rawA * quotePrice * 2;
                    }

                    if (priceUsd > 0) success = true;
                }

            } catch (err) {
                // logger.error(`Math Error ${p.address}: ${err.message}`);
            }
        }

        // --- VOLUME CHECK ---
        const volKey = `vol_last_check:${p.address}`;
        const lastCheck = stateCache.get(volKey) || 0;

        if (now - lastCheck > 120000) { 
            const sigKey = `vol_sig:${p.address}`;
            const lastSig = stateCache.get(sigKey);
            // Async volume check, doesn't block
            getRealVolume(p.address, lastSig, solPriceCache).then(volData => {
                if (volData.txCount > 0) {
                    stateCache.set(sigKey, volData.latestSignature);
                }
            }).catch(() => {});
            stateCache.set(volKey, now);
        }

        if (success) {
            affectedMints.add(p.mint);
            
            // 1. Update Pool State
            updates.push(db.query(`UPDATE pools SET price_usd = $1, liquidity_usd = $2 WHERE address = $3`, [priceUsd, liquidityUsd, p.address]));
            
            // 2. Insert Candle
            updates.push(db.query(`
                INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close, volume) 
                VALUES ($1, $2, $3, $3, $3, $3, $4) 
                ON CONFLICT(pool_address, timestamp) 
                DO UPDATE SET 
                    close = $3, 
                    high = GREATEST(candles_1m.high, $3), 
                    low = LEAST(candles_1m.low, $3),
                    volume = candles_1m.volume + $4
            `, [p.address, timestamp, priceUsd, volumeUsd]));
            
            // 3. Update Tracker
            trackerUpdates.push(db.query(`UPDATE active_trackers SET last_check = $1 WHERE pool_address = $2`, [now, p.address]));
        }
    }
    
    await Promise.allSettled([...updates, ...trackerUpdates]);

    if (affectedMints.size > 0) {
        for (const mint of affectedMints) await aggregateAndSaveToken(db, mint);
    }
}

async function runSnapshotCycle() {
    try {
        const db = getDB();
        const connection = getSolanaConnection(); 
        await updateSolPrice(db);
        
        // Fetch pools that need updating
        const res = await db.query(`
            SELECT tr.pool_address, tr.last_check, p.* FROM active_trackers tr 
            JOIN pools p ON tr.pool_address = p.address 
            ORDER BY tr.priority DESC, tr.last_check ASC 
            LIMIT 200
        `);
        const pools = res.rows;

        // Batch Process
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
    const res = await db.query(`SELECT * FROM pools WHERE address = ANY($1)`, [poolAddresses]); 
    await processPoolBatch(db, connection, res.rows, null);
}

module.exports = { startSnapshotter, snapshotPools };
