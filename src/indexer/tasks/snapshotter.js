const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection, retryRPC } = require('../../services/solana'); 
const { getDB, aggregateAndSaveToken } = require('../../services/database');
const { getClient } = require('../../services/redis');
const logger = require('../../services/logger');
const { getRealVolume } = require('../services/volume_tracker');
const { enrichPoolsWithReserves } = require('../../services/pool_finder');

const stateCache = new Map();
const QUOTE_TOKENS = {
    'So11111111111111111111111111111111111111112': { decimals: 9, symbol: 'SOL' },
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': { decimals: 6, symbol: 'USDC' },
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': { decimals: 6, symbol: 'USDT' },
};

let solPriceCache = 200; 

async function updateSolPrice(db) {
    try {
        const pool = await db.get(`SELECT price_usd FROM pools WHERE token_a = 'So11111111111111111111111111111111111111112' AND liquidity_usd > 10000 ORDER BY liquidity_usd DESC LIMIT 1`);
        if (pool && pool.price_usd > 0) solPriceCache = pool.price_usd;
    } catch(e) {}
}

async function processPoolBatch(db, connection, pools, redis) {
    const timestamp = Math.floor(Date.now() / 60000) * 60000;
    const now = Date.now();
    
    // --- SELF-HEAL: Check for missing reserves ---
    const poolsNeedingReserves = pools.filter(p => p.dex !== 'pumpfun' && (!p.reserve_a || !p.reserve_b));
    if (poolsNeedingReserves.length > 0) {
        // logger.info(`🩹 Self-Healing: Fetching reserves for ${poolsNeedingReserves.length} pools...`);
        await enrichPoolsWithReserves(poolsNeedingReserves);
        
        // Save enriched reserves to DB so next time is faster
        const fixPromises = poolsNeedingReserves.map(p => {
            if (p.reserve_a && p.reserve_b) {
                return db.query(`UPDATE pools SET reserve_a = $1, reserve_b = $2 WHERE address = $3`, [p.reserve_a, p.reserve_b, p.address]);
            }
        });
        await Promise.allSettled(fixPromises);
    }
    // ---------------------------------------------

    const keysToFetch = [];
    const poolMap = new Map();
    const affectedMints = new Set();

    pools.forEach(p => {
        try {
            if (p.dex === 'pumpfun' && p.reserve_a) {
                 keysToFetch.push(new PublicKey(p.reserve_a)); 
                 keysToFetch.push(new PublicKey(p.reserve_b)); 
                 poolMap.set(p.address, { type: 'pumpfun', idxA: keysToFetch.length - 2, idxB: keysToFetch.length - 1, pool: p });
            } else if (p.reserve_a && p.reserve_b) {
                 keysToFetch.push(new PublicKey(p.reserve_a));
                 keysToFetch.push(new PublicKey(p.reserve_b));
                 poolMap.set(p.address, { type: 'standard', idxA: keysToFetch.length - 2, idxB: keysToFetch.length - 1, pool: p });
            }
        } catch(e) {}
    });

    if (keysToFetch.length === 0) {
        // Mark checked to allow rotation even if broken
        for (const p of pools) {
            await db.query(`UPDATE active_trackers SET last_check = $1 WHERE pool_address = $2`, [now, p.address]);
        }
        return;
    }

    let accounts = [];
    try { 
        accounts = await retryRPC((conn) => conn.getMultipleAccountsInfo(keysToFetch));
    } catch (e) {
        // Failed RPC, skip but don't crash
        return;
    }

    const updates = [];
    const trackerUpdates = [];
    
    for (const p of pools) {
        // Always rotate
        trackerUpdates.push(db.query(`UPDATE active_trackers SET last_check = $1 WHERE pool_address = $2`, [now, p.address]));

        const task = poolMap.get(p.address);
        if (!task || !accounts) continue;

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
                let decA = 6; 
                let decB = 9; 

                const readAmount = (buffer) => {
                    if (buffer.length < 72) return 0;
                    return Number(buffer.readBigUInt64LE(64));
                };

                if (task.type === 'pumpfun') {
                    reserveA = Number(accB.data.readBigUInt64LE(8)); 
                    reserveB = Number(accB.data.readBigUInt64LE(16)); 
                    decA = 6;
                    decB = 9;
                } else {
                    reserveA = readAmount(accA.data);
                    reserveB = readAmount(accB.data);
                    
                    const isBQuote = QUOTE_TOKENS[p.token_b];
                    const isAQuote = QUOTE_TOKENS[p.token_a];
                    
                    if (isBQuote) {
                        decB = isBQuote.decimals;
                        // Use default 6 for unknown token base
                        decA = 6; 
                    } else if (isAQuote) {
                        decA = isAQuote.decimals;
                        decB = 6;
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
                    }

                    if (priceUsd > 0) success = true;
                }
            } catch (err) {}
        }

        // Volume Check (Async)
        const volKey = `vol_last_check:${p.address}`;
        const lastCheck = stateCache.get(volKey) || 0;
        if (now - lastCheck > 120000) { 
            const sigKey = `vol_sig:${p.address}`;
            const lastSig = stateCache.get(sigKey);
            getRealVolume(p.address, lastSig, solPriceCache).then(volData => {
                if (volData.txCount > 0) stateCache.set(sigKey, volData.latestSignature);
            }).catch(() => {});
            stateCache.set(volKey, now);
        }

        if (success) {
            affectedMints.add(p.mint);
            updates.push(db.query(`UPDATE pools SET price_usd = $1, liquidity_usd = $2 WHERE address = $3`, [priceUsd, liquidityUsd, p.address]));
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
        
        // Fetch pools, prioritizing those not recently checked
        const res = await db.query(`
            SELECT tr.pool_address, tr.last_check, p.* FROM active_trackers tr 
            JOIN pools p ON tr.pool_address = p.address 
            ORDER BY tr.priority DESC, tr.last_check ASC 
            LIMIT 200
        `);
        const pools = res.rows;

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
