const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection } = require('../../services/solana'); // Use Singleton
const { getDB } = require('../../services/database');
const { getClient } = require('../../services/redis');
const { dequeueBatch } = require('../../services/queue'); // Use Queue
const logger = require('../../services/logger');

// Distributed Lock Configuration
const LOCK_KEY = 'lock:snapshotter_cycle';
const LOCK_TTL = 30; // Seconds

// Centralized Constants
const POOL_OFFSETS = {
    RAY_COIN: 320,
    RAY_PC: 352,
    ORCA_A: 101,
    ORCA_B: 133
};

const QUOTE_MINTS = new Set([
    'So11111111111111111111111111111111111111112',
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
]);

let solPriceCache = 200;

async function updateSolPrice(db) {
    try {
        const pool = await db.get(`SELECT price_usd FROM pools WHERE token_a = 'So11111111111111111111111111111111111111112' AND liquidity_usd > 10000 ORDER BY liquidity_usd DESC LIMIT 1`);
        if (pool && pool.price_usd > 0) solPriceCache = pool.price_usd;
    } catch (e) {}
}

async function syncTokenPrice(db, mint, priceUsd, liquidityUsd, marketCap) {
    if (!mint || priceUsd <= 0) return;
    try {
        await db.run(`UPDATE tokens SET priceUsd = $1, liquidity = $2, marketCap = $3 WHERE mint = $4`, [priceUsd, liquidityUsd, marketCap, mint]);
    } catch (e) {}
}

/**
 * Process a specific list of pools (Batch Logic)
 */
async function processPoolBatch(db, connection, pools, redis) {
    const keysToFetch = [];
    const poolMap = new Map();
    const timestamp = Math.floor(Date.now() / 60000) * 60000;

    pools.forEach((p) => {
        try {
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
        } catch(e) { /* Invalid PK, skip */ }
    });

    if (keysToFetch.length === 0) return;

    try {
        const accounts = await connection.getMultipleAccountsInfo(keysToFetch);
        const updates = []; // Store promises for parallel execution

        for (const p of pools) {
            const task = poolMap.get(p.address);
            if (!task) continue;

            let nativePrice = 0, liquidityUsd = 0, quoteIsSol = false;
            let currentQuoteReserve = 0;
            const tokenSupply = 1000000000;

            try {
                if (task.type === 'pump') {
                    const data = accounts[task.index]?.data;
                    if (data && data.length >= 40) {
                        const vToken = data.readBigUInt64LE(8);
                        const vSol = data.readBigUInt64LE(16);
                        const realSol = data.readBigUInt64LE(32);
                        if (Number(vToken) > 0) {
                            nativePrice = Number(vSol) / Number(vToken);
                            liquidityUsd = (Number(realSol) / 1e9) * solPriceCache * 2;
                            quoteIsSol = true;
                            currentQuoteReserve = Number(realSol) / 1e9;
                        }
                    }
                } else if (task.type === 'discovery') {
                    const data = accounts[task.index]?.data;
                    if (data) {
                        let vaultA, vaultB;
                        if (task.dex === 'raydium' && data.length >= 500) {
                            vaultA = new PublicKey(data.subarray(POOL_OFFSETS.RAY_COIN, POOL_OFFSETS.RAY_COIN + 32));
                            vaultB = new PublicKey(data.subarray(POOL_OFFSETS.RAY_PC, POOL_OFFSETS.RAY_PC + 32));
                        }
                        if (vaultA && vaultB) {
                            updates.push(db.run(`UPDATE pools SET reserve_a = $1, reserve_b = $2 WHERE address = $3`, [vaultA.toString(), vaultB.toString(), p.address]));
                        }
                    }
                } else if (task.type === 'direct') {
                    const accA = accounts[task.idxA];
                    const accB = accounts[task.idxB];
                    if (accA && accB && accA.data.length >= 8) {
                        const balA = Number(accA.data.readBigUInt64LE(64));
                        const balB = Number(accB.data.readBigUInt64LE(64));
                        const isAQuote = QUOTE_MINTS.has(p.token_a);
                        
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

                if (nativePrice > 0) {
                    const finalPriceUsd = quoteIsSol ? (nativePrice * solPriceCache) : nativePrice;
                    const marketCap = finalPriceUsd * tokenSupply;

                    let approxVolume = 0;
                    if (redis) {
                        const volKey = `vol:${p.address}`;
                        const prev = await redis.get(volKey);
                        if (prev) {
                            const delta = Math.abs(currentQuoteReserve - parseFloat(prev));
                            if (delta > 0.000001 && delta < (currentQuoteReserve * 0.5)) approxVolume = delta * solPriceCache;
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

    } catch (e) { logger.error(`Batch Error: ${e.message}`); }
}

async function runSnapshotCycle() {
    const redis = getClient();
    // 1. Distributed Lock Check
    if (redis) {
        const acquired = await redis.set(LOCK_KEY, '1', 'NX', 'EX', LOCK_TTL);
        if (!acquired) return; // Lock exists, another worker is running
    } else {
        // Fallback for no-redis local dev
        if (global.isSnapshotRunning) return;
        global.isSnapshotRunning = true;
    }

    const db = getDB();
    const connection = getSolanaConnection();
    await updateSolPrice(db);

    try {
        // 2. High Priority: Process New Tokens from Queue
        // This ensures new adds get data FAST before we scan the generic list
        const queuedMints = await dequeueBatch(20);
        if (queuedMints.length > 0) {
            logger.info(`⚡ Processing ${queuedMints.length} priority tokens from queue`);
            const poolRes = await db.all(`SELECT p.address, p.mint, p.dex, p.reserve_a, p.reserve_b, p.token_a, p.token_b FROM pools p WHERE p.mint IN (${queuedMints.map(m => `'${m}'`).join(',')})`);
            if (poolRes.length > 0) await processPoolBatch(db, connection, poolRes, redis);
        }

        // 3. Standard Cycle
        const BATCH_SIZE = 100;
        let offset = 0;
        let keepFetching = true;

        while (keepFetching) {
            // Only active trackers
            const pools = await db.all(`SELECT p.address, p.mint, p.dex, p.reserve_a, p.reserve_b, p.token_a, p.token_b FROM active_trackers t JOIN pools p ON t.pool_address = p.address ORDER BY t.priority DESC, t.pool_address ASC LIMIT ${BATCH_SIZE} OFFSET ${offset}`);
            
            if (pools.length === 0) break;
            
            await processPoolBatch(db, connection, pools, redis);
            
            if (pools.length < BATCH_SIZE) keepFetching = false;
            offset += BATCH_SIZE;
        }
    } catch (err) { 
        logger.error(`Snapshot Cycle Error: ${err.message}`); 
    } finally {
        if (!redis) global.isSnapshotRunning = false;
        // Lock expires automatically via TTL, but we can delete to be nice
        if (redis) await redis.del(LOCK_KEY);
    }
}

function startSnapshotter() {
    setTimeout(() => {
        runSnapshotCycle();
        setInterval(runSnapshotCycle, 30000);
    }, 5000);
}

// Fallback for direct immediate calls (if needed by other services)
async function snapshotPools(poolAddresses) {
    if (!poolAddresses.length) return;
    const db = getDB();
    const redis = getClient();
    const connection = getSolanaConnection();
    const pools = await db.all(`SELECT p.address, p.mint, p.dex, p.reserve_a, p.reserve_b, p.token_a, p.token_b FROM pools p WHERE p.address IN (${poolAddresses.map(p => `'${p}'`).join(',')})`);
    await processPoolBatch(db, connection, pools, redis);
}

module.exports = { startSnapshotter, snapshotPools };
