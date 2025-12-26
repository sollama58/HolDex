const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection } = require('../../services/solana'); 
const { getDB } = require('../../services/database');
const { getClient } = require('../../services/redis');
const { dequeueBatch } = require('../../services/queue');
const logger = require('../../services/logger');
const axios = require('axios');

const LOCK_KEY = 'lock:snapshotter_cycle';
const LOCK_TTL = 30;

// In-Memory Fallback for State (If Redis is down/slow)
// Map<poolAddress, { k: number, quoteAmount: number, timestamp: number, complete?: boolean }>
const stateCache = new Map();

// Known Quote Tokens
const QUOTE_TOKENS = {
    'So11111111111111111111111111111111111111112': { decimals: 9, symbol: 'SOL' },
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': { decimals: 6, symbol: 'USDC' },
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': { decimals: 6, symbol: 'USDT' },
};

let solPriceCache = 0;
const decimalCache = new Map();

async function updateSolPrice(db) {
    try {
        const response = await axios.get('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd', { timeout: 2000 });
        if (response.data?.solana?.usd) {
            solPriceCache = response.data.solana.usd;
            return;
        }
        // DB Fallback
        const pool = await db.get(`SELECT price_usd FROM pools WHERE token_a = 'So11111111111111111111111111111111111111112' ORDER BY liquidity_usd DESC LIMIT 1`);
        if (pool && pool.price_usd > 0) solPriceCache = pool.price_usd;
    } catch (e) {
        if (solPriceCache === 0) solPriceCache = 150; 
    }
}

async function fetchMintDecimals(connection, mints) {
    const missing = mints.filter(m => !decimalCache.has(m) && !QUOTE_TOKENS[m]);
    if (missing.length === 0) return;
    try {
        const publicKeys = missing.map(m => new PublicKey(m));
        for (let i = 0; i < publicKeys.length; i += 50) {
            const chunk = publicKeys.slice(i, i + 50);
            const infos = await connection.getMultipleAccountsInfo(chunk);
            infos.forEach((info, idx) => {
                const mint = missing[i + idx];
                if (info && info.data.length === 82) {
                    decimalCache.set(mint, info.data[44]);
                } else {
                    decimalCache.set(mint, 9);
                }
            });
        }
    } catch (e) { logger.warn(`Decimal fetch failed: ${e.message}`); }
}

async function processPoolBatch(db, connection, pools, redis) {
    const keysToFetch = [];
    const poolMap = new Map();
    const uniqueMints = new Set();
    const timestamp = Math.floor(Date.now() / 60000) * 60000;

    pools.forEach(p => {
        if (p.mint) uniqueMints.add(p.mint);
        
        if (p.dexId === 'pumpfun') {
            if (p.reserve_a) {
                keysToFetch.push(new PublicKey(p.reserve_a));
                poolMap.set(p.address, { type: 'pumpfun', idx: keysToFetch.length - 1 });
            }
        } else {
            if (p.reserve_a && p.reserve_b) {
                keysToFetch.push(new PublicKey(p.reserve_a));
                keysToFetch.push(new PublicKey(p.reserve_b));
                poolMap.set(p.address, { type: 'standard', idxA: keysToFetch.length - 2, idxB: keysToFetch.length - 1 });
            }
        }
    });

    await fetchMintDecimals(connection, Array.from(uniqueMints));

    let accounts = [];
    if (keysToFetch.length > 0) {
        try { accounts = await connection.getMultipleAccountsInfo(keysToFetch); } 
        catch (e) { return; }
    }

    const updates = [];
    
    for (const p of pools) {
        const task = poolMap.get(p.address);
        if (!task) continue;

        let quoteAmount = 0;
        let priceUsd = 0;
        let liquidityUsd = 0;
        let rawA = 0;
        let rawB = 0;
        let quotePrice = 0;
        let isPumpFunComplete = false;

        // --- 1. DATA EXTRACTION ---
        if (task.type === 'pumpfun') {
            const acc = accounts[task.idx];
            if (!acc) continue;
            
            try {
                const data = acc.data;
                const virtualToken = Number(data.readBigUInt64LE(8));
                const virtualSol = Number(data.readBigUInt64LE(16));
                const realSol = Number(data.readBigUInt64LE(32));
                isPumpFunComplete = data[48] === 1;

                if (virtualToken > 0 && virtualSol > 0) {
                    const vSolNorm = virtualSol / 1e9;
                    const vTokenNorm = virtualToken / 1e6; 
                    priceUsd = (vSolNorm / vTokenNorm) * solPriceCache;
                }

                const realSolNorm = realSol / 1e9;
                liquidityUsd = realSolNorm * solPriceCache * 2; 

                // For volume calc, we use Virtual Reserves as they track the bonding curve movement
                rawA = virtualToken;
                rawB = virtualSol;
                quoteAmount = virtualSol / 1e9; // Change in SOL is Volume
                quotePrice = solPriceCache;
                
            } catch(e) { continue; }

        } else {
            // STANDARD AMM
            const accA = accounts[task.idxA];
            const accB = accounts[task.idxB];

            if (!accA || !accB) continue;
            if (accA.data.length < 72 || accB.data.length < 72) continue;

            rawA = Number(accA.data.readBigUInt64LE(64));
            rawB = Number(accB.data.readBigUInt64LE(64));

            if (rawA === 0 || rawB === 0) continue;

            let quoteIsA = false;
            let quoteDecimals = 9;
            quotePrice = 0;

            if (QUOTE_TOKENS[p.token_a]) {
                quoteIsA = true;
                quoteDecimals = QUOTE_TOKENS[p.token_a].decimals;
                quotePrice = p.token_a.startsWith('So11') ? solPriceCache : 1.0;
            } else if (QUOTE_TOKENS[p.token_b]) {
                quoteIsA = false;
                quoteDecimals = QUOTE_TOKENS[p.token_b].decimals;
                quotePrice = p.token_b.startsWith('So11') ? solPriceCache : 1.0;
            } else {
                continue; 
            }

            const quoteRaw = quoteIsA ? rawA : rawB;
            const baseRaw = quoteIsA ? rawB : rawA;
            const baseDecimals = decimalCache.get(p.mint) || 9;
            
            quoteAmount = quoteRaw / Math.pow(10, quoteDecimals);
            const baseAmount = baseRaw / Math.pow(10, baseDecimals);

            priceUsd = (quoteAmount / baseAmount) * quotePrice;
            liquidityUsd = quoteAmount * quotePrice * 2;
        }

        // --- 2. VOLUME LOGIC (Robust) ---
        let volumeUsd = 0;
        const cacheKey = `pool_state:${p.address}`;
        let lastState = null;

        // Try Redis, Fallback to Memory
        try {
            if (redis) {
                const s = await redis.get(cacheKey);
                if (s) lastState = JSON.parse(s);
            }
        } catch (e) { /* Redis Fail silent */ }
        
        if (!lastState) {
            lastState = stateCache.get(cacheKey);
        }

        if (lastState) {
            const currentK = rawA * rawB;
            
            if (task.type === 'pumpfun') {
                // PUMPFUN LOGIC:
                // If Migration just happened (false -> true), assume reserves moved out (NOT Buy/Sell Volume)
                // If NOT migrating, any change in reserve is a Trade.
                const migrationEvent = (!lastState.complete && isPumpFunComplete);
                
                if (!migrationEvent) {
                    const delta = Math.abs(quoteAmount - lastState.quoteAmount);
                    // Filter tiny dust (precision errors)
                    if (delta > 0.0000001) {
                        volumeUsd = delta * quotePrice;
                    }
                }
            } else {
                // STANDARD AMM LOGIC (K-Constant):
                const prevK = lastState.k;
                const kRatio = prevK > 0 ? currentK / prevK : 1;
                
                // If K changes by > 0.5%, it's likely a liquidity Add/Remove, not a Swap
                const isLiquidityEvent = (kRatio > 1.005 || kRatio < 0.995);

                if (!isLiquidityEvent) {
                    const delta = Math.abs(quoteAmount - lastState.quoteAmount);
                    if (delta > 0.000001) {
                        volumeUsd = delta * quotePrice;
                    }
                }
            }
        }

        // Update Cache (Both Redis and Memory)
        const newState = {
            k: rawA * rawB,
            quoteAmount: quoteAmount,
            timestamp: Date.now(),
            complete: isPumpFunComplete
        };
        
        stateCache.set(cacheKey, newState);
        if (redis) {
            // Fire and forget
            redis.set(cacheKey, JSON.stringify(newState), 'EX', 600).catch(() => {});
        }

        // --- 3. DB UPDATES ---
        if (isFinite(priceUsd)) {
            // Update Pool
            updates.push(db.run(`UPDATE pools SET price_usd = $1, liquidity_usd = $2 WHERE address = $3`, [priceUsd, liquidityUsd, p.address]));
            
            // Upsert Candle
            // CRITICAL: We sum volume into the existing candle for this minute
            updates.push(db.run(`
                INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close, volume) 
                VALUES ($1, $2, $3, $3, $3, $3, $4) 
                ON CONFLICT(pool_address, timestamp) 
                DO UPDATE SET 
                    close = $3, 
                    high = GREATEST(candles_1m.high, $3), 
                    low = LEAST(candles_1m.low, $3),
                    volume = candles_1m.volume + $4
            `, [p.address, timestamp, priceUsd, volumeUsd]));

            // Simplified Token Update
            // We only update if we have a valid mint
            if (p.mint) {
                const supply = p.supply ? parseFloat(p.supply) / Math.pow(10, task.type === 'pumpfun' ? 6 : 9) : 1000000000;
                const mcap = priceUsd * supply;
                
                // Allow update if liquidity is present OR it's pumpfun (always relevant)
                if (liquidityUsd > 10 || task.type === 'pumpfun') {
                     updates.push(db.run(`UPDATE tokens SET priceUsd=$1, marketCap=$2, liquidity=$3 WHERE mint=$4`, [priceUsd, mcap, liquidityUsd, p.mint]));
                }
            }
        }
    }

    await Promise.all(updates);
}

async function runSnapshotCycle() {
    const redis = getClient();
    
    // Simple Distributed Lock using Redis (if avail)
    // If not, we just run. (Assuming single worker in dev)
    if (redis) {
        try {
            const acquired = await redis.set(LOCK_KEY, '1', 'NX', 'EX', LOCK_TTL);
            if (!acquired) return;
        } catch (e) {
            // If redis fails, proceed anyway but log warn
            // logger.warn("Redis lock failed, running unsafe cycle");
        }
    }

    const db = getDB();
    const connection = getSolanaConnection();
    await updateSolPrice(db);

    try {
        // 1. High Priority Queue
        const queuedMints = await dequeueBatch(10);
        if (queuedMints.length > 0) {
             const pools = await db.all(`SELECT * FROM pools WHERE mint IN (${queuedMints.map(m => `'${m}'`).join(',')})`);
             if (pools.length) await processPoolBatch(db, connection, pools, redis);
        }

        // 2. Continuous Rotation
        const BATCH_SIZE = 50;
        let offset = 0;
        // Limit total execution time to ~12s to allow next cycle
        const startTime = Date.now();
        
        while ((Date.now() - startTime) < 12000) {
            const pools = await db.all(`SELECT * FROM active_trackers tr JOIN pools p ON tr.pool_address = p.address ORDER BY tr.priority DESC LIMIT ${BATCH_SIZE} OFFSET ${offset}`);
            if (pools.length === 0) break;
            
            await processPoolBatch(db, connection, pools, redis);
            offset += BATCH_SIZE;
            await new Promise(r => setTimeout(r, 200)); 
        }

    } catch (e) {
        logger.error(`Snapshot Error: ${e.message}`);
    } finally {
        if (redis) {
            try { await redis.del(LOCK_KEY); } catch(e) {}
        }
    }
}

function startSnapshotter() {
    setTimeout(() => {
        logger.info("🟢 Snapshotter Started (Memory + Redis Hybrid)");
        runSnapshotCycle();
        setInterval(runSnapshotCycle, 15000); 
    }, 5000);
}

async function snapshotPools(poolAddresses) {
    if (!poolAddresses.length) return;
    const db = getDB();
    const redis = getClient();
    const connection = getSolanaConnection();
    await updateSolPrice(db);
    
    const pools = await db.all(`SELECT * FROM pools WHERE address IN (${poolAddresses.map(p => `'${p}'`).join(',')})`);
    await processPoolBatch(db, connection, pools, redis);
}

module.exports = { startSnapshotter, snapshotPools };
