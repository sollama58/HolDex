const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection } = require('../../services/solana'); 
const { getDB } = require('../../services/database');
const { getClient } = require('../../services/redis');
const { dequeueBatch } = require('../../services/queue');
const logger = require('../../services/logger');
const axios = require('axios');

const LOCK_KEY = 'lock:snapshotter_cycle';
const LOCK_TTL = 30;

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
        uniqueMints.add(p.mint);
        
        if (p.dexId === 'pumpfun') {
            // For PumpFun, reserve_a is the BondingCurve Account.
            keysToFetch.push(new PublicKey(p.reserve_a));
            poolMap.set(p.address, { 
                type: 'pumpfun',
                idx: keysToFetch.length - 1 
            });
        } else {
            // Standard AMM (Raydium, Meteora)
            if (p.reserve_a && p.reserve_b) {
                keysToFetch.push(new PublicKey(p.reserve_a));
                keysToFetch.push(new PublicKey(p.reserve_b));
                poolMap.set(p.address, { 
                    type: 'standard',
                    idxA: keysToFetch.length - 2, 
                    idxB: keysToFetch.length - 1 
                });
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

        if (task.type === 'pumpfun') {
            const acc = accounts[task.idx];
            if (!acc) continue;
            
            // PUMPFUN BONDING CURVE LAYOUT
            // 0-7: Discriminator
            // 8-15: Virtual Token Reserves (u64)
            // 16-23: Virtual Sol Reserves (u64)
            // 24-31: Real Token Reserves (u64)
            // 32-39: Real Sol Reserves (u64)
            // 40-47: Token Total Supply (u64)
            // 48: Complete (bool)
            
            try {
                const data = acc.data;
                const virtualToken = Number(data.readBigUInt64LE(8));
                const virtualSol = Number(data.readBigUInt64LE(16));
                const realToken = Number(data.readBigUInt64LE(24));
                const realSol = Number(data.readBigUInt64LE(32));
                const complete = data[48] === 1;

                // 1. PRICE: Always based on Virtual Reserves (Bonding Curve Math)
                if (virtualToken > 0 && virtualSol > 0) {
                    const vSolNorm = virtualSol / 1e9;
                    const vTokenNorm = virtualToken / 1e6; // PumpFun tokens are 6 decimals
                    priceUsd = (vSolNorm / vTokenNorm) * solPriceCache;
                }

                // 2. LIQUIDITY: Based on REAL Reserves
                // CRITICAL FIX: If migrated, Real SOL is 0 (or close to it). 
                // Using Real SOL ensures we report 0 liquidity for migrated curves,
                // forcing the system to prefer the Raydium pool.
                const realSolNorm = realSol / 1e9;
                liquidityUsd = realSolNorm * solPriceCache * 2; 

                // Raw values for Volume K-check
                rawA = virtualToken;
                rawB = virtualSol;
                quoteAmount = virtualSol / 1e9;
                quotePrice = solPriceCache;
                
            } catch(e) { continue; }

        } else {
            // RAYDIUM / METEORA
            const accA = accounts[task.idxA];
            const accB = accounts[task.idxB];

            if (!accA || !accB) continue;

            if (accA.data.length < 72 || accB.data.length < 72) continue;

            rawA = Number(accA.data.readBigUInt64LE(64));
            rawB = Number(accB.data.readBigUInt64LE(64));

            if (rawA === 0 || rawB === 0) continue;

            let quoteIsA = false;
            let quoteDecimals = 9;
            let baseDecimals = decimalCache.get(p.mint) || 9;
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
            
            quoteAmount = quoteRaw / Math.pow(10, quoteDecimals);
            const baseAmount = baseRaw / Math.pow(10, baseDecimals);

            priceUsd = (quoteAmount / baseAmount) * quotePrice;
            liquidityUsd = quoteAmount * quotePrice * 2;
        }

        // --- VOLUME & UPDATE LOGIC (Common) ---
        let volumeUsd = 0;
        if (redis) {
            const cacheKey = `pool_state:${p.address}`;
            const lastStateStr = await redis.get(cacheKey);
            
            if (lastStateStr) {
                const lastState = JSON.parse(lastStateStr);
                const prevK = lastState.k;
                const currentK = rawA * rawB; 
                
                const kRatio = prevK > 0 ? currentK / prevK : 1;
                const tolerance = task.type === 'pumpfun' ? 0.05 : 0.005; 
                const isLiquidityEvent = (kRatio > (1 + tolerance) || kRatio < (1 - tolerance));

                if (!isLiquidityEvent) {
                    const reserveDelta = Math.abs(quoteAmount - lastState.quoteAmount);
                    if (reserveDelta > 0.000001) {
                        volumeUsd = reserveDelta * quotePrice;
                    }
                }
            }

            await redis.set(cacheKey, JSON.stringify({
                k: rawA * rawB,
                quoteAmount: quoteAmount,
                timestamp: Date.now()
            }), 'EX', 600);
        }

        if (isFinite(priceUsd)) {
            updates.push(db.run(`UPDATE pools SET price_usd = $1, liquidity_usd = $2 WHERE address = $3`, [priceUsd, liquidityUsd, p.address]));
            
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

            // Simplified token update (Metadata Updater does the heavy lifting, but this keeps live price fresh)
            // For PumpFun, we assume 1B supply if not set
            const supply = p.supply ? parseFloat(p.supply) / Math.pow(10, task.type === 'pumpfun' ? 6 : 9) : 1000000000;
            const mcap = priceUsd * supply;
            
            // Only update token stats if this pool has liquidity OR it's the only pool we have
            if (liquidityUsd > 100 || task.type === 'pumpfun') {
                 updates.push(db.run(`UPDATE tokens SET priceUsd=$1, marketCap=$2, liquidity=$3 WHERE mint=$4`, [priceUsd, mcap, liquidityUsd, p.mint]));
            }
        }
    }

    await Promise.all(updates);
}

async function runSnapshotCycle() {
    const redis = getClient();
    if (redis) {
        const acquired = await redis.set(LOCK_KEY, '1', 'NX', 'EX', LOCK_TTL);
        if (!acquired) return;
    }

    const db = getDB();
    const connection = getSolanaConnection();
    await updateSolPrice(db);

    try {
        const queuedMints = await dequeueBatch(10);
        if (queuedMints.length > 0) {
             const pools = await db.all(`SELECT * FROM pools WHERE mint IN (${queuedMints.map(m => `'${m}'`).join(',')})`);
             if (pools.length) await processPoolBatch(db, connection, pools, redis);
        }

        const BATCH_SIZE = 50;
        let offset = 0;
        while (true) {
            const pools = await db.all(`SELECT * FROM active_trackers tr JOIN pools p ON tr.pool_address = p.address ORDER BY tr.priority DESC LIMIT ${BATCH_SIZE} OFFSET ${offset}`);
            if (pools.length === 0) break;
            
            await processPoolBatch(db, connection, pools, redis);
            offset += BATCH_SIZE;
            await new Promise(r => setTimeout(r, 200)); 
        }

    } catch (e) {
        logger.error(`Snapshot Error: ${e.message}`);
    } finally {
        if (redis) await redis.del(LOCK_KEY);
    }
}

function startSnapshotter() {
    setTimeout(() => {
        logger.info("🟢 Snapshotter Started (Raydium + Meteora + PumpFun)");
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
