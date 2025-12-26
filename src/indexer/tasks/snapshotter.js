const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection, retryRPC } = require('../../services/solana'); 
const { getDB, aggregateAndSaveToken } = require('../../services/database');
const { getClient } = require('../../services/redis');
const logger = require('../../services/logger');

const stateCache = new Map();

const QUOTE_TOKENS = {
    'So11111111111111111111111111111111111111112': { decimals: 9, symbol: 'SOL' },
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': { decimals: 6, symbol: 'USDC' },
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': { decimals: 6, symbol: 'USDT' },
};

let solPriceCache = 0;
const decimalCache = new Map();

async function updateSolPrice(db) {
    try {
        const res = await fetch('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd');
        const data = await res.json();
        if (data?.solana?.usd) {
            solPriceCache = data.solana.usd;
            return;
        }
    } catch (e) {}
    
    // Fallback to DB
    try {
        const pool = await db.get(`SELECT price_usd FROM pools WHERE token_a = 'So11111111111111111111111111111111111111112' ORDER BY liquidity_usd DESC LIMIT 1`);
        if (pool && pool.price_usd > 0) solPriceCache = pool.price_usd;
    } catch(e) {}

    if (solPriceCache === 0) solPriceCache = 200; // Safe default
}

async function fetchMintDecimals(connection, mints) {
    const missing = mints.filter(m => !decimalCache.has(m) && !QUOTE_TOKENS[m]);
    if (missing.length === 0) return;

    missing.forEach(m => decimalCache.set(m, 9)); 

    try {
        const publicKeys = [];
        for(const m of missing) {
            try { publicKeys.push(new PublicKey(m)); } catch(e){}
        }

        for (let i = 0; i < publicKeys.length; i += 50) {
            const chunk = publicKeys.slice(i, i + 50);
            const infos = await retryRPC((conn) => conn.getMultipleAccountsInfo(chunk));
            
            infos.forEach((info, idx) => {
                const mint = missing[i + idx];
                if (info && info.data.length === 82) { // SPL Token Mint size
                    decimalCache.set(mint, info.data[44]); // Decimals at offset 44
                }
            });
        }
    } catch (e) { 
        logger.warn(`Decimal fetch warning: ${e.message}`);
    }
}

async function processPoolBatch(db, connection, pools, redis) {
    const keysToFetch = [];
    const poolMap = new Map();
    const uniqueMints = new Set();
    const affectedMints = new Set();
    const timestamp = Math.floor(Date.now() / 60000) * 60000;

    pools.forEach(p => {
        try {
            if (p.mint) uniqueMints.add(p.mint);
            
            if (p.dex === 'pumpfun' && p.reserve_a) {
                 keysToFetch.push(new PublicKey(p.reserve_a));
                 poolMap.set(p.address, { type: 'pumpfun', idx: keysToFetch.length - 1 });
            } else if (p.reserve_a && p.reserve_b) {
                 keysToFetch.push(new PublicKey(p.reserve_a));
                 keysToFetch.push(new PublicKey(p.reserve_b));
                 poolMap.set(p.address, { type: 'standard', idxA: keysToFetch.length - 2, idxB: keysToFetch.length - 1 });
            }
        } catch(e) {}
    });

    await fetchMintDecimals(connection, Array.from(uniqueMints));

    let accounts = [];
    if (keysToFetch.length > 0) {
        try { 
            accounts = await retryRPC((conn) => conn.getMultipleAccountsInfo(keysToFetch));
        } catch (e) { 
            logger.warn(`Snapshot RPC Batch Failed: ${e.message}`);
            return; 
        }
    }

    const updates = [];
    
    for (const p of pools) {
        const task = poolMap.get(p.address);
        if (!task) continue;

        let quoteAmount = 0;
        let priceUsd = 0;
        let liquidityUsd = 0;
        let success = false;
        let volumeUsd = 0;

        try {
            if (task.type === 'pumpfun') {
                const acc = accounts[task.idx];
                if (!acc) continue;
                const data = acc.data;
                if (data.length < 40) continue; 

                const virtualToken = Number(data.readBigUInt64LE(8));
                const virtualSol = Number(data.readBigUInt64LE(16));
                const realSol = Number(data.readBigUInt64LE(32));

                if (virtualToken > 0 && virtualSol > 0) {
                    const priceInSol = (virtualSol / 1e9) / (virtualToken / 1e6);
                    priceUsd = priceInSol * solPriceCache;
                }
                liquidityUsd = (realSol / 1e9) * solPriceCache * 2; 

                quoteAmount = virtualSol / 1e9;
                success = true;

            } else {
                const accA = accounts[task.idxA];
                const accB = accounts[task.idxB];
                if (!accA || !accB) continue;
                
                const rawA = Number(accA.data.readBigUInt64LE(64));
                const rawB = Number(accB.data.readBigUInt64LE(64));
                
                if (rawA === 0 || rawB === 0) continue;

                let quoteIsA = false;
                let quoteDecimals = 9;
                let quotePrice = 1;

                if (QUOTE_TOKENS[p.token_a]) {
                    quoteIsA = true;
                    quoteDecimals = QUOTE_TOKENS[p.token_a].decimals;
                    quotePrice = p.token_a.startsWith('So11') ? solPriceCache : 1.0;
                } else if (QUOTE_TOKENS[p.token_b]) {
                    quoteIsA = false;
                    quoteDecimals = QUOTE_TOKENS[p.token_b].decimals;
                    quotePrice = p.token_b.startsWith('So11') ? solPriceCache : 1.0;
                } else { continue; }

                const quoteRaw = quoteIsA ? rawA : rawB;
                const baseRaw = quoteIsA ? rawB : rawA;
                const baseDecimals = decimalCache.get(p.mint) || 9;
                
                quoteAmount = quoteRaw / Math.pow(10, quoteDecimals);
                const baseAmount = baseRaw / Math.pow(10, baseDecimals);

                if (baseAmount > 0) {
                    priceUsd = (quoteAmount / baseAmount) * quotePrice;
                    liquidityUsd = quoteAmount * quotePrice * 2;
                    success = true;
                }
            }
        } catch(e) { continue; }

        if (!success) continue;

        affectedMints.add(p.mint);

        // Volume Calc
        const cacheKey = `pool_state:${p.address}`;
        try {
            let lastState = stateCache.get(cacheKey);
            if (!lastState && redis) {
                const s = await redis.get(cacheKey);
                if (s) lastState = JSON.parse(s);
            }

            if (lastState) {
                const delta = Math.abs(quoteAmount - lastState.quoteAmount);
                if (delta > 0.000001) { 
                    const quotePrice = p.dex === 'pumpfun' ? solPriceCache : 1; 
                    volumeUsd = delta * quotePrice;
                }
            }
            const newState = { quoteAmount, timestamp: Date.now() };
            stateCache.set(cacheKey, newState);
            if(redis) redis.set(cacheKey, JSON.stringify(newState), 'EX', 3600).catch(()=>{});
        } catch(e){}

        if (priceUsd > 0) {
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
        }
    }
    
    await Promise.allSettled(updates);

    // CRITICAL: ROLL UP TOKENS
    if (affectedMints.size > 0) {
        for (const mint of affectedMints) {
            await aggregateAndSaveToken(db, mint);
        }
    }
}

async function runSnapshotCycle() {
    const redis = getClient();
    const db = getDB();
    const connection = getSolanaConnection(); 
    await updateSolPrice(db);

    try {
        const pools = await db.all(`SELECT * FROM active_trackers tr JOIN pools p ON tr.pool_address = p.address ORDER BY tr.priority DESC LIMIT 500`);
        if (pools.length === 0) return;

        for (let i = 0; i < pools.length; i += 50) {
            const batch = pools.slice(i, i + 50);
            await processPoolBatch(db, connection, batch, redis);
            await new Promise(r => setTimeout(r, 100));
        }
    } catch (e) {
        logger.error(`Snapshot Cycle Failed: ${e.message}`);
    }
}

function startSnapshotter() {
    setTimeout(() => {
        logger.info("🟢 Snapshotter Started");
        runSnapshotCycle();
        setInterval(runSnapshotCycle, 20000); 
    }, 5000);
}

async function snapshotPools(poolAddresses) {
    if (!poolAddresses.length) return;
    const db = getDB();
    const redis = getClient();
    const connection = getSolanaConnection();
    await updateSolPrice(db);
    
    // Postgres array placeholder syntax
    const placeholders = poolAddresses.map((_, i) => `$${i+1}`).join(',');
    const pools = await db.all(`SELECT * FROM pools WHERE address IN (${placeholders})`, poolAddresses);
    
    await processPoolBatch(db, connection, pools, redis);
}

module.exports = { startSnapshotter, snapshotPools };
