const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection } = require('../../services/solana'); 
const { getDB } = require('../../services/database');
const { getClient } = require('../../services/redis');
const logger = require('../../services/logger');

// Memory Fallback
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
        // 1. Try External (Native Fetch)
        const res = await fetch('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd');
        const data = await res.json();
        if (data?.solana?.usd) {
            solPriceCache = data.solana.usd;
            return;
        }

        // 2. Fallback to DB if external fails
        const pool = await db.get(`SELECT price_usd FROM pools WHERE token_a = 'So11111111111111111111111111111111111111112' ORDER BY liquidity_usd DESC LIMIT 1`);
        if (pool && pool.price_usd > 0) {
            solPriceCache = pool.price_usd;
            return;
        }
    } catch (e) {
        // logger.warn(`SOL Price Fetch Warning: ${e.message}`);
    }
    
    if (solPriceCache === 0) solPriceCache = 150; // Hard fallback to keep math working
}

async function fetchMintDecimals(connection, mints) {
    // Filter out known quotes and cached mints
    const missing = mints.filter(m => !decimalCache.has(m) && !QUOTE_TOKENS[m]);
    if (missing.length === 0) return;

    try {
        const publicKeys = [];
        for(const m of missing) {
            try { publicKeys.push(new PublicKey(m)); } catch(e){}
        }

        // Batch fetch in chunks of 50
        for (let i = 0; i < publicKeys.length; i += 50) {
            const chunk = publicKeys.slice(i, i + 50);
            const infos = await connection.getMultipleAccountsInfo(chunk);
            
            infos.forEach((info, idx) => {
                const mintStr = missing[i + idx];
                if (info && info.data.length === 82) {
                    // Offset 44 is standard for SPL Token Decimals
                    decimalCache.set(mintStr, info.data[44]);
                } else {
                    // Default to 9 if we can't find it
                    decimalCache.set(mintStr, 9);
                }
            });
        }
    } catch (e) { 
        logger.warn(`Decimal fetch partial failure: ${e.message}`);
    }
}

async function processPoolBatch(db, connection, pools, redis) {
    if (pools.length === 0) return;

    const keysToFetch = [];
    const poolMap = new Map();
    const uniqueMints = new Set();
    const timestamp = Math.floor(Date.now() / 60000) * 60000;

    // 1. PRE-PROCESS & IDENTIFY KEYS NEEDED
    pools.forEach(p => {
        try {
            if (p.mint) uniqueMints.add(p.mint);
            
            if (p.dexId === 'pumpfun') {
                if (p.reserve_a) { // Bonding Curve Address
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
        } catch(e) {
            // logger.warn(`Invalid pool keys for ${p.address}: ${e.message}`);
        }
    });

    // 2. FETCH DEPENDENCIES (Decimals + Account Data)
    await fetchMintDecimals(connection, Array.from(uniqueMints));

    let accounts = [];
    if (keysToFetch.length > 0) {
        try { 
            accounts = await connection.getMultipleAccountsInfo(keysToFetch); 
        } catch (e) { 
            logger.warn(`RPC Account Fetch Failed: ${e.message}`); 
            return; 
        }
    }

    const updates = [];
    let processedCount = 0;

    // 3. CALCULATE & QUEUE UPDATES
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
        let calculationSuccess = false;

        try {
            if (task.type === 'pumpfun') {
                const acc = accounts[task.idx];
                if (!acc) continue;
                const data = acc.data;
                if (data.length < 40) continue; 

                const virtualToken = Number(data.readBigUInt64LE(8));
                const virtualSol = Number(data.readBigUInt64LE(16));
                const realSol = Number(data.readBigUInt64LE(32));
                isPumpFunComplete = data[48] === 1;

                if (virtualToken > 0 && virtualSol > 0) {
                    const vSolNorm = virtualSol / 1e9;
                    const vTokenNorm = virtualToken / 1e6; // PumpFun uses 6 decimals internally usually
                    priceUsd = (vSolNorm / vTokenNorm) * solPriceCache;
                }
                const realSolNorm = realSol / 1e9;
                liquidityUsd = realSolNorm * solPriceCache * 2; 

                rawA = virtualToken;
                rawB = virtualSol;
                quoteAmount = virtualSol / 1e9; 
                quotePrice = solPriceCache;
                calculationSuccess = true;

            } else {
                // STANDARD POOL (Raydium, Orca, Meteora)
                const accA = accounts[task.idxA];
                const accB = accounts[task.idxB];
                
                if (!accA || !accB) continue;
                if (accA.data.length < 64 || accB.data.length < 64) continue; // SPL Token Account layout is 165 bytes usually

                // Read Amount (Offset 64 for standard SPL token layout)
                rawA = Number(accA.data.readBigUInt64LE(64));
                rawB = Number(accB.data.readBigUInt64LE(64));
                
                if (rawA <= 0 && rawB <= 0) continue;

                let quoteIsA = false;
                let quoteDecimals = 9;
                
                // Identify Quote Token
                if (QUOTE_TOKENS[p.token_a]) {
                    quoteIsA = true;
                    quoteDecimals = QUOTE_TOKENS[p.token_a].decimals;
                    quotePrice = p.token_a.startsWith('So11') ? solPriceCache : 1.0;
                } else if (QUOTE_TOKENS[p.token_b]) {
                    quoteIsA = false;
                    quoteDecimals = QUOTE_TOKENS[p.token_b].decimals;
                    quotePrice = p.token_b.startsWith('So11') ? solPriceCache : 1.0;
                } else {
                    // Fallback: Assume one side is SOL if wrapped SOL is involved but not in our list
                    // Or skip. Skipping is safer to avoid bad data.
                    continue; 
                }

                const quoteRaw = quoteIsA ? rawA : rawB;
                const baseRaw = quoteIsA ? rawB : rawA;
                
                const baseDecimals = decimalCache.get(p.mint) || 9;
                
                quoteAmount = quoteRaw / Math.pow(10, quoteDecimals);
                const baseAmount = baseRaw / Math.pow(10, baseDecimals);

                if (baseAmount > 0) {
                    priceUsd = (quoteAmount / baseAmount) * quotePrice;
                    liquidityUsd = quoteAmount * quotePrice * 2;
                    calculationSuccess = true;
                }
            }
        } catch(e) {
            // logger.error(`Parse error ${p.address}: ${e.message}`);
            continue;
        }

        if (!calculationSuccess) continue;

        // 4. VOLUME & STATE TRACKING
        let volumeUsd = 0;
        const cacheKey = `pool_state:${p.address}`;
        let lastState = null;

        try {
            if (redis) {
                const s = await redis.get(cacheKey);
                if (s) lastState = JSON.parse(s);
            }
            if (!lastState) lastState = stateCache.get(cacheKey);

            if (lastState) {
                const delta = Math.abs(quoteAmount - lastState.quoteAmount);
                const currentK = rawA * rawB;
                const prevK = lastState.k;
                
                // Basic check for valid swaps vs liquidity adds/removes
                // For PumpFun, volume is straightforward (reserves change)
                // For AMMs, we check K constant stability roughly
                if (delta > 0 && delta * quotePrice > 0.1) { 
                    volumeUsd = delta * quotePrice;
                }
            }
        } catch(e) {}

        const newState = { k: rawA * rawB, quoteAmount, timestamp: Date.now(), complete: isPumpFunComplete };
        stateCache.set(cacheKey, newState);
        if (redis) redis.set(cacheKey, JSON.stringify(newState), 'EX', 3600).catch(() => {});

        // 5. DB COMMIT
        if (isFinite(priceUsd) && priceUsd > 0) {
            processedCount++;
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
    if (processedCount > 0) {
        logger.info(`📸 Snapshot: Updated ${processedCount} pools.`);
    }
}

async function runSnapshotCycle() {
    const redis = getClient();
    const db = getDB();
    const connection = getSolanaConnection();
    await updateSolPrice(db);

    try {
        // Only fetch pools that are actively tracked to save RPC
        const pools = await db.all(`SELECT * FROM active_trackers tr JOIN pools p ON tr.pool_address = p.address ORDER BY tr.priority DESC LIMIT 300`);
        
        if (pools.length === 0) {
             // logger.info("Snapshotter idle: No active pools.");
             return;
        }

        // Process in chunks to avoid blowing up RPC limits
        for (let i = 0; i < pools.length; i += 50) {
            const batch = pools.slice(i, i + 50);
            await processPoolBatch(db, connection, batch, redis);
            await new Promise(r => setTimeout(r, 100)); // Small delay between chunks
        }
    } catch (e) {
        logger.error(`Snapshot Cycle Error: ${e.message}`);
    }
}

function startSnapshotter() {
    setTimeout(() => {
        logger.info("🟢 Snapshotter Service Started (Indexer Context)");
        runSnapshotCycle();
        setInterval(runSnapshotCycle, 15000); 
    }, 5000);
}

// For immediate indexing upon discovery
async function snapshotPools(poolAddresses) {
    if (!poolAddresses.length) return;
    const db = getDB();
    const redis = getClient();
    const connection = getSolanaConnection();
    await updateSolPrice(db);
    try {
        const pools = await db.all(`SELECT * FROM pools WHERE address IN (${poolAddresses.map(p => `'${p}'`).join(',')})`);
        await processPoolBatch(db, connection, pools, redis);
    } catch (e) {
        logger.error("Immediate snapshot failed:", e);
    }
}

module.exports = { startSnapshotter, snapshotPools };
