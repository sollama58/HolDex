const { PublicKey } = require('@solana/web3.js');
const { getConnection } = require('../services/helius');
const { getDB } = require('../../services/database');
const logger = require('../../services/logger');

let isSnapshotRunning = false;
let solPriceCache = 200; // Default fallback

// --- PROTOCOL LAYOUT OFFSETS ---
const RAY_COIN_VAULT_OFFSET = 432; 
const RAY_PC_VAULT_OFFSET = 464;
const ORCA_VAULT_A_OFFSET = 101;
const ORCA_VAULT_B_OFFSET = 133;

// Known Quotes for Price Calc
const QUOTE_MINTS = new Set([
    'So11111111111111111111111111111111111111112', // SOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
]);

// Helper: Refresh SOL Price from DB
async function updateSolPrice(db) {
    try {
        const pool = await db.get(`
            SELECT price_usd FROM pools 
            WHERE (token_a = 'So11111111111111111111111111111111111111112' 
            AND token_b = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')
            OR (token_b = 'So11111111111111111111111111111111111111112' 
            AND token_a = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')
            ORDER BY liquidity_usd DESC LIMIT 1
        `);
        if (pool && pool.price_usd > 0) {
            solPriceCache = pool.price_usd;
        }
    } catch (e) {}
}

async function runSnapshotCycle() {
    if (isSnapshotRunning) return;
    isSnapshotRunning = true;

    const db = getDB();
    const connection = getConnection();
    const BATCH_SIZE = 100;
    const timestamp = Math.floor(Date.now() / 60000) * 60000;
    
    await updateSolPrice(db);
    
    logger.info(`📸 Snapshot Cycle: Updating (SOL: ~$${solPriceCache.toFixed(2)})...`);

    let offset = 0;
    let keepFetching = true;
    let processedCount = 0;

    try {
        while (keepFetching) {
            const pools = await db.all(`
                SELECT t.pool_address, p.mint, p.dex, p.reserve_a, p.reserve_b, p.token_a, p.token_b
                FROM active_trackers t
                JOIN pools p ON t.pool_address = p.address
                ORDER BY t.priority DESC, t.pool_address ASC 
                LIMIT ${BATCH_SIZE} OFFSET ${offset}
            `);

            if (pools.length === 0) break;

            const keysToFetch = [];
            const poolMap = new Map();

            for (let i = 0; i < pools.length; i++) {
                const p = pools[i];
                const pid = p.pool_address;

                if (p.dex === 'pump') {
                    keysToFetch.push(new PublicKey(pid));
                    poolMap.set(pid, { type: 'pump', index: keysToFetch.length - 1 });
                } 
                else if (['raydium', 'orca', 'meteora'].includes(p.dex)) {
                    if (p.reserve_a && p.reserve_b) {
                        keysToFetch.push(new PublicKey(p.reserve_a));
                        keysToFetch.push(new PublicKey(p.reserve_b));
                        poolMap.set(pid, { 
                            type: 'direct_balance', 
                            idxA: keysToFetch.length - 2, 
                            idxB: keysToFetch.length - 1 
                        });
                    } else {
                        keysToFetch.push(new PublicKey(pid));
                        poolMap.set(pid, { type: 'discovery', dex: p.dex, index: keysToFetch.length - 1 });
                    }
                }
            }

            if (keysToFetch.length === 0) {
                offset += BATCH_SIZE;
                continue;
            }

            const accounts = await connection.getMultipleAccountsInfo(keysToFetch);
            const updates = [];

            for (const p of pools) {
                const task = poolMap.get(p.pool_address);
                if (!task) continue;

                let nativePrice = 0;
                let liquidityUsd = 0;
                let quoteIsSol = false;

                try {
                    // --- 1. PUMP.FUN ---
                    if (task.type === 'pump') {
                        const data = accounts[task.index]?.data;
                        if (data && data.length >= 49) {
                            const vToken = data.readBigUInt64LE(8);
                            const vSol = data.readBigUInt64LE(16);
                            const realSol = data.readBigUInt64LE(32);
                            // const complete = data.readUInt8(48);

                            if (Number(vToken) > 0) {
                                nativePrice = Number(vSol) / Number(vToken);
                                // Real Sol determines liquidity. Drops to 0 when bonded.
                                liquidityUsd = (Number(realSol) / 1e9) * solPriceCache;
                                quoteIsSol = true;
                            }
                        }
                    }

                    // --- 2. DISCOVERY ---
                    else if (task.type === 'discovery') {
                        const data = accounts[task.index]?.data;
                        if (data) {
                            let vaultA, vaultB;

                            if (task.dex === 'raydium' && data.length >= 500) {
                                vaultA = new PublicKey(data.subarray(RAY_COIN_VAULT_OFFSET, RAY_COIN_VAULT_OFFSET + 32));
                                vaultB = new PublicKey(data.subarray(RAY_PC_VAULT_OFFSET, RAY_PC_VAULT_OFFSET + 32));
                            } 
                            // RESTORED ORCA LOGIC
                            else if (task.dex === 'orca' && data.length >= 165) {
                                vaultA = new PublicKey(data.subarray(ORCA_VAULT_A_OFFSET, ORCA_VAULT_A_OFFSET + 32));
                                vaultB = new PublicKey(data.subarray(ORCA_VAULT_B_OFFSET, ORCA_VAULT_B_OFFSET + 32));
                            }
                            
                            if (vaultA && vaultB) {
                                updates.push(db.run(`UPDATE pools SET reserve_a = $1, reserve_b = $2 WHERE address = $3`, 
                                    [vaultA.toString(), vaultB.toString(), p.pool_address]));
                            }
                        }
                    }

                    // --- 3. DIRECT BALANCE ---
                    else if (task.type === 'direct_balance') {
                        const accA = accounts[task.idxA];
                        const accB = accounts[task.idxB];

                        if (accA && accB && accA.data.length >= 72) {
                            const balA = Number(accA.data.readBigUInt64LE(64));
                            const balB = Number(accB.data.readBigUInt64LE(64));
                            
                            const isAQuote = QUOTE_MINTS.has(p.token_a);
                            const isBQuote = QUOTE_MINTS.has(p.token_b);

                            // Determine Price & Liquidity
                            if (isAQuote && !isBQuote) {
                                if (balB > 0) nativePrice = balA / balB; 
                                liquidityUsd = (balA / (p.token_a.includes('So11') ? 1e9 : 1e6)) * (p.token_a.includes('So11') ? solPriceCache : 1);
                                if (p.token_a.includes('So11')) quoteIsSol = true;
                            } 
                            else if (isBQuote && !isAQuote) {
                                if (balA > 0) nativePrice = balB / balA;
                                liquidityUsd = (balB / (p.token_b.includes('So11') ? 1e9 : 1e6)) * (p.token_b.includes('So11') ? solPriceCache : 1);
                                if (p.token_b.includes('So11')) quoteIsSol = true;
                            }
                            // Fallback: If neither is strictly known quote, assume standard DexScreener order
                            else {
                                if (balA > 0) nativePrice = balB / balA; // Guessing B is quote
                            }
                        }
                    }

                    // SAVE CANDLE & POOL STATS
                    if (nativePrice > 0) {
                        const finalPriceUsd = quoteIsSol ? (nativePrice * solPriceCache) : nativePrice;

                        updates.push(db.run(`
                            INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close, volume)
                            VALUES ($1, $2, $3, $3, $3, $3, 0)
                            ON CONFLICT(pool_address, timestamp) 
                            DO UPDATE SET close = $3, high = GREATEST(candles_1m.high, $3), low = LEAST(candles_1m.low, $3)
                        `, [p.pool_address, timestamp, finalPriceUsd]));
                        
                        updates.push(db.run(`
                            UPDATE pools 
                            SET price_usd = $1, liquidity_usd = $2 
                            WHERE address = $3
                        `, [finalPriceUsd, liquidityUsd, p.pool_address]));
                    }

                } catch (e) { /* skip */ }
            }

            await Promise.all(updates);
            offset += BATCH_SIZE;
            processedCount += pools.length;
            await new Promise(r => setTimeout(r, 50));
        }
    } catch (err) {
        logger.error(`Snapshot Error: ${err.message}`);
    } finally {
        isSnapshotRunning = false;
        if(processedCount > 0) logger.info(`📸 Snapshot Cycle: Updated ${processedCount} pools.`);
    }
}

function startSnapshotter() {
    const now = new Date();
    const delay = 60000 - (now.getSeconds() * 1000 + now.getMilliseconds());
    setTimeout(() => {
        runSnapshotCycle();
        setInterval(runSnapshotCycle, 60000);
    }, delay);
}

module.exports = { startSnapshotter };
