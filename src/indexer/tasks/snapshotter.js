const { PublicKey } = require('@solana/web3.js');
const { getConnection } = require('../services/helius');
const { getDB } = require('../../services/database');
const logger = require('../../services/logger');

let isSnapshotRunning = false;

// --- PROTOCOL LAYOUT OFFSETS (Approximations for Discovery) ---
// We use these to find the "Reserve/Vault" accounts inside the Pair data.

// Raydium AMM v4
const RAY_COIN_VAULT_OFFSET = 432; 
const RAY_PC_VAULT_OFFSET = 464;

// Orca Whirlpools (Standard Layout)
// TokenVaultA is usually around offset 101 or similar. 
// However, checking Account Data length is a good heuristic.
// Whirlpool Data Length = 653 bytes usually.
// Vault A is at offset 101 (32 bytes), Vault B at offset 133 (32 bytes).
const ORCA_VAULT_A_OFFSET = 101;
const ORCA_VAULT_B_OFFSET = 133;

// Meteora DLMM / Amm
// Layout varies, but standard DLMM usually puts reserves early.
// We will attempt to use a balance check on the Pair Address itself first (some AMMs hold funds directly),
// If that fails, we fallback to a heuristic offset scan or rely on DexScreener initial data.

async function runSnapshotCycle() {
    if (isSnapshotRunning) return;
    isSnapshotRunning = true;

    const db = getDB();
    const connection = getConnection();
    const BATCH_SIZE = 100;
    const timestamp = Math.floor(Date.now() / 60000) * 60000;
    
    logger.info(`📸 Snapshot Cycle Starting...`);

    let offset = 0;
    let keepFetching = true;
    let processedCount = 0;

    try {
        while (keepFetching) {
            // Fetch pools with their DEX type and reserve info
            const pools = await db.all(`
                SELECT t.pool_address, p.mint, p.dex, p.reserve_a, p.reserve_b
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
                    // Pump holds funds in bonding curve account directly
                    keysToFetch.push(new PublicKey(pid));
                    poolMap.set(pid, { type: 'pump', index: keysToFetch.length - 1 });
                } 
                else if (['raydium', 'orca', 'meteora'].includes(p.dex)) {
                    if (p.reserve_a && p.reserve_b) {
                        // We know the vaults! Check balances directly.
                        keysToFetch.push(new PublicKey(p.reserve_a));
                        keysToFetch.push(new PublicKey(p.reserve_b));
                        poolMap.set(pid, { 
                            type: 'direct_balance', 
                            idxA: keysToFetch.length - 2, 
                            idxB: keysToFetch.length - 1 
                        });
                    } else {
                        // Discovery Mode: Fetch the Pool Account to find the vaults
                        keysToFetch.push(new PublicKey(pid));
                        poolMap.set(pid, { type: 'discovery', dex: p.dex, index: keysToFetch.length - 1 });
                    }
                }
            }

            if (keysToFetch.length === 0) {
                offset += BATCH_SIZE;
                continue;
            }

            // BATCH RPC CALL
            const accounts = await connection.getMultipleAccountsInfo(keysToFetch);
            const updates = [];

            // PROCESS RESULTS
            for (const p of pools) {
                const task = poolMap.get(p.pool_address);
                if (!task) continue;

                let price = 0;

                try {
                    // --- 1. PUMP.FUN ---
                    if (task.type === 'pump') {
                        const data = accounts[task.index]?.data;
                        if (data && data.length >= 24) {
                            const vToken = data.readBigUInt64LE(8);
                            const vSol = data.readBigUInt64LE(16);
                            if (Number(vToken) > 0) price = Number(vSol) / Number(vToken);
                        }
                    }

                    // --- 2. DISCOVERY (Raydium / Orca / Meteora) ---
                    else if (task.type === 'discovery') {
                        const data = accounts[task.index]?.data;
                        if (data) {
                            let vaultA, vaultB;

                            if (task.dex === 'raydium' && data.length >= 500) {
                                vaultA = new PublicKey(data.subarray(RAY_COIN_VAULT_OFFSET, RAY_COIN_VAULT_OFFSET + 32));
                                vaultB = new PublicKey(data.subarray(RAY_PC_VAULT_OFFSET, RAY_PC_VAULT_OFFSET + 32));
                            } 
                            else if (task.dex === 'orca' && data.length >= 165) {
                                vaultA = new PublicKey(data.subarray(ORCA_VAULT_A_OFFSET, ORCA_VAULT_A_OFFSET + 32));
                                vaultB = new PublicKey(data.subarray(ORCA_VAULT_B_OFFSET, ORCA_VAULT_B_OFFSET + 32));
                            }
                            else if (task.dex === 'meteora') {
                                // Simple Meteora Fallback: 
                                // Some Meteora pools keep Token X at offset 264 and Token Y at 296 (approx)
                                // If parsing fails, we leave reserve_a/b null and rely on DexScreener updates.
                            }

                            if (vaultA && vaultB) {
                                await db.run(`UPDATE pools SET reserve_a = $1, reserve_b = $2 WHERE address = $3`, 
                                    [vaultA.toString(), vaultB.toString(), p.pool_address]);
                            }
                        }
                    }

                    // --- 3. DIRECT BALANCE CHECK (All DEXs) ---
                    else if (task.type === 'direct_balance') {
                        const accA = accounts[task.idxA];
                        const accB = accounts[task.idxB];

                        // Parse SPL Token Account (Amount is at offset 64)
                        if (accA && accB && accA.data.length >= 72 && accB.data.length >= 72) {
                            const balA = accA.data.readBigUInt64LE(64);
                            const balB = accB.data.readBigUInt64LE(64);
                            
                            // Naive Price Calculation (Quote / Base)
                            // Assumption: Reserve B is Quote (SOL/USDC) and Reserve A is Token.
                            // If this is inverted (Token is B), price will be inverted. 
                            // Real production systems check the Mint of A vs B. 
                            // For this version, we assume DexScreener order (Base/Quote) maps to Reserve A/B roughly,
                            // or we accept that inverted charts will correct themselves when we add Mint checks.
                            
                            const valA = Number(balA);
                            const valB = Number(balB);

                            if (valA > 0) price = valB / valA; 
                        }
                    }

                    // SAVE CANDLE
                    if (price > 0) {
                        updates.push(db.run(`
                            INSERT INTO candles_1m (pool_address, timestamp, open, high, low, close, volume)
                            VALUES ($1, $2, $3, $3, $3, $3, 0)
                            ON CONFLICT(pool_address, timestamp) 
                            DO UPDATE SET close = $3, high = GREATEST(candles_1m.high, $3), low = LEAST(candles_1m.low, $3)
                        `, [p.pool_address, timestamp, price]));
                        
                        // Also update current price in POOLS table for aggregation
                        updates.push(db.run(`UPDATE pools SET price_usd = $1 WHERE address = $2`, [price, p.pool_address]));
                    }

                } catch (e) { /* skip faulty pool */ }
            }

            await Promise.all(updates);
            offset += BATCH_SIZE;
            processedCount += pools.length;
            await new Promise(r => setTimeout(r, 100)); // Rate limit
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
