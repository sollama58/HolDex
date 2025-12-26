const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

// --- PROGRAM IDs ---
const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const ORCA_PROGRAM_ID = new PublicKey('whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc');
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');

const connection = new Connection(config.SOLANA_RPC_URL, 'confirmed');

// Helper: Retry RPC calls
async function retryRPC(fn, retries = 3, delay = 1000) {
    try {
        return await fn();
    } catch (err) {
        if (retries <= 0) throw err;
        await new Promise(r => setTimeout(r, delay));
        return retryRPC(fn, retries - 1, delay * 2);
    }
}

/**
 * Strategy: Transaction Scan
 * Scans recent transactions of the Mint to find interactions with DEX programs.
 * This is often more reliable than getProgramAccounts for finding the specific pool.
 */
async function findPoolsFromTransactions(mint) {
    const pools = [];
    try {
        // 1. Get recent signatures for the mint
        const signatures = await retryRPC(() => connection.getSignaturesForAddress(mint, { limit: 25 }));
        
        if (signatures.length === 0) return [];

        const sigList = signatures.map(s => s.signature);
        
        // 2. Fetch parsed transactions to see involved accounts
        const txs = await retryRPC(() => connection.getParsedTransactions(sigList, { 
            maxSupportedTransactionVersion: 0,
            commitment: 'confirmed'
        }));

        const candidateAccounts = new Set();

        for (const tx of txs) {
            if (!tx || !tx.transaction) continue;
            
            const accountKeys = tx.transaction.message.accountKeys;
            
            // Check for Program interactions
            const isRaydium = accountKeys.some(k => k.pubkey.equals(RAYDIUM_PROGRAM_ID));
            const isOrca = accountKeys.some(k => k.pubkey.equals(ORCA_PROGRAM_ID));
            const isPump = accountKeys.some(k => k.pubkey.equals(PUMP_PROGRAM_ID));

            if (isRaydium || isOrca || isPump) {
                // Collect all writable accounts as candidates
                accountKeys.forEach(k => {
                    if (k.writable && !k.signer) {
                        candidateAccounts.add(k.pubkey.toBase58());
                    }
                });
            }
        }

        if (candidateAccounts.size === 0) return [];

        // 3. Verify Candidates
        const candidates = Array.from(candidateAccounts).map(s => new PublicKey(s));
        
        // Split into chunks to avoid hitting limits
        while (candidates.length > 0) {
            const batch = candidates.splice(0, 100);
            const infos = await retryRPC(() => connection.getMultipleAccountsInfo(batch));

            infos.forEach((info, i) => {
                if (!info) return;

                // Check Raydium V4
                if (info.owner.equals(RAYDIUM_PROGRAM_ID) && info.data.length === 752) {
                    // Parse Base/Quote Mints immediately (Offsets 400 & 432)
                    const baseMint = new PublicKey(info.data.subarray(400, 432)).toBase58();
                    const quoteMint = new PublicKey(info.data.subarray(432, 464)).toBase58();

                    pools.push({
                        pairAddress: batch[i].toString(),
                        dexId: 'raydium',
                        liquidity: { usd: 0 },
                        baseToken: { address: baseMint },
                        quoteToken: { address: quoteMint } 
                    });
                }
                
                // Check Orca
                if (info.owner.equals(ORCA_PROGRAM_ID) && info.data.length === 653) {
                     pools.push({
                        pairAddress: batch[i].toString(),
                        dexId: 'orca',
                        liquidity: { usd: 0 },
                        baseToken: { address: 'Unknown' }, // Orca layout varies, snapshotter handles discovery
                        quoteToken: { address: 'Unknown' }
                    });
                }

                // Check Pump.fun (PumpSwap)
                // Pump Bonding Curves are owned by the Pump Program
                if (info.owner.equals(PUMP_PROGRAM_ID) && info.data.length >= 40) {
                    pools.push({
                        pairAddress: batch[i].toString(),
                        dexId: 'pump',
                        liquidity: { usd: 0 },
                        baseToken: { address: mint.toBase58() }, // Pump pools are always Mint/SOL
                        quoteToken: { address: 'So11111111111111111111111111111111111111112' }
                    });
                }
            });
        }

    } catch (e) {
        logger.warn(`Tx Scan failed for ${mint}: ${e.message}`);
    }
    return pools;
}

async function findPoolsOnChain(mintAddress) {
    const pools = [];
    const mint = new PublicKey(mintAddress);
    const mintBase58 = mint.toBase58();

    logger.info(`🔍 Deep Scan: Searching pools for ${mintBase58}...`);

    try {
        const promises = [];

        // --- STRATEGY 1: TRANSACTION HISTORY SCAN (Activity Based) ---
        // Finds pools via recent swaps/interactions. Catches Pump, Raydium, Orca.
        promises.push(findPoolsFromTransactions(mint));

        // --- STRATEGY 2: GLOBAL SCAN (Backup) ---
        // Fallback for Raydium if Tx Scan doesn't find recent activity
        
        // Raydium Base
        promises.push(
            retryRPC(() => connection.getProgramAccounts(RAYDIUM_PROGRAM_ID, {
                filters: [{ dataSize: 752 }, { memcmp: { offset: 400, bytes: mintBase58 } }]
            })).then(res => res.map(p => {
                const quote = new PublicKey(p.account.data.subarray(432, 464)).toBase58();
                return {
                    pairAddress: p.pubkey.toString(), 
                    dexId: 'raydium', 
                    baseToken: { address: mintBase58 },
                    quoteToken: { address: quote }
                };
            })).catch(e => [])
        );

        // Raydium Quote
        promises.push(
            retryRPC(() => connection.getProgramAccounts(RAYDIUM_PROGRAM_ID, {
                filters: [{ dataSize: 752 }, { memcmp: { offset: 432, bytes: mintBase58 } }]
            })).then(res => res.map(p => {
                const base = new PublicKey(p.account.data.subarray(400, 432)).toBase58();
                return {
                    pairAddress: p.pubkey.toString(), 
                    dexId: 'raydium', 
                    baseToken: { address: base },
                    quoteToken: { address: mintBase58 }
                };
            })).catch(e => [])
        );

        // --- EXECUTE ---
        const results = await Promise.allSettled(promises);
        
        results.forEach(res => {
            if (res.status === 'fulfilled' && Array.isArray(res.value)) {
                pools.push(...res.value);
            }
        });

        // --- DEDUPLICATE & NORMALIZE ---
        const uniquePools = [];
        const seen = new Set();
        
        for (const p of pools) {
            if (!seen.has(p.pairAddress)) {
                seen.add(p.pairAddress);
                p.liquidity = p.liquidity || { usd: 0 };
                
                // Defaults
                if (!p.quoteToken) p.quoteToken = { address: 'So11111111111111111111111111111111111111112' }; 
                if (!p.baseToken) p.baseToken = { address: 'Unknown' }; 
                
                uniquePools.push(p);
            }
        }

        logger.info(`✅ Found ${uniquePools.length} total pools on-chain for ${mintBase58}`);
        return uniquePools;

    } catch (e) {
        logger.error(`On-Chain Pool Find Fatal Error: ${e.message}`);
        return pools; 
    }
}

module.exports = { findPoolsOnChain };
