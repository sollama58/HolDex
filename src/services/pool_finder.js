const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection, retryRPC } = require('./solana'); 
const logger = require('./logger');

// --- CONSTANTS ---
const RAYDIUM_V4_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const METEORA_DLMM_PROGRAM_ID = new PublicKey('LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo');
const PUMPFUN_PROGRAM_ID = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');

// --- OFFSETS ---

// Raydium V4
const RAY_V4_OFFSET_BASE_MINT = 400;
const RAY_V4_OFFSET_QUOTE_MINT = 432;
const RAY_V4_OFFSET_BASE_VAULT = 320;
const RAY_V4_OFFSET_QUOTE_VAULT = 352;

// Meteora DLMM (Standard Anchor Layout assumption)
const METEORA_OFFSET_TOKEN_X = 8;
const METEORA_OFFSET_TOKEN_Y = 40;
const METEORA_OFFSET_RESERVE_X = 72;
const METEORA_OFFSET_RESERVE_Y = 104;

/**
 * STRATEGY: "Multi-Protocol Deep Scan"
 * Finds pools across PumpFun (Bonding Curve), Raydium, and Meteora simultaneously.
 */
async function findPoolsOnChain(mintAddress) {
    const connection = getSolanaConnection();
    const pools = [];
    const mint = new PublicKey(mintAddress);
    const mintBase58 = mint.toBase58();

    logger.info(`🔍 Deep Scan for ${mintBase58} (Raydium, Meteora, PumpFun)...`);

    try {
        const promises = [];

        // 1. PUMPFUN CHECK (Deterministic PDA)
        // This is the "Pre-Bonded" or "Bonded on PumpSwap" state.
        promises.push((async () => {
            try {
                const [bondingCurve] = PublicKey.findProgramAddressSync(
                    [Buffer.from("bonding-curve"), mint.toBuffer()],
                    PUMPFUN_PROGRAM_ID
                );
                
                // Check if account exists
                const info = await connection.getAccountInfo(bondingCurve);
                if (info) {
                    pools.push({
                        pairAddress: bondingCurve.toBase58(),
                        dexId: 'pumpfun',
                        baseToken: { address: mintBase58 },
                        quoteToken: { address: 'So11111111111111111111111111111111111111112' }, // PumpFun is always paired with SOL
                        reserve_a: bondingCurve.toBase58(), // Reserves are inside the account itself
                        reserve_b: bondingCurve.toBase58(),
                        liquidity: { usd: 0 },
                        labels: ['bonding-curve']
                    });
                    logger.info(`💊 Found PumpFun Bonding Curve for ${mintBase58}`);
                }
            } catch (err) {
                // Ignore derivation errors
            }
        })());

        // 2. RAYDIUM V4 SCAN
        // This catches "Bonded on Raydium" (Migrated tokens).
        promises.push((async () => {
            const filtersBase = [
                { dataSize: 752 },
                { memcmp: { offset: RAY_V4_OFFSET_BASE_MINT, bytes: mintBase58 } }
            ];
            const filtersQuote = [
                { dataSize: 752 },
                { memcmp: { offset: RAY_V4_OFFSET_QUOTE_MINT, bytes: mintBase58 } }
            ];

            const [baseAccounts, quoteAccounts] = await Promise.all([
                retryRPC(() => connection.getProgramAccounts(RAYDIUM_V4_PROGRAM_ID, { filters: filtersBase })),
                retryRPC(() => connection.getProgramAccounts(RAYDIUM_V4_PROGRAM_ID, { filters: filtersQuote }))
            ]);

            const processRayAccount = (acc, isBase) => {
                const data = acc.account.data;
                const pairAddress = acc.pubkey.toBase58();
                
                // Extract Mints
                const baseMint = new PublicKey(data.subarray(RAY_V4_OFFSET_BASE_MINT, RAY_V4_OFFSET_BASE_MINT + 32)).toBase58();
                const quoteMint = new PublicKey(data.subarray(RAY_V4_OFFSET_QUOTE_MINT, RAY_V4_OFFSET_QUOTE_MINT + 32)).toBase58();
                
                // Extract Vaults
                const baseVault = new PublicKey(data.subarray(RAY_V4_OFFSET_BASE_VAULT, RAY_V4_OFFSET_BASE_VAULT + 32)).toBase58();
                const quoteVault = new PublicKey(data.subarray(RAY_V4_OFFSET_QUOTE_VAULT, RAY_V4_OFFSET_QUOTE_VAULT + 32)).toBase58();

                pools.push({
                    pairAddress,
                    dexId: 'raydium',
                    baseToken: { address: baseMint },
                    quoteToken: { address: quoteMint },
                    reserve_a: baseVault,
                    reserve_b: quoteVault,
                    liquidity: { usd: 0 }
                });
            };

            baseAccounts.forEach(a => processRayAccount(a, true));
            quoteAccounts.forEach(a => processRayAccount(a, false));
        })());

        // 3. METEORA DLMM SCAN
        promises.push((async () => {
             // Look for LbPair accounts where TokenX or TokenY is our mint
             const filtersX = [
                 { memcmp: { offset: METEORA_OFFSET_TOKEN_X, bytes: mintBase58 } }
             ];
             const filtersY = [
                 { memcmp: { offset: METEORA_OFFSET_TOKEN_Y, bytes: mintBase58 } }
             ];

             const [xAccounts, yAccounts] = await Promise.all([
                 retryRPC(() => connection.getProgramAccounts(METEORA_DLMM_PROGRAM_ID, { filters: filtersX })),
                 retryRPC(() => connection.getProgramAccounts(METEORA_DLMM_PROGRAM_ID, { filters: filtersY }))
             ]);

             const processMeteoraAccount = (acc) => {
                 const data = acc.account.data;
                 const pairAddress = acc.pubkey.toBase58();
                 
                 if (data.length < 140) return; 

                 const tokenX = new PublicKey(data.subarray(METEORA_OFFSET_TOKEN_X, METEORA_OFFSET_TOKEN_X + 32)).toBase58();
                 const tokenY = new PublicKey(data.subarray(METEORA_OFFSET_TOKEN_Y, METEORA_OFFSET_TOKEN_Y + 32)).toBase58();
                 const reserveX = new PublicKey(data.subarray(METEORA_OFFSET_RESERVE_X, METEORA_OFFSET_RESERVE_X + 32)).toBase58();
                 const reserveY = new PublicKey(data.subarray(METEORA_OFFSET_RESERVE_Y, METEORA_OFFSET_RESERVE_Y + 32)).toBase58();

                 pools.push({
                     pairAddress,
                     dexId: 'meteora',
                     baseToken: { address: tokenX },
                     quoteToken: { address: tokenY },
                     reserve_a: reserveX,
                     reserve_b: reserveY,
                     liquidity: { usd: 0 }
                 });
             };

             xAccounts.forEach(processMeteoraAccount);
             yAccounts.forEach(processMeteoraAccount);
        })());

        await Promise.all(promises);

        // Deduplicate Pools
        const uniquePools = [];
        const seen = new Set();
        for (const p of pools) {
            if (!seen.has(p.pairAddress)) {
                seen.add(p.pairAddress);
                uniquePools.push(p);
            }
        }
        
        if (uniquePools.length > 0) {
            logger.info(`✅ Found ${uniquePools.length} pools for ${mintBase58}`);
        }
        
        return uniquePools;

    } catch (e) {
        logger.error(`Pool Discovery Error: ${e.message}`);
        return []; 
    }
}

module.exports = { findPoolsOnChain };
