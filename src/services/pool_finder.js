const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection, retryRPC } = require('./solana');
const logger = require('./logger');

// --- PROTOCOL CONSTANTS ---
const PROG_ID_PUMPFUN = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');
const PROG_ID_RAYDIUM_V4 = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const PROG_ID_ORCA_WHIRLPOOL = new PublicKey('whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc');
const PROG_ID_METEORA_DLMM = new PublicKey('LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo');

// --- DATA LAYOUT OFFSETS ---
// Raydium V4
const RAY_OFF_BASE_MINT = 400;
const RAY_OFF_QUOTE_MINT = 432;
const RAY_OFF_BASE_VAULT = 320;
const RAY_OFF_QUOTE_VAULT = 352;

// Orca Whirlpool (Approximate based on standard layout)
const ORCA_OFF_MINT_A = 101;
const ORCA_OFF_VAULT_A = 133;
const ORCA_OFF_MINT_B = 181;
const ORCA_OFF_VAULT_B = 213;

// Meteora DLMM (LbPair)
// Discriminator (8) + TokenX (32) + TokenY (32) + ReserveX (32) + ReserveY (32)
const MET_OFF_MINT_X = 8;
const MET_OFF_MINT_Y = 40;
const MET_OFF_RES_X = 72;
const MET_OFF_RES_Y = 104;

const QUOTE_TOKENS = new Set([
    'So11111111111111111111111111111111111111112', // SOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
]);

/**
 * 1. PUMPFUN DETECTION (Deterministic PDA)
 */
async function findPumpFunPool(mint, results) {
    try {
        const [bondingCurve] = PublicKey.findProgramAddressSync(
            [Buffer.from("bonding-curve"), mint.toBuffer()],
            PROG_ID_PUMPFUN
        );

        // Verify it exists on-chain
        const info = await retryRPC((c) => c.getAccountInfo(bondingCurve));
        
        if (info) {
            logger.info(`✅ Found PumpFun Curve: ${bondingCurve.toBase58()}`);
            results.push({
                pairAddress: bondingCurve.toBase58(),
                dexId: 'pumpfun',
                baseToken: { address: mint.toBase58() },
                quoteToken: { address: 'So11111111111111111111111111111111111111112' }, // SOL
                reserve_a: bondingCurve.toBase58(), // PumpFun logic uses same address
                reserve_b: bondingCurve.toBase58(),
                liquidity: { usd: 0 },
                priceUsd: 0
            });
        }
    } catch (err) {
        logger.debug(`PumpFun check failed: ${err.message}`);
    }
}

/**
 * 2. RAYDIUM V4 DETECTION (getProgramAccounts)
 */
async function findRaydiumPools(mint, mintB58, results) {
    try {
        const filtersBase = [
            { dataSize: 752 }, 
            { memcmp: { offset: RAY_OFF_BASE_MINT, bytes: mintB58 } }
        ];
        const filtersQuote = [
            { dataSize: 752 }, 
            { memcmp: { offset: RAY_OFF_QUOTE_MINT, bytes: mintB58 } }
        ];

        const [baseAccts, quoteAccts] = await Promise.all([
            retryRPC(c => c.getProgramAccounts(PROG_ID_RAYDIUM_V4, { filters: filtersBase })),
            retryRPC(c => c.getProgramAccounts(PROG_ID_RAYDIUM_V4, { filters: filtersQuote }))
        ]);

        const process = (acc, isBase) => {
            const d = acc.account.data;
            const bMint = new PublicKey(d.subarray(RAY_OFF_BASE_MINT, RAY_OFF_BASE_MINT + 32)).toBase58();
            const qMint = new PublicKey(d.subarray(RAY_OFF_QUOTE_MINT, RAY_OFF_QUOTE_MINT + 32)).toBase58();
            
            // Only add if paired with a known quote token (reduces junk pools)
            const otherToken = isBase ? qMint : bMint;
            if (!QUOTE_TOKENS.has(otherToken)) return;

            results.push({
                pairAddress: acc.pubkey.toBase58(),
                dexId: 'raydium',
                baseToken: { address: bMint },
                quoteToken: { address: qMint },
                reserve_a: new PublicKey(d.subarray(RAY_OFF_BASE_VAULT, RAY_OFF_BASE_VAULT + 32)).toBase58(),
                reserve_b: new PublicKey(d.subarray(RAY_OFF_QUOTE_VAULT, RAY_OFF_QUOTE_VAULT + 32)).toBase58(),
                liquidity: { usd: 0 },
                priceUsd: 0
            });
        };

        if(baseAccts) baseAccts.forEach(a => process(a, true));
        if(quoteAccts) quoteAccts.forEach(a => process(a, false));
        
        if (baseAccts?.length || quoteAccts?.length) logger.info(`✅ Found Raydium Pools: ${baseAccts?.length + quoteAccts?.length}`);

    } catch (err) {
        logger.warn(`Raydium Scan Error: ${err.message}`);
    }
}

/**
 * 3. ORCA WHIRLPOOL DETECTION
 */
async function findOrcaPools(mint, mintB58, results) {
    try {
        // Search where Token is Mint A
        const filtersA = [{ memcmp: { offset: ORCA_OFF_MINT_A, bytes: mintB58 } }];
        // Search where Token is Mint B
        const filtersB = [{ memcmp: { offset: ORCA_OFF_MINT_B, bytes: mintB58 } }];

        // Note: Whirlpool accounts are large, we use dataSlice to save bandwidth if possible,
        // but we need the vault addresses, so we fetch full data (or slice smartly).
        // Let's fetch full data for now to be safe with parsing.
        
        const [acctsA, acctsB] = await Promise.all([
            retryRPC(c => c.getProgramAccounts(PROG_ID_ORCA_WHIRLPOOL, { filters: filtersA })),
            retryRPC(c => c.getProgramAccounts(PROG_ID_ORCA_WHIRLPOOL, { filters: filtersB }))
        ]);

        const process = (acc) => {
            const d = acc.account.data;
            const mintA = new PublicKey(d.subarray(ORCA_OFF_MINT_A, ORCA_OFF_MINT_A + 32)).toBase58();
            const mintB = new PublicKey(d.subarray(ORCA_OFF_MINT_B, ORCA_OFF_MINT_B + 32)).toBase58();
            
            results.push({
                pairAddress: acc.pubkey.toBase58(),
                dexId: 'orca',
                baseToken: { address: mintA },
                quoteToken: { address: mintB },
                reserve_a: new PublicKey(d.subarray(ORCA_OFF_VAULT_A, ORCA_OFF_VAULT_A + 32)).toBase58(),
                reserve_b: new PublicKey(d.subarray(ORCA_OFF_VAULT_B, ORCA_OFF_VAULT_B + 32)).toBase58(),
                liquidity: { usd: 0 },
                priceUsd: 0
            });
        };

        if(acctsA) acctsA.forEach(process);
        if(acctsB) acctsB.forEach(process);
        
        if (acctsA?.length || acctsB?.length) logger.info(`✅ Found Orca Pools: ${acctsA?.length + acctsB?.length}`);

    } catch (err) {
        logger.warn(`Orca Scan Error: ${err.message}`);
    }
}

/**
 * 4. METEORA DLMM DETECTION
 */
async function findMeteoraPools(mint, mintB58, results) {
    try {
        const filtersX = [{ memcmp: { offset: MET_OFF_MINT_X, bytes: mintB58 } }];
        const filtersY = [{ memcmp: { offset: MET_OFF_MINT_Y, bytes: mintB58 } }];

        const [acctsX, acctsY] = await Promise.all([
            retryRPC(c => c.getProgramAccounts(PROG_ID_METEORA_DLMM, { filters: filtersX })),
            retryRPC(c => c.getProgramAccounts(PROG_ID_METEORA_DLMM, { filters: filtersY }))
        ]);

        const process = (acc) => {
            const d = acc.account.data;
            const mintX = new PublicKey(d.subarray(MET_OFF_MINT_X, MET_OFF_MINT_X + 32)).toBase58();
            const mintY = new PublicKey(d.subarray(MET_OFF_MINT_Y, MET_OFF_MINT_Y + 32)).toBase58();
            
            // Note: Offsets for reserves inferred from standard layouts
            results.push({
                pairAddress: acc.pubkey.toBase58(),
                dexId: 'meteora',
                baseToken: { address: mintX },
                quoteToken: { address: mintY },
                reserve_a: new PublicKey(d.subarray(MET_OFF_RES_X, MET_OFF_RES_X + 32)).toBase58(),
                reserve_b: new PublicKey(d.subarray(MET_OFF_RES_Y, MET_OFF_RES_Y + 32)).toBase58(),
                liquidity: { usd: 0 },
                priceUsd: 0
            });
        };

        if(acctsX) acctsX.forEach(process);
        if(acctsY) acctsY.forEach(process);
        
        if (acctsX?.length || acctsY?.length) logger.info(`✅ Found Meteora Pools: ${acctsX?.length + acctsY?.length}`);

    } catch (err) {
        logger.warn(`Meteora Scan Error: ${err.message}`);
    }
}

/**
 * MAIN DISCOVERY FUNCTION
 */
async function findPoolsOnChain(mintAddress) {
    const pools = [];
    const mint = new PublicKey(mintAddress);
    const mintB58 = mint.toBase58();

    logger.info(`🔍 Deep Scan initiated for ${mintB58}...`);

    // Run scans in parallel
    await Promise.allSettled([
        findPumpFunPool(mint, pools),
        findRaydiumPools(mint, mintB58, pools),
        findOrcaPools(mint, mintB58, pools),
        findMeteoraPools(mint, mintB58, pools)
    ]);

    // Deduplicate by address
    const unique = [];
    const seen = new Set();
    for (const p of pools) {
        if (!seen.has(p.pairAddress)) {
            seen.add(p.pairAddress);
            unique.push(p);
        }
    }

    logger.info(`🏁 Scan Complete. Found ${unique.length} unique pools.`);
    return unique;
}

module.exports = { findPoolsOnChain };
