const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection, retryRPC } = require('./solana'); // Uses Singleton
const logger = require('./logger');

// --- PROGRAM IDs ---
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');
const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const RAYDIUM_CPMM_PROGRAM_ID = new PublicKey('CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK'); // Newer PumpFun migrations often use this or Standard
const TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');

// Known Quote Mints (SOL, USDC)
const QUOTES = [
    'So11111111111111111111111111111111111111112', 
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
];

/**
 * STRATEGY: "The Supply Hog"
 * Liquidity Pools (almost) always hold the vast majority of a token's supply.
 * Instead of scanning history, we just look at who owns the tokens RIGHT NOW.
 * * 1. Get Top Token Account.
 * 2. Check who owns that account (The Pool Authority).
 * 3. Check who owns the Authority (The DEX Program).
 */
async function findPoolsByLiquidityDistribution(mint) {
    const connection = getSolanaConnection();
    const pools = [];
    const mintPubkey = new PublicKey(mint);

    try {
        // 1. Get Top Holders (The LP Vault is usually #1 or #2)
        const largestAccounts = await retryRPC(() => connection.getTokenLargestAccounts(mintPubkey));
        if (!largestAccounts.value || largestAccounts.value.length === 0) return [];

        // Check top 3 accounts (bonding curve, raydium vault, etc)
        const topAccounts = largestAccounts.value.slice(0, 3);
        
        for (const account of topAccounts) {
            // Filter out tiny accounts (noise)
            if (account.uiAmount < 1000) continue; 

            const vaultAddress = new PublicKey(account.address);
            
            // 2. Get Vault Info to find its Owner (The Pool Authority)
            const vaultInfo = await retryRPC(() => connection.getAccountInfo(vaultAddress));
            if (!vaultInfo) continue;

            // SPL Token Layout: Owner is at offset 32 (32 bytes)
            const ownerAddress = new PublicKey(vaultInfo.data.subarray(32, 64));
            
            // 3. Get Pool Authority Info to find the DEX Program
            const ownerInfo = await retryRPC(() => connection.getAccountInfo(ownerAddress));
            if (!ownerInfo) continue;

            const ownerProgram = ownerInfo.owner.toBase58();
            let dexId = 'unknown';

            if (ownerProgram === RAYDIUM_PROGRAM_ID.toBase58()) dexId = 'raydium';
            else if (ownerProgram === RAYDIUM_CPMM_PROGRAM_ID.toBase58()) dexId = 'raydium_cpmm';
            else if (ownerProgram === PUMP_PROGRAM_ID.toBase58()) dexId = 'pump';
            else if (ownerProgram === 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA') continue; // User wallet
            else if (ownerProgram === '11111111111111111111111111111111') continue; // System
            
            // If we found a DEX program owning the supply, we found the pool!
            if (dexId !== 'unknown' || (account.uiAmount > 100000000)) { // Capture generic heavy holders too
                 pools.push({
                    pairAddress: ownerAddress.toBase58(), // The Pool Authority/State
                    dexId: dexId,
                    baseToken: { address: mint },
                    quoteToken: { address: 'So11111111111111111111111111111111111111112' }, // Default SOL
                    liquidity: { usd: 0 },
                    // We save the Vault address so Snapshotter doesn't have to search again
                    reserve_a: vaultAddress.toBase58(), 
                    // reserve_b will be found by snapshotter scanning the pairAddress
                });
            }
        }

    } catch (e) {
        logger.warn(`Supply Hog Scan failed for ${mint}: ${e.message}`);
    }

    return pools;
}

async function findPoolsOnChain(mintAddress) {
    const pools = [];
    const mint = new PublicKey(mintAddress);
    const mintBase58 = mint.toBase58();

    logger.info(`🔍 Deep Scan (Supply Hog) for ${mintBase58}...`);

    try {
        const promises = [];

        // 1. DETERMINISTIC: Pump Bonding Curve (Always check this first)
        const [pumpCurve] = PublicKey.findProgramAddressSync(
            [Buffer.from("bonding-curve"), mint.toBuffer()],
            PUMP_PROGRAM_ID
        );
        promises.push(
             retryRPC(() => getSolanaConnection().getAccountInfo(pumpCurve)).then(info => {
                if (info && info.owner.equals(PUMP_PROGRAM_ID) && info.data.length >= 40) {
                     // We add it. Snapshotter will determine if it's bonded (empty) or active.
                     return [{
                        pairAddress: pumpCurve.toString(),
                        dexId: 'pump',
                        baseToken: { address: mintBase58 },
                        quoteToken: { address: 'So11111111111111111111111111111111111111112' }
                    }];
                }
                return [];
             }).catch(() => [])
        );

        // 2. DISCOVERY: Supply Hog Strategy (Finds Raydium, PumpSwap, Etc)
        promises.push(findPoolsByLiquidityDistribution(mintBase58));

        // 3. LEGACY: Raydium Standard Program Account Scan (Backup)
        const conn = getSolanaConnection();
        promises.push(
            retryRPC(() => conn.getProgramAccounts(RAYDIUM_PROGRAM_ID, {
                filters: [{ dataSize: 752 }, { memcmp: { offset: 400, bytes: mintBase58 } }]
            })).then(res => res.map(p => ({
                pairAddress: p.pubkey.toString(),
                dexId: 'raydium',
                baseToken: { address: mintBase58 },
                quoteToken: { address: new PublicKey(p.account.data.subarray(432, 464)).toBase58() }
            }))).catch(() => [])
        );

        const results = await Promise.allSettled(promises);
        results.forEach(res => {
            if (res.status === 'fulfilled' && Array.isArray(res.value)) {
                pools.push(...res.value);
            }
        });

        // Deduplicate (Prefer 'raydium' over 'unknown' if duplicates exist)
        const uniquePools = [];
        const seen = new Set();
        
        // Sort pools: Raydium/Pump first, unknown last
        pools.sort((a, b) => {
            const scoreA = (a.dexId === 'raydium' || a.dexId === 'pump') ? 1 : 0;
            const scoreB = (b.dexId === 'raydium' || b.dexId === 'pump') ? 1 : 0;
            return scoreB - scoreA;
        });

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
        
        return uniquePools;

    } catch (e) {
        logger.error(`Pool Discovery Error: ${e.message}`);
        return pools; 
    }
}

module.exports = { findPoolsOnChain };
