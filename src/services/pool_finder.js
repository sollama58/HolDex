const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection, retryRPC } = require('./solana'); 
const logger = require('./logger');

// --- PROGRAM IDs ---
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');
const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
// Common Program ID for Standard AMMs including some Raydium forks
const TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');

/**
 * STRATEGY: "The Supply Hog"
 * Finds pools by checking who holds the most tokens.
 */
async function findPoolsByLiquidityDistribution(mint) {
    const connection = getSolanaConnection();
    const pools = [];
    const mintPubkey = new PublicKey(mint);

    try {
        // 1. Get Top Holders
        const largestAccounts = await retryRPC(() => connection.getTokenLargestAccounts(mintPubkey));
        if (!largestAccounts.value || largestAccounts.value.length === 0) return [];

        // Check top 5 accounts to be safe
        const topAccounts = largestAccounts.value.slice(0, 5);
        
        for (const account of topAccounts) {
            // Filter out tiny accounts
            if (account.uiAmount < 100) continue; 

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

            // Heuristics for DEX identification
            if (ownerProgram === RAYDIUM_PROGRAM_ID.toBase58()) dexId = 'raydium';
            else if (ownerProgram === PUMP_PROGRAM_ID.toBase58()) dexId = 'pump';
            // Meteora / Orca / Others could be added here
            
            // If it's a known DEX or we just want to track generic AMMs
            if (dexId !== 'unknown' || ownerProgram !== TOKEN_PROGRAM_ID.toBase58()) {
                 pools.push({
                    pairAddress: ownerAddress.toBase58(), 
                    dexId: dexId === 'unknown' ? 'raydium' : dexId, // Default to raydium logic for unknown AMMs as it's most common
                    baseToken: { address: mint },
                    quoteToken: { address: 'So11111111111111111111111111111111111111112' }, // Assume SOL pairing initially
                    liquidity: { usd: 0 },
                    reserve_a: vaultAddress.toBase58() // We found one vault, snapshotter will find the other
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

        // 1. DETERMINISTIC: Pump Bonding Curve
        const [pumpCurve] = PublicKey.findProgramAddressSync(
            [Buffer.from("bonding-curve"), mint.toBuffer()],
            PUMP_PROGRAM_ID
        );
        promises.push(
             retryRPC(() => getSolanaConnection().getAccountInfo(pumpCurve)).then(info => {
                if (info && info.owner.equals(PUMP_PROGRAM_ID) && info.data.length >= 40) {
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

        // 2. DISCOVERY: Supply Hog Strategy
        promises.push(findPoolsByLiquidityDistribution(mintBase58));

        const results = await Promise.allSettled(promises);
        results.forEach(res => {
            if (res.status === 'fulfilled' && Array.isArray(res.value)) {
                pools.push(...res.value);
            }
        });

        // Deduplicate
        const uniquePools = [];
        const seen = new Set();
        
        pools.sort((a, b) => {
            // Prioritize named DEXes over unknown
            const scoreA = (a.dexId === 'raydium' || a.dexId === 'pump') ? 1 : 0;
            const scoreB = (b.dexId === 'raydium' || b.dexId === 'pump') ? 1 : 0;
            return scoreB - scoreA;
        });

        for (const p of pools) {
            if (!seen.has(p.pairAddress)) {
                seen.add(p.pairAddress);
                p.liquidity = p.liquidity || { usd: 0 };
                if (!p.quoteToken) p.quoteToken = { address: 'So11111111111111111111111111111111111111112' }; 
                if (!p.baseToken) p.baseToken = { address: mintBase58 }; 
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
