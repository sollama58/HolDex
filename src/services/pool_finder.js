const { PublicKey } = require('@solana/web3.js');
const { retryRPC, getSolanaConnection } = require('./solana');
const logger = require('./logger');

// --- PROGRAM IDS ---
const PROG_ID_RAYDIUM_V4 = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const PROG_ID_RAYDIUM_CPMM = new PublicKey('CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C'); // New Standard
const PROG_ID_PUMPSWAP = new PublicKey('pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'); // PumpSwap (Fall 2025)
const PROG_ID_METEORA_DLMM = new PublicKey('LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo');
const PROG_ID_ORCA_WHIRLPOOL = new PublicKey('whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc');
const PROG_ID_PUMPFUN = new PublicKey('6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P');

// --- OFFSETS (Raydium V4) ---
// We only fetch the bytes we NEED to parse.
const RAY_SLICE = { offset: 320, length: 144 };
const SLICE_OFF_BASE_VAULT = 0;   
const SLICE_OFF_QUOTE_VAULT = 32; 
const SLICE_OFF_BASE_MINT = 80;   
const SLICE_OFF_QUOTE_MINT = 112; 

/**
 * Strategy 1: Direct GPA (GetProgramAccounts) Scan
 * Best for Raydium V4 where we know exact offsets and there are many pools.
 */
async function findRaydiumV4Pools(mintB58, results) {
    try {
        const filtersBase = [{ dataSize: 752 }, { memcmp: { offset: 400, bytes: mintB58 } }];
        const filtersQuote = [{ dataSize: 752 }, { memcmp: { offset: 432, bytes: mintB58 } }];

        const [baseAccts, quoteAccts] = await Promise.all([
            retryRPC(c => c.getProgramAccounts(PROG_ID_RAYDIUM_V4, { filters: filtersBase, dataSlice: RAY_SLICE })),
            retryRPC(c => c.getProgramAccounts(PROG_ID_RAYDIUM_V4, { filters: filtersQuote, dataSlice: RAY_SLICE }))
        ]);

        const process = (acc) => {
            const d = acc.account.data;
            const bMint = new PublicKey(d.subarray(SLICE_OFF_BASE_MINT, SLICE_OFF_BASE_MINT + 32)).toBase58();
            const qMint = new PublicKey(d.subarray(SLICE_OFF_QUOTE_MINT, SLICE_OFF_QUOTE_MINT + 32)).toBase58();
            
            // Basic sanity check
            if (!bMint || !qMint) return;

            results.push({
                pairAddress: acc.pubkey.toBase58(),
                dexId: 'raydium',
                type: 'v4',
                baseToken: { address: bMint },
                quoteToken: { address: qMint },
                reserve_a: new PublicKey(d.subarray(SLICE_OFF_BASE_VAULT, SLICE_OFF_BASE_VAULT + 32)).toBase58(),
                reserve_b: new PublicKey(d.subarray(SLICE_OFF_QUOTE_VAULT, SLICE_OFF_QUOTE_VAULT + 32)).toBase58(),
                liquidity: { usd: 0 },
                volume: { h24: 0 },
                priceUsd: 0
            });
        };

        if(baseAccts) baseAccts.forEach(process);
        if(quoteAccts) quoteAccts.forEach(process);
        
    } catch (err) {
        logger.warn(`Raydium V4 Scan Error: ${err.message}`);
    }
}

/**
 * Strategy 2: Pump.fun Bonding Curve
 * Uses exact seeds to find the bonding curve account.
 */
async function findPumpFunCurve(mintAddress, results) {
    try {
        const mint = new PublicKey(mintAddress);
        const [bondingCurve] = PublicKey.findProgramAddressSync(
            [Buffer.from("bonding-curve"), mint.toBuffer()],
            PROG_ID_PUMPFUN
        );
        const [bondingCurveVault] = PublicKey.findProgramAddressSync(
            [Buffer.from("bonding-curve"), mint.toBuffer(), Buffer.from("token-account")],
            PROG_ID_PUMPFUN
        );

        // Verify it exists
        const connection = getSolanaConnection();
        const info = await retryRPC(c => c.getAccountInfo(bondingCurve));
        
        if (info) {
             results.push({
                pairAddress: bondingCurve.toBase58(),
                dexId: 'pumpfun',
                type: 'bonding_curve',
                baseToken: { address: mintAddress },
                quoteToken: { address: 'So11111111111111111111111111111111111111112' }, // SOL
                reserve_a: bondingCurveVault.toBase58(), // Token Reserve
                reserve_b: bondingCurve.toBase58(),      // SOL Reserve (Virtual)
                liquidity: { usd: 0 },
                volume: { h24: 0 },
                priceUsd: 0
            });
        }
    } catch (e) {
        // Bonding curve might be closed/migrated, ignore error
    }
}

/**
 * Strategy 3: Token Account Trace (Robust Fallback)
 * * Used for: PumpSwap, Raydium CPMM, Meteora DLMM, Orca
 * * Logic:
 * 1. Find all Token Accounts for the Mint.
 * 2. Check the OWNER of those token accounts.
 * 3. If the owner is a known DEX Program (or PDA of it), that's the Pool Vault.
 * 4. Fetch that Pool Account to find the *other* token.
 */
async function findPoolsByTokenOwnership(mintAddress, results) {
    try {
        const connection = getSolanaConnection();
        const mint = new PublicKey(mintAddress);

        // Get all token accounts for this mint (filter by large size to find pools?)
        // We fetch all because liquidity pools usually have the largest balances, 
        // but we filter by Owner Program ID which is safer.
        const tokenAccounts = await retryRPC(c => c.getTokenAccountsByMint(mint, { programId: new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA') }));

        const candidatePools = [];

        for (const { pubkey, account } of tokenAccounts.value) {
            // We need to parse the account data to get the 'owner' (The Pool or Authority)
            // standard SPL layout: mint(32), owner(32), amount(8)...
            if (account.data.length < 64) continue;
            
            const owner = new PublicKey(account.data.subarray(32, 64));
            const ownerB58 = owner.toBase58();
            const tokenAccountAddr = pubkey.toBase58();

            // Check if Owner is a known DEX Program or derived from it
            // NOTE: Most Anchor programs (Orca, PumpSwap, CPMM) use a PDA as owner.
            // We check the "owner" of the "owner" (The Program).
            
            try {
                const ownerInfo = await retryRPC(c => c.getAccountInfo(owner));
                if (!ownerInfo) continue;
                
                const programOwner = ownerInfo.owner.toBase58();

                if (programOwner === PROG_ID_PUMPSWAP.toBase58()) {
                    await parseGenericAnchorPool(connection, owner, 'pumpswap', 'pumpswap', mintAddress, tokenAccountAddr, results);
                }
                else if (programOwner === PROG_ID_RAYDIUM_CPMM.toBase58()) {
                    await parseGenericAnchorPool(connection, owner, 'raydium', 'cpmm', mintAddress, tokenAccountAddr, results);
                }
                else if (programOwner === PROG_ID_METEORA_DLMM.toBase58()) {
                     // Meteora specific parsing if needed, or generic
                     await parseGenericAnchorPool(connection, owner, 'meteora', 'dlmm', mintAddress, tokenAccountAddr, results);
                }
                else if (programOwner === PROG_ID_ORCA_WHIRLPOOL.toBase58()) {
                     await parseGenericAnchorPool(connection, owner, 'orca', 'whirlpool', mintAddress, tokenAccountAddr, results);
                }

            } catch (innerErr) {
                // Ignore single account errors
            }
        }

    } catch (e) {
        logger.warn(`Token Trace Scan Error: ${e.message}`);
    }
}

/**
 * Generic Parser for Anchor-based AMMs (PumpSwap, Raydium CPMM, Orca)
 * We assume the "Pool Address" is the owner of the vault.
 * We fetch the Pool Address data and try to find the "other" mint.
 */
async function parseGenericAnchorPool(connection, poolAddress, dexId, type, myMint, myVault, results) {
    // Avoid duplicates
    if (results.find(r => r.pairAddress === poolAddress.toBase58())) return;

    try {
        const info = await retryRPC(c => c.getAccountInfo(poolAddress));
        if (!info) return;

        const data = info.data;
        // Anchor accounts have 8 byte discriminator.
        // We look for 2 Pubkeys (Mint A, Mint B) usually appearing early.
        // Heuristic: Scan for myMint. The OTHER pubkey nearby is the paired mint.
        
        const myMintBuffer = new PublicKey(myMint).toBuffer();
        let matchedOffset = -1;
        
        // Simple scan for my Mint in the pool state
        for (let i = 8; i < 200; i++) {
            if (data.subarray(i, i + 32).equals(myMintBuffer)) {
                matchedOffset = i;
                break;
            }
        }

        if (matchedOffset === -1) return; // Couldn't find my mint in pool state

        // Look for the OTHER mint. Usually it's right before or right after.
        // Standard Layouts often: [MintA, MintB] or [MintA, VaultA, MintB, VaultB]
        
        // Try finding another valid Pubkey nearby (within 64 bytes)
        // This is a heuristic for unknown layouts (like new PumpSwap), 
        // effectively making us "competitive" without waiting for SDK updates.
        
        let pairedMint = null;
        
        // Check 32 bytes before
        if (matchedOffset >= 40) {
             // potential mint before?
        }
        
        // Heuristic: Just find the first 2 pubkeys in the struct that look like Mints
        // (Not perfectly safe but effective for MVP pool discovery)
        
        // BETTER: Use known offsets for known progs
        let mintA, mintB;
        
        if (dexId === 'raydium' && type === 'cpmm') {
            // CPMM Layout: MintA @ 168, MintB @ 200
            mintA = new PublicKey(data.subarray(168, 200)).toBase58();
            mintB = new PublicKey(data.subarray(200, 232)).toBase58();
        } else if (dexId === 'orca') {
             // Whirlpool: TokenMintA @ 65, TokenMintB @ 97 (variable packing, but approx)
             // Actually Whirlpool is: ticks(..), fee(..), liquidity(..), tickCurrent(..), 
             // tokenMintA is at offset 8 + ... it's deep.
             // Fallback to "Found Pool" but maybe incomplete data if we can't parse.
             // We'll skip complex parsing here and just mark it found.
             return; 
        } else {
             // PumpSwap / Generic
             // Assume Mint A and Mint B are the first two Pubkeys after Discriminator
             // or check adjacent to myMint
             // For now, if we can't parse the pair, we can't list it reliably.
             // Let's assume PumpSwap is [Discriminator(8), Creator(32), MintA(32), MintB(32)...]
             // or [Disc, MintA, MintB]
             
             // We will scan for unique Pubkeys in the first 200 bytes
             // If we find myMint and one other Unique Pubkey, assume that's the pair.
             const candidates = [];
             for(let i=8; i<200; i+=32) { // 32 byte alignment guess
                 try {
                     const p = new PublicKey(data.subarray(i, i+32));
                     const s = p.toBase58();
                     if (PublicKey.isOnCurve(p.toBuffer()) && s !== '11111111111111111111111111111111') {
                         candidates.push(s);
                     }
                 } catch(e){}
             }
             
             const other = candidates.find(c => c !== myMint);
             if (other) {
                 pairedMint = other;
             }
        }

        if (pairedMint || (mintA && mintB)) {
            results.push({
                pairAddress: poolAddress.toBase58(),
                dexId: dexId,
                type: type,
                baseToken: { address: mintA || myMint },
                quoteToken: { address: mintB || pairedMint },
                liquidity: { usd: 0 },
                volume: { h24: 0 },
                priceUsd: 0,
                // Note: We don't have Vault addresses for Generic yet, 
                // would need further parsing. This allows listing "Pool Exists" 
                // but Snapshotter might fail to read reserves without correct Vaults.
            });
        }

    } catch (e) {
        logger.warn(`Generic Parser Error for ${dexId}: ${e.message}`);
    }
}


async function findPoolsOnChain(mintAddress) {
    const pools = [];
    const mintB58 = mintAddress;

    // 1. Parallel Search
    await Promise.all([
        findRaydiumV4Pools(mintB58, pools),
        findPumpFunCurve(mintAddress, pools),
        findPoolsByTokenOwnership(mintAddress, pools) // Handles PumpSwap, CPMM, Meteora
    ]);

    logger.info(`🔍 Discovery: Found ${pools.length} pools for ${mintAddress}`);
    return pools;
}

module.exports = { findPoolsOnChain };
