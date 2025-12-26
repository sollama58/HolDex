const { PublicKey, Connection } = require('@solana/web3.js');
const { retryRPC, getSolanaConnection } = require('./solana');
const logger = require('./logger');

// --- PROGRAM IDS ---
const PROG_ID_RAYDIUM_V4 = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const PROG_ID_RAYDIUM_CPMM = new PublicKey('CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C'); 
const PROG_ID_PUMPSWAP = new PublicKey('pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'); 
const PROG_ID_METEORA_DLMM = new PublicKey('LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo');
const PROG_ID_ORCA_WHIRLPOOL = new PublicKey('whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc');
const PROG_ID_PUMPFUN = new PublicKey('6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P');

// --- OFFSETS ---
const RAY_SLICE = { offset: 320, length: 144 };
const SLICE_OFF_BASE_VAULT = 0;   
const SLICE_OFF_QUOTE_VAULT = 32; 
const SLICE_OFF_BASE_MINT = 80;   
const SLICE_OFF_QUOTE_MINT = 112; 

/**
 * Robust Connection Getter
 * Ensures we have a valid web3.Connection object with required methods.
 */
function getValidConnection() {
    let conn = null;
    try {
        conn = getSolanaConnection();
    } catch (e) {
        logger.warn('getSolanaConnection failed, falling back to new instance');
    }

    // Check if the returned object is a valid Connection (has key methods)
    if (conn && typeof conn.getTokenAccountsByMint === 'function') {
        return conn;
    }

    logger.warn('⚠️ getSolanaConnection() returned invalid object. Instantiating fallback Connection.');
    
    // Fallback: Create a standard connection
    const rpcUrl = process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com';
    return new Connection(rpcUrl, 'confirmed');
}

/**
 * Strategy 1: Direct GPA Scan (Raydium V4)
 */
async function findRaydiumV4Pools(mintB58, results) {
    try {
        const filtersBase = [{ dataSize: 752 }, { memcmp: { offset: 400, bytes: mintB58 } }];
        const filtersQuote = [{ dataSize: 752 }, { memcmp: { offset: 432, bytes: mintB58 } }];

        const connection = getValidConnection();

        const [baseAccts, quoteAccts] = await Promise.all([
            retryRPC(() => connection.getProgramAccounts(PROG_ID_RAYDIUM_V4, { filters: filtersBase, dataSlice: RAY_SLICE })),
            retryRPC(() => connection.getProgramAccounts(PROG_ID_RAYDIUM_V4, { filters: filtersQuote, dataSlice: RAY_SLICE }))
        ]);

        const process = (acc) => {
            const d = acc.account.data;
            const bMint = new PublicKey(d.subarray(SLICE_OFF_BASE_MINT, SLICE_OFF_BASE_MINT + 32)).toBase58();
            const qMint = new PublicKey(d.subarray(SLICE_OFF_QUOTE_MINT, SLICE_OFF_QUOTE_MINT + 32)).toBase58();
            
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

        const connection = getValidConnection();
        const info = await retryRPC(() => connection.getAccountInfo(bondingCurve));
        
        if (info) {
             results.push({
                pairAddress: bondingCurve.toBase58(),
                dexId: 'pumpfun',
                type: 'bonding_curve',
                baseToken: { address: mintAddress },
                quoteToken: { address: 'So11111111111111111111111111111111111111112' }, 
                reserve_a: bondingCurveVault.toBase58(), 
                reserve_b: bondingCurve.toBase58(),      
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
 * Handles PumpSwap, Raydium CPMM, Meteora, Orca
 */
async function findPoolsByTokenOwnership(mintAddress, results) {
    try {
        const connection = getValidConnection();
        const mint = new PublicKey(mintAddress);
        const TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');

        // Check if connection is valid before call
        if (typeof connection.getTokenAccountsByMint !== 'function') {
            throw new Error("Connection object missing getTokenAccountsByMint");
        }

        const tokenAccounts = await retryRPC(() => connection.getTokenAccountsByMint(mint, { programId: TOKEN_PROGRAM_ID }));

        for (const { pubkey, account } of tokenAccounts.value) {
            if (account.data.length < 64) continue;
            
            const owner = new PublicKey(account.data.subarray(32, 64));
            const tokenAccountAddr = pubkey.toBase58();
            
            try {
                const ownerInfo = await retryRPC(() => connection.getAccountInfo(owner));
                if (!ownerInfo) continue;
                
                const programOwner = ownerInfo.owner.toBase58();

                if (programOwner === PROG_ID_PUMPSWAP.toBase58()) {
                    await parseGenericAnchorPool(connection, owner, 'pumpswap', 'pumpswap', mintAddress, tokenAccountAddr, results);
                }
                else if (programOwner === PROG_ID_RAYDIUM_CPMM.toBase58()) {
                    await parseGenericAnchorPool(connection, owner, 'raydium', 'cpmm', mintAddress, tokenAccountAddr, results);
                }
                else if (programOwner === PROG_ID_METEORA_DLMM.toBase58()) {
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

async function parseGenericAnchorPool(connection, poolAddress, dexId, type, myMint, myVault, results) {
    if (results.find(r => r.pairAddress === poolAddress.toBase58())) return;

    try {
        const info = await retryRPC(() => connection.getAccountInfo(poolAddress));
        if (!info) return;

        const data = info.data;
        const myMintBuffer = new PublicKey(myMint).toBuffer();
        let matchedOffset = -1;
        
        // Scan for my mint in the pool state
        for (let i = 8; i < 200; i++) {
            if (data.subarray(i, i + 32).equals(myMintBuffer)) {
                matchedOffset = i;
                break;
            }
        }

        if (matchedOffset === -1) return; 
        
        let mintA, mintB;
        let pairedMint = null;
        
        if (dexId === 'raydium' && type === 'cpmm') {
            mintA = new PublicKey(data.subarray(168, 200)).toBase58();
            mintB = new PublicKey(data.subarray(200, 232)).toBase58();
        } else {
             // Generic Heuristic
             const candidates = [];
             for(let i=8; i<200; i+=32) {
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
            });
        }

    } catch (e) {
        logger.warn(`Generic Parser Error for ${dexId}: ${e.message}`);
    }
}

async function findPoolsOnChain(mintAddress) {
    const pools = [];
    const mintB58 = mintAddress;

    await Promise.all([
        findRaydiumV4Pools(mintB58, pools),
        findPumpFunCurve(mintAddress, pools),
        findPoolsByTokenOwnership(mintAddress, pools)
    ]);

    logger.info(`🔍 Discovery: Found ${pools.length} pools for ${mintAddress}`);
    return pools;
}

module.exports = { findPoolsOnChain };
