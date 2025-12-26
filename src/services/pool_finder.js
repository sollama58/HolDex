const axios = require('axios');
const { PublicKey } = require('@solana/web3.js');
const { retryRPC, getSolanaConnection, getRpcUrl } = require('./solana');
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
 * DIRECT RPC HELPER
 * Bypasses web3.js Connection object issues by using raw Axios HTTP calls.
 */
async function fetchTokenAccountsRaw(mintAddress) {
    const rpcUrl = getRpcUrl();
    const payload = {
        jsonrpc: "2.0",
        id: "holdex-finder",
        method: "getTokenAccountsByMint",
        params: [
            mintAddress,
            { programId: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" },
            { encoding: "jsonParsed" }
        ]
    };

    try {
        const response = await axios.post(rpcUrl, payload, { timeout: 10000 });
        if (response.data && response.data.result) {
            return response.data.result.value || []; // Raw array of accounts
        } else if (response.data && response.data.error) {
            logger.warn(`RPC Error for ${mintAddress}: ${JSON.stringify(response.data.error)}`);
            return [];
        }
    } catch (e) {
        logger.warn(`Raw RPC Fetch Failed: ${e.message}`);
    }
    return [];
}

/**
 * Strategy 1: Direct GPA Scan (Raydium V4)
 * Reliable for standard Raydium pools.
 */
async function findRaydiumV4Pools(mintB58, results) {
    try {
        const filtersBase = [{ dataSize: 752 }, { memcmp: { offset: 400, bytes: mintB58 } }];
        const filtersQuote = [{ dataSize: 752 }, { memcmp: { offset: 432, bytes: mintB58 } }];

        // We use the standard retryRPC for this as GPA usually works fine
        const connection = getSolanaConnection(); 
        
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
        // logger.warn(`Raydium V4 Scan Error: ${err.message}`);
    }
}

/**
 * Strategy 2: Pump.fun Bonding Curve
 * Deterministic address derivation.
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

        const connection = getSolanaConnection();
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
 * Strategy 3: Token Account Trace (Robust Raw RPC)
 * Finds generic pools (PumpSwap, Raydium CPMM, Meteora, Orca) by seeing who owns the token accounts.
 */
async function findPoolsByTokenOwnership(mintAddress, results) {
    try {
        // Use RAW RPC Fetch to avoid web3.js "missing function" errors
        const tokenAccounts = await fetchTokenAccountsRaw(mintAddress);
        
        if (!tokenAccounts || tokenAccounts.length === 0) return;

        const connection = getSolanaConnection();

        for (const accountObj of tokenAccounts) {
            const pubkey = accountObj.pubkey;
            const accountData = accountObj.account.data.parsed.info;
            const ownerStr = accountData.owner;
            const tokenAccountAddr = pubkey;

            try {
                // We need to check who the OWNER is (the Program ID)
                // accountData.owner is the Address of the Pool/Vault
                const ownerPubkey = new PublicKey(ownerStr);
                
                const ownerInfo = await retryRPC(() => connection.getAccountInfo(ownerPubkey));
                if (!ownerInfo) continue;
                
                const programOwner = ownerInfo.owner.toBase58();

                if (programOwner === PROG_ID_PUMPSWAP.toBase58()) {
                    await parseGenericAnchorPool(connection, ownerPubkey, 'pumpswap', 'pumpswap', mintAddress, tokenAccountAddr, results);
                }
                else if (programOwner === PROG_ID_RAYDIUM_CPMM.toBase58()) {
                    await parseGenericAnchorPool(connection, ownerPubkey, 'raydium', 'cpmm', mintAddress, tokenAccountAddr, results);
                }
                else if (programOwner === PROG_ID_METEORA_DLMM.toBase58()) {
                     await parseGenericAnchorPool(connection, ownerPubkey, 'meteora', 'dlmm', mintAddress, tokenAccountAddr, results);
                }
                else if (programOwner === PROG_ID_ORCA_WHIRLPOOL.toBase58()) {
                     await parseGenericAnchorPool(connection, ownerPubkey, 'orca', 'whirlpool', mintAddress, tokenAccountAddr, results);
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
    // Avoid Duplicates
    if (results.find(r => r.pairAddress === poolAddress.toBase58())) return;

    try {
        const info = await retryRPC(() => connection.getAccountInfo(poolAddress));
        if (!info) return;

        const data = info.data;
        const myMintBuffer = new PublicKey(myMint).toBuffer();
        let matchedOffset = -1;
        
        // Scan for my mint in the pool state
        // Most Anchor pools store the mints in the first 200 bytes
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
            // Raydium CPMM Layout
            mintA = new PublicKey(data.subarray(168, 200)).toBase58();
            mintB = new PublicKey(data.subarray(200, 232)).toBase58();
        } else {
             // Generic Heuristic for Unknown Layouts
             // We look for OTHER public keys in the data that are valid mints
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
        // logger.warn(`Generic Parser Error for ${dexId}: ${e.message}`);
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
