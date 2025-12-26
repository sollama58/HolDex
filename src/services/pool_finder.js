const axios = require('axios');
const { PublicKey } = require('@solana/web3.js');
const { retryRPC, getSolanaConnection } = require('./solana');
const logger = require('./logger');

// --- PROGRAM IDS ---
const PROG_ID_RAYDIUM_V4 = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const PROG_ID_PUMPFUN = new PublicKey('6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P');

// --- OFFSETS (For Raydium Fallback) ---
const RAY_SLICE = { offset: 320, length: 144 };
const SLICE_OFF_BASE_VAULT = 0;   
const SLICE_OFF_QUOTE_VAULT = 32; 
const SLICE_OFF_BASE_MINT = 80;   
const SLICE_OFF_QUOTE_MINT = 112; 

/**
 * STRATEGY 1: GeckoTerminal API (Primary)
 * Replaces DexScreener. Uses GeckoTerminal's public API to find pools.
 * Rate Limit: Approx 30 req/min for free tier.
 */
async function findPoolsViaGeckoTerminal(mintAddress) {
    try {
        // GeckoTerminal Endpoint for Solana Pools by Token
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mintAddress}/pools?page=1`;
        const response = await axios.get(url, { timeout: 10000 });
        
        if (!response.data || !response.data.data) return [];

        return response.data.data.map(item => {
            const attr = item.attributes;
            const rel = item.relationships;
            
            return {
                pairAddress: attr.address,
                dexId: rel?.dex?.data?.id || 'unknown',
                type: 'standard', // GeckoTerminal doesn't explicitly label bonding curves easily, assume standard
                baseToken: { address: rel?.base_token?.data?.id?.replace('solana_', '') || mintAddress },
                quoteToken: { address: rel?.quote_token?.data?.id?.replace('solana_', '') || 'So11111111111111111111111111111111111111112' },
                liquidity: { usd: parseFloat(attr.reserve_in_usd || 0) },
                volume: { h24: parseFloat(attr.volume_usd?.h24 || 0) },
                priceUsd: parseFloat(attr.base_token_price_usd || 0),
                // GeckoTerminal doesn't provide vault addresses in this endpoint
                reserve_a: null, 
                reserve_b: null 
            };
        });

    } catch (e) {
        // 404 means no pools found, which is common for new tokens
        if (e.response && e.response.status !== 404) {
             logger.warn(`GeckoTerminal Lookup Failed: ${e.message}`);
        }
        return [];
    }
}

/**
 * STRATEGY 2: Pump.fun Bonding Curve (On-Chain Check)
 * Handles brand new launches not yet on GeckoTerminal.
 */
async function findPumpFunCurve(mintAddress, results) {
    // Avoid duplicate if already found
    if (results.some(r => r.dexId === 'pumpfun')) return;

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
        // Simple getAccountInfo is very cheap and reliable
        const info = await retryRPC(() => connection.getAccountInfo(bondingCurve));
        
        if (info) {
             results.push({
                pairAddress: bondingCurve.toBase58(),
                dexId: 'pumpfun',
                type: 'bonding_curve',
                baseToken: { address: mintAddress },
                quoteToken: { address: 'So11111111111111111111111111111111111111112' }, // WSOL
                reserve_a: bondingCurveVault.toBase58(), 
                reserve_b: bondingCurve.toBase58(),      
                liquidity: { usd: 0 }, 
                volume: { h24: 0 },
                priceUsd: 0
            });
        }
    } catch (e) {
        // Curve likely doesn't exist, ignore
    }
}

/**
 * STRATEGY 3: Raydium V4 GPA (Fallback & Enricher)
 * Only runs if GeckoTerminal fails to find anything OR to enrich missing reserves.
 */
async function findRaydiumV4Pools(mintAddress, results) {
    try {
        const mintB58 = new PublicKey(mintAddress).toBuffer();
        const filtersBase = [{ dataSize: 752 }, { memcmp: { offset: 400, bytes: mintB58 } }];
        const filtersQuote = [{ dataSize: 752 }, { memcmp: { offset: 432, bytes: mintB58 } }];

        const connection = getSolanaConnection();
        
        const [baseAccts, quoteAccts] = await Promise.all([
            retryRPC(() => connection.getProgramAccounts(PROG_ID_RAYDIUM_V4, { filters: filtersBase, dataSlice: RAY_SLICE })).catch(() => []),
            retryRPC(() => connection.getProgramAccounts(PROG_ID_RAYDIUM_V4, { filters: filtersQuote, dataSlice: RAY_SLICE })).catch(() => [])
        ]);

        const process = (acc) => {
            const pairAddress = acc.pubkey.toBase58();
            const d = acc.account.data;
            const reserveA = new PublicKey(d.subarray(SLICE_OFF_BASE_VAULT, SLICE_OFF_BASE_VAULT + 32)).toBase58();
            const reserveB = new PublicKey(d.subarray(SLICE_OFF_QUOTE_VAULT, SLICE_OFF_QUOTE_VAULT + 32)).toBase58();

            // CHECK: Do we already have this pool from GeckoTerminal?
            const existing = results.find(r => r.pairAddress === pairAddress);
            if (existing) {
                // ENRICH: Add the vault addresses if missing so Snapshotter can work
                if (!existing.reserve_a) existing.reserve_a = reserveA;
                if (!existing.reserve_b) existing.reserve_b = reserveB;
                return;
            }

            const bMint = new PublicKey(d.subarray(SLICE_OFF_BASE_MINT, SLICE_OFF_BASE_MINT + 32)).toBase58();
            const qMint = new PublicKey(d.subarray(SLICE_OFF_QUOTE_MINT, SLICE_OFF_QUOTE_MINT + 32)).toBase58();
            
            if (!bMint || !qMint) return;

            results.push({
                pairAddress: pairAddress,
                dexId: 'raydium',
                type: 'v4',
                baseToken: { address: bMint },
                quoteToken: { address: qMint },
                reserve_a: reserveA,
                reserve_b: reserveB,
                liquidity: { usd: 0 },
                volume: { h24: 0 },
                priceUsd: 0
            });
        };

        if(baseAccts) baseAccts.forEach(process);
        if(quoteAccts) quoteAccts.forEach(process);
        
    } catch (err) {
        logger.warn(`Raydium Fallback Scan Error: ${err.message}`);
    }
}

/**
 * MAIN DISCOVERY FUNCTION
 */
async function findPoolsOnChain(mintAddress) {
    const pools = [];
    
    // 1. Try GeckoTerminal (Best Public Data)
    const gtPools = await findPoolsViaGeckoTerminal(mintAddress);
    if (gtPools.length > 0) {
        pools.push(...gtPools);
    }

    // 2. Check Pump.fun On-Chain (Critical for new launches)
    await findPumpFunCurve(mintAddress, pools);

    // 3. ALWAYS Run Raydium Scan
    // This ensures we get the "Reserve/Vault" addresses that GeckoTerminal usually misses.
    // Without Reserves, the Snapshotter cannot index candles.
    await findRaydiumV4Pools(mintAddress, pools);

    if (pools.length > 0) {
        logger.info(`🔍 Discovery: Found ${pools.length} pools for ${mintAddress}`);
    } else {
        logger.warn(`🔍 Discovery: No pools found for ${mintAddress}`);
    }
    
    return pools;
}

module.exports = { findPoolsOnChain };
