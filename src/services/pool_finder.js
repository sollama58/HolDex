const axios = require('axios');
const { PublicKey } = require('@solana/web3.js');
// Import retryRPC and connection getter from centralized service
const { getSolanaConnection, retryRPC } = require('./solana'); 
const logger = require('./logger');

// --- PROGRAM IDS ---
const PROG_ID_RAYDIUM_V4 = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8';
const PROG_ID_METEORA_AMM = 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB';
const PROG_ID_RAYDIUM_CPMM = 'CPMMoo8L3F4NbTneafuJ3B7rbzjaJ4Kfjbzx5391tqs';
const PROG_ID_PUMPFUN = '6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P';

const LAYOUTS = {
    [PROG_ID_RAYDIUM_V4]: { name: 'Raydium V4', offA: 320, offB: 352 },
    [PROG_ID_METEORA_AMM]: { name: 'Meteora', offA: 72, offB: 104 }, 
    [PROG_ID_RAYDIUM_CPMM]: { name: 'Raydium CPMM', offA: 168, offB: 200 } 
};

async function findPoolsViaGeckoTerminal(mintAddress) {
    try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mintAddress}/pools?page=1`;
        const response = await axios.get(url, { timeout: 10000 });
        
        if (!response.data || !response.data.data) return [];

        return response.data.data.map(item => {
            const attr = item.attributes;
            const rel = item.relationships;
            
            return {
                pairAddress: attr.address,
                dexId: rel?.dex?.data?.id || 'unknown',
                type: 'standard', 
                baseToken: { address: rel?.base_token?.data?.id?.replace('solana_', '') || mintAddress },
                quoteToken: { address: rel?.quote_token?.data?.id?.replace('solana_', '') || 'So11111111111111111111111111111111111111112' },
                liquidity: { usd: parseFloat(attr.reserve_in_usd || 0) },
                volume: { h24: parseFloat(attr.volume_usd?.h24 || 0) },
                priceUsd: parseFloat(attr.base_token_price_usd || 0),
                reserve_a: null, 
                reserve_b: null 
            };
        });

    } catch (e) {
        if (e.response && e.response.status !== 404) {
             logger.warn(`GeckoTerminal Lookup Failed: ${e.message}`);
        }
        return [];
    }
}

// Exported for use in Snapshotter (Self-Healing)
async function enrichPoolsWithReserves(pools) {
    if (pools.length === 0) return;
    const targets = pools.filter(p => p.dexId !== 'pumpfun' && !p.reserve_a);
    if (targets.length === 0) return;

    // Use the centralized connection that has the Helius RPC
    // FIX: Ensure this is called as a function
    const connection = getSolanaConnection();
    
    // Process in batches
    for (let i = 0; i < targets.length; i += 50) {
        const batch = targets.slice(i, i + 50);
        const pubkeys = batch.map(p => new PublicKey(p.pairAddress || p.address)); 

        try {
            // Now using the imported retryRPC correctly
            const accounts = await retryRPC(() => connection.getMultipleAccountsInfo(pubkeys));
            
            accounts.forEach((acc, idx) => {
                if (!acc) return;
                const pool = batch[idx];
                const owner = acc.owner.toBase58();
                const layout = LAYOUTS[owner];

                if (layout) {
                    try {
                        const reserveA = new PublicKey(acc.data.subarray(layout.offA, layout.offA + 32));
                        const reserveB = new PublicKey(acc.data.subarray(layout.offB, layout.offB + 32));
                        pool.reserve_a = reserveA.toBase58();
                        pool.reserve_b = reserveB.toBase58();
                    } catch (parseErr) {}
                } else if (pool.dexId === 'whirlpool' || owner === 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc') {
                    try {
                        const reserveA = new PublicKey(acc.data.subarray(101, 133));
                        const reserveB = new PublicKey(acc.data.subarray(133, 165));
                        pool.reserve_a = reserveA.toBase58();
                        pool.reserve_b = reserveB.toBase58();
                    } catch(e) {}
                }
            });
        } catch (err) {
            logger.warn(`Enrichment Error: ${err.message}`);
        }
    }
}

async function findPumpFunCurve(mintAddress, results) {
    if (results.some(r => r.dexId === 'pumpfun')) return;
    try {
        const mint = new PublicKey(mintAddress);
        const pId = new PublicKey(PROG_ID_PUMPFUN);
        const [bondingCurve] = PublicKey.findProgramAddressSync([Buffer.from("bonding-curve"), mint.toBuffer()], pId);
        const [bondingCurveVault] = PublicKey.findProgramAddressSync([Buffer.from("bonding-curve"), mint.toBuffer(), Buffer.from("token-account")], pId);

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
    } catch (e) {}
}

async function findPoolsOnChain(mintAddress) {
    const pools = [];
    const gtPools = await findPoolsViaGeckoTerminal(mintAddress);
    if (gtPools.length > 0) pools.push(...gtPools);
    await findPumpFunCurve(mintAddress, pools);
    await enrichPoolsWithReserves(pools);
    
    if (pools.length > 0) logger.info(`🔍 Discovery: Found ${pools.length} pools for ${mintAddress}`);
    return pools;
}

module.exports = { findPoolsOnChain, enrichPoolsWithReserves };
