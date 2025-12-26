const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

// --- PROGRAM IDs ---
const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const ORCA_PROGRAM_ID = new PublicKey('whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc');
const METEORA_DLMM_PROGRAM = new PublicKey('LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo');
const METEORA_AMM_PROGRAM = new PublicKey('Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB');
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');
const FLUXBEAM_PROGRAM_ID = new PublicKey('FLUXubRmkEi2q6K3Y9kBPg9248gga8U928ay3ViUhVan');

// Common Quote Mints to help identify the "other side" of the trade
const QUOTE_MINTS = {
    'So11111111111111111111111111111111111111112': 'SOL',
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 'USDC',
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': 'USDT'
};

const connection = new Connection(config.SOLANA_RPC_URL, 'confirmed');

async function findPoolsOnChain(mintAddress) {
    const pools = [];
    const mint = new PublicKey(mintAddress);
    const mintBase58 = mint.toBase58();

    logger.info(`🔍 Searching on-chain pools for ${mintBase58}...`);

    try {
        const promises = [];

        // 1. RAYDIUM AMM v4
        // Base Mint offset: 400, Quote Mint offset: 432
        promises.push(connection.getProgramAccounts(RAYDIUM_PROGRAM_ID, {
            filters: [{ dataSize: 752 }, { memcmp: { offset: 400, bytes: mintBase58 } }]
        }).then(res => res.map(p => ({
            pairAddress: p.pubkey.toString(),
            dexId: 'raydium',
            liquidity: { usd: 0 },
            volume: { h24: 0 },
            baseToken: { address: mintBase58 },
            quoteToken: { address: 'UNKNOWN' } // We fix this in snapshotter/discovery
        }))));

        promises.push(connection.getProgramAccounts(RAYDIUM_PROGRAM_ID, {
            filters: [{ dataSize: 752 }, { memcmp: { offset: 432, bytes: mintBase58 } }]
        }).then(res => res.map(p => ({
            pairAddress: p.pubkey.toString(),
            dexId: 'raydium',
            liquidity: { usd: 0 }, 
            volume: { h24: 0 },
            baseToken: { address: 'UNKNOWN' }, // It's the quote in this case
            quoteToken: { address: mintBase58 }
        }))));

        // 2. ORCA WHIRLPOOLS
        // Token Mint A: 33, Token Mint B: 65. Size: 653
        promises.push(connection.getProgramAccounts(ORCA_PROGRAM_ID, {
            filters: [{ dataSize: 653 }, { memcmp: { offset: 33, bytes: mintBase58 } }]
        }).then(res => res.map(p => ({ pairAddress: p.pubkey.toString(), dexId: 'orca' }))));

        promises.push(connection.getProgramAccounts(ORCA_PROGRAM_ID, {
            filters: [{ dataSize: 653 }, { memcmp: { offset: 65, bytes: mintBase58 } }]
        }).then(res => res.map(p => ({ pairAddress: p.pubkey.toString(), dexId: 'orca' }))));

        // 3. METEORA DLMM
        // Token X: 40, Token Y: 72 (Offsets are approx, need verification, but memcmp works if consistent)
        // DLMM Bin Array Bitmap Extension is huge, but the Pair state itself is smaller.
        // Let's filter by Mint X and Mint Y.
        // Known offsets for DLMM ActiveId: TokenX=40, TokenY=72
        promises.push(connection.getProgramAccounts(METEORA_DLMM_PROGRAM, {
            filters: [{ memcmp: { offset: 40, bytes: mintBase58 } }]
        }).then(res => res.map(p => ({ pairAddress: p.pubkey.toString(), dexId: 'meteora' }))));

        promises.push(connection.getProgramAccounts(METEORA_DLMM_PROGRAM, {
            filters: [{ memcmp: { offset: 72, bytes: mintBase58 } }]
        }).then(res => res.map(p => ({ pairAddress: p.pubkey.toString(), dexId: 'meteora' }))));

        // 4. PUMP.FUN
        // Bonding Curve is usually a PDA seeded with "bonding-curve" + Mint.
        // We can just derive it directly instead of searching all accounts.
        try {
            const [bondingCurve] = PublicKey.findProgramAddressSync(
                [Buffer.from("bonding-curve"), mint.toBuffer()],
                PUMP_PROGRAM_ID
            );
            // Check if it exists
            const info = await connection.getAccountInfo(bondingCurve);
            if (info) {
                pools.push({
                    pairAddress: bondingCurve.toString(),
                    dexId: 'pump',
                    liquidity: { usd: 0 },
                    volume: { h24: 0 },
                    baseToken: { address: mintBase58 },
                    quoteToken: { address: 'So11111111111111111111111111111111111111112' }
                });
            }
        } catch (e) {}

        // 5. FLUXBEAM
        // Similar to Raydium (Token A / Token B offsets)
        // Swap Account Layout: Token A Mint (32), Token B Mint (64) - Approx check needed or use dataSize filter
        // Fluxbeam uses Token2022 often.
        // We will try a generic memcmp scan at start of account data for the mint
        // This is "expensive" if not filtered by size, but Fluxbeam accounts are distinct.
        // Standard Fluxbeam Pool size ~ 1000 bytes? Let's skip precise size for now and trust the Program ID filter scope.
        /* promises.push(connection.getProgramAccounts(FLUXBEAM_PROGRAM_ID, {
            filters: [ { memcmp: { offset: 32, bytes: mintBase58 } } ] // Guessing offset A
        }).then(res => res.map(p => ({ pairAddress: p.pubkey.toString(), dexId: 'fluxbeam' }))));
        */

        // Execute All Searches
        const results = await Promise.allSettled(promises);
        
        results.forEach(res => {
            if (res.status === 'fulfilled' && Array.isArray(res.value)) {
                pools.push(...res.value);
            }
        });

        // Deduplicate
        const uniquePools = [];
        const seen = new Set();
        for (const p of pools) {
            if (!seen.has(p.pairAddress)) {
                seen.add(p.pairAddress);
                // Ensure defaults
                p.liquidity = p.liquidity || { usd: 0 };
                p.volume = p.volume || { h24: 0 };
                p.priceUsd = 0;
                uniquePools.push(p);
            }
        }

        logger.info(`✅ Found ${uniquePools.length} pools on-chain for ${mintBase58}`);
        return uniquePools;

    } catch (e) {
        logger.error(`On-Chain Pool Find Error: ${e.message}`);
        return [];
    }
}

module.exports = { findPoolsOnChain };
