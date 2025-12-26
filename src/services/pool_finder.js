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

const connection = new Connection(config.SOLANA_RPC_URL, 'confirmed');

async function findPoolsOnChain(mintAddress) {
    const pools = [];
    const mint = new PublicKey(mintAddress);
    const mintBase58 = mint.toBase58();

    logger.info(`🔍 Deep Scan: Searching pools for ${mintBase58}...`);

    try {
        const promises = [];

        // --- 1. Check PUMP.FUN Bonding Curve (Pre-Bond & Bonded) ---
        // We check this explicitly first.
        try {
            const [bondingCurve] = PublicKey.findProgramAddressSync(
                [Buffer.from("bonding-curve"), mint.toBuffer()],
                PUMP_PROGRAM_ID
            );
            
            // Just check if it exists (Snapshotter handles liquidity/bonded status)
            const info = await connection.getAccountInfo(bondingCurve);
            if (info) {
                pools.push({
                    pairAddress: bondingCurve.toString(),
                    dexId: 'pump',
                    liquidity: { usd: 0 }, // Updated by snapshotter
                    volume: { h24: 0 },
                    baseToken: { address: mintBase58 },
                    quoteToken: { address: 'So11111111111111111111111111111111111111112' }
                });
                logger.info(`✅ Found Pump.fun Bonding Curve`);
            }
        } catch (e) {}

        // --- 2. RAYDIUM AMM v4 ---
        // Base Mint offset: 400, Quote Mint offset: 432
        promises.push(connection.getProgramAccounts(RAYDIUM_PROGRAM_ID, {
            filters: [{ dataSize: 752 }, { memcmp: { offset: 400, bytes: mintBase58 } }]
        }).then(res => res.map(p => ({
            pairAddress: p.pubkey.toString(),
            dexId: 'raydium',
            liquidity: { usd: 0 },
            volume: { h24: 0 },
            baseToken: { address: mintBase58 },
            quoteToken: { address: 'So11111111111111111111111111111111111111112' }
        }))));

        promises.push(connection.getProgramAccounts(RAYDIUM_PROGRAM_ID, {
            filters: [{ dataSize: 752 }, { memcmp: { offset: 432, bytes: mintBase58 } }]
        }).then(res => res.map(p => ({
            pairAddress: p.pubkey.toString(),
            dexId: 'raydium',
            liquidity: { usd: 0 }, 
            volume: { h24: 0 },
            baseToken: { address: 'Unknown' }, // Quote scan
            quoteToken: { address: mintBase58 }
        }))));

        // --- 3. ORCA WHIRLPOOLS ---
        // Token Mint A: 33, Token Mint B: 65. Size: 653
        promises.push(connection.getProgramAccounts(ORCA_PROGRAM_ID, {
            filters: [{ dataSize: 653 }, { memcmp: { offset: 33, bytes: mintBase58 } }]
        }).then(res => res.map(p => ({ pairAddress: p.pubkey.toString(), dexId: 'orca' }))));

        promises.push(connection.getProgramAccounts(ORCA_PROGRAM_ID, {
            filters: [{ dataSize: 653 }, { memcmp: { offset: 65, bytes: mintBase58 } }]
        }).then(res => res.map(p => ({ pairAddress: p.pubkey.toString(), dexId: 'orca' }))));

        // --- 4. METEORA DLMM ---
        // Offsets: TokenX=40, TokenY=72
        promises.push(connection.getProgramAccounts(METEORA_DLMM_PROGRAM, {
            filters: [{ memcmp: { offset: 40, bytes: mintBase58 } }]
        }).then(res => res.map(p => ({ pairAddress: p.pubkey.toString(), dexId: 'meteora' }))));

        promises.push(connection.getProgramAccounts(METEORA_DLMM_PROGRAM, {
            filters: [{ memcmp: { offset: 72, bytes: mintBase58 } }]
        }).then(res => res.map(p => ({ pairAddress: p.pubkey.toString(), dexId: 'meteora' }))));

        // --- Execute All ---
        const results = await Promise.allSettled(promises);
        
        results.forEach(res => {
            if (res.status === 'fulfilled' && Array.isArray(res.value)) {
                pools.push(...res.value);
            }
        });

        // --- Deduplicate ---
        const uniquePools = [];
        const seen = new Set();
        // Add Pump curve explicitly first if it exists (it's in `pools` array already)
        
        for (const p of pools) {
            if (!seen.has(p.pairAddress)) {
                seen.add(p.pairAddress);
                p.liquidity = p.liquidity || { usd: 0 };
                p.volume = p.volume || { h24: 0 };
                p.priceUsd = 0;
                uniquePools.push(p);
            }
        }

        logger.info(`✅ Found ${uniquePools.length} total pools on-chain for ${mintBase58}`);
        return uniquePools;

    } catch (e) {
        logger.error(`On-Chain Pool Find Error: ${e.message}`);
        return [];
    }
}

module.exports = { findPoolsOnChain };
