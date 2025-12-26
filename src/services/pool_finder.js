const { PublicKey } = require('@solana/web3.js');
const axios = require('axios'); // Requires axios
const { getSolanaConnection, retryRPC } = require('./solana'); 
const logger = require('./logger');

// --- CONSTANTS ---
const RAYDIUM_V4_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const PUMPFUN_PROGRAM_ID = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');

const RAY_V4_OFFSET_BASE_MINT = 400;
const RAY_V4_OFFSET_QUOTE_MINT = 432;
const RAY_V4_OFFSET_BASE_VAULT = 320;
const RAY_V4_OFFSET_QUOTE_VAULT = 352;

async function findPoolsOnChain(mintAddress) {
    const pools = [];
    const mint = new PublicKey(mintAddress);
    const mintBase58 = mint.toBase58();

    logger.info(`🔍 Deep Scan for ${mintBase58}...`);

    try {
        // STRATEGY A: RPC SCAN (Program Accounts)
        // Note: This often fails on public RPCs due to resource limits.
        const rpcPromises = [];

        // 1. PUMPFUN (PDA - Deterministic, usually works)
        rpcPromises.push((async () => {
            try {
                const [bondingCurve] = PublicKey.findProgramAddressSync(
                    [Buffer.from("bonding-curve"), mint.toBuffer()],
                    PUMPFUN_PROGRAM_ID
                );
                const info = await retryRPC((conn) => conn.getAccountInfo(bondingCurve));
                if (info) {
                    pools.push({
                        pairAddress: bondingCurve.toBase58(),
                        dexId: 'pumpfun',
                        baseToken: { address: mintBase58 },
                        quoteToken: { address: 'So11111111111111111111111111111111111111112' },
                        reserve_a: bondingCurve.toBase58(), 
                        reserve_b: bondingCurve.toBase58(),
                        liquidity: { usd: 0 },
                        labels: ['bonding-curve']
                    });
                }
            } catch (err) {}
        })());

        // 2. RAYDIUM (Program Accounts - often fails)
        const onChainScan = async () => {
            const filtersBase = [{ dataSize: 752 }, { memcmp: { offset: RAY_V4_OFFSET_BASE_MINT, bytes: mintBase58 } }];
            const filtersQuote = [{ dataSize: 752 }, { memcmp: { offset: RAY_V4_OFFSET_QUOTE_MINT, bytes: mintBase58 } }];

            const [baseAccounts, quoteAccounts] = await Promise.all([
                retryRPC((c) => c.getProgramAccounts(RAYDIUM_V4_PROGRAM_ID, { filters: filtersBase })),
                retryRPC((c) => c.getProgramAccounts(RAYDIUM_V4_PROGRAM_ID, { filters: filtersQuote }))
            ]);

            const processRay = (acc, isBase) => {
                const data = acc.account.data;
                const pairAddress = acc.pubkey.toBase58();
                const baseMint = new PublicKey(data.subarray(RAY_V4_OFFSET_BASE_MINT, RAY_V4_OFFSET_BASE_MINT + 32)).toBase58();
                const quoteMint = new PublicKey(data.subarray(RAY_V4_OFFSET_QUOTE_MINT, RAY_V4_OFFSET_QUOTE_MINT + 32)).toBase58();
                const baseVault = new PublicKey(data.subarray(RAY_V4_OFFSET_BASE_VAULT, RAY_V4_OFFSET_BASE_VAULT + 32)).toBase58();
                const quoteVault = new PublicKey(data.subarray(RAY_V4_OFFSET_QUOTE_VAULT, RAY_V4_OFFSET_QUOTE_VAULT + 32)).toBase58();

                pools.push({
                    pairAddress,
                    dexId: 'raydium',
                    baseToken: { address: baseMint },
                    quoteToken: { address: quoteMint },
                    reserve_a: baseVault,
                    reserve_b: quoteVault,
                    liquidity: { usd: 0 }
                });
            };

            baseAccounts.forEach(a => processRay(a, true));
            quoteAccounts.forEach(a => processRay(a, false));
        };
        
        rpcPromises.push(onChainScan().catch(e => logger.warn(`On-Chain Raydium Scan skipped: ${e.message}`)));
        
        await Promise.all(rpcPromises);

        // STRATEGY B: API FALLBACK
        // If RPC found nothing, or just pumpfun, try external API to find the main pool.
        if (pools.length === 0 || (pools.length === 1 && pools[0].dexId === 'pumpfun')) {
             try {
                 logger.info("🌍 RPC Scan yielded low results. Trying external API fallback...");
                 const res = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${mintBase58}`);
                 if (res.data && res.data.pairs) {
                     res.data.pairs.forEach(pair => {
                         if (pair.chainId === 'solana' && pair.dexId === 'raydium') {
                             pools.push({
                                 pairAddress: pair.pairAddress,
                                 dexId: 'raydium',
                                 baseToken: pair.baseToken,
                                 quoteToken: pair.quoteToken,
                                 liquidity: pair.liquidity,
                                 volume: pair.volume,
                                 priceUsd: Number(pair.priceUsd), // Pre-fill price in case on-chain fails
                                 // We don't have vaults here, so snapshotter might skip on-chain calc
                                 // but at least the pool exists in DB now.
                             });
                         }
                     });
                 }
             } catch (e) {
                 logger.warn(`API Fallback failed: ${e.message}`);
             }
        }

        // Deduplicate
        const uniquePools = [];
        const seen = new Set();
        for (const p of pools) {
            if (!seen.has(p.pairAddress)) {
                seen.add(p.pairAddress);
                uniquePools.push(p);
            }
        }
        
        return uniquePools;

    } catch (e) {
        logger.error(`Pool Discovery Error: ${e.message}`);
        return []; 
    }
}

module.exports = { findPoolsOnChain };
