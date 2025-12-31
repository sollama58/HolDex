const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

let connection = null;

const TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');
const TOKEN_2022_PROGRAM_ID = new PublicKey('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb');

/**
 * Creates a NEW Connection instance.
 */
function createConnection() {
    const rpcUrl = config.SOLANA_RPC_URL || config.RPC_URL || 'https://api.mainnet-beta.solana.com';
    
    // PRIORITY 1: Explicit Env Var
    let wsUrl = config.SOLANA_WSS_URL;

    // PRIORITY 2: Helius API Key (Most Robust)
    if (!wsUrl && config.HELIUS_API_KEY) {
        wsUrl = `wss://mainnet.helius-rpc.com/?api-key=${config.HELIUS_API_KEY}`;
    }

    // PRIORITY 3: Auto-Derive from RPC URL
    if (!wsUrl) {
        if (rpcUrl.includes('helius') || rpcUrl.includes('quicknode') || rpcUrl.includes('alchemy') || rpcUrl.includes('devnet')) {
             wsUrl = rpcUrl.replace('https://', 'wss://').replace('http://', 'ws://');
        }
    }

    // --- CRITICAL CHECK ---
    if (!wsUrl && process.env.SERVICE_TYPE === 'listener') {
        logger.warn("⚠️  WARNING: No explicit WSS URL found. Listeners might fail on Public RPC.");
    }

    const confirmTimeout = 60000;

    const conn = new Connection(rpcUrl, {
        commitment: 'confirmed',
        confirmTransactionInitialTimeout: confirmTimeout,
        wsEndpoint: wsUrl,
        disableRetryOnRateLimit: false,
    });
    
    // Logging (Masked)
    const logUrl = rpcUrl.replace(/\?api-key=[^&]+/, '?api-key=***');
    const logWs = wsUrl ? wsUrl.replace(/\?api-key=[^&]+/, '?api-key=***') : 'Standard (Auto)';
    
    // Log at INFO level so we can see it in production logs
    logger.info(`🔌 Solana Connection Init: ${logUrl}`);
    logger.info(`🔌 WSS Endpoint: ${logWs}`);

    return conn;
}

/**
 * Singleton connection provider.
 * @param {boolean} forceNew - If true, creates a fresh connection instance (useful for listener restarts)
 */
function getSolanaConnection(forceNew = false) {
    if (!connection || forceNew) {
        connection = createConnection();
    }
    return connection;
}

async function retryRPC(fn, retries = 3, delay = 1000) {
    for (let i = 0; i < retries; i++) {
        try {
            return await fn();
        } catch (e) {
            if (e.message && (e.message.includes('400') || e.message.includes('Invalid param'))) throw e;
            const isRateLimit = e.message && (e.message.includes('429') || e.message.includes('Too Many Requests'));
            if (i === retries - 1) throw e; 
            const waitTime = isRateLimit ? delay * 3 * (i + 1) : delay * (i + 1);
            await new Promise(r => setTimeout(r, waitTime));
        }
    }
}

async function fetchAccountsForProgram(conn, programId, mintAddress) {
    try {
        const filters = [
            programId.equals(TOKEN_PROGRAM_ID) ? { dataSize: 165 } : null,
            { memcmp: { offset: 0, bytes: mintAddress } }
        ].filter(Boolean);

        let accounts = await retryRPC(() => conn.getProgramAccounts(programId, {
            filters: filters,
            dataSlice: { offset: 64, length: 8 }
        }), 2, 500); 

        let activeHolders = 0;
        if (accounts) {
            for (const acc of accounts) {
                if (acc.account.data && acc.account.data.length === 8) {
                    const balance = acc.account.data.readBigUInt64LE(0);
                    if (balance > 0n) activeHolders++;
                }
            }
        }
        return activeHolders;
    } catch (e) {
        return 0;
    }
}

async function getHolderCountFromRPC(mintAddress) {
    if (!mintAddress) return 0;
    const cleanMint = mintAddress.trim();
    const conn = getSolanaConnection();
    
    let count = await fetchAccountsForProgram(conn, TOKEN_PROGRAM_ID, cleanMint);
    if (count === 0) {
        const count2022 = await fetchAccountsForProgram(conn, TOKEN_2022_PROGRAM_ID, cleanMint);
        count += count2022;
    }
    return count;
}

/**
 * Analyze top 20 holders (excluding pools) for conviction and hold time
 *
 * Conviction analysis:
 * - For each holder, look at their recent transactions for THIS token
 * - Count buys vs sells to classify as accumulator/holder/reducer/extractor
 * - Conviction score = % of accumulators + holders
 *
 * @param {string} mintAddress - Token mint
 * @param {string[]} excludeAddresses - Pool addresses to exclude
 * @returns {Object} { avgHoldHours, conviction: { score, accumulators, holders, reducers, extractors, analyzed } }
 */
async function analyzeTokenHolders(mintAddress, excludeAddresses = []) {
    const conn = getSolanaConnection();
    const TOP_HOLDERS = 20;

    try {
        const mint = new PublicKey(mintAddress);
        const largest = await retryRPC(() => conn.getTokenLargestAccounts(mint), 2, 2000);
        if (!largest || !largest.value || largest.value.length === 0) {
            return { avgHoldHours: 0, conviction: { score: 0, analyzed: 0 } };
        }

        const topAccounts = largest.value;
        const nowSec = Math.floor(Date.now() / 1000);
        const excludeSet = new Set(excludeAddresses.map(a => a ? a.toString() : ''));

        // Filter out pools and get top 20 real holders
        const realHolders = topAccounts
            .filter(acc => !excludeSet.has(acc.address.toString()))
            .slice(0, TOP_HOLDERS);

        let totalDuration = 0;
        let validSamples = 0;
        let accumulators = 0;
        let holders = 0;
        let reducers = 0;
        let extractors = 0;

        for (const acc of realHolders) {
            try {
                const pubkey = new PublicKey(acc.address);
                const signatures = await retryRPC(() => conn.getSignaturesForAddress(pubkey, { limit: 30 }), 2, 1000);

                if (signatures.length > 0) {
                    // Hold time: oldest transaction
                    const oldestTx = signatures[signatures.length - 1];
                    const txTime = oldestTx.blockTime || nowSec;
                    totalDuration += (nowSec - txTime);

                    // Conviction: analyze buy/sell pattern for this token
                    // Count transactions involving this token
                    let buyCount = 0;
                    let sellCount = 0;

                    // Get parsed transactions to analyze token transfers
                    for (const sig of signatures.slice(0, 10)) {  // Last 10 transactions
                        try {
                            const tx = await retryRPC(() => conn.getParsedTransaction(sig.signature, {
                                maxSupportedTransactionVersion: 0
                            }), 1, 500);

                            if (!tx || !tx.meta) continue;

                            // Check pre/post token balances for this mint
                            const preBalances = tx.meta.preTokenBalances || [];
                            const postBalances = tx.meta.postTokenBalances || [];

                            const preBal = preBalances.find(b => b.mint === mintAddress && b.owner === acc.address.toString());
                            const postBal = postBalances.find(b => b.mint === mintAddress && b.owner === acc.address.toString());

                            const preAmount = preBal ? parseFloat(preBal.uiTokenAmount?.uiAmount || 0) : 0;
                            const postAmount = postBal ? parseFloat(postBal.uiTokenAmount?.uiAmount || 0) : 0;

                            if (postAmount > preAmount) buyCount++;
                            else if (preAmount > postAmount) sellCount++;
                        } catch (txErr) {
                            // Skip failed transaction fetches
                        }
                    }

                    // Classify holder based on buy/sell ratio
                    const total = buyCount + sellCount;
                    if (total === 0) {
                        holders++;  // No activity = diamond hands
                    } else {
                        const buyRatio = buyCount / total;
                        if (buyRatio >= 0.7) accumulators++;      // 70%+ buys
                        else if (buyRatio >= 0.4) holders++;       // 40-70% buys
                        else if (buyRatio >= 0.2) reducers++;      // 20-40% buys
                        else extractors++;                         // <20% buys
                    }

                    validSamples++;
                } else {
                    // No transactions = probably diamond hands
                    totalDuration += (24 * 3600);
                    holders++;
                    validSamples++;
                }
            } catch (err) {
                // Skip failed holders
            }

            // Small delay to avoid rate limits
            await new Promise(r => setTimeout(r, 100));
        }

        if (validSamples === 0) {
            return { avgHoldHours: 0, conviction: { score: 0, analyzed: 0 } };
        }

        // Conviction score = % of accumulators + holders (diamond hands)
        const convictionScore = Math.round(((accumulators + holders) / validSamples) * 100);

        return {
            avgHoldHours: (totalDuration / validSamples) / 3600,
            conviction: {
                score: convictionScore,
                accumulators,
                holders,
                reducers,
                extractors,
                analyzed: validSamples
            }
        };
    } catch (e) {
        logger.error(`[analyzeTokenHolders] ${mintAddress}: ${e.message}`);
        return { avgHoldHours: 0, conviction: { score: 0, analyzed: 0 } };
    }
}

module.exports = { 
    getSolanaConnection, 
    analyzeTokenHolders,
    retryRPC,
    getHolderCountFromRPC
};
