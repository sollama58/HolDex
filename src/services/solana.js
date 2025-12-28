const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

let connection;

const TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');
const TOKEN_2022_PROGRAM_ID = new PublicKey('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb');

/**
 * Singleton connection provider for Solana RPC.
 * Automatically handles Helius WSS upgrades and connection configuration.
 */
function getSolanaConnection() {
    if (!connection) {
        const rpcUrl = config.SOLANA_RPC_URL || config.RPC_URL || 'https://api.mainnet-beta.solana.com';
        
        // Rate limit mitigation for public endpoints
        // Longer timeout for Mainnet to avoid premature timeouts during congestion
        const confirmTimeout = rpcUrl.includes('mainnet-beta') ? 120000 : 60000;

        // Fix: Explicitly derive WSS URL for Helius/Custom RPCs to ensure onLogs works reliably.
        // Standard 'https' connections often default to a public WSS endpoint which drops logs.
        let wsUrl = undefined;
        if (rpcUrl.includes('helius')) {
             wsUrl = rpcUrl.replace('https://', 'wss://').replace('http://', 'ws://');
        }

        connection = new Connection(rpcUrl, {
            commitment: 'confirmed',
            confirmTransactionInitialTimeout: confirmTimeout,
            wsEndpoint: wsUrl,
            disableRetryOnRateLimit: false,
        });
        
        logger.info(`🔌 Solana Connection Init: ${rpcUrl.includes('helius') ? 'High-Performance Mode' : 'Standard Mode'}`);
    }
    return connection;
}

/**
 * Retries an RPC call with exponential backoff.
 * Handles Rate Limits (429) specifically.
 */
async function retryRPC(fn, retries = 3, delay = 1000) {
    for (let i = 0; i < retries; i++) {
        try {
            return await fn();
        } catch (e) {
            // Permanent errors - do not retry
            if (e.message && (e.message.includes('400') || e.message.includes('Invalid param'))) throw e;
            
            // RPC Specific Load errors
            const isRateLimit = e.message && (e.message.includes('429') || e.message.includes('Too Many Requests') || e.message.includes('Busy'));
            
            if (i === retries - 1) throw e; 
            
            // Exponential backoff strategy
            const waitTime = isRateLimit ? delay * 3 * (i + 1) : delay * (i + 1);
            await new Promise(r => setTimeout(r, waitTime));
        }
    }
}

/**
 * Low-level helper to fetch accounts for a specific token program (Token or Token2022).
 */
async function fetchAccountsForProgram(conn, programId, mintAddress) {
    try {
        const filters = [];
        // Data size filter for standard SPL tokens
        if (programId.equals(TOKEN_PROGRAM_ID)) filters.push({ dataSize: 165 });
        
        // Memcmp filter to match the Mint Address within the Account data
        filters.push({ memcmp: { offset: 0, bytes: mintAddress } });

        // Use getProgramAccounts with a slice to minimize data transfer
        let accounts = await retryRPC(() => conn.getProgramAccounts(programId, {
            filters: filters,
            dataSlice: { offset: 64, length: 8 } // We only need the Amount (u64)
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
        accounts = null; // GC help
        return activeHolders;
    } catch (e) {
        if (e.message.includes('Too many accounts') || e.message.includes('Size limit')) {
            return 0; 
        }
        return 0;
    }
}

/**
 * Gets the total holder count for a mint, checking both Standard and Token2022 programs.
 */
async function getHolderCountFromRPC(mintAddress) {
    if (!mintAddress) return 0;
    const cleanMint = mintAddress.trim();
    const conn = getSolanaConnection();
    
    // Check standard SPL Token Program
    let count = await fetchAccountsForProgram(conn, TOKEN_PROGRAM_ID, cleanMint);
    
    // If none found, or just to be safe, check Token-2022 Program
    if (count === 0) {
        const count2022 = await fetchAccountsForProgram(conn, TOKEN_2022_PROGRAM_ID, cleanMint);
        count += count2022;
    }
    return count;
}

/**
 * Optimized Deep Analysis of Token Holders.
 * Strategy: Check Top 20 holders. Only look back 50 transactions per holder.
 */
async function analyzeTokenHolders(mintAddress, excludeAddresses = []) {
    const conn = getSolanaConnection();
    try {
        const mint = new PublicKey(mintAddress);
        
        // 1. Get Top 20 Token Accounts
        const largest = await retryRPC(() => conn.getTokenLargestAccounts(mint), 2, 2000);
        
        if (!largest || !largest.value || largest.value.length === 0) {
            return { avgHoldHours: 0 };
        }

        const topAccounts = largest.value;
        const nowSec = Math.floor(Date.now() / 1000);
        let totalDuration = 0;
        let validSamples = 0;
        
        const excludeSet = new Set(excludeAddresses.map(a => a ? a.toString() : ''));

        // 2. Iterate Top Holders
        for (const acc of topAccounts) {
            // Limit to checking 15 accounts to save RPC/Time
            if (validSamples >= 15) break; 

            if (excludeSet.has(acc.address.toString())) continue;

            try {
                const pubkey = new PublicKey(acc.address);
                
                // Limit to 50 signatures. 
                const signatures = await retryRPC(() => conn.getSignaturesForAddress(pubkey, { limit: 50 }), 2, 1000);
                
                if (signatures.length > 0) {
                    // The last item is the oldest in this batch
                    const oldestTx = signatures[signatures.length - 1];
                    const txTime = oldestTx.blockTime || nowSec;
                    const durationSeconds = nowSec - txTime;
                    
                    totalDuration += durationSeconds;
                    validSamples++;
                } else {
                    // No transactions found in history? Assume safe holding.
                    totalDuration += (24 * 3600);
                    validSamples++;
                }
            } catch (err) {
               // checking individual accounts can fail, ignore.
            }
        }

        if (validSamples === 0) return { avgHoldHours: 0 };

        const avgSeconds = totalDuration / validSamples;
        return { avgHoldHours: avgSeconds / 3600 };

    } catch (e) {
        logger.error(`Deep Analysis Failed for ${mintAddress}: ${e.message}`);
        return { avgHoldHours: 0 };
    }
}

module.exports = { 
    getSolanaConnection, 
    analyzeTokenHolders,
    retryRPC,
    getHolderCountFromRPC
};
