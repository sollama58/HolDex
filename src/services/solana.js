const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

let connection;

/**
 * Returns the singleton Solana Connection instance.
 */
function getSolanaConnection() {
    if (!connection) {
        // Prioritize SOLANA_RPC_URL, fallback to RPC_URL, then default
        const rpcUrl = config.SOLANA_RPC_URL || config.RPC_URL || 'https://api.mainnet-beta.solana.com';
        
        if (rpcUrl.includes('helius')) {
            logger.info(`🔌 RPC: Connected to Helius via config.`);
        } else if (rpcUrl.includes('mainnet-beta')) {
            logger.warn(`⚠️ RPC: Using Public Solana Endpoint (Rate Limits Likely).`);
        } else {
            logger.info(`🔌 RPC: Connected to Custom Endpoint.`);
        }

        connection = new Connection(rpcUrl, {
            commitment: 'confirmed',
            confirmTransactionInitialTimeout: 60000
        });
    }
    return connection;
}

/**
 * Generic retry wrapper for RPC calls.
 * @param {Function} fn - Async function to retry
 * @param {number} retries - Number of retries (default 3)
 * @param {number} delay - Base delay in ms (default 1000)
 */
async function retryRPC(fn, retries = 3, delay = 1000) {
    for (let i = 0; i < retries; i++) {
        try {
            return await fn();
        } catch (e) {
            if (i === retries - 1) throw e; // Throw on last failure
            
            // Check for specific RPC errors to handle smarter
            const isRateLimit = e.message && (e.message.includes('429') || e.message.includes('Too Many Requests'));
            const waitTime = isRateLimit ? delay * 2 * (i + 1) : delay * (i + 1); // Aggressive backoff for 429
            
            await new Promise(r => setTimeout(r, waitTime));
        }
    }
}

/**
 * Fetches the number of holders directly from the RPC.
 * FILTERS: Only counts accounts with Balance > 0 (Active Holders).
 */
async function getHolderCountFromRPC(mintAddress) {
    if (!mintAddress) return 0;
    
    try {
        const conn = getSolanaConnection();
        const TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');
        
        // Filter: Data Size = 165 (Standard Token Account) AND Mint = mintAddress
        const filters = [
            { dataSize: 165 }, 
            { memcmp: { offset: 0, bytes: mintAddress } }
        ];

        // Fetch just the Amount (offset 64, length 8) to verify balance > 0
        // This is crucial to avoid counting closed/empty accounts
        const accounts = await retryRPC(() => conn.getProgramAccounts(TOKEN_PROGRAM_ID, {
            filters: filters,
            dataSlice: { offset: 64, length: 8 } 
        }));

        let activeHolders = 0;
        for (const acc of accounts) {
            // Ensure we got the data we requested
            if (acc.account.data && acc.account.data.length === 8) {
                // Read the u64 balance (Little Endian)
                const balance = acc.account.data.readBigUInt64LE(0);
                if (balance > 0n) {
                    activeHolders++;
                }
            }
        }

        return activeHolders;
    } catch (e) {
        // Suppress errors for now to prevent log spam if RPC limits are hit
        // logger.debug(`RPC Holder Check failed: ${e.message}`);
        return 0;
    }
}

module.exports = { 
    getSolanaConnection, 
    getHolderCountFromRPC,
    retryRPC 
};
