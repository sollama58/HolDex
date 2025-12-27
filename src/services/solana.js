const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

let connection;

// Program IDs
const TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');
const TOKEN_2022_PROGRAM_ID = new PublicKey('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb');

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
            // Don't retry if it's a 400 Bad Request (Invalid input)
            if (e.message && e.message.includes('400')) throw e;

            if (i === retries - 1) throw e; // Throw on last failure
            
            // Check for specific RPC errors to handle smarter
            const isRateLimit = e.message && (e.message.includes('429') || e.message.includes('Too Many Requests'));
            const waitTime = isRateLimit ? delay * 2 * (i + 1) : delay * (i + 1); // Aggressive backoff for 429
            
            await new Promise(r => setTimeout(r, waitTime));
        }
    }
}

/**
 * Helper to fetch accounts for a specific program ID.
 */
async function fetchAccountsForProgram(conn, programId, mintAddress) {
    try {
        const filters = [
            { dataSize: 165 }, 
            { memcmp: { offset: 0, bytes: mintAddress } }
        ];

        // Fetch just the Amount (offset 64, length 8) to verify balance > 0
        const accounts = await retryRPC(() => conn.getProgramAccounts(programId, {
            filters: filters,
            dataSlice: { offset: 64, length: 8 } 
        }), 2, 500); // Fewer retries for sub-tasks

        let activeHolders = 0;
        for (const acc of accounts) {
            if (acc.account.data && acc.account.data.length === 8) {
                const balance = acc.account.data.readBigUInt64LE(0);
                if (balance > 0n) activeHolders++;
            }
        }
        return activeHolders;
    } catch (e) {
        // Log detailed error for debugging
        if (e.message.includes('429')) {
             logger.warn(`⚠️ RPC Rate Limit (Holders Check): ${e.message}`);
        } else {
             // logger.debug(`RPC Check failed for ${programId.toString()}: ${e.message}`);
        }
        return 0;
    }
}

/**
 * Fetches the number of holders directly from the RPC.
 * CHECKS BOTH LEGACY TOKEN PROGRAM AND TOKEN-2022 PROGRAM.
 */
async function getHolderCountFromRPC(mintAddress) {
    if (!mintAddress) return 0;
    
    const conn = getSolanaConnection();

    // 1. Check Legacy Token Program first (Most common)
    let count = await fetchAccountsForProgram(conn, TOKEN_PROGRAM_ID, mintAddress);

    // 2. If Legacy returns 0, it MIGHT be a Token-2022 token. Check that.
    if (count === 0) {
        const count2022 = await fetchAccountsForProgram(conn, TOKEN_2022_PROGRAM_ID, mintAddress);
        if (count2022 > 0) {
            count = count2022;
        }
    } else {
        // Edge Case: Hybrid tokens (rare, but possible to have accounts in both)
        // We check 2022 anyway just in case, but only if the first one succeeded quickly
        // Actually, for performance, if we found legacy holders, we assume it's a legacy token.
    }

    return count;
}

module.exports = { 
    getSolanaConnection, 
    getHolderCountFromRPC,
    retryRPC 
};
