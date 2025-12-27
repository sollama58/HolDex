const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

let connection;

/**
 * Returns the singleton Solana Connection instance.
 */
function getSolanaConnection() {
    if (!connection) {
        if (!config.RPC_URL) {
            logger.warn("⚠️ RPC_URL is missing in config/env.js, using default public RPC.");
        }
        connection = new Connection(config.RPC_URL || 'https://api.mainnet-beta.solana.com', {
            commitment: 'confirmed',
            confirmTransactionInitialTimeout: 60000
        });
    }
    return connection;
}

/**
 * Fetches the number of holders directly from the RPC.
 * Uses getProgramAccounts with filters to be lightweight (dataSlice).
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

        // Fetch only keys (dataSlice length 0) to save bandwidth
        const accounts = await conn.getProgramAccounts(TOKEN_PROGRAM_ID, {
            filters: filters,
            dataSlice: { offset: 0, length: 0 }
        });

        return accounts.length;
    } catch (e) {
        // Suppress errors for now to prevent log spam if RPC limits are hit
        return 0;
    }
}

module.exports = { 
    getSolanaConnection, 
    getHolderCountFromRPC 
};
