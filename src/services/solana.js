const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

let connection;

function getSolanaConnection() {
    if (!connection) {
        // Use the configured RPC URL (likely Helius/QuickNode based on your env)
        connection = new Connection(config.RPC_URL, {
            commitment: 'confirmed',
            confirmTransactionInitialTimeout: 60000
        });
    }
    return connection;
}

/**
 * Fetches the number of holders directly from the RPC.
 * Uses getProgramAccounts with filters to be lightweight (dataSlice).
 * Recommended to use with a paid RPC (Helius/QuickNode) as public RPCs may 429 this.
 */
async function getHolderCountFromRPC(mintAddress) {
    try {
        const conn = getSolanaConnection();
        const TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');
        
        // Filter: Data Size = 165 (Standard Token Account) AND Mint = mintAddress
        const filters = [
            { dataSize: 165 }, 
            { memcmp: { offset: 0, bytes: mintAddress } }
        ];

        // Fetch only keys (dataSlice length 0) to save massive bandwidth
        const accounts = await conn.getProgramAccounts(TOKEN_PROGRAM_ID, {
            filters: filters,
            dataSlice: { offset: 0, length: 0 }
        });

        return accounts.length;
    } catch (e) {
        // It's common for public RPCs to block getProgramAccounts
        // logger.warn(`RPC Holder Count failed for ${mintAddress}: ${e.message}`);
        return 0;
    }
}

/**
 * Validates a Solana public key
 */
function isValidPubkey(str) {
    try {
        new PublicKey(str);
        return true;
    } catch (e) {
        return false;
    }
}

module.exports = { 
    getSolanaConnection, 
    isValidPubkey,
    getHolderCountFromRPC 
};
