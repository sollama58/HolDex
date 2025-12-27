const { Connection } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

let connection = null;

/**
 * Returns a singleton Solana Connection instance.
 * Automatically selects Helius or Standard RPC from config.
 */
function getSolanaConnection() {
    if (connection) return connection;

    // config/env.js already handles the logic of constructing the URL
    // based on HELIUS_API_KEY presence, so we can trust SOLANA_RPC_URL.
    const rpcUrl = config.SOLANA_RPC_URL;
    
    // Derive WebSocket URL
    const wsUrl = config.HELIUS_API_KEY
        ? `wss://mainnet.helius-rpc.com/?api-key=${config.HELIUS_API_KEY}`
        : rpcUrl.replace('http', 'ws');

    // logger.info(`🔌 Solana Service: Connecting to RPC...`);

    connection = new Connection(rpcUrl, {
        commitment: 'confirmed',
        wsEndpoint: wsUrl,
        confirmTransactionInitialTimeout: 60000,
        disableRetryOnRateLimit: false,
    });

    return connection;
}

/**
 * Retries a Solana RPC call with exponential backoff.
 * @param {Function} fn - Function to execute (receives connection as arg)
 * @param {number} retries - Max retries
 * @param {number} delay - Initial delay in ms
 */
async function retryRPC(fn, retries = 3, delay = 1000) {
    const conn = getSolanaConnection();
    try {
        return await fn(conn);
    } catch (err) {
        if (retries <= 0) throw err;
        
        // Check for specific retryable errors
        const msg = err.message.toLowerCase();
        const isRetryable = msg.includes('429') || 
                           msg.includes('timeout') || 
                           msg.includes('network') ||
                           msg.includes('econnreset');
                           
        if (!isRetryable && retries < 2) throw err; // Fail fast on non-network errors

        await new Promise(r => setTimeout(r, delay));
        return retryRPC(fn, retries - 1, delay * 2);
    }
}

module.exports = {
    getSolanaConnection,
    retryRPC
};
