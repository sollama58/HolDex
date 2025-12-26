const { Connection } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

let connectionInstance = null;

/**
 * Singleton Connection Manager
 * Prevents multiple connection pools from being created.
 */
function getSolanaConnection() {
    if (!connectionInstance) {
        logger.info(`🔌 Establishing new Solana RPC Connection to ${config.SOLANA_RPC_URL}`);
        connectionInstance = new Connection(config.SOLANA_RPC_URL, {
            commitment: 'confirmed',
            confirmTransactionInitialTimeout: 60000,
            disableRetryOnRateLimit: false,
        });
    }
    return connectionInstance;
}

/**
 * Safe RPC Retry Wrapper
 * Retries failed calls with exponential backoff.
 */
async function retryRPC(fn, retries = 3, delay = 1000) {
    try {
        return await fn();
    } catch (err) {
        if (retries <= 0) {
            logger.warn(`RPC Failed after retries: ${err.message}`);
            throw err;
        }
        await new Promise(r => setTimeout(r, delay));
        return retryRPC(fn, retries - 1, delay * 2);
    }
}

module.exports = { 
    getSolanaConnection,
    retryRPC
};
