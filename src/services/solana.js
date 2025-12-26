const { Connection } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

// STRICT SINGLE RPC POLICY
// We only use the URL defined in config (which prioritizes HELIUS_API_KEY)
const RPC_URL = config.SOLANA_RPC_URL;

let connectionInstance = null;

function createConnection(url) {
    logger.info(`🔌 Connecting to Primary RPC: [${url}]`);
    
    return new Connection(url, {
        commitment: 'confirmed',
        confirmTransactionInitialTimeout: 60000,
        disableRetryOnRateLimit: true, // We handle retries manually
    });
}

function getSolanaConnection() {
    if (!connectionInstance) {
        connectionInstance = createConnection(RPC_URL);
    }
    return connectionInstance;
}

function getRpcUrl() {
    return RPC_URL;
}

function rotateConnection() {
    // Since we are strictly using Helius, "rotation" just means 
    // re-instantiating the connection to clear internal state.
    logger.warn(`⚠️  Refreshing RPC Connection State...`);
    connectionInstance = createConnection(RPC_URL);
    return connectionInstance;
}

async function retryRPC(fn, retries = 3, delay = 1000) {
    try {
        const conn = getSolanaConnection();
        if (!conn) throw new Error("Connection initialization failed");
        return await fn(conn);
    } catch (err) {
        if (retries <= 0) {
            throw err;
        }

        // Simple exponential backoff on the SAME endpoint
        await new Promise(r => setTimeout(r, delay));
        return retryRPC(fn, retries - 1, delay * 2);
    }
}

module.exports = { 
    getSolanaConnection,
    getRpcUrl,
    rotateConnection,
    retryRPC
};
