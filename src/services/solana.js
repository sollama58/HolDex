const { Connection } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

// DETERMINE RPC ENDPOINTS
// 1. Primary: Configured URL (Helius if API key was present)
// 2. Secondary: Public Fallbacks (in case premium quota runs out)
const RPC_ENDPOINTS = [
    config.SOLANA_RPC_URL,
    'https://api.mainnet-beta.solana.com',
    'https://rpc.ankr.com/solana',
    'https://solana-api.projectserum.com'
].filter((url, index, self) => url && self.indexOf(url) === index); // Dedupe

let currentEndpointIndex = 0;
let connectionInstance = null;

function createConnection(url) {
    const isHelius = url.includes('helius');
    logger.info(`🔌 Connecting to Solana RPC: ${isHelius ? 'HELIUS (Premium)' : 'Public/Custom'} [${url}]`);
    
    return new Connection(url, {
        commitment: 'confirmed',
        confirmTransactionInitialTimeout: 60000,
        disableRetryOnRateLimit: true, // We handle retries manually
    });
}

function getSolanaConnection() {
    if (!connectionInstance) {
        connectionInstance = createConnection(RPC_ENDPOINTS[0]);
    }
    return connectionInstance;
}

function getRpcUrl() {
    return RPC_ENDPOINTS[currentEndpointIndex];
}

function rotateConnection() {
    currentEndpointIndex = (currentEndpointIndex + 1) % RPC_ENDPOINTS.length;
    const newUrl = RPC_ENDPOINTS[currentEndpointIndex];
    
    logger.warn(`⚠️  RPC Rate Limit/Error. Rotating to: ${newUrl}`);
    connectionInstance = createConnection(newUrl);
    return connectionInstance;
}

async function retryRPC(fn, retries = 3, delay = 1000) {
    try {
        const conn = getSolanaConnection();
        if (!conn) throw new Error("Connection initialization failed");
        return await fn(conn);
    } catch (err) {
        const msg = err.message ? err.message.toLowerCase() : '';
        
        // Critical Errors where we should rotate
        if (msg.includes('429') || msg.includes('limit') || msg.includes('network') || msg.includes('timeout')) {
            rotateConnection();
        }

        if (retries <= 0) {
            throw err;
        }

        // Exponential backoff
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
