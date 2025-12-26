const { Connection } = require('@solana/web3.js');
const config = require('../../config/env');
const logger = require('../../services/logger');

let connection = null;

function getConnection() {
    if (!connection) {
        // Use Helius API key if available for higher limits/reliability
        const rpcUrl = config.HELIUS_API_KEY 
            ? `https://mainnet.helius-rpc.com/?api-key=${config.HELIUS_API_KEY}` 
            : config.SOLANA_RPC_URL;
            
        // Construct WebSocket URL correctly
        const wsUrl = config.HELIUS_API_KEY
            ? `wss://mainnet.helius-rpc.com/?api-key=${config.HELIUS_API_KEY}`
            : rpcUrl.replace('http', 'ws');

        logger.info(`🔌 Indexer connecting to RPC: ${rpcUrl.includes('helius') ? 'Helius Verified' : 'Standard'}`);
        
        connection = new Connection(rpcUrl, {
            commitment: 'confirmed',
            wsEndpoint: wsUrl
        });
    }
    return connection;
}

module.exports = { getConnection };
