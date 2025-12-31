const { Connection } = require('@solana/web3.js');
const config = require('../../config/env');
const logger = require('../../services/logger');

let connection = null;

/**
 * Get Solana RPC connection for indexer
 *
 * SECURITY NOTE: @solana/web3.js Connection class doesn't support custom headers
 * for RPC authentication. The API key must be in the URL query string.
 * This is a limitation of the library, not our design choice.
 *
 * For WebSocket connections, custom headers aren't supported by the protocol
 * during handshake, so API key in URL is standard practice.
 */
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

        // Log masked URLs (never expose API key in logs)
        const maskedRpc = rpcUrl.replace(/api-key=[^&]+/, 'api-key=***');
        logger.info(`🔌 Indexer RPC: ${maskedRpc}`);

        connection = new Connection(rpcUrl, {
            commitment: 'confirmed',
            wsEndpoint: wsUrl,
            disableRetryOnRateLimit: false
        });
    }
    return connection;
}

module.exports = { getConnection };
