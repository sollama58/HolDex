require('dotenv').config();
const path = require('path');

module.exports = {
    PORT: process.env.PORT || 3000,
    SOLANA_RPC_URL: process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com',
    PRIVATE_KEY: process.env.PRIVATE_KEY, // Dev wallet (fee receiver)
    
    // Auth & Security
    ADMIN_API_KEY: process.env.ADMIN_API_KEY || 'change_me_in_prod',
    CORS_ORIGINS: process.env.CORS_ORIGINS ? process.env.CORS_ORIGINS.split(',') : '*',

    // Service Configs
    METADATA_UPDATE_INTERVAL: parseInt(process.env.METADATA_UPDATE_INTERVAL) || 60000, // 60s
    HOLDER_SCAN_INTERVAL: parseInt(process.env.HOLDER_SCAN_INTERVAL) || 300000, // 5m
    
    // File System (Legacy/Logs)
    DISK_ROOT: process.env.DISK_ROOT || path.join(__dirname, '../../data'),
    
    // Scalability Upgrades
    DATABASE_URL: process.env.DATABASE_URL, // PostgreSQL Connection String
    REDIS_URL: process.env.REDIS_URL,       // Redis Connection String
    
    // External APIs
    PINATA_JWT: process.env.PINATA_JWT,
    PINATA_GATEWAY: process.env.PINATA_GATEWAY || 'gateway.pinata.cloud',
    TWITTER_BEARER_TOKEN: process.env.TWITTER_BEARER_TOKEN
};
