require('dotenv').config();

// Validate required environment variables
const requiredEnvVars = ['DATABASE_URL'];
const missingVars = requiredEnvVars.filter(v => !process.env[v]);
if (missingVars.length > 0 && process.env.NODE_ENV !== 'test') {
    console.error(`Missing required environment variables: ${missingVars.join(', ')}`);
    console.error('Please set these in your .env file or environment.');
    process.exit(1);
}

module.exports = {
    PORT: process.env.PORT || 3000,
    DATABASE_URL: process.env.DATABASE_URL,
    REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379',
    SOLANA_RPC_URL: process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com',
    CORS_ORIGINS: process.env.CORS_ORIGINS ? process.env.CORS_ORIGINS.split(',') : '*',
    METADATA_UPDATE_INTERVAL: parseInt(process.env.METADATA_UPDATE_INTERVAL) || 300000,
    HOLDER_SCAN_INTERVAL: parseInt(process.env.HOLDER_SCAN_INTERVAL) || 300000,
    ADMIN_PASSWORD: process.env.ADMIN_PASSWORD,
    HELIUS_API_KEY: process.env.HELIUS_API_KEY || '',

    // --- PAYMENT CONFIGURATION ---
    TREASURY_WALLET: process.env.TREASURY_WALLET,
    FEE_SOL: parseFloat(process.env.FEE_SOL) || 0.1,
    FEE_TOKEN_AMOUNT: parseFloat(process.env.FEE_TOKEN_AMOUNT) || 5000,
    FEE_TOKEN_MINT: process.env.FEE_TOKEN_MINT
};
