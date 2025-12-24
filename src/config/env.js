require('dotenv').config();

module.exports = {
    PORT: process.env.PORT || 3000,
    DATABASE_URL: process.env.DATABASE_URL,
    REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379',
    SOLANA_RPC_URL: process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com',
    CORS_ORIGINS: process.env.CORS_ORIGINS ? process.env.CORS_ORIGINS.split(',') : '*',
    // Updated to 5 minutes (300,000 ms) as requested
    METADATA_UPDATE_INTERVAL: parseInt(process.env.METADATA_UPDATE_INTERVAL) || 300000, 
    HOLDER_SCAN_INTERVAL: parseInt(process.env.HOLDER_SCAN_INTERVAL) || 300000,
    ADMIN_PASSWORD: process.env.ADMIN_PASSWORD || 'admin123',
    HELIUS_API_KEY: process.env.HELIUS_API_KEY || '',

    // --- PAYMENT CONFIGURATION ---
    // The wallet that collects the fees
    TREASURY_WALLET: process.env.TREASURY_WALLET || 'EbZ4wYq4bYq4bYq4bYq4bYq4bYq4bYq4bYq4bYq4bYq4', 
    // Fee in SOL (e.g., 0.1 SOL)
    FEE_SOL: parseFloat(process.env.FEE_SOL) || 0.1, 
    // Fee in Tokens (e.g., 5000 tokens)
    FEE_TOKEN_AMOUNT: parseFloat(process.env.FEE_TOKEN_AMOUNT) || 5000,
    // The Token Mint ($ASDFASDFA)
    FEE_TOKEN_MINT: '9zB5wRarXMj86MymwLumSKA1Dx35zPqqKfcZtK1Spump' 
};
