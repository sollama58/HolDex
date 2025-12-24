require('dotenv').config();

module.exports = {
    PORT: process.env.PORT || 3000,
    // UPDATED: Use the provided PostgreSQL URL
    DATABASE_URL: process.env.DATABASE_URL || 'postgresql://asdev_db_user:W6DVTVxCduqc8bwhouqACp8S3ncmW7Cd@dpg-d55etl3e5dus73c68i40-a/asdev_db',
    REDIS_URL: process.env.REDIS_URL || 'redis://redis:6379',
    SOLANA_RPC_URL: process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com',
    CORS_ORIGINS: process.env.CORS_ORIGINS ? process.env.CORS_ORIGINS.split(',') : '*',
    METADATA_UPDATE_INTERVAL: parseInt(process.env.METADATA_UPDATE_INTERVAL) || 300000, 
    HOLDER_SCAN_INTERVAL: parseInt(process.env.HOLDER_SCAN_INTERVAL) || 300000,
    ADMIN_PASSWORD: process.env.ADMIN_PASSWORD || 'admin123',
    HELIUS_API_KEY: process.env.HELIUS_API_KEY || '',

    // --- PAYMENT CONFIGURATION ---
    TREASURY_WALLET: process.env.TREASURY_WALLET || 'EbZ4wYq4bYq4bYq4bYq4bYq4bYq4bYq4bYq4bYq4bYq4', 
    FEE_SOL: parseFloat(process.env.FEE_SOL) || 0.1, 
    FEE_TOKEN_AMOUNT: parseFloat(process.env.FEE_TOKEN_AMOUNT) || 5000,
    FEE_TOKEN_MINT: process.env.FEE_TOKEN_MINT || '9zB5wRarXMj86MymwLumSKA1Dx35zPqqKfcZtK1Spump' 
};
