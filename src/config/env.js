require('dotenv').config();

// Helper to parse comma-separated lists, defaulting to '*'
const parseCors = (val) => {
    // If explicitly set to '*', return '*'
    if (val === '*') return '*';
    
    // If not set, default to '*' (Public API behavior)
    if (!val) return '*';

    // Parse the list
    const origins = val.split(',').map(origin => origin.trim());
    
    // AUTO-FIX: Always ensure your production domains are allowed
    // This prevents accidental lockouts if you set CORS_ORIGINS to just 'localhost'
    const required = ['https://www.alonisthe.dev', 'https://alonisthe.dev'];
    required.forEach(req => {
        if (!origins.includes(req)) origins.push(req);
    });
    
    return origins;
};

module.exports = {
    PORT: process.env.PORT || 3000,
    DATABASE_URL: process.env.DATABASE_URL || 'postgresql://user:password@localhost:5432/holdex',
    REDIS_URL: process.env.REDIS_URL || 'redis://redis:6379',
    SOLANA_RPC_URL: process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com',
    
    // Updated Parsing Logic
    CORS_ORIGINS: parseCors(process.env.CORS_ORIGINS),
    
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
