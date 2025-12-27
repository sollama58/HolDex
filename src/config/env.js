require('dotenv').config();

// Helper to parse comma-separated lists
const parseCors = (val) => {
    // Default allowed origins (Production Frontend & Dev)
    const defaults = [
        'https://www.alonisthe.dev', 
        'https://alonisthe.dev',
        'http://localhost:3000', 
        'http://localhost:5173',
        'http://127.0.0.1:3000',
        'http://127.0.0.1:5173'
    ];

    if (val === '*') return '*';
    
    // If no specific env var is set, use defaults
    if (!val || val.trim() === '') return defaults;

    // Parse env var and merge with defaults
    // Handles spaces after commas gracefully
    const envOrigins = val.split(',').map(origin => origin.trim()).filter(o => o.length > 0);
    const combined = [...defaults];
    
    envOrigins.forEach(origin => {
        if (origin && !combined.includes(origin)) {
            combined.push(origin);
        }
    });

    return combined;
};

// --- AUTO-DETECT RPC CONFIGURATION ---
let rpcUrl = process.env.SOLANA_RPC_URL;
const heliusKey = process.env.HELIUS_API_KEY;

if (!rpcUrl && heliusKey) {
    rpcUrl = `https://mainnet.helius-rpc.com/?api-key=${heliusKey}`;
}

if (!rpcUrl) {
    rpcUrl = 'https://api.mainnet-beta.solana.com';
}

module.exports = {
    PORT: process.env.PORT || 3000,
    DATABASE_URL: process.env.DATABASE_URL || 'postgresql://user:password@localhost:5432/holdex',
    REDIS_URL: process.env.REDIS_URL || 'redis://redis:6379',
    
    // EXPORT BOTH NAMES TO PREVENT MISMATCHES
    SOLANA_RPC_URL: rpcUrl,
    RPC_URL: rpcUrl, 
    
    // CORS Configuration
    CORS_ORIGINS: parseCors(process.env.CORS_ORIGINS),
    
    METADATA_UPDATE_INTERVAL: parseInt(process.env.METADATA_UPDATE_INTERVAL) || 300000, 
    HOLDER_SCAN_INTERVAL: parseInt(process.env.HOLDER_SCAN_INTERVAL) || 300000,
    ADMIN_PASSWORD: process.env.ADMIN_PASSWORD || 'admin123',
    HELIUS_API_KEY: heliusKey || '',

    // --- PAYMENT CONFIGURATION ---
    TREASURY_WALLET: process.env.TREASURY_WALLET || 'EbZ4wYq4bYq4bYq4bYq4bYq4bYq4bYq4bYq4bYq4bYq4', 
    FEE_SOL: parseFloat(process.env.FEE_SOL) || 0.1, 
    FEE_TOKEN_AMOUNT: parseFloat(process.env.FEE_TOKEN_AMOUNT) || 5000,
    FEE_TOKEN_MINT: process.env.FEE_TOKEN_MINT || '9zB5wRarXMj86MymwLumSKA1Dx35zPqqKfcZtK1Spump',
    
    // --- MEMORY PROTECTION ---
    // CHANGED: Default to TRUE (Enable checks) unless explicitly disabled
    ENABLE_RPC_HOLDER_CHECK: process.env.ENABLE_RPC_HOLDER_CHECK !== 'false'
};
