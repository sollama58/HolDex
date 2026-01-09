require('dotenv').config();

// ============================================
// ENVIRONMENT VALIDATION
// ============================================

/**
 * Validate required environment variables at startup
 * CRITICAL: Fail fast if production config is missing
 */
function validateEnv() {
    const isProduction = process.env.NODE_ENV === 'production';
    const errors = [];
    const warnings = [];

    // CRITICAL: Database is always required
    if (!process.env.DATABASE_URL) {
        errors.push('DATABASE_URL is required');
    }

    // CRITICAL: Admin password required in production
    if (isProduction && !process.env.ADMIN_PASSWORD) {
        errors.push('ADMIN_PASSWORD is required in production');
    }

    // HIGH: Webhook secret required in production if using webhooks
    if (isProduction && (process.env.WEBHOOK_URL || process.env.API_URL) && !process.env.WEBHOOK_SECRET) {
        errors.push('WEBHOOK_SECRET is required when webhooks are enabled in production');
    }

    // SECURITY: Data signing secret required in production for integrity verification (M6)
    if (isProduction && !process.env.DATA_SIGNING_SECRET) {
        errors.push('DATA_SIGNING_SECRET is required in production for K-Score integrity verification');
    }

    // SECURITY: Oracle webhook secret required in production for GASdf integration
    if (isProduction && !process.env.ORACLE_WEBHOOK_SECRET) {
        warnings.push('ORACLE_WEBHOOK_SECRET not set - Oracle burn webhook will reject all requests');
    }

    // Validate ORACLE_WEBHOOK_SECRET strength
    if (process.env.ORACLE_WEBHOOK_SECRET && process.env.ORACLE_WEBHOOK_SECRET.length < 32) {
        warnings.push('ORACLE_WEBHOOK_SECRET should be at least 32 characters for security');
    }

    // Validate DATA_SIGNING_SECRET strength
    if (process.env.DATA_SIGNING_SECRET && process.env.DATA_SIGNING_SECRET.length < 32) {
        warnings.push('DATA_SIGNING_SECRET should be at least 32 characters for security');
    }

    // MEDIUM: Warn about missing optional but recommended vars
    if (!process.env.HELIUS_API_KEY) {
        warnings.push('HELIUS_API_KEY not set - some features will be limited');
    }

    if (!process.env.REDIS_URL) {
        warnings.push('REDIS_URL not set - rate limiting will use in-memory fallback');
    }

    if (isProduction && !process.env.CORS_ORIGINS) {
        warnings.push('CORS_ORIGINS not set - using default whitelist');
    }

    // Log warnings
    for (const warn of warnings) {
        console.warn(`⚠️  [ENV] ${warn}`);
    }

    // Fail on errors
    if (errors.length > 0) {
        console.error('❌ [ENV] Configuration errors:');
        for (const err of errors) {
            console.error(`   - ${err}`);
        }
        if (isProduction) {
            console.error('❌ [ENV] Cannot start in production with missing configuration');
            process.exit(1);
        } else {
            console.warn('⚠️  [ENV] Running in development mode with incomplete config');
        }
    }
}

// Run validation on module load
validateEnv();

// Helper to parse comma-separated lists
const parseCors = (val) => {
    const defaults = [
        'https://www.alonisthe.dev', 
        'https://alonisthe.dev',
        'http://localhost:3000', 
        'http://localhost:5173',
        'http://127.0.0.1:3000',
        'http://127.0.0.1:5173'
    ];

    if (val === '*') return '*';
    if (!val || val.trim() === '') return defaults;

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
    // SECURITY: No hardcoded credentials - must be set via environment
    // Development: use .env file with DATABASE_URL
    // Production: validateEnv() will fail if not set
    DATABASE_URL: process.env.DATABASE_URL || null,
    REDIS_URL: process.env.REDIS_URL || null,
    
    // RPC & WSS CONFIG
    SOLANA_RPC_URL: rpcUrl,
    RPC_URL: rpcUrl,
    // NEW: Explicit WSS URL Support
    SOLANA_WSS_URL: process.env.SOLANA_WSS_URL,
    
    // CORS Configuration
    CORS_ORIGINS: parseCors(process.env.CORS_ORIGINS),
    
    METADATA_UPDATE_INTERVAL: parseInt(process.env.METADATA_UPDATE_INTERVAL) || 300000, 
    HOLDER_SCAN_INTERVAL: parseInt(process.env.HOLDER_SCAN_INTERVAL) || 300000,
    ADMIN_PASSWORD: process.env.ADMIN_PASSWORD || null, // REQUIRED - no default
    HELIUS_API_KEY: heliusKey || '',

    // --- JUPITER API ---
    // Optional: Set for higher rate limits on Jupiter Price API
    // Get your API key at: https://portal.jup.ag/
    JUPITER_API_KEY: process.env.JUPITER_API_KEY || null,

    // --- PAYMENT CONFIGURATION ---
    TREASURY_WALLET: process.env.TREASURY_WALLET || null, // REQUIRED - set in production
    FEE_SOL: parseFloat(process.env.FEE_SOL) || 0.1,
    FEE_TOKEN_AMOUNT: parseFloat(process.env.FEE_TOKEN_AMOUNT) || 5000,
    FEE_TOKEN_MINT: process.env.FEE_TOKEN_MINT || '9zB5wRarXMj86MymwLumSKA1Dx35zPqqKfcZtK1Spump',

    // --- BURN CREDITS SYSTEM ---
    // "Hold to enter. Burn to use. 1 token burned = 1 API call forever."
    BURN_CREDITS: {
        minHoldings: 10000,      // Must hold 10K $ASDFASDFA to access API
        callsPerBurn: 1,         // 1 token burned = 1 API call
        philosophy: 'Burn = Value Creation = Lifetime API Access'
    },
    
    // --- MEMORY PROTECTION ---
    ENABLE_RPC_HOLDER_CHECK: process.env.ENABLE_RPC_HOLDER_CHECK !== 'false',

    // --- WEBHOOK CONFIGURATION ---
    // API_URL: Public URL for webhook callbacks (e.g., https://holdex.io)
    API_URL: process.env.API_URL || null,
    // WEBHOOK_URL: Full webhook callback URL (auto-detected from API_URL if not set)
    WEBHOOK_URL: process.env.WEBHOOK_URL || null,
    // USE_WEBHOOKS: Enable webhook mode (auto-detected if WEBHOOK_URL or API_URL is set)
    USE_WEBHOOKS: !!(process.env.WEBHOOK_URL || process.env.API_URL),
    // WEBHOOK_SECRET: Secret for verifying Helius webhook signatures (HMAC-SHA256)
    // CRITICAL: Set this in production to prevent webhook spoofing
    WEBHOOK_SECRET: process.env.WEBHOOK_SECRET || null,

    // --- DATA INTEGRITY (Proof-of-History) ---
    // Signs K-Score data with HMAC-SHA256 to detect tampering
    // Even if DB credentials are leaked, attackers cannot forge valid signatures
    DATA_SIGNING_SECRET: process.env.DATA_SIGNING_SECRET || null,

    // VERIFY_DATA: Enable signature verification on API responses
    // Set to 'strict' to reject tampered data, 'warn' to log only
    VERIFY_DATA_MODE: process.env.VERIFY_DATA_MODE || 'strict',

    // --- ORACLE API CONFIGURATION ---
    // ORACLE_WEBHOOK_SECRET: Shared secret for GASdf → HolDex webhook authentication
    // GASdf signs requests with HMAC-SHA256, HolDex verifies before processing
    // CRITICAL: Set this in production to prevent forged burn notifications
    ORACLE_WEBHOOK_SECRET: process.env.ORACLE_WEBHOOK_SECRET || null,

    // ORACLE_RATE_LIMIT: Requests per minute for Oracle API endpoints
    ORACLE_RATE_LIMIT: parseInt(process.env.ORACLE_RATE_LIMIT) || 100,

    // NODE_ENV: Environment mode
    NODE_ENV: process.env.NODE_ENV || 'development'
};
