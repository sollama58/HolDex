const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const config = require('./config/env');
const { initDB } = require('./services/database');
const { initRedis } = require('./services/redis');
const tokenRoutes = require('./routes/tokens');

// NOTE: Background tasks (metadataUpdater, etc.) are NO LONGER imported here.
// They run in the separate 'worker' service defined in docker-compose.

const app = express();

app.set('trust proxy', 1);

// --- SECURITY & MIDDLEWARE ---
app.use(helmet());
app.use(cors({ origin: config.CORS_ORIGINS }));
app.use(express.json());

// --- RATE LIMITING ---

// 1. Global Limiter: 3000 requests per 15 mins (Increased for scale)
const globalLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, 
    max: 3000, 
    standardHeaders: true,
    legacyHeaders: false,
    message: { success: false, error: "Too many requests, please try again later." }
});
app.use(globalLimiter);

// 2. Strict Limiter: For Search & Updates
const strictLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, 
    max: 150, // Slight increase to handle bursts
    message: { success: false, error: "Rate limit exceeded. Please slow down." }
});

// --- INITIALIZATION ---

async function startServer() {
    try {
        // 1. Init Data Layer
        const db = await initDB();
        const redis = await initRedis();

        // 2. Dependencies
        const deps = { db, redis };
        
        // 3. Init Routes
        // Search and Write operations get strict limits
        app.use('/api/request-update', strictLimiter);
        app.use('/api/tokens', strictLimiter); 
        
        app.use('/api', tokenRoutes.init(deps));

        // 4. Start Listener
        app.listen(config.PORT, () => {
            console.log(`🔥 HolDex API Node Online on port ${config.PORT}`);
            console.log(`🛡️  Mode: API Only (Workers decoupled)`);
        });
    } catch (err) {
        console.error("Fatal Server Startup Error:", err);
        process.exit(1);
    }
}

startServer();
