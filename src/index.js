const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit'); // NEW: Import Rate Limiter
const config = require('./config/env');
const { initDB } = require('./services/database');
const { initRedis } = require('./services/redis');
const tokenRoutes = require('./routes/tokens');
const metadataUpdater = require('./tasks/metadataUpdater');
const kScoreUpdater = require('./tasks/kScoreUpdater');
const newTokenListener = require('./tasks/newTokenListener');

const app = express();

// --- PROXY CONFIGURATION (FIX) ---
// Required because we are behind a Load Balancer (Render/Docker/Nginx).
app.set('trust proxy', 1);

// --- SECURITY & MIDDLEWARE ---
app.use(helmet());
app.use(cors({ origin: config.CORS_ORIGINS }));
app.use(express.json());

// --- RATE LIMITING (RELAXED) ---

// 1. Global Limiter: 2000 requests per 15 mins per IP (Approx ~2 req/sec sustained)
const globalLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, 
    max: 2000, 
    standardHeaders: true,
    legacyHeaders: false,
    message: { success: false, error: "Too many requests, please try again later." }
});
app.use(globalLimiter);

// 2. Strict Limiter: For Search & Updates (120 requests per 1 min = 2 req/sec bursts)
const strictLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, 
    max: 120, 
    message: { success: false, error: "Rate limit exceeded. Please slow down." }
});

// --- INITIALIZATION ---

async function startServer() {
    try {
        // 1. Init Services
        const db = await initDB();
        const redis = await initRedis();

        // 2. Init Tasks
        // Pass dependencies to tasks
        const deps = { db, redis, globalState: { lastBackendUpdate: Date.now() } };
        
        // Start Background Workers
        metadataUpdater.start(deps);
        kScoreUpdater.start(deps);
        newTokenListener.start(deps);

        // 3. Init Routes
        // Apply Strict Limiter to Search & Updates
        app.use('/api/request-update', strictLimiter);
        app.use('/api/tokens', strictLimiter); // Protects the search query
        
        app.use('/api', tokenRoutes.init(deps));

        // 4. Start Listener
        app.listen(config.PORT, () => {
            console.log(`🔥 HolDex Backend v2.3 running on port ${config.PORT}`);
            console.log(`🛡️ Rate Limiting Active (Relaxed Mode)`);
        });
    } catch (err) {
        console.error("Fatal Server Startup Error:", err);
        process.exit(1);
    }
}

startServer();
