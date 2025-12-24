require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const http = require('http');
const rateLimit = require('express-rate-limit');
const config = require('./config/env');
const { initDB, getDB } = require('./services/database');
const { initRedis } = require('./services/redis');
const tokenRoutes = require('./routes/tokens');
const { calculateTokenScore } = require('./tasks/kScoreUpdater'); // Keep for on-demand admin endpoint

const globalState = {
    lastBackendUpdate: Date.now()
};

// Rate Limiter: 200 requests per 15 minutes per IP
const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, 
    max: 200, 
    standardHeaders: true, 
    legacyHeaders: false,
    message: { success: false, error: "Too many requests, please try again later." }
});

async function startServer() {
    console.log('💎 Starting HolDex API Server...');
    await initDB();
    const redis = initRedis();

    const app = express();
    
    // Security & Middleware
    app.use(helmet());
    app.use(cors({ origin: config.CORS_ORIGINS }));
    app.use(express.json({ limit: '10kb' })); 
    app.use(limiter); 

    const deps = { db: getDB(), redis, globalState, devKeypair: null };

    app.use('/api', tokenRoutes.init(deps));
    
    app.get('/health', (req, res) => res.json({ status: 'ok', timestamp: Date.now(), uptime: process.uptime(), role: 'API' }));

    // NOTE: Background tasks are now run by 'src/worker.js' to decouple load.
    // The API server only handles HTTP requests.

    const server = http.createServer(app);
    server.listen(config.PORT, () => {
        console.log(`✅ API Server running on port ${config.PORT}`);
    });
}

startServer().catch(err => {
    console.error('❌ Fatal Server Error:', err);
    process.exit(1);
});
