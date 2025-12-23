require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const http = require('http');
const config = require('./config/env');
const { initDB, getDB } = require('./services/database');
const { initRedis } = require('./services/redis');
const tokenRoutes = require('./routes/tokens');
const metadataUpdater = require('./tasks/metadataUpdater');
const holderScanner = require('./tasks/holderScanner');

// Global State Object (Shared across modules)
const globalState = {
    asdfTop50Holders: new Set(),
    userExpectedAirdrops: new Map(),
    lastBackendUpdate: Date.now()
};

async function startServer() {
    // 1. Initialize Infrastructure
    console.log('💎 Starting HolDex Backend...');
    await initDB();
    const redis = initRedis(); // Optional, logs warning if missing

    // 2. Initialize App
    const app = express();
    app.use(helmet());
    app.use(cors({ origin: config.CORS_ORIGINS }));
    app.use(express.json());

    // 3. Dependencies Container
    const deps = {
        db: getDB(),
        redis,
        globalState,
        // Mock devKeypair if not strictly needed for read-only mode, 
        // or load it from config.PRIVATE_KEY if you have write ops.
        devKeypair: null 
    };

    // 4. Routes
    // Mount token routes at /api
    app.use('/api', tokenRoutes.init(deps));

    // Health Check
    app.get('/health', (req, res) => res.json({ status: 'ok', timestamp: Date.now() }));

    // 5. Start Background Tasks
    console.log('🚀 Starting Background Tasks...');
    metadataUpdater.start(deps);
    holderScanner.start(deps);

    // 6. Start Server
    const server = http.createServer(app);
    server.listen(config.PORT, () => {
        console.log(`✅ Server running on port ${config.PORT}`);
    });
}

startServer().catch(err => {
    console.error('❌ Fatal Server Error:', err);
    process.exit(1);
});
