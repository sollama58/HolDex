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
// REMOVED: holderScanner
const newTokenListener = require('./tasks/newTokenListener');
const autoSeeder = require('./tasks/autoSeeder'); 

const globalState = {
    asdfTop50Holders: new Set(),
    userExpectedAirdrops: new Map(),
    lastBackendUpdate: Date.now()
};

async function startServer() {
    console.log('💎 Starting HolDex Backend...');
    await initDB();
    const redis = initRedis();

    const app = express();
    app.use(helmet());
    app.use(cors({ origin: config.CORS_ORIGINS }));
    app.use(express.json());

    const deps = { db: getDB(), redis, globalState, devKeypair: null };

    app.use('/api', tokenRoutes.init(deps));
    app.get('/health', (req, res) => res.json({ status: 'ok', timestamp: Date.now() }));

    // --- START BACKGROUND TASKS ---
    console.log('🚀 Starting Background Tasks...');
    autoSeeder.start(deps);        // 1. Seed DB
    newTokenListener.start(deps);  // 2. Listen for new mints
    metadataUpdater.start(deps);   // 3. Keep prices fresh
    // REMOVED: holderScanner start

    const server = http.createServer(app);
    server.listen(config.PORT, () => {
        console.log(`✅ Server running on port ${config.PORT}`);
    });
}

startServer().catch(err => {
    console.error('❌ Fatal Server Error:', err);
    process.exit(1);
});
