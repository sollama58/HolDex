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
const newTokenListener = require('./tasks/newTokenListener');
// REMOVED: autoSeeder (Deprecated due to 530 Cloudflare blocks)

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
    newTokenListener.start(deps);  // 1. Listen for new/migrated tokens via DexScreener
    metadataUpdater.start(deps);   // 2. Keep prices/volume fresh
    holderScanner.start(deps);     // 3. Scan holders for top tokens

    const server = http.createServer(app);
    server.listen(config.PORT, () => {
        console.log(`✅ Server running on port ${config.PORT}`);
    });
}

startServer().catch(err => {
    console.error('❌ Fatal Server Error:', err);
    process.exit(1);
});
