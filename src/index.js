/**
 * HolDex Backend Entry Point
 */
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const config = require('./config/env');
const dbService = require('./services/database');
const redisService = require('./services/redis');
const logger = require('./services/logger');

// Routes
const tokenRoutes = require('./routes/tokens');
const deployRoutes = require('./routes/deploy');
const solanaRoutes = require('./routes/solana');
const healthRoutes = require('./routes/health');

// Background Tasks
const workers = require('./tasks/workers');

async function startServer() {
    // 1. Initialize Infrastructure
    await dbService.initDB(); // Connects to Postgres
    redisService.initRedis(); // Connects to Redis

    const app = express();
    
    // 2. Middleware
    app.use(helmet());
    app.use(cors({ origin: config.CORS_ORIGINS }));
    app.use(express.json());

    // 3. Dependencies Container
    const deps = {
        db: dbService.getDB(), // Wrapper for Postgres
        globalState: {
            // In-memory fallback if Redis fails, or for non-critical fast access
            asdfTop50Holders: new Set(),
            userExpectedAirdrops: new Map(),
            lastBackendUpdate: Date.now()
        },
        devKeypair: config.PRIVATE_KEY ? require('@solana/web3.js').Keypair.fromSecretKey(new Uint8Array(JSON.parse(config.PRIVATE_KEY))) : null
    };

    // 4. Register Routes
    app.use('/api', tokenRoutes.init(deps));
    app.use('/api/deploy', deployRoutes.init(deps));
    app.use('/api/solana', solanaRoutes.init(deps));
    app.use('/health', healthRoutes);

    // 5. Start Background Workers
    workers.start(deps);

    // 6. Launch
    app.listen(config.PORT, () => {
        logger.info(`HolDex Backend running on port ${config.PORT}`);
    });
}

startServer().catch(err => {
    logger.error('Fatal Server Error', err);
    process.exit(1);
});
