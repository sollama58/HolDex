require('dotenv').config();
const { initDB } = require('./services/database');
const { connectRedis } = require('./services/redis');
const { startNewTokenListener } = require('./services/new_token_listener');
const logger = require('./services/logger');

async function startWorker() {
    try {
        logger.info('🛰️ Background Worker: Initializing...');

        // 1. Initialize Database Connection
        await initDB();
        
        // 2. Initialize Redis (if needed by listener/queue)
        await connectRedis();

        // 3. Start the Token Listener
        logger.info('🛰️ Background Worker: Starting Token Listener...');
        await startNewTokenListener();

        // Keep process alive
        process.on('SIGINT', () => {
            logger.info('🛑 Background Worker: Shutting down...');
            process.exit(0);
        });

    } catch (error) {
        logger.error(`🔥 Background Worker Fatal Error: ${error.message}`);
        logger.error(error.stack);
        process.exit(1);
    }
}

startWorker();
