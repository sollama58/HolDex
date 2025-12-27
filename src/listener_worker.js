require('dotenv').config();
const { initDB, getDB } = require('./services/database');
const { connectRedis } = require('./services/redis');
const { startNewTokenListener } = require('./services/new_token_listener');
const growerScanner = require('./tasks/growerScanner'); 
const logger = require('./services/logger');

async function startListener() {
    try {
        logger.info('🛰️ Listener Worker: Initializing...');

        // 1. Initialize Database Connection
        await initDB();
        const db = getDB();
        
        // 2. Initialize Redis (CRITICAL: Listener writes pending tokens, Scanner reads them)
        await connectRedis();

        // 3. Start the Token Listener (Finds new pools on chain)
        logger.info('🛰️ Listener Worker: Connecting to Solana...');
        await startNewTokenListener();

        // 4. Start Grower Scanner (Promotes pending tokens that cross market cap threshold)
        // Runs alongside listener to handle the full ingestion pipeline in one place
        growerScanner.start({ db });
        logger.info("✅ Listener Worker: Grower Scanner Started.");

        // Keep process alive
        process.on('SIGINT', () => {
            logger.info('🛑 Listener Worker: Shutting down...');
            process.exit(0);
        });

    } catch (error) {
        logger.error(`🔥 Listener Worker Fatal Error: ${error.message}`);
        logger.error(error.stack);
        process.exit(1);
    }
}

if (require.main === module) {
    startListener();
}
