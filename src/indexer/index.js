require('dotenv').config();
const { initDB } = require('../services/database');
const { startPumpListener } = require('./listeners/pumpfun.js');
const { startSnapshotter } = require('./tasks/snapshotter.js');
const logger = require('../services/logger');

async function main() {
    console.log("🔥 HolDEX Internal Indexer v1.0 Starting...");
    
    // 1. Initialize DB (Creates new tables if missing)
    await initDB(); 

    // 2. Start Services
    
    // A. Real-time Sniping (Pump.fun)
    // Listens for 'Create' instructions via WebSocket
    startPumpListener();

    // B. Price Snapshots (Every 60s)
    // Polls reserves of active pools to build 1m candles
    startSnapshotter();

    logger.info("✅ Indexer Services Running.");
    
    // Keep process alive
    process.on('SIGINT', () => {
        logger.info('Indexer shutting down...');
        process.exit();
    });
}

main().catch((err) => {
    console.error("Indexer Fatal Error:", err);
    process.exit(1);
});
