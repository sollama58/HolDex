require('dotenv').config();
const { initDB } = require('../services/database');
const { startSnapshotter } = require('./tasks/snapshotter');
const logger = require('../services/logger');

async function main() {
    console.log("🔥 HolDEX Background Service Starting...");
    
    // 1. Initialize DB
    await initDB(); 

    // 2. Start ONLY the Snapshotter (Price Updates)
    // The "Sniper" (New Token Listener) has been removed.
    startSnapshotter();

    logger.info("✅ Background Service Running (Snapshotter Only).");
}

main().catch((err) => {
    console.error("Indexer Fatal Error:", err);
    process.exit(1);
});
