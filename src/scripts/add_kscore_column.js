const { initDB, getDB } = require('../services/database');
const logger = require('../services/logger');

(async () => {
    logger.info("🔧 Migration: Adding K-Score tracking columns...");
    await initDB();
    const db = getDB();
    try {
        await db.run(`ALTER TABLE tokens ADD COLUMN last_k_score_update BIGINT DEFAULT 0;`);
        logger.info("✅ Added last_k_score_update column");
    } catch (e) {
        logger.info("ℹ️ Column likely exists (Error ignored): " + e.message);
    }
    
    // Also ensure the history table exists if running on an old DB
    try {
        await db.run(`
            CREATE TABLE IF NOT EXISTS holders_history (
                mint TEXT,
                count INTEGER,
                timestamp BIGINT,
                PRIMARY KEY (mint, timestamp)
            );
        `);
        logger.info("✅ Verified holders_history table");
    } catch (e) {
        logger.error("❌ Failed to create holders_history: " + e.message);
    }

    process.exit(0);
})();
