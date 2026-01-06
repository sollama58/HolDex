require('dotenv').config();
const { initDB, getDB } = require('../services/database');
const _logger = require('../services/logger');

(async () => {
    try {
        console.log("🔌 Connecting to Database...");
        await initDB();
        const db = getDB();

        console.log("🛠️ Adding 'last_holder_check' column to 'tokens' table...");
        
        // Add the column if it doesn't exist
        await db.run(`
            ALTER TABLE tokens 
            ADD COLUMN IF NOT EXISTS last_holder_check BIGINT DEFAULT 0
        `);

        console.log("✅ Migration Success: Column added.");
        process.exit(0);
    } catch (e) {
        console.error(`❌ Migration Failed: ${e.message}`);
        process.exit(1);
    }
})();
