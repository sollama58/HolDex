require('dotenv').config();
const { initDB, getDB } = require('../services/database');

(async () => {
    try {
        console.log("🔌 Connecting to Database...");
        await initDB();
        const db = getDB();

        console.log("🛠️ Adding 'updated_at' column to 'tokens' table...");
        
        // Add the column if it doesn't exist. 
        // We default to CURRENT_TIMESTAMP so existing rows aren't null.
        await db.run(`
            ALTER TABLE tokens 
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        `);

        // Index it for performance since we sort by it
        await db.run(`
            CREATE INDEX IF NOT EXISTS idx_tokens_updated_at ON tokens(updated_at ASC);
        `);

        console.log("✅ Migration Success: 'updated_at' column added.");
        process.exit(0);
    } catch (e) {
        console.error(`❌ Migration Failed: ${e.message}`);
        process.exit(1);
    }
})();
