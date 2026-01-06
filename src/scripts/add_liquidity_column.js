const { getDB } = require('../services/database');
const _logger = require('../services/logger');

(async () => {
    try {
        const db = getDB();
        console.log("🛠️  Migrating Database Schema...");

        // Try adding the column. 
        // Note: 'IF NOT EXISTS' is supported in Postgres 9.6+ and newer SQLite.
        // If this fails on older SQLite, we catch the error.
        
        try {
            await db.run(`ALTER TABLE tokens ADD COLUMN liquidity NUMERIC DEFAULT 0;`);
            console.log("✅ Added 'liquidity' column to 'tokens' table.");
        } catch (e) {
            if (e.message.includes('duplicate column') || e.message.includes('exists')) {
                console.log("⚠️  Column 'liquidity' already exists.");
            } else {
                console.error("❌ Failed to alter table:", e.message);
            }
        }

        // Optional: Backfill liquidity from pools if possible
        // This is a rough update to populate existing 0s
        console.log("🔄 Backfilling liquidity data from pools...");
        
        // This query assumes a Postgres-like syntax for UPDATE-FROM or SQLite correlated subquery.
        // We'll use a safer correlated subquery that works in both.
        await db.run(`
            UPDATE tokens 
            SET liquidity = (
                SELECT liquidity_usd 
                FROM pools 
                WHERE pools.mint = tokens.mint 
                ORDER BY liquidity_usd DESC 
                LIMIT 1
            )
            WHERE liquidity IS NULL OR liquidity = 0;
        `);

        console.log("✅ Backfill complete.");
        process.exit(0);

    } catch (err) {
        console.error("Fatal Error:", err);
        process.exit(1);
    }
})();
