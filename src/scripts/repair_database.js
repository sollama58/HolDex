require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

async function repair() {
    console.log("🔥 HolDEX Database HARD RESET Tool");
    console.log("--------------------------------");
    console.log("WARNING: This will delete ALL data (tokens, pools, candles, etc).");
    console.log("Starting in 3 seconds... Ctrl+C to cancel.");
    
    await new Promise(r => setTimeout(r, 3000));

    try {
        console.log("♻️  Dropping all HolDEX tables...");
        
        // Drop in specific order to handle Foreign Key constraints
        await pool.query(`DROP TABLE IF EXISTS k_scores CASCADE;`);
        console.log("✅ Dropped 'k_scores'");

        await pool.query(`DROP TABLE IF EXISTS token_updates CASCADE;`);
        console.log("✅ Dropped 'token_updates'");
        
        await pool.query(`DROP TABLE IF EXISTS candles_1m CASCADE;`);
        console.log("✅ Dropped 'candles_1m'");

        await pool.query(`DROP TABLE IF EXISTS active_trackers CASCADE;`);
        console.log("✅ Dropped 'active_trackers'");

        await pool.query(`DROP TABLE IF EXISTS pools CASCADE;`);
        console.log("✅ Dropped 'pools'");
        
        await pool.query(`DROP TABLE IF EXISTS tokens CASCADE;`);
        console.log("✅ Dropped 'tokens'");

        console.log("--------------------------------");
        console.log("🎉 Reset Complete. Database is clean.");
        console.log("👉 Restart your API/Indexer to recreate tables automatically.");

    } catch (error) {
        console.error("❌ Reset Failed:", error.message);
    } finally {
        await pool.end();
    }
}

repair();
