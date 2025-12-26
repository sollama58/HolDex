require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

async function hardReset() {
    console.log("🔥 HolDEX Database HARD RESET");
    console.log("--------------------------------");
    
    try {
        console.log("♻️  Dropping all tables...");
        
        await pool.query(`DROP TABLE IF EXISTS k_scores CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS token_updates CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS candles_1m CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS active_trackers CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS pools CASCADE;`);
        await pool.query(`DROP TABLE IF EXISTS tokens CASCADE;`);

        console.log("✅ All tables dropped successfully.");
        console.log("👉 Please restart your server now.");

    } catch (error) {
        console.error("❌ Reset Failed:", error.message);
    } finally {
        await pool.end();
    }
}

hardReset();
