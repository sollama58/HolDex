require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

async function repair() {
    console.log("🛠️  HolDEX Database Repair Tool");
    console.log("--------------------------------");
    
    try {
        // 1. Check if 'pools' exists and has the wrong schema
        const res = await pool.query(`
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'pools' AND column_name = 'address';
        `);

        if (res.rowCount === 0) {
            console.log("⚠️  Detected invalid 'pools' table (missing 'address' column).");
            console.log("♻️  Dropping incompatible tables...");
            
            // Drop tables to allow initDB to recreate them correctly
            await pool.query(`DROP TABLE IF EXISTS pools CASCADE;`);
            console.log("✅ Dropped 'pools'");
            
            // We also drop candles to ensure the foreign keys/indexes align
            await pool.query(`DROP TABLE IF EXISTS candles_1m CASCADE;`);
            console.log("✅ Dropped 'candles_1m'");
            
            console.log("--------------------------------");
            console.log("🎉 Repair Complete. Please restart your application.");
            console.log("   (The tables will be recreated automatically on startup)");
        } else {
            console.log("✅ Schema looks correct. No repair needed.");
        }

    } catch (error) {
        console.error("❌ Repair Failed:", error.message);
    } finally {
        await pool.end();
    }
}

repair();
