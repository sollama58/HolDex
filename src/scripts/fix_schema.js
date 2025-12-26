require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

async function fix() {
    console.log("🛠️  Schema Fix: Checking for 'decimals' column...");
    try {
        await pool.query(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS decimals INTEGER DEFAULT 9;`);
        console.log("✅ 'decimals' column ensured.");
        
        await pool.query(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS change24h DOUBLE PRECISION DEFAULT 0;`);
        console.log("✅ 'change24h' column ensured.");
        
    } catch (e) {
        console.error("Fix error:", e.message);
    } finally {
        pool.end();
    }
}

fix();
