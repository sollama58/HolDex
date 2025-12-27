require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

async function fix() {
    console.log("🛠️  Schema Fix: Checking for missing columns...");
    try {
        // Ensure decimals
        await pool.query(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS decimals INTEGER DEFAULT 9;`);
        console.log("✅ 'decimals' column ensured.");
        
        // Ensure change24h
        await pool.query(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS change24h DOUBLE PRECISION DEFAULT 0;`);
        console.log("✅ 'change24h' column ensured.");
        
        // Ensure holders (The main missing piece)
        await pool.query(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS holders INTEGER DEFAULT 0;`);
        console.log("✅ 'holders' column ensured.");

        // Ensure marketCap (In case it wasn't added by original scripts)
        await pool.query(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS marketCap DOUBLE PRECISION DEFAULT 0;`);
        console.log("✅ 'marketCap' column ensured.");

        // Ensure liquidity
        await pool.query(`ALTER TABLE tokens ADD COLUMN IF NOT EXISTS liquidity DOUBLE PRECISION DEFAULT 0;`);
        console.log("✅ 'liquidity' column ensured.");
        
    } catch (e) {
        console.error("Fix error:", e.message);
    } finally {
        pool.end();
    }
}

fix();
