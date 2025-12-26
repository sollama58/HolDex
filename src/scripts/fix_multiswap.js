require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

async function fixSchema() {
    console.log("🛠️  HolDEX Multi-Pool Schema Update");
    console.log("-----------------------------------");
    
    try {
        // 1. Remove the restrictive unique constraint (mint, dex)
        // We handle this via try/catch in case it's already gone or named differently
        try {
            await pool.query(`ALTER TABLE pools DROP CONSTRAINT IF EXISTS pools_mint_dex_key;`);
            console.log("✅ Removed single-pool constraint.");
        } catch (e) {
            console.log("⚠️  Constraint might not exist, skipping.");
        }

        // 2. Add Liquidity and Volume tracking to the POOLS table
        // This allows us to sort by liquidity to find the "Main" pool
        await pool.query(`ALTER TABLE pools ADD COLUMN IF NOT EXISTS liquidity_usd DOUBLE PRECISION DEFAULT 0;`);
        await pool.query(`ALTER TABLE pools ADD COLUMN IF NOT EXISTS volume_24h DOUBLE PRECISION DEFAULT 0;`);
        await pool.query(`ALTER TABLE pools ADD COLUMN IF NOT EXISTS price_usd DOUBLE PRECISION DEFAULT 0;`);
        
        console.log("✅ Added liquidity/volume columns to 'pools'.");
        console.log("🎉 Schema Update Complete.");

    } catch (error) {
        console.error("❌ Update Failed:", error.message);
    } finally {
        await pool.end();
    }
}

fixSchema();
