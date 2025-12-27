require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

async function optimize() {
    console.log("⚡ Optimizing Database Indexes...");
    
    try {
        const client = await pool.connect();
        
        // 1. Critical Index for Charts & Aggregation
        // The DESC order helps finding the 'latest' candle efficiently
        console.log("-> Creating idx_candles_address_time...");
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_candles_address_time 
            ON candles_1m (pool_address, timestamp DESC);
        `);

        // 2. Index for the new Smart Polling logic
        console.log("-> Creating idx_trackers_check...");
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_trackers_check 
            ON active_trackers (last_check ASC);
        `);

        console.log("✅ Optimization Complete.");
        client.release();
    } catch (e) {
        console.error("❌ Optimization Failed:", e.message);
    } finally {
        await pool.end();
        process.exit();
    }
}

optimize();
