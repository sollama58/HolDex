require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

/**
 * Prune Candles Script
 * --------------------
 * Even if the chart doesn't display 1-minute candles, the backend needs them
 * to calculate % changes (change24h, change1h) and to aggregate data for
 * the 5m/1h charts on the fly.
 * * Strategy: Keep 7 days of 1m data. This is enough for:
 * 1. 24h stats
 * 2. High-res recent charts
 * 3. 5m aggregations for the last week
 */

const RETENTION_DAYS = 7; 

async function prune() {
    console.log(`🧹 Pruning candles older than ${RETENTION_DAYS} days...`);
    
    try {
        const client = await pool.connect();
        
        const cutoff = Date.now() - (RETENTION_DAYS * 24 * 60 * 60 * 1000);
        
        const res = await client.query(`
            DELETE FROM candles_1m 
            WHERE timestamp < $1
        `, [cutoff]);
        
        console.log(`🗑️  Deleted ${res.rowCount} old candle rows.`);
        console.log("✅ Pruning Complete.");
        
        // Optional: Vacuum to reclaim space (Postgres specific)
        // await client.query('VACUUM ANALYZE candles_1m');
        
        client.release();
    } catch (e) {
        console.error("❌ Pruning Failed:", e.message);
    } finally {
        await pool.end();
        process.exit();
    }
}

prune();
