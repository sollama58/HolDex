require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

async function migrate() {
    console.log("⏳ Starting TimescaleDB Migration...");

    try {
        const client = await pool.connect();

        // 1. Enable Extension
        console.log("-> Enabling TimescaleDB extension...");
        await client.query('CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;');

        // 2. Check if already a hypertable
        const check = await client.query(`
            SELECT * FROM timescaledb_information.hypertables 
            WHERE hypertable_name = 'candles_1m';
        `);

        if (check.rows.length > 0) {
            console.log("✅ 'candles_1m' is already a hypertable.");
        } else {
            console.log("-> Converting 'candles_1m' to Hypertable (this may take a moment)...");
            
            // Note: migrate_data=true moves existing rows into the new partitions
            await client.query(`
                SELECT create_hypertable(
                    'candles_1m', 
                    'timestamp', 
                    chunk_time_interval => 86400000, -- 1 day chunks
                    migrate_data => true,
                    if_not_exists => true
                );
            `);
            console.log("✅ Conversion Complete.");
        }

        // 3. Enable Native Compression (Critical for scalability)
        // Compresses data older than 1 day to save 90% space
        console.log("-> Enabling Compression...");
        try {
            await client.query(`
                ALTER TABLE candles_1m SET (
                    timescaledb.compress, 
                    timescaledb.compress_segmentby = 'pool_address'
                );
            `);
            
            await client.query(`
                SELECT add_compression_policy('candles_1m', INTERVAL '1 day');
            `);
            console.log("✅ Compression Policy Enabled (Data > 1 day will be compressed).");
        } catch (e) {
            console.log("⚠️ Compression might already be enabled: " + e.message);
        }

        client.release();
        console.log("🚀 Level 2 Database Upgrade Complete!");

    } catch (e) {
        console.error("❌ Migration Failed:", e.message);
        console.error("Ensure you are running the 'timescale/timescaledb' docker image, not standard postgres.");
    } finally {
        await pool.end();
        process.exit();
    }
}

migrate();
