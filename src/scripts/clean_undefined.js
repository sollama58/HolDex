require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

async function clean() {
    console.log("🧹 Cleaning up invalid tokens...");
    
    // Delete tokens where name/symbol is literally "undefined" string or "Unknown"
    // CAUTION: This deletes data. Ensure you want this.
    // Or simpler: Update them to trigger a re-fetch on next search.
    
    // Option 1: Hard Delete (Forces re-indexing on next search)
    const res = await pool.query(`DELETE FROM tokens WHERE name = 'undefined' OR symbol = 'undefined' OR name = 'Unknown'`);
    
    console.log(`🗑️  Deleted ${res.rowCount} invalid tokens.`);
    console.log("✅ Done. Restart server.");
    process.exit();
}

clean();
