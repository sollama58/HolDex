#!/usr/bin/env node
/**
 * HolDex Calculator - Copy of the WORKING minimal version
 */

console.log('=== CALCULATOR STARTING ===');
console.log('Node version:', process.version);
console.log('Platform:', process.platform);
console.log('Memory:', JSON.stringify(process.memoryUsage()));

console.log('DATABASE_URL:', process.env.DATABASE_URL ? 'SET' : 'MISSING');
console.log('HELIUS_API_KEY:', process.env.HELIUS_API_KEY ? 'SET' : 'MISSING');

let counter = 0;

function heartbeat() {
    counter++;
    const mem = process.memoryUsage();
    console.log(`[${new Date().toISOString()}] Heartbeat #${counter} | RSS: ${Math.round(mem.rss / 1024 / 1024)}MB | Heap: ${Math.round(mem.heapUsed / 1024 / 1024)}MB`);
}

setInterval(heartbeat, 10000);

setTimeout(async () => {
    console.log('=== TESTING DB CONNECTION ===');
    try {
        const { Pool } = require('pg');
        const pool = new Pool({
            connectionString: process.env.DATABASE_URL,
            ssl: { rejectUnauthorized: false },
            max: 3
        });

        const result = await pool.query('SELECT COUNT(*) as count FROM tokens');
        console.log('DB OK - Token count:', result.rows[0].count);
        await pool.end();
    } catch (err) {
        console.error('DB ERROR:', err.message);
    }
}, 5000);

console.log('=== ENTERING MAIN LOOP ===');

process.on('SIGTERM', () => {
    console.log('Received SIGTERM, exiting...');
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('Received SIGINT, exiting...');
    process.exit(0);
});

process.stdin.resume();
