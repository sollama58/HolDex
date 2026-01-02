#!/usr/bin/env node
/**
 * HolDex Calculator - Minimal Diagnostic Version
 * Just to test if basic Node.js works on Render
 */

console.log('=== CALCULATOR STARTING ===');
console.log('Node version:', process.version);
console.log('Platform:', process.platform);
console.log('Memory:', JSON.stringify(process.memoryUsage()));

// Check env vars
console.log('DATABASE_URL:', process.env.DATABASE_URL ? 'SET' : 'MISSING');
console.log('HELIUS_API_KEY:', process.env.HELIUS_API_KEY ? 'SET' : 'MISSING');

// Simple keep-alive loop
let counter = 0;

function heartbeat() {
    counter++;
    const mem = process.memoryUsage();
    console.log(`[${new Date().toISOString()}] Heartbeat #${counter} | RSS: ${Math.round(mem.rss / 1024 / 1024)}MB | Heap: ${Math.round(mem.heapUsed / 1024 / 1024)}MB`);
}

// Heartbeat every 10 seconds
setInterval(heartbeat, 10000);

// Test DB connection after 5 seconds
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

// Test GeckoTerminal after 15 seconds
setTimeout(async () => {
    console.log('=== TESTING GECKO API ===');
    try {
        const axios = require('axios');
        const res = await axios.get('https://api.geckoterminal.com/api/v2/networks/solana/tokens/So11111111111111111111111111111111111111112', { timeout: 5000 });
        console.log('Gecko OK - SOL price:', res.data?.data?.attributes?.price_usd);
    } catch (err) {
        console.error('Gecko ERROR:', err.message);
    }
}, 15000);

// Log that we're running
console.log('=== ENTERING MAIN LOOP ===');
console.log('Will heartbeat every 10 seconds...');

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('Received SIGTERM, exiting...');
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('Received SIGINT, exiting...');
    process.exit(0);
});

// Keep process alive
process.stdin.resume();
