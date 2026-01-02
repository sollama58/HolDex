#!/usr/bin/env node
/**
 * HolDex Calculator - Module Isolation Test
 * Testing which module causes the crash
 */

console.log('=== CALCULATOR STARTING ===');
console.log('Node version:', process.version);

// Test 1: Basic modules (these should be safe)
console.log('\n[TEST 1] Loading basic modules...');
try {
    require('dotenv').config();
    console.log('  ✓ dotenv OK');
} catch (e) {
    console.log('  ✗ dotenv FAILED:', e.message);
}

try {
    const { Pool } = require('pg');
    console.log('  ✓ pg OK');
} catch (e) {
    console.log('  ✗ pg FAILED:', e.message);
}

try {
    const axios = require('axios');
    console.log('  ✓ axios OK');
} catch (e) {
    console.log('  ✗ axios FAILED:', e.message);
}

// Test 2: App config
console.log('\n[TEST 2] Loading config...');
try {
    const config = require('./config/env');
    console.log('  ✓ config/env OK');
    console.log('    DATABASE_URL:', config.DATABASE_URL ? 'SET' : 'MISSING');
    console.log('    HELIUS_API_KEY:', config.HELIUS_API_KEY ? 'SET' : 'MISSING');
} catch (e) {
    console.log('  ✗ config/env FAILED:', e.message);
}

// Test 3: Logger
console.log('\n[TEST 3] Loading logger...');
try {
    const logger = require('./services/logger');
    console.log('  ✓ services/logger OK');
} catch (e) {
    console.log('  ✗ services/logger FAILED:', e.message);
}

// Test 4: Services index - DISABLED
console.log('\n[TEST 4] services/index SKIPPED');

// Test 5: priceService - DISABLED
console.log('\n[TEST 5] priceService SKIPPED');

// Test 6: kScoreUpdater - DISABLED to isolate crash
console.log('\n[TEST 6] kScoreUpdater SKIPPED (testing without it)');

console.log('\n=== ALL MODULES LOADED ===');
console.log('If you see this, all require() calls succeeded.\n');

// Simple keep-alive loop
let counter = 0;
function heartbeat() {
    counter++;
    const mem = process.memoryUsage();
    console.log(`[${new Date().toISOString()}] Heartbeat #${counter} | RSS: ${Math.round(mem.rss / 1024 / 1024)}MB | Heap: ${Math.round(mem.heapUsed / 1024 / 1024)}MB`);
}

setInterval(heartbeat, 15000);

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('Received SIGTERM, exiting...');
    process.exit(0);
});

process.on('uncaughtException', (err) => {
    console.error('UNCAUGHT EXCEPTION:', err.message);
    console.error(err.stack);
});

process.on('unhandledRejection', (reason) => {
    console.error('UNHANDLED REJECTION:', reason);
});

console.log('Entering keep-alive loop...');
process.stdin.resume();
