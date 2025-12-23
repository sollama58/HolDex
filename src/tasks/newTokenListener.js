/**
 * Auto Seeder Task (Hybrid Mode)
 * 1. Backfills history using offsets (every 10s).
 * 2. Syncs Top 100 Winners every 15 minutes to catch missed high-volume tokens.
 * * Update: Added robust headers to bypass Cloudflare 530/403 blocks.
 */
const axios = require('axios');
const { logger } = require('../services');
const { saveTokenData } = require('../services/database');

// Configuration
const MIN_MARKET_CAP = 10000; 
const BATCH_SIZE = 50;
let currentOffset = 0;
let isRunning = false;

// Pump.fun Endpoints
const PUMP_LIST_URL = 'https://frontend-api.pump.fun/coins';

// Standard Browser Headers to avoid 530/403 blocks
const API_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Origin': 'https://pump.fun',
    'Referer': 'https://pump.fun/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site'
};

// --- TASK 1: BACKFILL HISTORY (Deep Scan) ---
async function seedHistory(deps) {
    if (isRunning) return; 
    isRunning = true;

    try {
        const response = await axios.get(`${PUMP_LIST_URL}?offset=${currentOffset}&limit=${BATCH_SIZE}&sort=created_timestamp&order=DESC&includeNsfw=true`, {
            headers: API_HEADERS,
            timeout: 10000
        });

        const coins = response.data;

        if (!coins || coins.length === 0) {
            currentOffset = 0; // Reset to start
            isRunning = false;
            return;
        }

        for (const coin of coins) {
            // Only process if Bonded (complete)
            if (coin.complete) {
                await processCoin(coin);
            }
        }

        // logger.info(`🌱 AutoSeeder: Backfill offset ${currentOffset} complete.`);
        currentOffset += BATCH_SIZE;

    } catch (e) {
        if (e.response && (e.response.status === 530 || e.response.status === 403)) {
            logger.warn(`🌱 AutoSeeder Blocked (Status ${e.response.status}). Pausing for 60s...`);
            // Add a long delay if blocked to reset reputation
            await new Promise(r => setTimeout(r, 60000));
        } else {
            logger.error(`🌱 AutoSeeder Error: ${e.message}`);
        }
    } finally {
        isRunning = false;
    }
}

// --- TASK 2: TOP 100 WINNERS (Volume/MCap Sync) ---
async function syncTopWinners(deps) {
    logger.info("🏆 AutoSeeder: Syncing Top 100 High-Volume Tokens...");
    
    try {
        // Sort by Market Cap to find the "winners" regardless of age
        const response = await axios.get(`${PUMP_LIST_URL}?offset=0&limit=100&sort=market_cap&order=DESC&includeNsfw=true`, {
            headers: API_HEADERS,
            timeout: 15000
        });

        const coins = response.data;
        if (!coins) return;

        let newCount = 0;
        for (const coin of coins) {
            if (coin.complete) {
                await processCoin(coin);
                newCount++;
            }
        }
        
        if (newCount > 0) logger.info(`🏆 AutoSeeder: Synced ${newCount} Bonded Winners.`);

    } catch (e) {
        logger.error(`🏆 AutoSeeder Top Sync Error: ${e.message}`);
    }
}

// Helper to save coin data
async function processCoin(coin) {
    const mcap = coin.usd_market_cap || coin.market_cap || 0;

    if (mcap < MIN_MARKET_CAP) return;

    const metadata = {
        ticker: coin.symbol,
        name: coin.name,
        description: coin.description || '',
        twitter: coin.twitter || null,
        website: coin.website || null,
        telegram: coin.telegram || null,
        metadataUri: coin.uri,
        image: coin.image_uri,
        isMayhemMode: false,
        marketCap: mcap,
        volume24h: 0, 
        priceUsd: 0   
    };

    const createdAt = coin.created_timestamp || Date.now();
    await saveTokenData(coin.creator, coin.mint, metadata, createdAt);
}

function start(deps) {
    // 1. Backfill Loop (Fast)
    setTimeout(() => seedHistory(deps), 5000);
    setInterval(() => seedHistory(deps), 10000);

    // 2. Top Winners Loop (Every 15 Minutes)
    setTimeout(() => syncTopWinners(deps), 10000); // Run once shortly after boot
    setInterval(() => syncTopWinners(deps), 15 * 60 * 1000); // 15 mins
}

module.exports = { start };
