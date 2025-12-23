/**
 * Auto Seeder Task (Hybrid Mode)
 * 1. Backfills history using offsets (every 10s).
 * 2. Syncs Top 100 Winners every 15 minutes to catch missed high-volume tokens.
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

// --- TASK 1: BACKFILL HISTORY (Deep Scan) ---
async function seedHistory(deps) {
    if (isRunning) return; 
    isRunning = true;

    try {
        const response = await axios.get(`${PUMP_LIST_URL}?offset=${currentOffset}&limit=${BATCH_SIZE}&sort=created_timestamp&order=DESC&includeNsfw=true`, {
            timeout: 10000
        });

        const coins = response.data;

        if (!coins || coins.length === 0) {
            currentOffset = 0; // Reset to start
            isRunning = false;
            return;
        }

        for (const coin of coins) {
            await processCoin(coin);
        }

        // logger.info(`🌱 AutoSeeder: Backfill offset ${currentOffset} complete.`);
        currentOffset += BATCH_SIZE;

    } catch (e) {
        logger.error(`🌱 AutoSeeder Error: ${e.message}`);
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
            timeout: 15000
        });

        const coins = response.data;
        if (!coins) return;

        let newCount = 0;
        for (const coin of coins) {
            await processCoin(coin);
            newCount++;
        }
        
        logger.info(`🏆 AutoSeeder: Synced ${newCount} Top Tokens.`);

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
