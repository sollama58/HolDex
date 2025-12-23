/**
 * Auto Seeder (Pump.fun Logic)
 * Ensures we only index the primary Pump.fun bonding curve pool initially.
 */
const axios = require('axios');
const { logger } = require('../services');
const { saveTokenData } = require('../services/database');

const MIN_MARKET_CAP = 10000; 
const BATCH_SIZE = 50;
let currentOffset = 0;
let isRunning = false;

const PUMP_LIST_URL = 'https://frontend-api.pump.fun/coins';

// --- TASK 1: BACKFILL HISTORY ---
async function seedHistory(deps) {
    if (isRunning) return; 
    isRunning = true;

    try {
        const response = await axios.get(`${PUMP_LIST_URL}?offset=${currentOffset}&limit=${BATCH_SIZE}&sort=created_timestamp&order=DESC&includeNsfw=true`, {
            timeout: 10000
        });

        const coins = response.data;

        if (!coins || coins.length === 0) {
            currentOffset = 0; 
            isRunning = false;
            return;
        }

        for (const coin of coins) {
            // Strict: Only process if it's the official pump curve state
            // (Pump API implies this, but we filter for completeness/bonding)
            if (coin.complete) {
                await processCoin(coin);
            }
        }

        currentOffset += BATCH_SIZE;

    } catch (e) {
        logger.error(`🌱 AutoSeeder Error: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

// --- TASK 2: TOP 100 WINNERS ---
async function syncTopWinners(deps) {
    logger.info("🏆 AutoSeeder: Syncing Top 100 Bonded Tokens...");
    
    try {
        const response = await axios.get(`${PUMP_LIST_URL}?offset=0&limit=100&sort=market_cap&order=DESC&includeNsfw=true`, {
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

    // Use creation timestamp.
    // Since this is the Pump API, we are guaranteed this is the Pump contract/pool.
    const createdAt = coin.created_timestamp || Date.now();
    
    // Save/Upsert based on MINT address (Unique Key)
    await saveTokenData(coin.creator, coin.mint, metadata, createdAt);
}

function start(deps) {
    setTimeout(() => seedHistory(deps), 5000);
    setInterval(() => seedHistory(deps), 10000);

    setTimeout(() => syncTopWinners(deps), 10000); 
    setInterval(() => syncTopWinners(deps), 15 * 60 * 1000); 
}

module.exports = { start };
