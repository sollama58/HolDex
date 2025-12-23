/**
 * Auto Seeder Task (Backfill Engine)
 * Switches to Pump.fun API to support deep pagination (offsets).
 * This allows backfilling thousands of older tokens reliably.
 */
const axios = require('axios');
const { logger } = require('../services');
const { saveTokenData, getDB } = require('../services/database');

// Configuration
const MIN_MARKET_CAP = 10000; 
const BATCH_SIZE = 50;
let currentOffset = 0;
let isRunning = false;

// We use a dedicated Pump.fun endpoint for history
const PUMP_LIST_URL = 'https://frontend-api.pump.fun/coins';

async function seedHistory(deps) {
    if (isRunning) return; // Prevent overlapping runs
    isRunning = true;

    try {
        // 1. Get current count to determine offset (optional, or just keep incrementing)
        // For now, we will just keep a running offset in memory. 
        // If the server restarts, it resets to 0 (checking newest first), which is fine.
        
        logger.info(`🌱 AutoSeeder: Fetching batch at offset ${currentOffset}...`);

        const response = await axios.get(`${PUMP_LIST_URL}?offset=${currentOffset}&limit=${BATCH_SIZE}&sort=created_timestamp&order=DESC&includeNsfw=true`, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json'
            },
            timeout: 10000
        });

        const coins = response.data;

        if (!coins || coins.length === 0) {
            logger.info("🌱 AutoSeeder: No coins returned. Resetting offset to 0.");
            currentOffset = 0; // Loop back to start (newest)
            isRunning = false;
            return;
        }

        let addedCount = 0;
        let skippedCount = 0;

        for (const coin of coins) {
            // Market Cap Check (Pump.fun API returns 'market_cap' or 'usd_market_cap')
            const mcap = coin.usd_market_cap || coin.market_cap || 0;

            if (mcap < MIN_MARKET_CAP) {
                skippedCount++;
                continue;
            }

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
                volume24h: 0, // Pump API might not have 24h vol easily, defaults to 0
                priceUsd: 0   // Will be updated by metadataUpdater later
            };

            // Fix: Use created_timestamp from Pump.fun
            const createdAt = coin.created_timestamp || Date.now();

            await saveTokenData(coin.creator, coin.mint, metadata, createdAt);
            addedCount++;
        }

        logger.info(`🌱 AutoSeeder: Processed batch. Added: ${addedCount}, Skipped Low Cap: ${skippedCount}. Next Offset: ${currentOffset + BATCH_SIZE}`);
        
        // Move offset forward to get older tokens next time
        currentOffset += BATCH_SIZE;

    } catch (e) {
        logger.error(`🌱 AutoSeeder Error: ${e.message}`);
        // If 429, back off?
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    // Run immediately
    setTimeout(() => seedHistory(deps), 5000);

    // Run frequently (every 10 seconds) to churn through the backlog quickly
    // Pump.fun API is robust enough for this rate.
    setInterval(() => seedHistory(deps), 10000);
}

module.exports = { start };
