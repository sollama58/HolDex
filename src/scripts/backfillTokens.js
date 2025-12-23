/**
 * Backfill Script
 * Populates the database with historical Pump.fun tokens.
 * Run this once to seed your database.
 * * Usage: node src/scripts/backfillTokens.js
 */
require('dotenv').config();
const axios = require('axios');
const { initDB, saveTokenData } = require('../services/database');
const { logger } = require('../services');

// CONFIGURATION
const BATCH_SIZE = 50;
const TOTAL_TO_FETCH = 2000; // Adjust: How many historical tokens do you want?
const DELAY_MS = 1000; // Delay between requests to avoid rate limits

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function runBackfill() {
    console.log('⏳ Initializing Database for Backfill...');
    await initDB();

    let offset = 0;
    let totalAdded = 0;

    console.log(`🚀 Starting Backfill: Aiming for ${TOTAL_TO_FETCH} tokens...`);

    while (offset < TOTAL_TO_FETCH) {
        try {
            // Pump.fun API for paginated history
            const url = `https://frontend-api.pump.fun/coins?offset=${offset}&limit=${BATCH_SIZE}&sort=created_timestamp&order=DESC&includeNsfw=true`;
            
            const response = await axios.get(url, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/json'
                },
                timeout: 8000
            });

            const coins = response.data;
            if (!coins || coins.length === 0) {
                console.log('⚠️ No more coins returned from API. Stopping.');
                break;
            }

            // Process Batch
            for (const coin of coins) {
                const metadata = {
                    ticker: coin.symbol,
                    name: coin.name,
                    description: coin.description || '',
                    twitter: coin.twitter || null,
                    website: coin.website || null,
                    telegram: coin.telegram || null,
                    metadataUri: coin.uri,
                    image: coin.image_uri,
                    isMayhemMode: false
                };

                // The DB service handles "ON CONFLICT DO UPDATE", so duplicates are safe
                await saveTokenData(coin.creator, coin.mint, metadata);
            }

            totalAdded += coins.length;
            offset += BATCH_SIZE;

            console.log(`✅ Processed batch: ${offset}/${TOTAL_TO_FETCH} (Total: ${totalAdded})`);
            
            // Respect Rate Limits
            await delay(DELAY_MS);

        } catch (e) {
            console.error(`❌ Error at offset ${offset}:`, e.message);
            // If 429 (Rate Limit), wait longer
            if (e.response && e.response.status === 429) {
                console.log('⏳ Hit Rate Limit. Pausing for 30 seconds...');
                await delay(30000);
            } else {
                // Skip this batch and try next? Or stop?
                // Let's stop to be safe
                break;
            }
        }
    }

    console.log('🎉 Backfill Complete!');
    process.exit(0);
}

// Handle script execution
runBackfill().catch(e => {
    console.error('Fatal Error:', e);
    process.exit(1);
});
