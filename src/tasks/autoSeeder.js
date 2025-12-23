/**
 * Auto Seeder Task
 * 1. Runs on startup to backfill tokens > $10k Market Cap.
 * 2. Runs periodically to discover tokens that recently grew > $10k.
 */
const axios = require('axios');
const { logger } = require('../services');
const { saveTokenData } = require('../services/database');

const MIN_MARKET_CAP = 10000; // $10,000 Threshold
const API_URL = 'https://frontend-api.pump.fun/coins';

async function seedHighValueTokens(deps) {
    logger.info("🌱 AutoSeeder: Starting scan for tokens > $10k MC...");
    
    let offset = 0;
    let limit = 50;
    let keepScanning = true;
    let addedCount = 0;

    while (keepScanning) {
        try {
            // Sort by Market Cap DESC to get the biggest tokens first
            const response = await axios.get(API_URL, {
                params: {
                    offset,
                    limit,
                    sort: 'market_cap',
                    order: 'DESC',
                    includeNsfw: true
                },
                timeout: 10000
            });

            const coins = response.data;
            if (!coins || coins.length === 0) {
                keepScanning = false;
                break;
            }

            for (const coin of coins) {
                // Check Market Cap Threshold
                const mcap = coin.usd_market_cap || 0;
                
                if (mcap < MIN_MARKET_CAP) {
                    // Since we sort DESC, once we hit < 10k, all subsequent coins are also < 10k.
                    // We can safely stop scanning.
                    keepScanning = false;
                    break; // Break the for-loop
                }

                // Prepare Metadata
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
                    marketCap: mcap // Pass initial mcap
                };

                // Save (Upsert)
                // The DB logic ensures we don't delete existing tokens, complying with "once in, stay in"
                await saveTokenData(coin.creator, coin.mint, metadata);
                addedCount++;
            }

            if (!keepScanning) break;

            offset += limit;
            
            // Safety break to prevent infinite loops in weird API states
            if (offset > 5000) {
                logger.info("🌱 AutoSeeder: Reached safety limit (5000 tokens). Stopping.");
                break;
            }

            // Respect Rate Limits
            await new Promise(r => setTimeout(r, 500)); 

        } catch (e) {
            logger.error(`🌱 AutoSeeder Error at offset ${offset}: ${e.message}`);
            // If API fails, stop this cycle
            keepScanning = false;
        }
    }

    if (addedCount > 0) {
        logger.info(`🌱 AutoSeeder: Cycle Complete. Synced ${addedCount} tokens > $10k.`);
    } else {
        logger.info("🌱 AutoSeeder: Cycle Complete. No new tokens found.");
    }
}

function start(deps) {
    // Run 5 seconds after boot
    setTimeout(() => seedHighValueTokens(deps), 5000);

    // Run every 2 minutes to catch tokens that just pumped
    setInterval(() => seedHighValueTokens(deps), 120000);
}

module.exports = { start };
