/**
 * New Token Listener
 * Watches for new launches on Pump.fun and populates the database.
 * FILTER: Only allows tokens > $10k Market Cap.
 * INTERVAL: Checks every 2 minutes.
 */
const axios = require('axios');
const { logger } = require('../services');
const { saveTokenData } = require('../services/database');

const PUMP_LATEST_API = 'https://frontend-api.pump.fun/coins/latest';
const MIN_MARKET_CAP = 10000;

// Set of known mints to prevent DB hammering
// We keep a history to avoid reprocessing, but clear it if it gets too large
const knownMints = new Set();
const MAX_HISTORY = 1000;

async function checkNewTokens(deps) {
    try {
        const response = await axios.get(PUMP_LATEST_API, {
            headers: { 
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json' 
            },
            timeout: 8000
        });

        const newCoins = response.data;
        if (!Array.isArray(newCoins)) return;

        // Process oldest first to maintain sequence
        const reversedCoins = [...newCoins].reverse();
        let addedCount = 0;
        let skippedCount = 0;

        for (const coin of reversedCoins) {
            if (knownMints.has(coin.mint)) continue;

            // --- FILTER: MARKET CAP > $10,000 ---
            const mcap = coin.usd_market_cap || 0;
            
            if (mcap < MIN_MARKET_CAP) {
                // Track that we saw it, but don't save to DB yet
                // If it pumps later, the 'autoSeeder' task (which sorts by Mcap) should catch it
                knownMints.add(coin.mint); 
                skippedCount++;
                continue; 
            }

            // Map API data
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
                marketCap: mcap
            };

            await saveTokenData(coin.creator, coin.mint, metadata);
            
            knownMints.add(coin.mint);
            addedCount++;
            logger.info(`💎 HIGH VALUE MINT: ${coin.symbol} ($${Math.floor(mcap)})`);
        }

        if (addedCount > 0) {
            logger.info(`Token Scan: Added ${addedCount} high-value tokens. Skipped ${skippedCount} low-cap.`);
        }

        // Memory Management
        if (knownMints.size > MAX_HISTORY) {
            knownMints.clear();
        }

    } catch (e) {
        logger.error('NewTokenListener Error:', { error: e.message });
    }
}

function start(deps) {
    logger.info("🚀 New Token Listener started (> $10k MC, 2m Interval)");
    
    // Run immediately
    checkNewTokens(deps);
    
    // Run every 2 minutes (120,000 ms)
    setInterval(() => checkNewTokens(deps), 120000);
}

module.exports = { start };
