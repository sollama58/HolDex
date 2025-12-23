/**
 * Auto Seeder Task (Top Volume & PumpSwap LP Focus)
 * 1. Fetches "Top" tokens based on volume/mcap from Pump.fun directly.
 * 2. Strict Filter: Only adds tokens if they are active on PumpSwap (raydium/bonded tokens handled by listener).
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

// --- CORE TASK: SYNC TOP TOKENS ---
async function syncTopTokens(deps) {
    if (isRunning) return;
    isRunning = true;

    logger.info("🏆 AutoSeeder: Syncing Top Volume Tokens (PumpSwap Only)...");

    try {
        // Fetch tokens sorted by Market Cap (Proxy for Volume/Success on Pump.fun)
        // Pump.fun API doesn't expose a direct 'volume' sort in this endpoint publicly, 
        // but 'market_cap' is the standard way to find the top active curves.
        const response = await axios.get(`${PUMP_LIST_URL}?offset=${currentOffset}&limit=${BATCH_SIZE}&sort=market_cap&order=DESC&includeNsfw=true`, {
            headers: API_HEADERS,
            timeout: 15000
        });

        const coins = response.data;

        if (!coins || coins.length === 0) {
            currentOffset = 0; // Reset pagination to start
            isRunning = false;
            return;
        }

        let addedCount = 0;

        for (const coin of coins) {
            // FILTER: Only add tokens that are on the Pump bonding curve (PumpSwap LP).
            // 'complete' is false = Bonding Curve Active (PumpSwap)
            // 'complete' is true  = Raydium/Bonded (We skip these here, handled by NewTokenListener or MetadataUpdater)
            
            // NOTE: If you want to index EVERYTHING, remove the !coin.complete check.
            // But user request specifically said "only ones on the PumpSwap LP" logic implies native curve.
            // Re-reading request: "add the Top tokens by volume... but only ones on the PumpSwap LP"
            // This usually means tokens that are *still* trading on the curve.
            
            // However, often "PumpSwap LP" is interpreted as "Tokens created on Pump". 
            // If you want ALL successful pump tokens, we include completed ones.
            // If you ONLY want pre-bonded curve tokens, we check !coin.complete.
            
            // Decision: Based on "auto-adding of pre-bonded tokens" issue earlier, you likely want 
            // the successful ones. But if the goal is "Only PumpSwap LP", that technically means un-bonded.
            // I will assume you want **active curve tokens** (pre-bond) that have high volume.
            
            if (coin.raydium_pool) continue; // Skip if it has graduated to Raydium
            
            // Market Cap Check
            const mcap = coin.usd_market_cap || coin.market_cap || 0;
            if (mcap < MIN_MARKET_CAP) continue;

            await processCoin(coin);
            addedCount++;
        }

        logger.info(`🏆 AutoSeeder: Scanned batch. Added ${addedCount} active PumpSwap tokens.`);
        
        // Advance offset to scan deeper into the top list
        currentOffset += BATCH_SIZE;
        
        // Reset if we go too deep (e.g. top 1000 is usually enough for "Top")
        if (currentOffset > 1000) currentOffset = 0;

    } catch (e) {
        if (e.response && (e.response.status === 530 || e.response.status === 403)) {
            logger.warn(`🌱 AutoSeeder Blocked (Status ${e.response.status}). Pausing for 60s...`);
            await new Promise(r => setTimeout(r, 60000));
        } else {
            logger.error(`🏆 AutoSeeder Error: ${e.message}`);
        }
    } finally {
        isRunning = false;
    }
}

// Helper to save coin data
async function processCoin(coin) {
    const mcap = coin.usd_market_cap || coin.market_cap || 0;

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
        volume24h: 0, // Will be updated by metadata scanner later
        priceUsd: 0   // Will be updated
    };

    const createdAt = coin.created_timestamp || Date.now();
    await saveTokenData(coin.creator, coin.mint, metadata, createdAt);
}

function start(deps) {
    // Run frequently to keep the "Top" list fresh
    setTimeout(() => syncTopTokens(deps), 5000);
    setInterval(() => syncTopTokens(deps), 15000); // Check every 15s (iterating through pages)
}

module.exports = { start };
