const axios = require('axios');
const logger = require('../services/logger');

let isRunning = false;

// GeckoTerminal Free Tier is ~30 requests per minute.
// We must throttle our updates to avoid 429 Errors.
const DELAY_BETWEEN_TOKENS_MS = 2000; 

async function fetchGeckoTerminalData(mintAddress) {
    try {
        // Fetch pools for this token
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mintAddress}/pools?page=1`;
        const response = await axios.get(url, { timeout: 5000 });
        
        if (!response.data || !response.data.data || response.data.data.length === 0) {
            return null;
        }

        const pools = response.data.data;
        
        // 1. Calculate Aggregate 24h Volume (Sum of all pools)
        let totalVolume24h = 0;
        let maxLiquidity = -1;
        let bestPrice = 0;
        let bestChange24h = 0;

        for (const pool of pools) {
            const attr = pool.attributes;
            
            // Sum Volume
            const vol = parseFloat(attr.volume_usd?.h24 || 0);
            totalVolume24h += vol;

            // Find "Main" Price (based on deepest liquidity pool)
            const liquidity = parseFloat(attr.reserve_in_usd || 0);
            if (liquidity > maxLiquidity) {
                maxLiquidity = liquidity;
                bestPrice = parseFloat(attr.base_token_price_usd || 0);
                bestChange24h = parseFloat(attr.price_change_percentage?.h24 || 0);
            }
        }

        return {
            priceUsd: bestPrice,
            liquidityUsd: maxLiquidity,
            volume24h: totalVolume24h,
            change24h: bestChange24h
        };

    } catch (e) {
        // 404 just means it's a new token not indexed yet
        if (e.response && e.response.status === 429) {
            logger.warn("⚠️ GeckoTerminal Rate Limit! Slowing down...");
            await new Promise(r => setTimeout(r, 5000)); // Cool down
        }
        return null;
    }
}

async function updateMetadata(deps) {
    if (isRunning) return;
    isRunning = true;
    const { db } = deps;
    const now = Date.now();

    try {
        const tokens = await db.all(`SELECT mint, supply, decimals FROM tokens`);
        if (tokens.length > 0) {
            logger.info(`🔄 Syncing Metadata with CoinGecko for ${tokens.length} tokens...`);
        }
        
        for (const t of tokens) {
            try {
                // 1. Fetch External Data
                const geckoData = await fetchGeckoTerminalData(t.mint);

                let volume24h = 0;
                let currentPrice = 0;
                let liquidityUsd = 0;
                let change24h = 0;

                if (geckoData) {
                    volume24h = geckoData.volume24h;
                    currentPrice = geckoData.priceUsd;
                    liquidityUsd = geckoData.liquidityUsd;
                    change24h = geckoData.change24h;
                } else {
                    // Fallback: If Gecko fails, keep existing DB values or calculate from local (Legacy)
                    // For now, we just skip updating to avoid overwriting with 0s if API is down
                    // logger.debug(`Skipping ${t.mint} - No API data`);
                    continue; 
                }

                // 2. Calculate Market Cap
                let marketCap = 0;
                if (currentPrice > 0) {
                    const decimals = t.decimals || 9;
                    let rawSupply = parseFloat(t.supply || '0');
                    if (rawSupply === 0) rawSupply = 1000000000 * Math.pow(10, decimals); // Default 1B supply fallback

                    const divisor = Math.pow(10, decimals);
                    const supply = rawSupply / divisor;
                    marketCap = supply * currentPrice;
                }

                // 3. Commit Updates
                // We use COALESCE/Logic to ensure we don't break things
                await db.run(`
                    UPDATE tokens 
                    SET volume24h = $1, marketCap = $2, priceUsd = $3, 
                        liquidity = $4, change24h = $5, timestamp = $6 
                    WHERE mint = $7
                `, [volume24h, marketCap, currentPrice, liquidityUsd, change24h, now, t.mint]);

                // 4. Also update the 'pools' table for the main pool record if needed
                // (Optional, but keeps the pools table in sync with reality)
                // await db.run(`UPDATE pools SET liquidity_usd = $1, price_usd = $2 WHERE mint = $3`, [liquidityUsd, currentPrice, t.mint]);

            } catch (err) {
                logger.error(`Token Update Failed [${t.mint}]: ${err.message}`);
            }

            // CRITICAL: Respect Rate Limits
            await new Promise(r => setTimeout(r, DELAY_BETWEEN_TOKENS_MS));
        }
    } catch (e) {
        logger.error(`Metadata Cycle Error: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    // Run every 2 minutes (Giving enough time for the loop to finish gracefully)
    setInterval(() => updateMetadata(deps), 120 * 1000);
    
    // Initial start delay
    setTimeout(() => updateMetadata(deps), 5000);
}

module.exports = { start };
