const { getDB } = require('../services/database');
const { logger } = require('../services');
const { fetchSolscanData } = require('../services/solscan');

let isRunning = false;

async function updateMetadata(deps) {
    if (isRunning) return;
    isRunning = true;
    const { db } = deps;
    const now = Date.now();
    
    // Time windows for internal calculations (Price History)
    const time24h = now - (24 * 60 * 60 * 1000);
    const time1h = now - (60 * 60 * 1000);
    const time5m = now - (5 * 60 * 1000);

    try {
        const tokens = await db.all(`SELECT mint, supply, decimals FROM tokens`);
        
        for (const t of tokens) {
            try {
                // 1. Fetch External Data FIRST (Priority for Volume)
                // We rely on Solscan for 24h Volume as requested
                let solscan = null;
                try {
                    solscan = await fetchSolscanData(t.mint);
                } catch (e) {
                    // logger.warn(`Solscan fetch failed for ${t.mint}`);
                }

                // 2. Get Best Internal Pool (for Real-Time Price)
                const pool = await db.get(`SELECT address, price_usd FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1`, [t.mint]);
                
                let currentPrice = pool?.price_usd || 0;
                let volume24h = 0;
                let change24h = 0;
                let change1h = 0;
                let change5m = 0;

                // --- ASSIGN VOLUME ---
                if (solscan && solscan.volume24h) {
                    volume24h = solscan.volume24h;
                }

                // --- CALCULATE PRICE & CHANGES ---
                // We prefer internal price/changes if we have the data (Instant updates), 
                // but fall back to Solscan if our history is empty.
                
                if (pool && currentPrice > 0) {
                    // Helper to get price at specific time
                    const getPriceAt = async (ts) => {
                        let res = await db.get(`SELECT close FROM candles_1m WHERE pool_address = $1 AND timestamp <= $2 ORDER BY timestamp DESC LIMIT 1`, [pool.address, ts]);
                        
                        // Fallback: If token is newer than 24h, use the first candle ever recorded
                        if (!res) {
                            res = await db.get(`SELECT open FROM candles_1m WHERE pool_address = $1 ORDER BY timestamp ASC LIMIT 1`, [pool.address]);
                        }
                        return res ? res.close || res.open : null;
                    };

                    const price24h = await getPriceAt(time24h);
                    const price1h = await getPriceAt(time1h);
                    const price5m = await getPriceAt(time5m);

                    if (price24h) change24h = ((currentPrice - price24h) / price24h) * 100;
                    if (price1h) change1h = ((currentPrice - price1h) / price1h) * 100;
                    if (price5m) change5m = ((currentPrice - price5m) / price5m) * 100;
                }

                // Fallbacks if internal data failed completely
                if (currentPrice === 0 && solscan && solscan.priceUsd) currentPrice = solscan.priceUsd;
                if (change24h === 0 && solscan && solscan.change24h) change24h = solscan.change24h;

                // 3. Calculate Market Cap
                let marketCap = 0;
                if (currentPrice > 0) {
                    const decimals = t.decimals || 9;
                    const divisor = Math.pow(10, decimals);
                    const supply = parseFloat(t.supply || '0') / divisor;
                    marketCap = supply * currentPrice;
                }

                // 4. Save Updates
                await db.run(`
                    UPDATE tokens 
                    SET volume24h = $1, marketCap = $2, priceUsd = $3, 
                        change1h = $4, change24h = $5, change5m = $6, 
                        timestamp = $7 
                    WHERE mint = $8
                `, [volume24h, marketCap, currentPrice, change1h, change24h, change5m, now, t.mint]);

                // Rate limiting to prevent Solscan 429s
                await new Promise(r => setTimeout(r, 200)); 

            } catch (err) {
                // logger.error(`Token meta update failed ${t.mint}: ${err.message}`);
            }
        }
    } catch (e) {
        logger.error(`Metadata Update Cycle Error: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    setInterval(() => updateMetadata(deps), 60000); // Run every minute
    setTimeout(() => updateMetadata(deps), 5000);   // Initial run
}

async function updateTokenStats(mint) {
   // Placeholder for API-triggered updates
}

module.exports = { start, updateTokenStats };
