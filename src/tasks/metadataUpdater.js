const { getDB } = require('../services/database');
const logger = require('../services/logger');
// Removed Solscan dependency as requested to rely on internal calculated metrics

let isRunning = false;

async function updateMetadata(deps) {
    if (isRunning) return;
    isRunning = true;
    const { db } = deps;
    const now = Date.now();
    
    // Time windows
    const time24h = now - (24 * 60 * 60 * 1000);
    const time1h = now - (60 * 60 * 1000);
    const time5m = now - (5 * 60 * 1000);

    try {
        // Fetch all tokens
        const tokens = await db.all(`SELECT mint, supply, decimals FROM tokens`);
        
        for (const t of tokens) {
            try {
                // 1. Get Best Internal Pool (for Real-Time Price)
                const pool = await db.get(`
                    SELECT address, price_usd 
                    FROM pools 
                    WHERE mint = $1 
                    ORDER BY liquidity_usd DESC 
                    LIMIT 1
                `, [t.mint]);
                
                // If no pool, we can't calculate anything meaningful internally
                if (!pool) continue;

                let currentPrice = pool.price_usd || 0;
                let volume24h = 0;
                let change24h = 0;
                let change1h = 0;
                let change5m = 0;

                // --- 2. CALCULATE INTERNAL VOLUME (Rolling 24h) ---
                // We sum the volume from our own candles instead of asking Solscan
                try {
                    const volResult = await db.get(`
                        SELECT SUM(volume) as total_vol 
                        FROM candles_1m 
                        WHERE pool_address = $1 
                        AND timestamp >= $2
                    `, [pool.address, time24h]);
                    
                    if (volResult && volResult.total_vol) {
                        volume24h = parseFloat(volResult.total_vol);
                    }
                } catch (err) {
                    logger.warn(`Volume calc failed for ${t.mint}: ${err.message}`);
                }

                // --- 3. CALCULATE PRICE CHANGES ---
                if (currentPrice > 0) {
                    // Helper to find the candle closest to a specific timestamp (but not newer than it)
                    const getPriceAt = async (targetTime) => {
                        // 1. Try to find a candle at or before the target time
                        let res = await db.get(`
                            SELECT close 
                            FROM candles_1m 
                            WHERE pool_address = $1 
                            AND timestamp <= $2 
                            ORDER BY timestamp DESC 
                            LIMIT 1
                        `, [pool.address, targetTime]);
                        
                        // 2. Fallback: If the indexer is newer than 24h, use the oldest candle available
                        // This effectively gives us "Change since inception"
                        if (!res) {
                            res = await db.get(`
                                SELECT open 
                                FROM candles_1m 
                                WHERE pool_address = $1 
                                ORDER BY timestamp ASC 
                                LIMIT 1
                            `, [pool.address]);
                        }
                        return res ? (res.close || res.open) : null;
                    };

                    const price24h = await getPriceAt(time24h);
                    const price1h = await getPriceAt(time1h);
                    const price5m = await getPriceAt(time5m);

                    if (price24h && price24h > 0) change24h = ((currentPrice - price24h) / price24h) * 100;
                    if (price1h && price1h > 0) change1h = ((currentPrice - price1h) / price1h) * 100;
                    if (price5m && price5m > 0) change5m = ((currentPrice - price5m) / price5m) * 100;
                }

                // --- 4. CALCULATE MARKET CAP ---
                let marketCap = 0;
                if (currentPrice > 0) {
                    const decimals = t.decimals || 9;
                    const divisor = Math.pow(10, decimals);
                    const supply = parseFloat(t.supply || '0') / divisor;
                    marketCap = supply * currentPrice;
                }

                // --- 5. SAVE UPDATES ---
                await db.run(`
                    UPDATE tokens 
                    SET volume24h = $1, marketCap = $2, priceUsd = $3, 
                        change1h = $4, change24h = $5, change5m = $6, 
                        timestamp = $7 
                    WHERE mint = $8
                `, [volume24h, marketCap, currentPrice, change1h, change24h, change5m, now, t.mint]);

                // Rate limiting protection for the loop itself
                await new Promise(r => setTimeout(r, 50)); 

            } catch (err) {
                logger.error(`Token meta update failed ${t.mint}: ${err.message}`);
            }
        }
    } catch (e) {
        logger.error(`Metadata Update Cycle Error: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    // Run every minute to keep 24h stats rolling
    setInterval(() => updateMetadata(deps), 60000);
    setTimeout(() => updateMetadata(deps), 5000);   // Initial run
}

async function updateTokenStats(mint) {
   // Placeholder for manual trigger if needed
}

module.exports = { start, updateTokenStats };
