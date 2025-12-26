const { getDB } = require('../services/database');
const logger = require('../services/logger');

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
        const tokens = await db.all(`SELECT mint, supply, decimals FROM tokens`);
        
        for (const t of tokens) {
            try {
                // 1. Fetch ALL pools for this mint
                // We need all pools to sum volume correctly, but only the best pool for price.
                const pools = await db.all(`
                    SELECT address, price_usd, liquidity_usd 
                    FROM pools 
                    WHERE mint = $1
                `, [t.mint]);
                
                if (!pools || pools.length === 0) continue;

                // 2. Determine Best Pool for Price (Highest Liquidity)
                // sort descending by liquidity
                pools.sort((a, b) => (b.liquidity_usd || 0) - (a.liquidity_usd || 0));
                const bestPool = pools[0];
                
                const currentPrice = bestPool.price_usd || 0;
                let volume24h = 0;
                let change24h = 0;
                let change1h = 0;
                let change5m = 0;

                // 3. Aggregate Volume across ALL pools
                // Fix: Previous version only checked bestPool, ignoring volume in other pairs
                const poolAddresses = pools.map(p => p.address);
                if (poolAddresses.length > 0) {
                    // Construct dynamic IN clause parameters ($2, $3, $4...)
                    const placeholders = poolAddresses.map((_, i) => `$${i + 2}`).join(',');
                    const volQuery = `
                        SELECT SUM(volume) as total_vol 
                        FROM candles_1m 
                        WHERE timestamp >= $1 
                        AND pool_address IN (${placeholders})
                    `;
                    
                    const volResult = await db.get(volQuery, [time24h, ...poolAddresses]);
                    if (volResult && volResult.total_vol) {
                        volume24h = parseFloat(volResult.total_vol);
                    }
                }

                // 4. Calculate Price Changes (Based on Best Pool History)
                if (currentPrice > 0) {
                    // Helper to get historical price
                    const getPriceAt = async (targetTime) => {
                        // Find closest candle BEFORE or AT target time
                        let res = await db.get(`
                            SELECT close 
                            FROM candles_1m 
                            WHERE pool_address = $1 
                            AND timestamp <= $2 
                            ORDER BY timestamp DESC 
                            LIMIT 1
                        `, [bestPool.address, targetTime]);

                        // Fallback: If no history that far back (Cold Start), grab the Oldest candle
                        // This ensures we display "Change since inception" instead of blank
                        if (!res) {
                            res = await db.get(`
                                SELECT open 
                                FROM candles_1m 
                                WHERE pool_address = $1 
                                ORDER BY timestamp ASC 
                                LIMIT 1
                            `, [bestPool.address]);
                        }
                        return res ? (res.close || res.open) : null;
                    };

                    const price24h = await getPriceAt(time24h);
                    const price1h = await getPriceAt(time1h);
                    const price5m = await getPriceAt(time5m);

                    if (price24h) change24h = ((currentPrice - price24h) / price24h) * 100;
                    if (price1h) change1h = ((currentPrice - price1h) / price1h) * 100;
                    if (price5m) change5m = ((currentPrice - price5m) / price5m) * 100;
                }

                // 5. Calculate Market Cap
                let marketCap = 0;
                if (currentPrice > 0) {
                    const decimals = t.decimals || 9;
                    const divisor = Math.pow(10, decimals);
                    const supply = parseFloat(t.supply || '0') / divisor;
                    marketCap = supply * currentPrice;
                }

                // 6. Save Updates
                await db.run(`
                    UPDATE tokens 
                    SET volume24h = $1, 
                        marketCap = $2, 
                        priceUsd = $3, 
                        change1h = $4, 
                        change24h = $5, 
                        change5m = $6, 
                        timestamp = $7 
                    WHERE mint = $8
                `, [
                    volume24h, 
                    marketCap, 
                    currentPrice, 
                    change1h || 0, // Ensure we store 0 instead of NULL/NaN
                    change24h || 0, 
                    change5m || 0, 
                    now, 
                    t.mint
                ]);

                // Rate limiting protection
                await new Promise(r => setTimeout(r, 20)); 

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
    setInterval(() => updateMetadata(deps), 60000); // Run every minute
    setTimeout(() => updateMetadata(deps), 5000);   // Initial run
}

async function updateTokenStats(mint) {
   // Placeholder for manual trigger
}

module.exports = { start, updateTokenStats };
