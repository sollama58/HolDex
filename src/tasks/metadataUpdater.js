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
                const pools = await db.all(`
                    SELECT address, price_usd, liquidity_usd 
                    FROM pools 
                    WHERE mint = $1
                `, [t.mint]);
                
                if (!pools || pools.length === 0) continue;

                // 2. Determine Best Pool for Price (Highest Liquidity)
                pools.sort((a, b) => (b.liquidity_usd || 0) - (a.liquidity_usd || 0));
                const bestPool = pools[0];
                const currentPrice = bestPool.price_usd || 0;

                // 3. Aggregate Volume (Sum of volume from all pools in last 24h)
                let volume24h = 0;
                const poolAddresses = pools.map(p => p.address);
                
                if (poolAddresses.length > 0) {
                    const placeholders = poolAddresses.map((_, i) => `$${i + 2}`).join(',');
                    // STRICTLY based on volume column in candles
                    const volQuery = `
                        SELECT COALESCE(SUM(volume), 0) as total_vol 
                        FROM candles_1m 
                        WHERE timestamp >= $1 
                        AND pool_address IN (${placeholders})
                    `;
                    
                    const volResult = await db.get(volQuery, [time24h, ...poolAddresses]);
                    volume24h = volResult ? parseFloat(volResult.total_vol) : 0;
                }

                // 4. Calculate Price Changes (STRICTLY based on Price History)
                let change24h = 0, change1h = 0, change5m = 0;
                
                if (currentPrice > 0) {
                    // Helper: Find price at specific time
                    const getPriceAt = async (targetTime) => {
                        // A. Try finding exact historical candle
                        const res = await db.get(`
                            SELECT close FROM candles_1m 
                            WHERE pool_address = $1 AND timestamp <= $2 
                            ORDER BY timestamp DESC LIMIT 1
                        `, [bestPool.address, targetTime]);

                        // B. Fallback: If we have NO history before targetTime, use the oldest known candle.
                        // This allows "Change since inception" for new tokens.
                        if (!res) {
                            const oldest = await db.get(`
                                SELECT open FROM candles_1m 
                                WHERE pool_address = $1 
                                ORDER BY timestamp ASC LIMIT 1
                            `, [bestPool.address]);
                            return oldest ? (oldest.open || oldest.close) : currentPrice; // Default to current if absolutely nothing found
                        }
                        return res.close;
                    };

                    const price24h = await getPriceAt(time24h);
                    const price1h = await getPriceAt(time1h);
                    const price5m = await getPriceAt(time5m);

                    // Calculation: (New - Old) / Old
                    if (price24h > 0) change24h = ((currentPrice - price24h) / price24h) * 100;
                    if (price1h > 0) change1h = ((currentPrice - price1h) / price1h) * 100;
                    if (price5m > 0) change5m = ((currentPrice - price5m) / price5m) * 100;
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
                // We use COALESCE/OR logic to ensure we never save NULL/NaN
                await db.run(`
                    UPDATE tokens 
                    SET volume24h = $1, marketCap = $2, priceUsd = $3, 
                        change1h = $4, change24h = $5, change5m = $6, 
                        timestamp = $7 
                    WHERE mint = $8
                `, [
                    volume24h, 
                    marketCap, 
                    currentPrice, 
                    change1h || 0, 
                    change24h || 0, 
                    change5m || 0, 
                    now, 
                    t.mint
                ]);

            } catch (err) {
                logger.error(`Meta Update Failed (${t.mint}): ${err.message}`);
            }
        }
    } catch (e) {
        logger.error(`Metadata Cycle Error: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    setInterval(() => updateMetadata(deps), 30000);
    setTimeout(() => updateMetadata(deps), 2000);
}

module.exports = { start };
