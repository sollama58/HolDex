const logger = require('../services/logger');

let isRunning = false;

async function updateMetadata(deps) {
    if (isRunning) return;
    isRunning = true;
    const { db } = deps;
    const now = Date.now();
    
    const time24h = now - (24 * 60 * 60 * 1000);
    const time1h = now - (60 * 60 * 1000);
    const time5m = now - (5 * 60 * 1000);

    try {
        // Fetch all tokens to update
        const tokens = await db.all(`SELECT mint, supply, decimals FROM tokens`);
        if (tokens.length > 0) {
            logger.info(`Running Metadata Aggregation for ${tokens.length} tokens...`);
        }
        
        for (const t of tokens) {
            try {
                // 1. Get Pools
                const pools = await db.all(`SELECT address, price_usd, liquidity_usd FROM pools WHERE mint = $1`, [t.mint]);
                if (!pools || pools.length === 0) continue;

                // 2. Best Pool (Highest Liquidity) determines Price
                pools.sort((a, b) => (b.liquidity_usd || 0) - (a.liquidity_usd || 0));
                const bestPool = pools[0];
                const currentPrice = bestPool.price_usd || 0;

                // 3. Aggregate Volume (Sum of all pools)
                let volume24h = 0;
                const poolAddresses = pools.map(p => p.address);
                if (poolAddresses.length > 0) {
                    const placeholders = poolAddresses.map((_, i) => `$${i + 2}`).join(',');
                    const volQuery = `SELECT COALESCE(SUM(volume), 0) as total_vol FROM candles_1m WHERE timestamp >= $1 AND pool_address IN (${placeholders})`;
                    const volResult = await db.get(volQuery, [time24h, ...poolAddresses]);
                    volume24h = volResult ? parseFloat(volResult.total_vol) : 0;
                }

                // 4. Calculate Changes
                let change24h = 0, change1h = 0, change5m = 0;
                if (currentPrice > 0) {
                    const getPriceAt = async (targetTime) => {
                        const res = await db.get(`SELECT close FROM candles_1m WHERE pool_address = $1 AND timestamp <= $2 ORDER BY timestamp DESC LIMIT 1`, [bestPool.address, targetTime]);
                        return res ? res.close : 0;
                    };

                    const price24h = await getPriceAt(time24h);
                    const price1h = await getPriceAt(time1h);
                    const price5m = await getPriceAt(time5m);

                    if (price24h > 0) change24h = ((currentPrice - price24h) / price24h) * 100;
                    if (price1h > 0) change1h = ((currentPrice - price1h) / price1h) * 100;
                    if (price5m > 0) change5m = ((currentPrice - price5m) / price5m) * 100;
                }

                // 5. Calculate Market Cap
                let marketCap = 0;
                if (currentPrice > 0) {
                    const decimals = t.decimals || 9; // Default 9 decimals
                    let rawSupply = parseFloat(t.supply || '0');
                    
                    // FALLBACK: If supply is 0 (failed fetch), assume 1 Billion (Standard for Memes)
                    // This prevents Mcap from being 0/hidden in the UI
                    if (rawSupply === 0) rawSupply = 1000000000 * Math.pow(10, decimals);

                    const divisor = Math.pow(10, decimals);
                    const supply = rawSupply / divisor;
                    marketCap = supply * currentPrice;
                }

                // 6. Commit Updates
                await db.run(`
                    UPDATE tokens 
                    SET volume24h = $1, marketCap = $2, priceUsd = $3, 
                        change1h = $4, change24h = $5, change5m = $6, 
                        timestamp = $7 
                    WHERE mint = $8
                `, [volume24h, marketCap, currentPrice, change1h, change24h, change5m, now, t.mint]);

            } catch (err) {
                // Silently fail per token, log only if critical
            }
        }
    } catch (e) {
        logger.error(`Metadata Cycle Error: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    setInterval(() => updateMetadata(deps), 30000); // Run every 30s
    setTimeout(() => updateMetadata(deps), 2000);   // Initial run
}

module.exports = { start };
