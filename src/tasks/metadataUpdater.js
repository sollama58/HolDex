/**
 * Metadata Updater (Internal Aggregation Engine)
 * Replaces DexScreener with local DB calculations.
 * Calculates: Volume, Price Change, Market Cap
 */
const { getDB } = require('../services/database');
const { logger } = require('../services');

let isRunning = false;

async function updateMetadata(deps) {
    if (isRunning) return;
    isRunning = true;

    const { db } = deps;
    const now = Date.now();
    const oneHourAgo = now - (60 * 60 * 1000);
    const twentyFourHoursAgo = now - (24 * 60 * 60 * 1000);
    const fiveMinsAgo = now - (5 * 60 * 1000);

    logger.info("🔄 Stats Engine: Calculating internal metrics...");

    try {
        // Fetch all active tokens
        const tokens = await db.all(`SELECT mint, supply FROM tokens`);
        
        for (const t of tokens) {
            try {
                // 1. Get Best Pool (Highest Liquidity) for Price
                const pool = await db.get(`
                    SELECT address, price_usd, liquidity_usd 
                    FROM pools 
                    WHERE mint = $1 
                    ORDER BY liquidity_usd DESC LIMIT 1
                `, [t.mint]);

                if (!pool) continue;

                const currentPrice = pool.price_usd || 0;

                // 2. Calculate Aggregated Volume (24h) from Candles
                // Sum of volume * close_price for all candles in last 24h across all pools for this mint
                const volumeRes = await db.get(`
                    SELECT SUM(c.volume * c.close) as vol_usd
                    FROM candles_1m c
                    JOIN pools p ON c.pool_address = p.address
                    WHERE p.mint = $1 AND c.timestamp >= $2
                `, [t.mint, twentyFourHoursAgo]);
                
                const volume24h = volumeRes?.vol_usd || 0;

                // 3. Calculate Price Changes (vs Best Pool History)
                const getPriceAt = async (ts) => {
                    const res = await db.get(`
                        SELECT close FROM candles_1m 
                        WHERE pool_address = $1 AND timestamp <= $2 
                        ORDER BY timestamp DESC LIMIT 1
                    `, [pool.address, ts]);
                    return res ? res.close : currentPrice;
                };

                const price1h = await getPriceAt(oneHourAgo);
                const price24h = await getPriceAt(twentyFourHoursAgo);
                const price5m = await getPriceAt(fiveMinsAgo);

                const change1h = price1h > 0 ? ((currentPrice - price1h) / price1h) * 100 : 0;
                const change24h = price24h > 0 ? ((currentPrice - price24h) / price24h) * 100 : 0;
                const change5m = price5m > 0 ? ((currentPrice - price5m) / price5m) * 100 : 0;

                // 4. Calculate Market Cap
                // Supply is stored as raw string (e.g. "1000000000000000"), assuming 6 decimals for most memes
                // Ideally, store decimals in DB. Defaulting to 6 for standard SPL.
                const supply = parseFloat(t.supply || '0') / 1e6; 
                const marketCap = supply * currentPrice;

                // 5. Update Token
                await db.run(`
                    UPDATE tokens SET 
                    volume24h = $1, 
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
                    change1h, 
                    change24h, 
                    change5m, 
                    now, 
                    t.mint
                ]);

            } catch (err) {
                // logger.warn(`Stats error for ${t.mint}: ${err.message}`);
            }
        }
        
        logger.info(`✅ Stats Engine: Updated ${tokens.length} tokens.`);

    } catch (e) {
        logger.error(`Stats Engine Fatal: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    setInterval(() => updateMetadata(deps), 60000); // Run every minute
    setTimeout(() => updateMetadata(deps), 5000); // Initial run
}

module.exports = { start };
