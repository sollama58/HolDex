const { getDB } = require('../services/database');
const { logger } = require('../services');
const { fetchSolscanData } = require('../services/solscan');

let isRunning = false;

// EXPORTED: Update a single token immediately
async function updateTokenStats(mint) {
    const db = getDB();
    try {
        const t = await db.get(`SELECT mint, supply, decimals FROM tokens WHERE mint = $1`, [mint]);
        if (!t) return;
        
        const pool = await db.get(`SELECT address, price_usd, liquidity_usd FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1`, [mint]);
        if (!pool) return;

        const currentPrice = pool.price_usd || 0;
        
        // Dynamic Decimals
        const decimals = t.decimals || 9;
        const divisor = Math.pow(10, decimals);
        const supply = parseFloat(t.supply || '0') / divisor; 
        
        const marketCap = supply * currentPrice;

        // Try fetch Solscan for initial volume/mcap data
        const solscan = await fetchSolscanData(mint);
        const volume24h = solscan?.volume24h || 0;

        await db.run(`UPDATE tokens SET marketCap = $1, priceUsd = $2, volume24h = $3, change1h = 0, change24h = 0 WHERE mint = $4`, 
            [marketCap, currentPrice, volume24h, mint]);
        
        logger.info(`⚡ Immediate Stats Update for ${mint} (Vol: $${volume24h})`);
    } catch (e) {
        logger.error(`Immediate Stats Failed: ${e.message}`);
    }
}

async function updateMetadata(deps) {
    if (isRunning) return;
    isRunning = true;
    const { db } = deps;
    const now = Date.now();
    const oneHourAgo = now - (60 * 60 * 1000);
    const twentyFourHoursAgo = now - (24 * 60 * 60 * 1000);
    const fiveMinsAgo = now - (5 * 60 * 1000);

    try {
        const tokens = await db.all(`SELECT mint, supply, decimals FROM tokens`);
        
        // Process sequentially to be gentle on Solscan rate limits
        for (const t of tokens) {
            try {
                const pool = await db.get(`SELECT address, price_usd, liquidity_usd FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1`, [t.mint]);
                if (!pool) continue;

                const currentPrice = pool.price_usd || 0;
                
                // 1. Internal Volume Calculation (Fallback)
                const volumeRes = await db.get(`SELECT SUM(c.volume * c.close) as vol_usd FROM candles_1m c JOIN pools p ON c.pool_address = p.address WHERE p.mint = $1 AND c.timestamp >= $2`, [t.mint, twentyFourHoursAgo]);
                let volume24h = volumeRes?.vol_usd || 0;

                // 2. Solscan Fetch (Priority)
                const solscan = await fetchSolscanData(t.mint);
                if (solscan && solscan.volume24h > 0) {
                    volume24h = solscan.volume24h;
                }

                // 3. Price Changes
                const getPriceAt = async (ts) => {
                    const res = await db.get(`SELECT close FROM candles_1m WHERE pool_address = $1 AND timestamp <= $2 ORDER BY timestamp DESC LIMIT 1`, [pool.address, ts]);
                    return res ? res.close : currentPrice;
                };

                const price1h = await getPriceAt(oneHourAgo);
                const price24h = await getPriceAt(twentyFourHoursAgo);
                const price5m = await getPriceAt(fiveMinsAgo);

                const change1h = price1h > 0 ? ((currentPrice - price1h) / price1h) * 100 : 0;
                const change24h = price24h > 0 ? ((currentPrice - price24h) / price24h) * 100 : 0;
                const change5m = price5m > 0 ? ((currentPrice - price5m) / price5m) * 100 : 0;

                // 4. Market Cap
                const decimals = t.decimals || 9;
                const divisor = Math.pow(10, decimals);
                const supply = parseFloat(t.supply || '0') / divisor; 
                const marketCap = supply * currentPrice;

                await db.run(`UPDATE tokens SET volume24h = $1, marketCap = $2, priceUsd = $3, change1h = $4, change24h = $5, change5m = $6, timestamp = $7 WHERE mint = $8`, 
                    [volume24h, marketCap, currentPrice, change1h, change24h, change5m, now, t.mint]);

                // Small delay to prevent rate limit spamming
                await new Promise(r => setTimeout(r, 200));

            } catch (err) {}
        }
    } catch (e) {
        logger.error(`Stats Engine Fatal: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    setInterval(() => updateMetadata(deps), 60000);
    setTimeout(() => updateMetadata(deps), 5000);
}

module.exports = { start, updateTokenStats };
