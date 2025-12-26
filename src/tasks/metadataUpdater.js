const { getDB } = require('../services/database');
const { logger } = require('../services');

let isRunning = false;

// EXPORTED: Update a single token immediately
async function updateTokenStats(mint) {
    const db = getDB();
    // Reusing the core logic for a single mint
    try {
        const t = await db.get(`SELECT mint, supply FROM tokens WHERE mint = $1`, [mint]);
        if (!t) return;
        
        const pool = await db.get(`SELECT address, price_usd, liquidity_usd FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1`, [mint]);
        if (!pool) return;

        const currentPrice = pool.price_usd || 0;
        const supply = parseFloat(t.supply || '0') / 1e6; 
        const marketCap = supply * currentPrice;

        await db.run(`UPDATE tokens SET marketCap = $1, priceUsd = $2, volume24h = 0, change1h = 0, change24h = 0 WHERE mint = $3`, 
            [marketCap, currentPrice, mint]);
        
        logger.info(`⚡ Immediate Stats Update for ${mint}: $${currentPrice}`);
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
        const tokens = await db.all(`SELECT mint, supply FROM tokens`);
        for (const t of tokens) {
            try {
                const pool = await db.get(`SELECT address, price_usd, liquidity_usd FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1`, [t.mint]);
                if (!pool) continue;

                const currentPrice = pool.price_usd || 0;
                const volumeRes = await db.get(`SELECT SUM(c.volume * c.close) as vol_usd FROM candles_1m c JOIN pools p ON c.pool_address = p.address WHERE p.mint = $1 AND c.timestamp >= $2`, [t.mint, twentyFourHoursAgo]);
                const volume24h = volumeRes?.vol_usd || 0;

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

                const supply = parseFloat(t.supply || '0') / 1e6; 
                const marketCap = supply * currentPrice;

                await db.run(`UPDATE tokens SET volume24h = $1, marketCap = $2, priceUsd = $3, change1h = $4, change24h = $5, change5m = $6, timestamp = $7 WHERE mint = $8`, 
                    [volume24h, marketCap, currentPrice, change1h, change24h, change5m, now, t.mint]);

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
