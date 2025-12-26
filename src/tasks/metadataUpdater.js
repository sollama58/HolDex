const { getDB } = require('../services/database');
const { logger } = require('../services');
const { fetchSolscanData } = require('../services/solscan');

let isRunning = false;

// EXPORTED: Update a single token immediately
async function updateTokenStats(mint) {
    const db = getDB();
    try {
        // 1. Fetch Internal Data
        const t = await db.get(`SELECT mint, supply, decimals FROM tokens WHERE mint = $1`, [mint]);
        if (!t) return;
        
        const pool = await db.get(`SELECT address, price_usd, liquidity_usd FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1`, [mint]);
        
        let currentPrice = pool?.price_usd || 0;
        let volume24h = 0;
        let change24h = 0;
        let marketCap = 0;

        // 2. Fetch Solscan Data (The Source of Truth)
        const solscan = await fetchSolscanData(mint);

        if (solscan) {
            // If Solscan works, use it.
            if (solscan.priceUsd > 0) currentPrice = solscan.priceUsd;
            if (solscan.volume24h > 0) volume24h = solscan.volume24h;
            if (solscan.change24h !== 0) change24h = solscan.change24h;
            if (solscan.marketCap > 0) marketCap = solscan.marketCap;
        }

        // 3. Fallback: Calculate MarketCap internally if Solscan failed
        if (marketCap === 0 && currentPrice > 0) {
            const decimals = t.decimals || 9;
            const divisor = Math.pow(10, decimals);
            const supply = parseFloat(t.supply || '0') / divisor;
            marketCap = supply * currentPrice;
        }

        // 4. Save
        await db.run(`UPDATE tokens SET marketCap = $1, priceUsd = $2, volume24h = $3, change24h = $4 WHERE mint = $5`, 
            [marketCap, currentPrice, volume24h, change24h, mint]);
        
        logger.info(`⚡ Stats Updated: ${mint} | Price: $${currentPrice} | Vol: $${volume24h}`);

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
    const fiveMinsAgo = now - (5 * 60 * 1000);

    try {
        const tokens = await db.all(`SELECT mint, supply, decimals FROM tokens`);
        
        for (const t of tokens) {
            try {
                // Internal Reference (for fallback 1h/5m changes)
                const pool = await db.get(`SELECT address, price_usd FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1`, [t.mint]);
                
                let currentPrice = pool?.price_usd || 0;
                let volume24h = 0;
                let change24h = 0;
                let marketCap = 0;

                // --- EXTERNAL FETCH (SOLSCAN) ---
                const solscan = await fetchSolscanData(t.mint);

                if (solscan) {
                    if (solscan.priceUsd > 0) currentPrice = solscan.priceUsd;
                    if (solscan.volume24h > 0) volume24h = solscan.volume24h;
                    if (solscan.marketCap > 0) marketCap = solscan.marketCap;
                    change24h = solscan.change24h; // Can be negative
                } else {
                    // Fallback to internal calculated volume
                     const volumeRes = await db.get(`SELECT SUM(c.volume * c.close) as vol_usd FROM candles_1m c JOIN pools p ON c.pool_address = p.address WHERE p.mint = $1 AND c.timestamp >= $2`, [t.mint, now - 86400000]);
                     volume24h = volumeRes?.vol_usd || 0;
                }

                // --- INTERNAL CALCULATIONS (Fill in gaps) ---
                
                // 1. Market Cap (if Solscan missed it)
                if (marketCap === 0 && currentPrice > 0) {
                    const decimals = t.decimals || 9;
                    const divisor = Math.pow(10, decimals);
                    const supply = parseFloat(t.supply || '0') / divisor;
                    marketCap = supply * currentPrice;
                }

                // 2. Short Term Changes (Solscan doesn't provide 1h/5m usually)
                let change1h = 0;
                let change5m = 0;

                if (pool) {
                    const getPriceAt = async (ts) => {
                        const res = await db.get(`SELECT close FROM candles_1m WHERE pool_address = $1 AND timestamp <= $2 ORDER BY timestamp DESC LIMIT 1`, [pool.address, ts]);
                        return res ? res.close : currentPrice;
                    };

                    const price1h = await getPriceAt(oneHourAgo);
                    const price5m = await getPriceAt(fiveMinsAgo);

                    change1h = price1h > 0 ? ((currentPrice - price1h) / price1h) * 100 : 0;
                    change5m = price5m > 0 ? ((currentPrice - price5m) / price5m) * 100 : 0;
                }

                // --- SAVE ---
                await db.run(`UPDATE tokens SET volume24h = $1, marketCap = $2, priceUsd = $3, change1h = $4, change24h = $5, change5m = $6, timestamp = $7 WHERE mint = $8`, 
                    [volume24h, marketCap, currentPrice, change1h, change24h, change5m, now, t.mint]);

                // Rate limit protection
                await new Promise(r => setTimeout(r, 250));

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
