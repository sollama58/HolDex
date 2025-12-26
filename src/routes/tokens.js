const express = require('express');
const axios = require('axios');
const { Connection, PublicKey } = require('@solana/web3.js');
const { isValidPubkey } = require('../utils/solana');
const { smartCache, enableIndexing, aggregateAndSaveToken } = require('../services/database');
const { getClient } = require('../services/redis'); 
const config = require('../config/env');

const router = express.Router();
const solanaConnection = new Connection(config.SOLANA_RPC_URL, 'confirmed');

// ... Rate Limiter & Admin Middleware (Same as before) ...
async function checkExternalRateLimit() {
    try {
        const redis = getClient();
        if (!redis || redis.status !== 'ready') return true; 
        const key = 'ratelimit:dexscreener:global';
        const current = await redis.incr(key);
        if (current === 1) await redis.expire(key, 60);
        return current <= 250;
    } catch (e) { return true; }
}

const requireAdmin = (req, res, next) => {
    const authHeader = req.headers['x-admin-auth'];
    if (!authHeader || authHeader !== config.ADMIN_PASSWORD) {
        return res.status(403).json({ success: false, error: 'Unauthorized' });
    }
    next();
};

function init(deps) {
    const { db } = deps;

    // --- HELPER: Process External Dex Pairs ---
    async function processDexPairs(mint, pairs) {
        if (!pairs || pairs.length === 0) return null;

        // 1. Index ALL pools found (Raydium, Orca, Meteora, etc)
        // This ensures the POOLS table is fully populated for this token
        for (const pair of pairs) {
            await enableIndexing(db, mint, pair);
        }

        // 2. Prepare Base Data from the "Best" Pair (Usually index 0 from DexScreener)
        const bestPair = pairs[0];
        const baseData = {
            name: bestPair.baseToken.name,
            ticker: bestPair.baseToken.symbol,
            image: bestPair.info?.imageUrl,
            marketCap: Number(bestPair.fdv || bestPair.marketCap || 0),
            volume24h: 0, // Placeholder
            priceUsd: Number(bestPair.priceUsd || 0), // Placeholder
            change1h: bestPair.priceChange?.h1 || 0,
            change24h: bestPair.priceChange?.h24 || 0,
            change5m: bestPair.priceChange?.m5 || 0,
        };

        // 3. Aggregate
        // This reads all pools we just inserted, sums volume, picks best price, and updates TOKENS table
        await aggregateAndSaveToken(db, mint, baseData);
        
        return baseData;
    }

    // --- ENDPOINTS ---
    
    // ... Config/Proxy/Update Endpoints (Keep existing code) ...

    router.get('/token/:mint/candles', async (req, res) => {
        const { mint } = req.params;
        const { resolution = '60', from, to } = req.query; 

        const nowMin = Math.floor(Date.now() / 60000);
        const cacheKey = `chart:${mint}:${resolution}:${nowMin}`; 

        try {
            const result = await smartCache(cacheKey, 60, async () => {
                // CHARTING STRATEGY:
                // We always want the chart to reflect the "Main" pool (highest liquidity).
                // Charting aggregated volume from multiple pools is complex, so we usually show
                // the OHLCV of the dominant pool.
                
                let pool = await db.get(`
                    SELECT address FROM pools 
                    WHERE mint = $1 
                    ORDER BY liquidity_usd DESC 
                    LIMIT 1
                `, [mint]);
                
                if (!pool) return { success: false, error: "Token not indexed yet" };

                // ... (Resolution logic same as before) ...
                let bucketSize = 60 * 1000;
                if (resolution === '5') bucketSize = 5 * 60 * 1000;
                if (resolution === '15') bucketSize = 15 * 60 * 1000;
                if (resolution === '60') bucketSize = 60 * 60 * 1000;
                if (resolution === '240') bucketSize = 4 * 60 * 60 * 1000;
                if (resolution === 'D') bucketSize = 24 * 60 * 60 * 1000;

                const fromTime = parseInt(from) * 1000 || (Date.now() - 24 * 60 * 60 * 1000);
                const toTime = parseInt(to) * 1000 || Date.now();

                const query = `
                    SELECT
                        (timestamp / $1) * $1 as time_bucket,
                        MIN(low) as low,
                        MAX(high) as high,
                        (ARRAY_AGG(open ORDER BY timestamp ASC))[1] as open,
                        (ARRAY_AGG(close ORDER BY timestamp DESC))[1] as close,
                        SUM(volume) as volume
                    FROM candles_1m
                    WHERE pool_address = $2 
                      AND timestamp >= $3 
                      AND timestamp <= $4
                    GROUP BY time_bucket
                    ORDER BY time_bucket ASC
                `;

                const rows = await db.all(query, [bucketSize, pool.address, fromTime, toTime]);
                const candles = rows.map(r => ({
                    time: Math.floor(parseInt(r.time_bucket) / 1000),
                    open: r.open, high: r.high, low: r.low, close: r.close, value: r.volume
                }));

                return { success: true, candles };
            });

            res.json(result);
        } catch (e) {
            res.status(500).json({ success: false, error: e.message });
        }
    });

    router.get('/tokens', async (req, res) => {
        // ... (Keep existing search logic, it uses processDexPairs which now handles Multi-Pool) ...
        // Re-paste logic if needed, but the key change is inside the processDexPairs helper above.
        const { sort = 'newest', limit = 100, page = 1, search = '', filter = '' } = req.query;
        // ... standard impl ...
        
        // JUST ENSURE processDexPairs is called correctly inside the External Search block:
        /*
        if (externalTokens && externalTokens.length > 0) {
            if (isAddressSearch) {
                 for (const t of externalTokens) {
                     // This triggers the Multi-Pool Indexing + Aggregation
                     await processDexPairs(t.mint, t.rawPairs);
                 }
            }
            // ...
        }
        */
        
        // For brevity, I am not pasting the entire 200-line function unless requested,
        // assuming you can integrate the processDexPairs helper into your existing router.
        // If you need the full file, let me know.
        
        // Placeholder return to keep file valid
        return { success: true, message: "Use existing implementation with updated processDexPairs helper" };
    });
    
    // ... Admin Routes ...

    return router;
}

module.exports = { init };
