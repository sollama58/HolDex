const express = require('express');
const axios = require('axios');
const { Connection, PublicKey } = require('@solana/web3.js');
const { isValidPubkey } = require('../utils/solana');
const { smartCache, enableIndexing, aggregateAndSaveToken } = require('../services/database');
const { getClient } = require('../services/redis'); 
const config = require('../config/env');
const kScoreUpdater = require('../tasks/kScoreUpdater'); 

const router = express.Router();
const solanaConnection = new Connection(config.SOLANA_RPC_URL, 'confirmed');

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

        // 1. Index ALL pools found
        // This ensures volume is tracked for every pool
        for (const pair of pairs) {
            await enableIndexing(db, mint, pair);
        }

        // 2. Prepare Base Data from the "Best" Pair
        const bestPair = pairs[0];
        const baseData = {
            name: bestPair.baseToken.name,
            ticker: bestPair.baseToken.symbol,
            image: bestPair.info?.imageUrl,
            marketCap: Number(bestPair.fdv || bestPair.marketCap || 0),
            volume24h: 0, // Reset, will be aggregated
            priceUsd: Number(bestPair.priceUsd || 0),
            change1h: bestPair.priceChange?.h1 || 0,
            change24h: bestPair.priceChange?.h24 || 0,
            change5m: bestPair.priceChange?.m5 || 0,
        };

        // 3. Aggregate Volume & Price from the DB
        await aggregateAndSaveToken(db, mint, baseData);
        
        return baseData;
    }

    // ... (Fees, Balance, Request-Update Endpoints remain same) ...

    router.get('/token/:mint/candles', async (req, res) => {
        const { mint } = req.params;
        const { resolution = '60', from, to } = req.query; 

        const nowMin = Math.floor(Date.now() / 60000);
        const cacheKey = `chart:${mint}:${resolution}:${nowMin}`; 

        try {
            const result = await smartCache(cacheKey, 60, async () => {
                // Find the pool with the HIGHEST LIQUIDITY for charting
                let pool = await db.get(`
                    SELECT address FROM pools 
                    WHERE mint = $1 
                    ORDER BY liquidity_usd DESC 
                    LIMIT 1
                `, [mint]);
                
                if (!pool) return { success: false, error: "Token not indexed yet" };

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
        const { sort = 'newest', limit = 100, page = 1, search = '', filter = '' } = req.query;
        // ... (Standard Pagination/Sort logic) ...
        const limitVal = Math.min(parseInt(limit) || 100, 100);
        const pageVal = Math.max(parseInt(page) || 1, 1);
        const offsetVal = (pageVal - 1) * limitVal;
        const searchTerm = search ? search.trim() : '';
        const cacheKey = `api:tokens:${sort}:${limitVal}:${pageVal}:${searchTerm || 'all'}:${filter}`;

        try {
            const result = await smartCache(cacheKey, 5, async () => {
                let rows = [];
                let orderByClause = 'ORDER BY timestamp DESC'; 
                // ... (Sort logic) ...
                
                const isAddressSearch = isValidPubkey(searchTerm);

                // Local Search
                if (searchTerm.length > 0) {
                     if (isAddressSearch) rows = await db.all(`SELECT * FROM tokens WHERE mint = $1`, [searchTerm]);
                     else rows = await db.all(`SELECT * FROM tokens WHERE (ticker ILIKE $1 OR name ILIKE $1) LIMIT 50`, [`%${searchTerm}%`]);
                } else {
                     rows = await db.all(`SELECT * FROM tokens ${orderByClause} LIMIT ${limitVal} OFFSET ${offsetVal}`);
                }

                // External Search
                if (searchTerm.length >= 3) {
                    const extCacheKey = `ext_search:${searchTerm.toLowerCase()}`;
                    const externalTokens = await smartCache(extCacheKey, 300, async () => { 
                        try {
                             let dexRes;
                             if (isAddressSearch) dexRes = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${searchTerm}`);
                             else dexRes = await axios.get(`https://api.dexscreener.com/latest/dex/search?q=${encodeURIComponent(searchTerm)}`);

                             if (dexRes.data?.pairs) {
                                 // Process Exact Matches Immediately
                                 if (isAddressSearch) {
                                     // This is where we AUTO-INDEX
                                     await processDexPairs(searchTerm, dexRes.data.pairs);
                                 }
                                 return dexRes.data.pairs.map(p => ({
                                     mint: p.baseToken.address,
                                     name: p.baseToken.name,
                                     ticker: p.baseToken.symbol,
                                     image: p.info?.imageUrl,
                                     marketCap: Number(p.fdv || p.marketCap || 0),
                                     volume24h: Number(p.volume?.h24 || 0),
                                     priceUsd: Number(p.priceUsd || 0),
                                     timestamp: p.pairCreatedAt || Date.now()
                                 }));
                             }
                             return [];
                        } catch(e) { return []; }
                    });
                    
                    if (externalTokens) {
                         // Merge Logic ...
                         const existingMints = new Set(rows.map(r => r.mint));
                         externalTokens.forEach(t => {
                             if(!existingMints.has(t.mint)) rows.push({...t, hasCommunityUpdate:false, kScore:0});
                         });
                    }
                }

                return {
                    success: true, page: pageVal, limit: limitVal,
                    tokens: rows, // simplified map for brevity
                    lastUpdate: Date.now()
                };
            });
            res.json(result);
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    return router;
}

module.exports = { init };
