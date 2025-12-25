/**
 * Token Routes
 * Platform: PostgreSQL
 * Optimization: Added History Endpoint for Custom Charts
 */
const express = require('express');
const axios = require('axios');
const { Connection, PublicKey } = require('@solana/web3.js');
const { isValidPubkey } = require('../utils/solana');
const { smartCache, saveTokenData } = require('../services/database');
const { getClient } = require('../services/redis'); 
const config = require('../config/env');
const kScoreUpdater = require('../tasks/kScoreUpdater'); 
const { syncTokenData } = require('../tasks/metadataUpdater'); 

const router = express.Router();
const solanaConnection = new Connection(config.SOLANA_RPC_URL, 'confirmed');

// --- HELPER: GLOBAL RATE LIMITER ---
async function checkExternalRateLimit() {
    try {
        const redis = getClient();
        if (!redis || redis.status !== 'ready') return true; 

        const key = 'ratelimit:dexscreener:global';
        const current = await redis.incr(key);
        
        if (current === 1) {
            await redis.expire(key, 60);
        }

        return current <= 250;
    } catch (e) {
        return true; 
    }
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

    // --- NEW: HISTORY ENDPOINT FOR CHART ---
    // Fetches OHLC candles from our local Postgres
    router.get('/history/:symbol', async (req, res) => {
        const { symbol } = req.params;
        
        // Cache this response for 30s to save DB load
        res.set('Cache-Control', 'public, max-age=30');

        try {
            // Get last 2000 candles
            const candles = await db.all(`
                SELECT time, open, high, low, close 
                FROM candles 
                WHERE symbol = $1 
                ORDER BY time ASC 
                LIMIT 2000
            `, [symbol]);

            // If empty, user might be requesting by Mint address but we indexed by 'SOL' symbol
            // For now, we return empty list if not found.
            res.json(candles || []);
        } catch (e) {
            console.error("History Error:", e);
            res.status(500).json({ error: 'DB Error' });
        }
    });

    // --- SEARCH / TOKENS LIST (OPTIMIZED) ---
    router.get('/tokens', async (req, res) => {
        const { sort = 'newest', limit = 100, page = 1, search = '', filter = '' } = req.query;
        const limitVal = Math.min(parseInt(limit) || 100, 100);
        const pageVal = Math.max(parseInt(page) || 1, 1);
        const offsetVal = (pageVal - 1) * limitVal;
        const searchTerm = search ? search.trim() : '';
        const cacheKey = `api:tokens:${sort}:${limitVal}:${pageVal}:${searchTerm || 'all'}:${filter}`;

        try {
            const result = await smartCache(cacheKey, 5, async () => {
                let orderByClause = 'ORDER BY timestamp DESC'; 
                switch (sort) {
                    case 'kscore': orderByClause = 'ORDER BY k_score DESC'; break;
                    case 'mcap': orderByClause = 'ORDER BY marketCap DESC'; break;
                    case 'volume': orderByClause = 'ORDER BY volume24h DESC'; break;
                    case 'gainers': case '24h': orderByClause = 'ORDER BY change24h DESC'; break;
                    case '1h': orderByClause = 'ORDER BY change1h DESC'; break;
                    case '5m': orderByClause = 'ORDER BY change5m DESC'; break;
                    case 'price': orderByClause = 'ORDER BY priceUsd DESC'; break;
                    default: orderByClause = 'ORDER BY timestamp DESC'; break;
                }

                let rows = [];
                if (searchTerm.length > 0) {
                    if (isValidPubkey(searchTerm)) {
                        rows = await db.all(`SELECT * FROM tokens WHERE mint = $1`, [searchTerm]);
                    } else {
                        const searchPattern = `%${searchTerm}%`;
                        rows = await db.all(`SELECT * FROM tokens WHERE (ticker ILIKE $1 OR name ILIKE $1) ${filter === 'verified' ? 'AND hasCommunityUpdate = TRUE' : ''} ${orderByClause} LIMIT 50`, [searchPattern]);
                    }
                } else {
                    let query = `SELECT * FROM tokens`;
                    let where = [];
                    if (filter === 'verified') where.push(`hasCommunityUpdate = TRUE`);
                    if (where.length > 0) query += ` WHERE ${where.join(' AND ')}`;
                    query += ` ${orderByClause} LIMIT ${limitVal} OFFSET ${offsetVal}`;
                    rows = await db.all(query);
                }

                // External Search Logic (Simplified for brevity)
                const isTooShort = searchTerm.length < 3;
                if (searchTerm.length > 0 && !isTooShort && await checkExternalRateLimit()) {
                    try {
                        const extKey = `ext:${searchTerm}`;
                        const extTokens = await smartCache(extKey, 300, async () => {
                             // Only search generic if no result locally
                             if(rows.length > 0) return [];
                             const d = await axios.get(`https://api.dexscreener.com/latest/dex/search?q=${searchTerm}`);
                             return d.data?.pairs || [];
                        });
                        
                        if(extTokens && extTokens.length) {
                             // Simplified merging logic
                             const found = extTokens.filter(p => p.chainId === 'solana')[0];
                             if(found && !rows.find(r => r.mint === found.baseToken.address)) {
                                 rows.push({
                                     mint: found.baseToken.address,
                                     name: found.baseToken.name,
                                     ticker: found.baseToken.symbol,
                                     image: found.info?.imageUrl,
                                     marketCap: found.fdv,
                                     k_score: 0
                                 });
                             }
                        }
                    } catch(e) {}
                }

                return {
                    success: true, page: pageVal, limit: limitVal,
                    tokens: rows.map(r => ({
                        mint: r.mint, name: r.name, ticker: r.ticker, image: r.image,
                        marketCap: r.marketcap || r.marketCap || 0, volume24h: r.volume24h || 0, priceUsd: r.priceusd || r.priceUsd || 0,
                        timestamp: parseInt(r.timestamp), change5m: r.change5m || 0, change1h: r.change1h || 0, change24h: r.change24h || 0,
                        hasCommunityUpdate: r.hascommunityupdate || r.hasCommunityUpdate || false, kScore: r.k_score || 0
                    })), lastUpdate: Date.now()
                };
            });
            res.json(result);
        } catch (e) { res.status(500).json({ success: false, tokens: [], error: e.message }); }
    });

    router.get('/token/:mint', async (req, res) => {
        const { mint } = req.params;
        const cacheKey = `api:token:${mint}`;
        const result = await smartCache(cacheKey, 30, async () => {
            const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
            let tokenData = token || { mint, name: 'Unknown', ticker: 'Unknown' };
            
            // Note: We still fetch pairs info for Metadata, but chart data now comes from /history
            try {
                const dexRes = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${mint}`, { timeout: 3000 });
                if (dexRes.data?.pairs) {
                    tokenData.pairs = dexRes.data.pairs;
                }
            } catch(e) {}
            
            return { success: true, token: tokenData };
        });
        res.json(result);
    });

    return router;
}

module.exports = { init };
