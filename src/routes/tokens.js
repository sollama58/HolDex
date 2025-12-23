/**
 * Token Routes (Optimized with Caching)
 */
const express = require('express');
const { isValidPubkey } = require('../utils/solana');
const { smartCache } = require('../services/database'); // We use the helper we defined

const router = express.Router();

function init(deps) {
    const { db } = deps;

    // MAIN ENDPOINT: /api/tokens
    // Cached for 5 seconds to prevent DB hammering
    router.get('/tokens', async (req, res) => {
        const { sort = 'newest', limit = 100, search = '' } = req.query;
        
        // Create a unique cache key based on query params
        const cacheKey = `api:tokens:${sort}:${limit}:${search || 'all'}`;

        try {
            // smartCache(key, ttl_seconds, callback)
            const result = await smartCache(cacheKey, 5, async () => {
                
                const limitVal = Math.min(parseInt(limit) || 100, 100);
                let orderByClause = 'ORDER BY timestamp DESC'; // Default

                if (sort === 'mcap') orderByClause = 'ORDER BY marketCap DESC';
                else if (sort === 'gainers') orderByClause = 'ORDER BY change24h DESC';
                else if (sort === 'volume') orderByClause = 'ORDER BY volume24h DESC';

                let query = `SELECT * FROM tokens`;
                let params = [];

                if (search && search.trim().length > 0) {
                    query += ` WHERE ticker ILIKE $1 OR name ILIKE $1 OR mint = $1`;
                    params.push(`%${search}%`);
                }

                query += ` ${orderByClause} LIMIT ${limitVal}`;

                const rows = params.length > 0 
                    ? await db.all(query, params) 
                    : await db.all(query);

                return {
                    success: true,
                    tokens: rows.map(r => ({
                        mint: r.mint,
                        userPubkey: r.userpubkey,
                        name: r.name,
                        ticker: r.ticker,
                        image: r.image,
                        marketCap: r.marketcap || 0,
                        volume24h: r.volume24h || 0,
                        priceUsd: r.priceusd || 0,
                        timestamp: parseInt(r.timestamp),
                        change5m: r.change5m || 0,
                        change1h: r.change1h || 0,
                        change24h: r.change24h || 0,
                        complete: !!r.complete
                    })),
                    lastUpdate: Date.now()
                };
            });

            res.json(result);
        } catch (e) {
            console.error("Fetch Tokens Error:", e);
            res.status(500).json({ success: false, tokens: [], error: e.message });
        }
    });

    // Get single token (Cached for 30s)
    router.get('/token/:mint', async (req, res) => {
        try {
            const { mint } = req.params;
            const cacheKey = `api:token:${mint}`;

            const result = await smartCache(cacheKey, 30, async () => {
                const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
                if (!token) return null;

                return { 
                    success: true, 
                    token: {
                        ...token,
                        marketCap: token.marketcap,
                        volume24h: token.volume24h,
                        priceUsd: token.priceusd,
                        change1h: token.change1h,
                        change24h: token.change24h
                    } 
                };
            });
            
            if (!result) return res.status(404).json({ success: false, error: "Not found" });
            res.json(result);
        } catch (e) {
            res.status(500).json({ success: false, error: "DB Error" });
        }
    });

    // KOTH (Cached for 10s)
    router.get('/koth', async (req, res) => {
        try {
            const result = await smartCache('api:koth', 10, async () => {
                const koth = await db.get(`
                    SELECT * FROM tokens 
                    WHERE marketCap > 0 
                    ORDER BY marketCap DESC 
                    LIMIT 1
                `);
                return koth ? { found: true, token: koth } : { found: false };
            });
            res.json(result);
        } catch (e) {
            res.status(500).json({ error: "DB Error" });
        }
    });

    // Check holder status (No Cache - User Specific)
    router.get('/check-holder', async (req, res) => {
        const { userPubkey } = req.query;
        if (!userPubkey || !isValidPubkey(userPubkey)) {
            return res.status(400).json({ isHolder: false, error: "Invalid address" });
        }

        try {
            // Get Top 10 Tokens
            const top10 = await db.all('SELECT mint FROM tokens ORDER BY volume24h DESC LIMIT 10');
            const top10Mints = top10.map(t => t.mint);

            let heldPositionsCount = 0;

            if (top10Mints.length > 0) {
                const placeholders = top10Mints.map((_, i) => `$${i + 2}`).join(',');
                const query = `SELECT COUNT(*) as count FROM token_holders WHERE holderPubkey = $1 AND mint IN (${placeholders})`;
                const result = await db.get(query, [userPubkey, ...top10Mints]);
                heldPositionsCount = parseInt(result?.count || 0);
            }

            res.json({
                isHolder: heldPositionsCount > 0,
                heldPositionsCount,
                checkedAgainst: top10Mints.length
            });
        } catch (e) {
            console.error(e);
            res.status(500).json({ error: "DB Error" });
        }
    });

    return router;
}

module.exports = { init };
