/**
 * Token Routes (Correctly Mapped for Detail View)
 */
const express = require('express');
const axios = require('axios');
const { isValidPubkey } = require('../utils/solana');
const { smartCache, saveTokenData } = require('../services/database');
const router = express.Router();

function getSocialLink(pair, type) {
    if (!pair.info || !pair.info.socials) return null;
    const social = pair.info.socials.find(s => s.type === type);
    return social ? social.url : null;
}

function init(deps) {
    const { db } = deps;

    // MAIN ENDPOINT: /api/tokens
    router.get('/tokens', async (req, res) => {
        const { sort = 'newest', limit = 100, search = '' } = req.query;
        const cacheKey = `api:tokens:${sort}:${limit}:${search || 'all'}`;

        try {
            const result = await smartCache(cacheKey, 5, async () => {
                const limitVal = Math.min(parseInt(limit) || 100, 100);
                
                let orderByClause = 'ORDER BY timestamp DESC'; 

                if (sort === 'leaders' || sort === 'mcap') {
                    orderByClause = 'ORDER BY marketCap DESC';
                } else if (sort === 'gainers') {
                    orderByClause = 'ORDER BY change24h DESC';
                }

                let query = `SELECT * FROM tokens`;
                let params = [];

                if (search && search.trim().length > 0) {
                    query += ` WHERE ticker ILIKE $1 OR name ILIKE $1 OR mint = $1`;
                    params.push(`%${search}%`);
                }

                query += ` ${orderByClause} LIMIT ${limitVal}`;

                let rows = params.length > 0 
                    ? await db.all(query, params) 
                    : await db.all(query);

                // External Search Fallback
                if (rows.length === 0 && search && search.trim().length > 2) {
                    try {
                        const dexRes = await axios.get(`https://api.dexscreener.com/latest/dex/search?q=${encodeURIComponent(search)}`, { timeout: 3000 });
                        if (dexRes.data && dexRes.data.pairs) {
                            const validPairs = dexRes.data.pairs.filter(p => p.chainId === 'solana').slice(0, 5);
                            for (const pair of validPairs) {
                                const metadata = {
                                    ticker: pair.baseToken.symbol,
                                    name: pair.baseToken.name,
                                    description: 'Imported via Search',
                                    twitter: getSocialLink(pair, 'twitter'),
                                    website: pair.info?.websites?.[0]?.url || null,
                                    metadataUri: null,
                                    image: pair.info?.imageUrl,
                                    isMayhemMode: false,
                                    marketCap: pair.fdv || pair.marketCap || 0,
                                    volume24h: pair.volume?.h24 || 0,
                                    priceUsd: pair.priceUsd
                                };
                                const createdAt = pair.pairCreatedAt || Date.now();
                                await saveTokenData(null, pair.baseToken.address, metadata, createdAt);
                                rows.push({
                                    mint: pair.baseToken.address,
                                    userPubkey: null,
                                    name: metadata.name,
                                    ticker: metadata.ticker,
                                    image: metadata.image,
                                    marketCap: metadata.marketCap,
                                    volume24h: metadata.volume24h,
                                    priceUsd: metadata.priceUsd,
                                    timestamp: createdAt,
                                    change5m: pair.priceChange?.m5 || 0,
                                    change1h: pair.priceChange?.h1 || 0,
                                    change24h: pair.priceChange?.h24 || 0,
                                    complete: false
                                });
                            }
                        }
                    } catch (extErr) { console.error("External search failed:", extErr.message); }
                }

                return {
                    success: true,
                    tokens: rows.map(r => ({
                        mint: r.mint,
                        userPubkey: r.userpubkey,
                        name: r.name,
                        ticker: r.ticker,
                        image: r.image,
                        marketCap: r.marketcap || r.marketCap || 0,
                        volume24h: r.volume24h || 0,
                        priceUsd: r.priceusd || r.priceUsd || 0,
                        timestamp: parseInt(r.timestamp),
                        change5m: r.change5m || 0,
                        change1h: r.change1h || 0,
                        change24h: r.change24h || 0
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

    // Get single token (Correctly mapped for frontend detail view)
    router.get('/token/:mint', async (req, res) => {
        try {
            const { mint } = req.params;
            const cacheKey = `api:token:${mint}`;
            const result = await smartCache(cacheKey, 30, async () => {
                const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
                if (!token) return null;
                
                // MAPPING FIX: Ensure DB lowercase columns map to frontend camelCase
                return { 
                    success: true, 
                    token: {
                        ...token,
                        marketCap: token.marketcap, 
                        volume24h: token.volume24h,
                        priceUsd: token.priceusd,
                        change1h: token.change1h,
                        change24h: token.change24h,
                        userPubkey: token.userpubkey,
                        // Ensure timestamp is a number for date math
                        timestamp: parseInt(token.timestamp) 
                    } 
                };
            });
            if (!result) return res.status(404).json({ success: false, error: "Not found" });
            res.json(result);
        } catch (e) { res.status(500).json({ success: false, error: "DB Error" }); }
    });

    return router;
}

module.exports = { init };
