/**
 * Token Routes
 * Added: Update Request & Admin Endpoints
 */
const express = require('express');
const axios = require('axios');
const { isValidPubkey } = require('../utils/solana');
const { smartCache, saveTokenData } = require('../services/database');
const config = require('../config/env');
const router = express.Router();

function getSocialLink(pair, type) {
    if (!pair.info || !pair.info.socials) return null;
    const social = pair.info.socials.find(s => s.type === type);
    return social ? social.url : null;
}

// Middleware for Admin Auth
const requireAdmin = (req, res, next) => {
    const authHeader = req.headers['x-admin-auth'];
    if (!authHeader || authHeader !== config.ADMIN_PASSWORD) {
        return res.status(403).json({ success: false, error: 'Unauthorized' });
    }
    next();
};

function init(deps) {
    const { db } = deps;

    // --- PUBLIC ENDPOINTS ---

    // Submit Update Request
    router.post('/request-update', async (req, res) => {
        try {
            const { mint, twitter, website, telegram } = req.body;

            if (!mint || mint.length < 30) {
                return res.status(400).json({ success: false, error: "Invalid Mint Address" });
            }

            // Basic validation
            const hasData = (twitter && twitter.length > 0) || (website && website.length > 0) || (telegram && telegram.length > 0);
            if (!hasData) {
                return res.status(400).json({ success: false, error: "At least one link is required" });
            }

            await db.run(`
                INSERT INTO token_updates (mint, twitter, website, telegram, submittedAt, status)
                VALUES ($1, $2, $3, $4, $5, 'pending')
            `, [mint, twitter, website, telegram, Date.now()]);

            res.json({ success: true, message: "Update queued for approval" });
        } catch (e) {
            console.error("Update Request Error:", e);
            res.status(500).json({ success: false, error: "Submission failed" });
        }
    });

    // Get Tokens (Existing Logic with Pagination)
    router.get('/tokens', async (req, res) => {
        const { sort = 'newest', limit = 100, page = 1, search = '' } = req.query;
        const limitVal = Math.min(parseInt(limit) || 100, 100);
        const pageVal = Math.max(parseInt(page) || 1, 1);
        const offsetVal = (pageVal - 1) * limitVal;
        const cacheKey = `api:tokens:${sort}:${limitVal}:${pageVal}:${search || 'all'}`;

        try {
            const result = await smartCache(cacheKey, 5, async () => {
                let orderByClause = 'ORDER BY timestamp DESC'; 
                switch (sort) {
                    case 'mcap': orderByClause = 'ORDER BY marketCap DESC'; break;
                    case 'volume': orderByClause = 'ORDER BY volume24h DESC'; break;
                    case 'gainers': case '24h': orderByClause = 'ORDER BY change24h DESC'; break;
                    case '1h': orderByClause = 'ORDER BY change1h DESC'; break;
                    case '5m': orderByClause = 'ORDER BY change5m DESC'; break;
                    case 'price': orderByClause = 'ORDER BY priceUsd DESC'; break;
                    case 'newest': case 'age': default: orderByClause = 'ORDER BY timestamp DESC'; break;
                }

                let query = `SELECT * FROM tokens`;
                let params = [];

                if (search && search.trim().length > 0) {
                    query += ` WHERE ticker ILIKE $1 OR name ILIKE $1 OR mint = $1`;
                    params.push(`%${search}%`);
                }

                query += ` ${orderByClause} LIMIT ${limitVal} OFFSET ${offsetVal}`;

                let rows = params.length > 0 ? await db.all(query, params) : await db.all(query);

                // External Search (Page 1 Only)
                if (pageVal === 1 && rows.length < 5 && search && search.trim().length > 2) {
                    try {
                        const dexRes = await axios.get(`https://api.dexscreener.com/latest/dex/search?q=${encodeURIComponent(search)}`, { timeout: 3000 });
                        if (dexRes.data && dexRes.data.pairs) {
                            const validPairs = dexRes.data.pairs.filter(p => p.chainId === 'solana').slice(0, 5);
                            for (const pair of validPairs) {
                                if (rows.some(r => r.mint === pair.baseToken.address)) continue;
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

                if (pageVal === 1 && rows.length > 0) {
                     if (sort === 'newest' || sort === 'age') rows.sort((a, b) => parseInt(b.timestamp) - parseInt(a.timestamp));
                     else if (sort === 'mcap') rows.sort((a, b) => (b.marketcap || b.marketCap) - (a.marketcap || a.marketCap));
                }

                return {
                    success: true,
                    page: pageVal,
                    limit: limitVal,
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
                        marketCap: token.marketcap || 0, 
                        volume24h: token.volume24h || 0,
                        priceUsd: token.priceusd || 0,
                        change1h: token.change1h || 0,
                        change24h: token.change24h || 0,
                        change5m: token.change5m || 0,
                        userPubkey: token.userpubkey,
                        timestamp: parseInt(token.timestamp) 
                    } 
                };
            });
            if (!result) return res.status(404).json({ success: false, error: "Not found" });
            res.json(result);
        } catch (e) { res.status(500).json({ success: false, error: "DB Error" }); }
    });

    // --- ADMIN ENDPOINTS ---

    // Get Pending Updates
    router.get('/admin/updates', requireAdmin, async (req, res) => {
        try {
            // Join with tokens table to get useful context (name/ticker)
            const updates = await db.all(`
                SELECT u.*, t.name, t.ticker, t.image 
                FROM token_updates u
                LEFT JOIN tokens t ON u.mint = t.mint
                WHERE u.status = 'pending'
                ORDER BY u.submittedAt ASC
            `);
            res.json({ success: true, updates });
        } catch (e) {
            res.status(500).json({ success: false, error: e.message });
        }
    });

    // Approve Update
    router.post('/admin/approve-update', requireAdmin, async (req, res) => {
        const { id } = req.body;
        try {
            const update = await db.get('SELECT * FROM token_updates WHERE id = $1', [id]);
            if (!update) return res.status(404).json({ success: false, error: 'Request not found' });

            // Build dynamic update query based on provided fields
            const fields = [];
            const params = [];
            let idx = 1;

            if (update.twitter) { fields.push(`twitter = $${idx++}`); params.push(update.twitter); }
            if (update.website) { fields.push(`website = $${idx++}`); params.push(update.website); }
            if (update.telegram) { fields.push(`tweetUrl = $${idx++}`); params.push(update.telegram); } // Assuming 'tweetUrl' stores generic social or rename col

            if (fields.length > 0) {
                params.push(update.mint);
                await db.run(`UPDATE tokens SET ${fields.join(', ')} WHERE mint = $${idx}`, params);
            }

            // Remove from queue
            await db.run('DELETE FROM token_updates WHERE id = $1', [id]);

            res.json({ success: true, message: 'Approved' });
        } catch (e) {
            res.status(500).json({ success: false, error: e.message });
        }
    });

    // Reject Update
    router.post('/admin/reject-update', requireAdmin, async (req, res) => {
        const { id } = req.body;
        try {
            await db.run('DELETE FROM token_updates WHERE id = $1', [id]);
            res.json({ success: true, message: 'Rejected' });
        } catch (e) {
            res.status(500).json({ success: false, error: e.message });
        }
    });

    return router;
}

module.exports = { init };
