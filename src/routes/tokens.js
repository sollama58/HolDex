/**
 * Token Routes
 * Platform: PostgreSQL
 * Updated: Candles queried from candles_1m via pool_address (production schema)
 *          with DexScreener API fallback
 */
const express = require('express');
const axios = require('axios');
const { isValidPubkey } = require('../utils/solana');
const { smartCache } = require('../services/database');
const { getClient } = require('../services/redis'); 
const config = require('../config/env');

const router = express.Router();

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

function init(deps) {
    const { db } = deps;

    // --- HELPER: Fetch candles from DB or production API ---
    async function fetchCandlesForMint(mint, from, to, limit = 100) {
        // 1. Try local candles_1m table first
        try {
            const pool = await db.get(
                `SELECT p.address FROM pools p
                 LEFT JOIN (SELECT pool_address, COUNT(*) as candle_count FROM candles_1m GROUP BY pool_address) c
                 ON p.address = c.pool_address
                 WHERE p.mint = $1
                 ORDER BY c.candle_count DESC NULLS LAST, p.liquidity_usd DESC NULLS LAST
                 LIMIT 1`,
                [mint]
            );

            if (pool?.address) {
                let query = `SELECT timestamp as time, open, high, low, close, volume FROM candles_1m WHERE pool_address = $1`;
                const params = [pool.address];

                if (from) {
                    const fromTs = parseInt(from);
                    if (!isNaN(fromTs)) {
                        query += ` AND timestamp >= $${params.length + 1}`;
                        params.push(fromTs * 1000);
                    }
                }
                if (to) {
                    const toTs = parseInt(to);
                    if (!isNaN(toTs)) {
                        query += ` AND timestamp <= $${params.length + 1}`;
                        params.push(toTs * 1000);
                    }
                }

                query += ` ORDER BY timestamp DESC LIMIT ${Math.min(parseInt(limit) || 100, 2000)}`;

                const rows = await db.all(query, params);
                if (rows && rows.length > 0) {
                    return rows.map(r => ({
                        time: Math.floor(parseInt(r.time) / 1000),
                        open: r.open,
                        high: r.high,
                        low: r.low,
                        close: r.close,
                        volume: r.volume || 0
                    })).reverse();
                }
            }
        } catch (e) {
            console.error("DB candles lookup failed:", e.message);
        }

        // 2. Fallback: fetch from production API
        try {
            const cacheKey = `prod:candles:${mint}`;
            return await smartCache(cacheKey, 30, async () => {
                const url = `https://asdev-backend.onrender.com/api/token/${mint}/candles`;
                const res = await axios.get(url, { timeout: 5000 });
                return res.data?.candles || [];
            });
        } catch (e) {
            console.error("Production candles fetch failed:", e.message);
        }

        return [];
    }

    // --- HISTORY ENDPOINT ---
    router.get('/history/:mint', async (req, res) => {
        const { mint } = req.params;
        res.set('Cache-Control', 'public, max-age=10');

        try {
            const candles = await fetchCandlesForMint(mint);
            res.json(candles);
        } catch (e) {
            console.error("History Error:", e);
            res.status(500).json({ error: 'DB Error' });
        }
    });

    // --- CANDLES ENDPOINT (for frontend compatibility) ---
    router.get('/token/:mint/candles', async (req, res) => {
        const { mint } = req.params;
        const { from, to, limit } = req.query;

        res.set('Cache-Control', 'public, max-age=10');

        try {
            const candles = await fetchCandlesForMint(mint, from, to, limit);
            res.json({ success: true, candles });
        } catch (e) {
            console.error("Candles Error:", e);
            res.status(500).json({ success: false, candles: [], error: 'DB Error' });
        }
    });

    // --- SEARCH / TOKENS LIST ---
    router.get('/tokens', async (req, res) => {
        const { sort = 'newest', limit = 100, page = 1, search = '', filter = '' } = req.query;
        const limitVal = Math.min(parseInt(limit) || 100, 100);
        const pageVal = Math.max(parseInt(page) || 1, 1);
        const offsetVal = (pageVal - 1) * limitVal;
        const searchTerm = search ? search.trim() : '';
        const cacheKey = `api:tokens:${sort}:${limitVal}:${pageVal}:${searchTerm || 'all'}:${filter}`;

        try {
            const result = await smartCache(cacheKey, 5, async () => {
                // Production uses lowercase column names
                let orderByClause = 'ORDER BY timestamp DESC';
                switch (sort) {
                    case 'kscore': orderByClause = 'ORDER BY k_score DESC NULLS LAST'; break;
                    case 'mcap': orderByClause = 'ORDER BY marketcap DESC NULLS LAST'; break;
                    case 'volume': orderByClause = 'ORDER BY volume24h DESC NULLS LAST'; break;
                    case 'gainers': case '24h': orderByClause = 'ORDER BY change24h DESC NULLS LAST'; break;
                    case '1h': orderByClause = 'ORDER BY change1h DESC NULLS LAST'; break;
                    case '5m': orderByClause = 'ORDER BY change5m DESC NULLS LAST'; break;
                    case 'price': orderByClause = 'ORDER BY priceusd DESC NULLS LAST'; break;
                    default: orderByClause = 'ORDER BY timestamp DESC NULLS LAST'; break;
                }

                let rows = [];
                if (searchTerm.length > 0) {
                    if (isValidPubkey(searchTerm)) {
                        rows = await db.all(`SELECT * FROM tokens WHERE mint = $1`, [searchTerm]);
                    } else {
                        const searchPattern = `%${searchTerm}%`;
                        // Production uses 'symbol' column, not 'ticker'
                        rows = await db.all(`SELECT * FROM tokens WHERE (symbol ILIKE $1 OR name ILIKE $1) ${filter === 'verified' ? 'AND hascommunityupdate = TRUE' : ''} ${orderByClause} LIMIT 50`, [searchPattern]);
                    }
                } else {
                    let query = `SELECT * FROM tokens`;
                    let where = [];
                    if (filter === 'verified') where.push(`hascommunityupdate = TRUE`);
                    if (where.length > 0) query += ` WHERE ${where.join(' AND ')}`;
                    query += ` ${orderByClause} LIMIT ${limitVal} OFFSET ${offsetVal}`;
                    rows = await db.all(query);
                }

                // External Search Fallback
                const isTooShort = searchTerm.length < 3;
                if (searchTerm.length > 0 && !isTooShort && await checkExternalRateLimit()) {
                    try {
                        const extKey = `ext:${searchTerm}`;
                        const extTokens = await smartCache(extKey, 300, async () => {
                             if(rows.length > 0) return [];
                             const d = await axios.get(`https://api.dexscreener.com/latest/dex/search?q=${searchTerm}`);
                             return d.data?.pairs || [];
                        });
                        
                        if(extTokens && extTokens.length) {
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
                        mint: r.mint, name: r.name, ticker: r.ticker || r.symbol, image: r.image,
                        marketCap: r.marketcap || r.marketCap || 0, volume24h: r.volume24h || 0, priceUsd: r.priceusd || r.priceUsd || 0,
                        timestamp: parseInt(r.timestamp), change5m: r.change5m || 0, change1h: r.change1h || 0, change24h: r.change24h || 0,
                        hasCommunityUpdate: r.hascommunityupdate || r.hasCommunityUpdate || false, kScore: r.k_score || 0,
                        holders: r.holders || 0, liquidity: r.liquidity || 0
                    })), lastUpdate: Date.now()
                };
            });
            res.json(result);
        } catch (e) {
            console.error('[API Error] /tokens:', e.message);
            res.status(500).json({ success: false, tokens: [], error: 'Internal server error' });
        }
    });

    router.get('/token/:mint', async (req, res) => {
        const { mint } = req.params;
        const cacheKey = `api:token:${mint}`;
        const result = await smartCache(cacheKey, 30, async () => {
            const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
            let tokenData = token || { mint, name: 'Unknown', ticker: 'Unknown' };

            // Normalize field names for frontend compatibility
            if (token) {
                tokenData.hasCommunityUpdate = token.hascommunityupdate;
                tokenData.ticker = token.symbol;
            }

            // Add conviction breakdown
            if (token) {
                tokenData.conviction = {
                    score: token.conviction_score || 0,
                    accumulators: token.conviction_accumulators || 0,
                    holders: token.conviction_holders || 0,
                    reducers: token.conviction_reducers || 0,
                    extractors: token.conviction_extractors || 0,
                    analyzed: token.conviction_analyzed || 0
                };

                // Add holder history (last 30 days) - format: { timestamp, count, realCount }
                try {
                    const history = await db.all(`
                        SELECT date, holders, real_holders
                        FROM holder_history
                        WHERE mint = $1
                        ORDER BY date DESC
                        LIMIT 30
                    `, [mint]);
                    tokenData.holderHistory = history.reverse().map(h => ({
                        timestamp: String(new Date(h.date).getTime()),
                        count: h.holders || 0,           // Total holders (for chart)
                        realCount: h.real_holders || 0   // $1+ holders (for breakdown)
                    }));
                } catch (e) {
                    tokenData.holderHistory = [];
                }
            }

            // Still fetch pairs for metadata purposes if needed
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

    // --- POOLS ENDPOINT: LP info, DEX ---
    router.get('/token/:mint/pools', async (req, res) => {
        const { mint } = req.params;
        const cacheKey = `api:pools:${mint}`;

        try {
            const result = await smartCache(cacheKey, 60, async () => {
                const pools = await db.all(`
                    SELECT address, dex, token_a, token_b,
                           liquidity_usd, price_usd, volume_24h,
                           reserve_a, reserve_b, symbol, created_at
                    FROM pools
                    WHERE mint = $1
                    ORDER BY liquidity_usd DESC NULLS LAST
                `, [mint]);

                return {
                    success: true,
                    mint,
                    pools: pools.map(p => ({
                        address: p.address,
                        dex: p.dex,
                        symbol: p.symbol,
                        tokenA: p.token_a,
                        tokenB: p.token_b,
                        reserveA: p.reserve_a || 0,
                        reserveB: p.reserve_b || 0,
                        liquidityUsd: p.liquidity_usd || 0,
                        priceUsd: p.price_usd || 0,
                        volume24h: p.volume_24h || 0,
                        createdAt: p.created_at
                    })),
                    count: pools.length
                };
            });
            res.json(result);
        } catch (e) {
            console.error('[API Error] /pools:', e.message);
            res.status(500).json({ success: false, pools: [], error: 'Internal server error' });
        }
    });

    // --- HOLDERS ENDPOINT: Top holders, distribution ---
    router.get('/token/:mint/holders', async (req, res) => {
        const { mint } = req.params;
        const { limit = 20 } = req.query;
        const limitVal = Math.min(parseInt(limit) || 20, 100);
        const cacheKey = `api:holders:${mint}:${limitVal}`;

        try {
            const result = await smartCache(cacheKey, 120, async () => {
                // Get token info for context
                const token = await db.get(
                    'SELECT holders, supply, decimals FROM tokens WHERE mint = $1',
                    [mint]
                );

                // Get top holders (schema: holderpubkey, balance, rank)
                const holders = await db.all(`
                    SELECT holderpubkey, balance, rank, updatedat
                    FROM token_holders
                    WHERE mint = $1
                    ORDER BY rank ASC NULLS LAST, balance DESC
                    LIMIT $2
                `, [mint, limitVal]);

                // Calculate percentage from supply
                const supply = token?.supply || 0;

                return {
                    success: true,
                    mint,
                    totalHolders: token?.holders || 0,
                    supply: supply,
                    holders: holders.map(h => ({
                        wallet: h.holderpubkey,
                        balance: h.balance,
                        rank: h.rank,
                        pct: supply > 0 ? Math.round((h.balance / supply) * 10000) / 100 : 0,
                        lastUpdated: h.updatedat
                    })),
                    count: holders.length
                };
            });
            res.json(result);
        } catch (e) {
            console.error('[API Error] /holders:', e.message);
            res.status(500).json({ success: false, holders: [], error: 'Internal server error' });
        }
    });

    // --- K-SCORE ENDPOINT: Full breakdown with pillars ---
    router.get('/token/:mint/kscore', async (req, res) => {
        const { mint } = req.params;
        const cacheKey = `api:kscore:${mint}`;

        try {
            const result = await smartCache(cacheKey, 60, async () => {
                const token = await db.get(`
                    SELECT k_score, holders,
                           conviction_score, conviction_accumulators, conviction_holders,
                           conviction_reducers, conviction_extractors, conviction_analyzed,
                           last_k_score_update, timestamp
                    FROM tokens WHERE mint = $1
                `, [mint]);

                if (!token || token.k_score === null) {
                    return { success: false, error: 'K-Score not calculated for this token' };
                }

                // Reconstruct pillar breakdown from stored data
                const conviction = token.conviction_score || 0;
                const accumulators = token.conviction_accumulators || 0;
                const extractors = token.conviction_extractors || 0;
                const holders = token.holders || 0;

                // Normalized values (approximations from stored data)
                const convictionPct = conviction;
                const accExtRatio = extractors > 0 ? Math.min(accumulators / extractors, 10) : (accumulators > 0 ? 10 : 0);

                return {
                    success: true,
                    mint,
                    kScore: token.k_score,
                    pillars: {
                        diamondHands: {
                            weight: '50%',
                            components: {
                                conviction: convictionPct,
                                accExtRatio: Math.round(accExtRatio * 100) / 100
                            }
                        },
                        organicGrowth: {
                            weight: '35%',
                            components: {
                                holders: holders
                            }
                        },
                        longevity: {
                            weight: '15%',
                            components: {
                                ageDays: token.timestamp ?
                                    Math.round((Date.now() - token.timestamp) / (1000 * 60 * 60 * 24) * 10) / 10 : 0
                            }
                        }
                    },
                    conviction: {
                        score: convictionPct,
                        accumulators: accumulators,
                        holders: token.conviction_holders || 0,
                        reducers: token.conviction_reducers || 0,
                        extractors: extractors,
                        analyzed: token.conviction_analyzed || 0
                    },
                    lastUpdate: token.last_k_score_update
                };
            });
            res.json(result);
        } catch (e) {
            console.error('[API Error] /kscore:', e.message);
            res.status(500).json({ success: false, error: 'Internal server error' });
        }
    });

    // --- BATCH ENDPOINT: Multiple tokens in one call ---
    router.post('/tokens/batch', async (req, res) => {
        const { mints } = req.body;

        if (!mints || !Array.isArray(mints)) {
            return res.status(400).json({ success: false, error: 'mints array required' });
        }

        // Limit batch size
        const mintList = mints.slice(0, 50);

        if (mintList.length === 0) {
            return res.json({ success: true, tokens: [] });
        }

        try {
            // Build parameterized query
            const placeholders = mintList.map((_, i) => `$${i + 1}`).join(', ');
            const tokens = await db.all(
                `SELECT * FROM tokens WHERE mint IN (${placeholders})`,
                mintList
            );

            const tokenMap = new Map(tokens.map(t => [t.mint, t]));

            res.json({
                success: true,
                tokens: mintList.map(mint => {
                    const t = tokenMap.get(mint);
                    if (!t) return { mint, found: false };
                    return {
                        mint: t.mint,
                        found: true,
                        name: t.name,
                        ticker: t.symbol,
                        image: t.image,
                        priceUsd: t.priceusd || 0,
                        marketCap: t.marketcap || 0,
                        volume24h: t.volume24h || 0,
                        change24h: t.change24h || 0,
                        holders: t.holders || 0,
                        kScore: t.k_score || 0,
                        conviction: {
                            score: t.conviction_score || 0,
                            accumulators: t.conviction_accumulators || 0
                        }
                    };
                }),
                count: tokens.length
            });
        } catch (e) {
            console.error('[API Error] /tokens/batch:', e.message);
            res.status(500).json({ success: false, error: 'Internal server error' });
        }
    });

    // --- FEE CONFIG (for frontend) ---
    router.get('/config/fees', (req, res) => {
        res.json({
            success: true,
            solFee: config.FEE_SOL || 0.025,
            tokenFee: config.FEE_TOKEN_AMOUNT || 100000,
            tokenMint: config.FEE_TOKEN_MINT || '9zB5wRarXMj86MymwLumSKA1Dx35zPqqKfcZtK1Spump',
            treasury: config.TREASURY_WALLET || '4AkNoCdhsGLwmYcYgwXXEoNtExQhEkzsxynsLfWjpRKg'
        });
    });

    // --- TRIGGER K-SCORE RECALC (for testing/admin) ---
    router.post('/token/:mint/recalc', async (req, res) => {
        const { mint } = req.params;

        try {
            const kScore = require('../tasks/kScoreUpdater');
            const result = await kScore.updateSingleToken(deps, mint);

            if (!result) {
                return res.status(404).json({ success: false, error: 'Token not found' });
            }

            res.json({
                success: true,
                mint,
                kScore: result.kScore || result.score,
                conviction: result.metrics?.conviction,
                broadcasted: !!deps.broadcast
            });
        } catch (e) {
            console.error('[API Error] /recalc:', e.message);
            res.status(500).json({ success: false, error: 'Recalculation failed' });
        }
    });

    return router;
}

module.exports = { init };
