const express = require('express');
const router = express.Router();
const { db } = require('../services/database'); 
const axios = require('axios');

// Simple in-memory cache helper
const cache = new Map();
async function smartCache(key, ttlSeconds, fetchFn) {
    if (cache.has(key)) {
        const { data, expire } = cache.get(key);
        if (Date.now() < expire) return data;
    }
    const data = await fetchFn();
    cache.set(key, { data, expire: Date.now() + ttlSeconds * 1000 });
    return data;
}

// -----------------------------------------------------------------------------
// HELPER: Fetch Initial Market Data (GeckoTerminal)
// -----------------------------------------------------------------------------
async function fetchInitialMarketData(mint) {
    try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mint}`;
        const res = await axios.get(url, { timeout: 3000 });
        const attrs = res.data.data.attributes;
        
        // Try to get holders immediately if available
        let holders = 0;
        if (attrs.holder_count) holders = parseInt(attrs.holder_count);
        else if (attrs.holders_count) holders = parseInt(attrs.holders_count);

        return {
            priceUsd: parseFloat(attrs.price_usd || 0),
            volume24h: parseFloat(attrs.volume_usd?.h24 || 0),
            change24h: parseFloat(attrs.price_change_percentage?.h24 || 0),
            change1h: parseFloat(attrs.price_change_percentage?.h1 || 0),
            change5m: parseFloat(attrs.price_change_percentage?.m5 || 0),
            marketCap: parseFloat(attrs.fdv_usd || attrs.market_cap_usd || 0),
            supply: parseFloat(attrs.total_supply || 0),
            holders: holders
        };
    } catch (e) {
        return null;
    }
}

// -----------------------------------------------------------------------------
// HELPER: Index Token On-Chain (If not exists)
// -----------------------------------------------------------------------------
async function indexTokenOnChain(mint) {
    const existing = await db.get('SELECT mint FROM tokens WHERE mint = $1', [mint]);
    if (existing) return;

    const marketData = await fetchInitialMarketData(mint);
    
    const initialPrice = marketData?.priceUsd || 0;
    const initialMcap = marketData?.marketCap || 0;
    const initialVol = marketData?.volume24h || 0;
    const initialHolders = marketData?.holders || 0;

    try {
        await db.run(`
            INSERT INTO tokens (
                mint, symbol, name, decimals, supply, 
                priceUsd, marketCap, volume24h, 
                liquidity, holders, k_score, 
                risk_score, dev_holding, created_at, updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, 
                $6, $7, $8, 
                0, $9, 50, 
                0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            ON CONFLICT(mint) DO NOTHING
        `, [
            mint, 'UNKNOWN', 'Unknown Token', 9, 0,
            initialPrice, initialMcap, initialVol,
            initialHolders 
        ]);

        const { enqueueTokenUpdate } = require('../services/queue');
        if (enqueueTokenUpdate) enqueueTokenUpdate(mint);

    } catch (err) {
        console.error(`Failed to index token ${mint}:`, err.message);
    }
}

// -----------------------------------------------------------------------------
// GET /tokens (List View)
// -----------------------------------------------------------------------------
router.get('/tokens', async (req, res) => {
    try {
        const { sort = 'k_score', order = 'desc', limit = 50, search = '' } = req.query;
        const cacheKey = `tokens_${sort}_${order}_${limit}_${search}`;

        const result = await smartCache(cacheKey, 10, async () => {
            let query = `
                SELECT * FROM tokens 
                WHERE (symbol ILIKE $1 OR name ILIKE $1 OR mint ILIKE $1)
            `;
            const params = [`%${search}%`];

            const safeSort = ['k_score', 'volume24h', 'liquidity', 'marketCap', 'created_at', 'change24h'].includes(sort) 
                ? sort 
                : 'k_score';
            
            query += ` ORDER BY "${safeSort}" ${order === 'asc' ? 'ASC' : 'DESC'} LIMIT $2`;
            params.push(limit);

            const rows = await db.all(query, params);
            
            return {
                success: true,
                tokens: rows.map(r => ({
                    mint: r.mint,
                    symbol: r.symbol,
                    name: r.name,
                    priceUsd: r.priceusd || r.priceUsd || 0,
                    marketCap: r.marketcap || r.marketCap || 0,
                    volume24h: r.volume24h || 0,
                    liquidity: r.liquidity || 0,
                    change24h: r.change24h || 0,
                    holders: r.holders || 0,
                    k_score: r.k_score || 0,
                    risk_score: r.risk_score || 0,
                    timestamp: r.timestamp 
                }))
            };
        });

        res.json(result);
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false, error: e.message });
    }
});

// -----------------------------------------------------------------------------
// GET /token/:mint (Detail View) - WITH LIVE REPAIR
// -----------------------------------------------------------------------------
router.get('/token/:mint', async (req, res) => {
    const { mint } = req.params;
    const cacheKey = `token_detail_${mint}`;

    try {
        await indexTokenOnChain(mint);

        const result = await smartCache(cacheKey, 5, async () => {
            let token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
            
            if (!token) return { success: false, error: "Token not found" };

            // --- LIVE DATA REPAIR ---
            // If essential data is missing (0), try to fetch it fresh NOW
            // instead of waiting for the background worker.
            let marketCap = token.marketcap || token.marketCap || 0;
            let holders = token.holders || 0;
            let priceUsd = token.priceusd || token.priceUsd || 0;

            if (marketCap === 0 || holders === 0) {
                const freshData = await fetchInitialMarketData(mint);
                if (freshData) {
                    if (marketCap === 0 && freshData.marketCap > 0) marketCap = freshData.marketCap;
                    if (holders === 0 && freshData.holders > 0) holders = freshData.holders;
                    if (priceUsd === 0 && freshData.priceUsd > 0) priceUsd = freshData.priceUsd;

                    // Fire-and-forget update to DB to save this fresh data
                    db.run(`
                        UPDATE tokens 
                        SET marketCap = $1, holders = $2, priceUsd = $3, updated_at = CURRENT_TIMESTAMP 
                        WHERE mint = $4
                    `, [marketCap, holders, priceUsd, mint]).catch(err => console.error("Live patch DB error:", err.message));
                }
            }

            const pairs = await db.all('SELECT * FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC', [mint]);
            const holderHistory = await db.all('SELECT * FROM holders_history WHERE mint = $1 ORDER BY timestamp ASC', [mint]);

            let tokenData = { ...token };
            if (tokenData.symbol) tokenData.ticker = tokenData.symbol;

            // Normalize Keys for Frontend
            tokenData.marketCap = marketCap;
            tokenData.holders = holders;
            tokenData.priceUsd = priceUsd;
            tokenData.volume24h = tokenData.volume24h || 0;
            tokenData.liquidity = tokenData.liquidity || 0;
            tokenData.change24h = tokenData.change24h || 0;
            tokenData.change1h = tokenData.change1h || 0;
            tokenData.change5m = tokenData.change5m || 0;
            tokenData.timestamp = tokenData.timestamp || 0;

            // Patch price from main pool if still zero
            if (pairs.length > 0 && tokenData.priceUsd === 0) {
                if (pairs[0].price_usd > 0) tokenData.priceUsd = pairs[0].price_usd;
            }

            return { 
                success: true, 
                token: { 
                    ...tokenData, 
                    pairs, 
                    holderHistory 
                } 
            };
        });

        res.json(result);

    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false, error: e.message });
    }
});

module.exports = router;
