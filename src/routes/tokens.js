const express = require('express');
const router = express.Router();
const axios = require('axios');

// Database reference injected via init()
let db = null;

// Helper for delay
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Simple in-memory cache
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

// Helper: Fetch Initial Market Data (GeckoTerminal)
async function fetchInitialMarketData(mint) {
    try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mint}`;
        const res = await axios.get(url, { timeout: 3000 });
        if (!res.data || !res.data.data) return null;

        const attrs = res.data.data.attributes;
        
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

// Helper: Index Token On-Chain
async function indexTokenOnChain(mint) {
    if (!db) return;
    try {
        const existing = await db.get('SELECT mint FROM tokens WHERE mint = $1', [mint]);
        if (existing) return;

        const marketData = await fetchInitialMarketData(mint);
        
        const initialPrice = marketData?.priceUsd || 0;
        const initialMcap = marketData?.marketCap || 0;
        const initialVol = marketData?.volume24h || 0;
        const initialHolders = marketData?.holders || 0;

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

        try {
            // Optional queue trigger
            const queue = require('../services/queue');
            if (queue && queue.enqueueTokenUpdate) queue.enqueueTokenUpdate(mint);
        } catch (qErr) { /* ignore */ }

    } catch (err) {
        console.error(`Failed to index token ${mint}:`, err.message);
    }
}

// GET /tokens (List View)
router.get('/tokens', async (req, res) => {
    try {
        if (!db) throw new Error("Database not initialized");

        const { sort = 'k_score', order = 'desc', limit = 50, search = '', page = 1 } = req.query;
        const offset = (page - 1) * limit;
        const cacheKey = `tokens_${sort}_${order}_${limit}_${offset}_${search}`;

        const result = await smartCache(cacheKey, 5, async () => {
            let query = `
                SELECT * FROM tokens 
                WHERE (symbol ILIKE $1 OR name ILIKE $1 OR mint ILIKE $1)
            `;
            const params = [`%${search}%`, limit, offset];
            
            // Safe sort columns - Maps frontend names to DB columns
            const safeSortMap = {
                'k_score': 'k_score',
                'volume24h': 'volume24h',
                'liquidity': 'liquidity', 
                'marketCap': 'marketcap', 
                'mcap': 'marketcap', 
                'created_at': 'created_at', 
                'newest': 'created_at',
                'change24h': 'change24h',
                'change1h': 'change1h',
                'age': 'created_at'
            };
            
            const sortCol = safeSortMap[sort] || 'k_score';
            
            query += ` ORDER BY "${sortCol}" ${order === 'asc' ? 'ASC' : 'DESC'} LIMIT $2 OFFSET $3`;

            const rows = await db.all(query, params) || [];
            
            return {
                success: true,
                tokens: rows.map(r => ({
                    mint: r.mint,
                    symbol: r.symbol,
                    name: r.name,
                    image: r.image,
                    priceUsd: r.priceusd || r.priceUsd || 0,
                    marketCap: r.marketcap || r.marketCap || 0,
                    volume24h: r.volume24h || 0,
                    liquidity: r.liquidity || 0,
                    change24h: r.change24h || 0,
                    change1h: r.change1h || 0,
                    change5m: r.change5m || 0,
                    holders: r.holders || 0,
                    k_score: r.k_score || 0,
                    risk_score: r.risk_score || 0,
                    timestamp: r.timestamp,
                    hasCommunityUpdate: r.hascommunityupdate || r.hasCommunityUpdate
                }))
            };
        });

        res.json(result);
    } catch (e) {
        console.error("API Error:", e);
        res.status(500).json({ success: false, error: e.message });
    }
});

// GET /token/:mint (Detail View)
router.get('/token/:mint', async (req, res) => {
    const { mint } = req.params;
    const cacheKey = `token_detail_${mint}`;

    try {
        if (!db) throw new Error("Database not initialized");
        await indexTokenOnChain(mint);

        const fetchAndValidate = async () => {
            let token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
            
            if (!token) {
                 const fresh = await fetchInitialMarketData(mint);
                 if (fresh) {
                     token = { ...fresh, mint, name: 'Loading...', symbol: 'LOAD' };
                 }
            }

            if (!token) return { success: false, error: "Token not found" };

            const pairs = await db.all('SELECT * FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC', [mint]) || [];
            const holderHistory = await db.all('SELECT * FROM holders_history WHERE mint = $1 ORDER BY timestamp ASC', [mint]) || [];

            // --- DATA NORMALIZATION ---
            let tokenData = { ...token };
            if (tokenData.symbol) tokenData.ticker = tokenData.symbol;

            tokenData.marketCap = tokenData.marketcap || tokenData.marketCap || 0;
            tokenData.holders = tokenData.holders || 0;
            tokenData.priceUsd = tokenData.priceusd || tokenData.priceUsd || 0;
            tokenData.volume24h = tokenData.volume24h || 0;
            tokenData.liquidity = tokenData.liquidity || 0;
            
            if (pairs.length > 0 && tokenData.priceUsd === 0) {
                if (pairs[0].price_usd > 0) tokenData.priceUsd = pairs[0].price_usd;
            }

            const cleanPairs = pairs.map(p => ({
                ...p,
                price_usd: p.price_usd || 0,
                liquidity_usd: p.liquidity_usd || 0,
                volume_24h: p.volume_24h || 0
            }));

            const cleanHolderHistory = holderHistory
                .map(h => ({
                    timestamp: Number(h.timestamp), 
                    count: (h.count !== null && h.count !== undefined) ? Number(h.count) : 0
                }))
                .filter(h => h.timestamp > 0)
                .sort((a, b) => a.timestamp - b.timestamp);

            return { 
                success: true, 
                token: { 
                    ...tokenData, 
                    pairs: cleanPairs, 
                    holderHistory: cleanHolderHistory 
                } 
            };
        };

        const result = await smartCache(cacheKey, 3, fetchAndValidate);
        res.json(result);

    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false, error: e.message });
    }
});

// GET /token/:mint/candles (Chart View)
router.get('/token/:mint/candles', async (req, res) => {
    const { mint } = req.params;
    const { resolution = 60, from, to } = req.query; // Resolution in minutes

    try {
        if (!db) throw new Error("Database not initialized");

        // 1. Find the main pool for this token
        const pool = await db.get(
            `SELECT address FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1`, 
            [mint]
        );

        if (!pool) {
            return res.json({ success: true, bars: [] }); // No pool, no data
        }

        // 2. Normalize Timestamps
        // Frontend often sends seconds (Unix), DB stores Milliseconds (JS Date)
        // Check heuristics: if 'from' is small (like 1700000000), it's seconds.
        let fromTs = parseInt(from);
        let toTs = parseInt(to);
        
        if (fromTs < 10000000000) fromTs *= 1000;
        if (toTs < 10000000000) toTs *= 1000;

        // 3. Query Candles
        // Using pool.address to fetch from candles_1m
        const rows = await db.all(`
            SELECT timestamp, open, high, low, close, volume
            FROM candles_1m 
            WHERE pool_address = $1 
            AND timestamp >= $2 
            AND timestamp <= $3 
            ORDER BY timestamp ASC
        `, [pool.address, fromTs, toTs]);

        // 4. Basic Aggregation (Optional but recommended if resolution > 1m)
        // For now, we return 1m candles. If you need strict resolution mapping:
        // You would group by (timestamp / (resolution * 60000)) here.
        // Returning raw 1m data is usually accepted by chart libs if they handle zooming.
        
        const bars = rows.map(r => ({
            time: Number(r.timestamp) / 1000, // Convert back to seconds for chart libs (TradingView default)
            open: r.open,
            high: r.high,
            low: r.low,
            close: r.close,
            volume: r.volume
        }));

        res.json(bars); // Standard TV chart format often expects Array directly or { s: 'ok', t: [], ... }
        // If your frontend expects { success: true, bars: [...] }, revert to that.
        // Based on "DexScreener competitor" usually expects Array or TV format.
        // Returning Array based on common practices.

    } catch (e) {
        console.error(`Candle Error ${mint}:`, e);
        res.status(500).json({ success: false, error: e.message });
    }
});

// Export init function matching index.js requirement
module.exports = {
    init: (deps) => {
        db = deps.db;
        return router;
    }
};
