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
    // 1. Check if exists
    const existing = await db.get('SELECT mint FROM tokens WHERE mint = $1', [mint]);
    if (existing) return;

    // 2. Fetch Metadata (Metaplex / Helius / Etc would go here, simplified for now)
    // For now we use a placeholder or basic fetch if we had a metadata service.
    // We'll rely on the background worker to fill in details later.
    
    // 3. Fetch Initial Market Data
    const marketData = await fetchInitialMarketData(mint);
    
    const initialPrice = marketData?.priceUsd || 0;
    const initialMcap = marketData?.marketCap || 0;
    const initialVol = marketData?.volume24h || 0;
    const initialHolders = marketData?.holders || 0;

    // 4. Insert into DB
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

        // Trigger background update immediately
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

            // Whitelist sort columns
            const safeSort = ['k_score', 'volume24h', 'liquidity', 'marketCap', 'created_at', 'change24h'].includes(sort) 
                ? sort 
                : 'k_score';
            
            // Handle casing for sort if needed (though Postgres is case insensitive for unquoted)
            // But if we used CamelCase in CREATE TABLE, we might need quotes or rely on it being lowercase in DB.
            // Assuming DB columns are effectively lowercase or case-insensitive.

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
                    timestamp: r.timestamp // Creation time or pool time
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
// GET /token/:mint (Detail View)
// -----------------------------------------------------------------------------
router.get('/token/:mint', async (req, res) => {
    const { mint } = req.params;
    const cacheKey = `token_detail_${mint}`;

    try {
        // 1. Try to index if missing (Lazy Indexing)
        // We do this outside cache so it happens once
        await indexTokenOnChain(mint);

        const result = await smartCache(cacheKey, 5, async () => {
            let token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
            
            if (!token) return { success: false, error: "Token not found" };

            // Fetch Pairs/Pools
            const pairs = await db.all('SELECT * FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC', [mint]);

            // Fetch Holder History
            const holderHistory = await db.all('SELECT * FROM holders_history WHERE mint = $1 ORDER BY timestamp ASC', [mint]);

            // Clone and Normalize Token Data
            let tokenData = { ...token };
            if (tokenData.symbol) tokenData.ticker = tokenData.symbol;

            // --- KEY FIX: Normalize Casing for Frontend ---
            // Postgres returns lowercase keys. Frontend expects camelCase.
            tokenData.marketCap = tokenData.marketcap || tokenData.marketCap || 0;
            tokenData.priceUsd = tokenData.priceusd || tokenData.priceUsd || 0;
            tokenData.volume24h = tokenData.volume24h || 0;
            tokenData.liquidity = tokenData.liquidity || 0;
            tokenData.holders = tokenData.holders || 0;
            tokenData.change24h = tokenData.change24h || 0;
            tokenData.change1h = tokenData.change1h || 0;
            tokenData.change5m = tokenData.change5m || 0;
            tokenData.timestamp = tokenData.timestamp || 0;
            
            // Patch price from main pool if available/better
            if (pairs.length > 0) {
                const mainPool = pairs[0];
                // If token price is 0 or missing, use pool price
                if (!tokenData.priceUsd || tokenData.priceUsd === 0) {
                    if (mainPool.price_usd > 0) tokenData.priceUsd = mainPool.price_usd;
                }
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
