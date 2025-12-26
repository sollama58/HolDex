const express = require('express');
const axios = require('axios'); // Only for internal use / metaplex json
const { Connection, PublicKey } = require('@solana/web3.js');
const { isValidPubkey } = require('../utils/solana');
const { smartCache, enableIndexing, aggregateAndSaveToken, saveTokenData } = require('../services/database');
const { findPoolsOnChain } = require('../services/pool_finder');
const { fetchTokenMetadata } = require('../utils/metaplex');
const { getClient } = require('../services/redis'); 
const config = require('../config/env');
const kScoreUpdater = require('../tasks/kScoreUpdater'); 

const router = express.Router();
const solanaConnection = new Connection(config.SOLANA_RPC_URL, 'confirmed');

// --- MIDDLEWARE ---
const requireAdmin = (req, res, next) => {
    const authHeader = req.headers['x-admin-auth'];
    if (!authHeader || authHeader !== config.ADMIN_PASSWORD) {
        return res.status(403).json({ success: false, error: 'Unauthorized' });
    }
    next();
};

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

function init(deps) {
    const { db } = deps;

    // --- PAYMENT VERIFICATION ---
    async function verifyPayment(signature, payerPubkey) {
        if (!signature) throw new Error("Payment signature required");
        const existing = await db.get('SELECT id FROM token_updates WHERE signature = $1', [signature]);
        if (existing) throw new Error("Transaction signature already used");
        return true; 
    }

    // --- HELPER: On-Chain Indexing (Replaces DexScreener) ---
    async function indexTokenOnChain(mint) {
        // 1. Fetch Metadata (Metaplex)
        const meta = await fetchTokenMetadata(mint);
        
        // 2. Find Pools (On-Chain)
        const pools = await findPoolsOnChain(mint);
        
        // 3. Save Pools
        for (const pool of pools) {
            // Map our internal pool object to DB schema
            await enableIndexing(db, mint, {
                pairAddress: pool.pairAddress,
                dexId: pool.dexId,
                liquidity: { usd: 0 }, // Will be filled by snapshotter
                volume: { h24: 0 },
                priceUsd: 0,
                baseToken: { address: mint },
                quoteToken: { address: 'So11111111111111111111111111111111111111112' }
            });
        }

        // 4. Save Token Data
        const baseData = {
            name: meta?.name || 'Unknown',
            ticker: meta?.symbol || 'UNKNOWN',
            image: meta?.image || null,
            marketCap: 0, // Will be calc by snapshotter
            volume24h: 0,
            priceUsd: 0,
            change1h: 0,
            change24h: 0,
            change5m: 0
        };

        await aggregateAndSaveToken(db, mint, baseData);
        return { ...baseData, pairs: pools };
    }

    // --- ENDPOINTS ---

    router.get('/config/fees', (req, res) => {
        res.json({ success: true, solFee: config.FEE_SOL, tokenFee: config.FEE_TOKEN_AMOUNT, tokenMint: config.FEE_TOKEN_MINT, treasury: config.TREASURY_WALLET });
    });
    
    router.get('/proxy/balance/:wallet', async (req, res) => {
        try {
            const { wallet } = req.params;
            const tokenMint = req.query.tokenMint || config.FEE_TOKEN_MINT;
            if (!isValidPubkey(wallet)) return res.status(400).json({ success: false, error: "Invalid wallet" });
            const pubKey = new PublicKey(wallet);
            const [solBalance, tokenAccounts] = await Promise.all([
                solanaConnection.getBalance(pubKey),
                solanaConnection.getParsedTokenAccountsByOwner(pubKey, { mint: new PublicKey(tokenMint) })
            ]);
            res.json({ success: true, sol: solBalance / 1e9, tokens: tokenAccounts.value.length > 0 ? tokenAccounts.value[0].account.data.parsed.info.tokenAmount.uiAmount : 0 });
        } catch (e) { res.status(500).json({ success: false, error: "Failed to fetch balance" }); }
    });
    
    router.post('/request-update', async (req, res) => {
        try {
            const { mint, twitter, website, telegram, banner, description, signature, userPublicKey } = req.body;
            if (!mint || mint.length < 30) return res.status(400).json({ success: false, error: "Invalid Mint" });
            let safeDesc = description ? description.substring(0, 250).replace(/<[^>]*>?/gm, '') : null; 
            
            try { await verifyPayment(signature, userPublicKey); } catch (payErr) { return res.status(402).json({ success: false, error: payErr.message }); }
            
            await db.run(`INSERT INTO token_updates (mint, twitter, website, telegram, banner, description, submittedAt, status, signature, payer) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9)`, [mint, twitter, website, telegram, banner, safeDesc, Date.now(), signature, userPublicKey]);
            
            // Auto-Index On-Chain
            try { await indexTokenOnChain(mint); } catch (err) { console.error("Auto-Index failed:", err.message); }

            res.json({ success: true, message: "Update queued. Indexing started." });
        } catch (e) { res.status(500).json({ success: false, error: "Submission failed: " + e.message }); }
    });

    router.get('/token/:mint/candles', async (req, res) => {
        const { mint } = req.params;
        const { resolution = '60', from, to } = req.query; 
        
        // FIX: Handle Unit Mismatch
        // Frontend sends Seconds (UNIX), DB stores Milliseconds
        const fromMs = parseInt(from) * 1000 || (Date.now() - 24 * 60 * 60 * 1000);
        const toMs = parseInt(to) * 1000 || Date.now();

        // Round to nearest minute for cache key
        const cacheKey = `chart:${mint}:${resolution}:${Math.floor(toMs / 60000)}`; 

        try {
            const result = await smartCache(cacheKey, 60, async () => {
                // Find best pool for this mint (by liquidity)
                let pool = await db.get(`SELECT address FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1`, [mint]);
                if (!pool) return { success: false, error: "Token not indexed yet" };

                // Fetch Time-Filtered Candles
                const rows = await db.all(`
                    SELECT timestamp, open, high, low, close, volume 
                    FROM candles_1m 
                    WHERE pool_address = $1 
                    AND timestamp >= $2 
                    AND timestamp <= $3 
                    ORDER BY timestamp ASC
                `, [pool.address, fromMs, toMs]);
                
                // Map back to Seconds for Lightweight Charts
                const candles = rows.map(r => ({
                    time: Math.floor(parseInt(r.timestamp) / 1000),
                    open: r.open, 
                    high: r.high, 
                    low: r.low, 
                    close: r.close
                }));
                
                return { success: true, candles };
            });
            res.json(result);
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    router.get('/tokens', async (req, res) => {
        const { sort = 'newest', limit = 100, page = 1, search = '', filter = '' } = req.query;
        // ... (Limit/Offset logic same) ...
        const limitVal = 100; 
        const offsetVal = (page - 1) * 100;

        try {
            const isAddressSearch = isValidPubkey(search);
            let rows = [];

            // 1. Local Search
            if (search.length > 0) {
                if (isAddressSearch) {
                    rows = await db.all(`SELECT * FROM tokens WHERE mint = $1`, [search]);
                } else {
                    rows = await db.all(`SELECT * FROM tokens WHERE (ticker ILIKE $1 OR name ILIKE $1) LIMIT 50`, [`%${search}%`]);
                }
            } else {
                rows = await db.all(`SELECT * FROM tokens ORDER BY timestamp DESC LIMIT 100`);
            }

            // 2. On-Chain Discovery (If address search & not found)
            if (isAddressSearch && rows.length === 0) {
                console.log(`🔎 Searching On-Chain for ${search}...`);
                const newData = await indexTokenOnChain(search);
                if (newData.name !== 'Unknown') {
                    rows.push({
                        ...newData,
                        mint: search,
                        hasCommunityUpdate: false,
                        kScore: 0,
                        timestamp: Date.now()
                    });
                }
            }

            // Return Result
            return res.json({
                success: true,
                page: 1, 
                limit: 100,
                tokens: rows.map(r => ({
                    mint: r.mint, 
                    name: r.name, 
                    ticker: r.symbol, // Important mapping
                    image: r.image,
                    marketCap: r.marketcap || r.marketCap || 0,
                    volume24h: r.volume24h || 0,
                    priceUsd: r.priceusd || r.priceUsd || 0,
                    timestamp: parseInt(r.timestamp),
                    kScore: r.k_score || 0
                }))
            });

        } catch (e) { res.status(500).json({ success: false, tokens: [], error: e.message }); }
    });

    router.get('/token/:mint', async (req, res) => {
        const { mint } = req.params;
        const result = await smartCache(`api:token:${mint}`, 30, async () => {
            let token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
            let pairs = await db.all('SELECT * FROM pools WHERE mint = $1', [mint]);
            
            // Auto-Index if missing
            if (!token) {
                try {
                    const indexed = await indexTokenOnChain(mint);
                    if (indexed.name !== 'Unknown') {
                        token = { ...indexed, mint };
                        pairs = indexed.pairs || [];
                    }
                } catch (e) {}
            }
            
            let tokenData = token || { mint, name: 'Unknown', ticker: 'Unknown' };
            // Map symbol to ticker for frontend
            if (tokenData.symbol) tokenData.ticker = tokenData.symbol;

            return { success: true, token: { ...tokenData, pairs } };
        });
        res.json(result);
    });

    // ... Admin Routes (Unchanged) ...
    router.get('/admin/updates', requireAdmin, async (req, res) => {
        const { type } = req.query;
        let sql = `SELECT u.*, t.name, t.symbol as ticker, t.image FROM token_updates u LEFT JOIN tokens t ON u.mint = t.mint`;
        sql += type === 'history' ? ` WHERE u.status != 'pending' ORDER BY u.submittedAt DESC LIMIT 100` : ` WHERE u.status = 'pending' ORDER BY u.submittedAt ASC`;
        res.json({ success: true, updates: await db.all(sql) });
    });

    router.post('/admin/approve-update', requireAdmin, async (req, res) => {
        const { id } = req.body;
        const update = await db.get('SELECT * FROM token_updates WHERE id = $1', [id]);
        if (!update) return res.status(404).json({error:'Not found'});
        
        const token = await db.get('SELECT metadata FROM tokens WHERE mint = $1', [update.mint]);
        let currentMeta = token?.metadata || {};
        const newMeta = { ...currentMeta, ...update }; // Simplified merge

        await db.run(
            `UPDATE tokens SET metadata = $1, hasCommunityUpdate = TRUE WHERE mint = $2`, 
            [JSON.stringify(newMeta), update.mint]
        );
        await db.run("UPDATE token_updates SET status = 'approved' WHERE id = $1", [id]);
        res.json({success: true});
    });

    router.post('/admin/reject-update', requireAdmin, async (req, res) => {
        await db.run("UPDATE token_updates SET status = 'rejected' WHERE id = $1", [req.body.id]); 
        res.json({ success: true }); 
    });

    return router;
}

module.exports = { init };
