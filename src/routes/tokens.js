const express = require('express');
const axios = require('axios'); 
const { Connection, PublicKey } = require('@solana/web3.js');
const { isValidPubkey } = require('../utils/solana');
const { smartCache, enableIndexing, aggregateAndSaveToken } = require('../services/database');
const { findPoolsOnChain } = require('../services/pool_finder');
const { fetchTokenMetadata } = require('../utils/metaplex');
const config = require('../config/env');
const kScoreUpdater = require('../tasks/kScoreUpdater'); 

const router = express.Router();
const solanaConnection = new Connection(config.SOLANA_RPC_URL, 'confirmed');

const requireAdmin = (req, res, next) => {
    const authHeader = req.headers['x-admin-auth'];
    if (!authHeader || authHeader !== config.ADMIN_PASSWORD) {
        return res.status(403).json({ success: false, error: 'Unauthorized' });
    }
    next();
};

function init(deps) {
    const { db } = deps;

    async function verifyPayment(signature, payerPubkey) {
        if (!signature) throw new Error("Payment signature required");
        const existing = await db.get('SELECT id FROM token_updates WHERE signature = $1', [signature]);
        if (existing) throw new Error("Transaction signature already used");
        return true; 
    }

    async function indexTokenOnChain(mint) {
        // 1. Fetch Metadata (Robust Helius/Metaplex)
        const meta = await fetchTokenMetadata(mint);
        
        // 2. Fetch Supply (Critical for Market Cap)
        let supply = '0';
        try {
            const supplyInfo = await solanaConnection.getTokenSupply(new PublicKey(mint));
            supply = supplyInfo.value.amount; // Raw amount (integer string)
        } catch (e) { console.warn(`Failed to fetch supply for ${mint}`); }

        // 3. Find Pools
        const pools = await findPoolsOnChain(mint);
        
        // 4. Save Pools
        for (const pool of pools) {
            await enableIndexing(db, mint, {
                pairAddress: pool.pairAddress,
                dexId: pool.dexId,
                liquidity: pool.liquidity || { usd: 0 },
                volume: { h24: 0 },
                priceUsd: 0,
                baseToken: { address: mint },
                quoteToken: { address: 'So11111111111111111111111111111111111111112' }
            });
        }

        // 5. Save Token Data
        const baseData = {
            name: meta?.name || 'Unknown',
            ticker: meta?.symbol || 'UNKNOWN',
            image: meta?.image || null,
            marketCap: 0, 
            volume24h: 0,
            priceUsd: 0,
            change1h: 0,
            change24h: 0,
            change5m: 0,
            description: meta?.description || ''
        };

        // We explicitly pass the fetched supply here so it can be stored
        await db.run(`
            INSERT INTO tokens (mint, name, symbol, image, supply, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT(mint) DO UPDATE SET
            name = EXCLUDED.name,
            symbol = EXCLUDED.symbol,
            image = EXCLUDED.image,
            supply = EXCLUDED.supply
        `, [mint, baseData.name, baseData.ticker, baseData.image, supply, Date.now()]);

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
            
            try { await verifyPayment(signature, userPublicKey); } catch (payErr) { return res.status(402).json({ success: false, error: payErr.message }); }
            
            await db.run(`INSERT INTO token_updates (mint, twitter, website, telegram, banner, description, submittedAt, status, signature, payer) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9)`, [mint, twitter, website, telegram, banner, description, Date.now(), signature, userPublicKey]);
            
            try { await indexTokenOnChain(mint); } catch (err) { console.error("Auto-Index failed:", err.message); }

            res.json({ success: true, message: "Update queued. Indexing started." });
        } catch (e) { res.status(500).json({ success: false, error: "Submission failed: " + e.message }); }
    });

    router.get('/token/:mint/candles', async (req, res) => {
        const { mint } = req.params;
        const { resolution = '60', from, to } = req.query; 
        
        const fromMs = parseInt(from) * 1000 || (Date.now() - 24 * 60 * 60 * 1000);
        const toMs = parseInt(to) * 1000 || Date.now();

        const cacheKey = `chart:${mint}:${resolution}:${Math.floor(toMs / 60000)}`; 

        try {
            const result = await smartCache(cacheKey, 60, async () => {
                let pool = await db.get(`SELECT address FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1`, [mint]);
                if (!pool) return { success: false, error: "Token not indexed yet" };

                const rows = await db.all(`
                    SELECT timestamp, open, high, low, close, volume 
                    FROM candles_1m 
                    WHERE pool_address = $1 
                    AND timestamp >= $2 
                    AND timestamp <= $3 
                    ORDER BY timestamp ASC
                `, [pool.address, fromMs, toMs]);
                
                const candles = rows.map(r => ({
                    time: Math.floor(parseInt(r.timestamp) / 1000),
                    open: r.open, high: r.high, low: r.low, close: r.close
                }));
                
                return { success: true, candles };
            });
            res.json(result);
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    router.get('/tokens', async (req, res) => {
        const { search = '' } = req.query;

        try {
            const isAddressSearch = isValidPubkey(search);
            let rows = [];

            if (search.length > 0) {
                if (isAddressSearch) {
                    rows = await db.all(`SELECT * FROM tokens WHERE mint = $1`, [search]);
                } else {
                    rows = await db.all(`SELECT * FROM tokens WHERE (ticker ILIKE $1 OR name ILIKE $1) LIMIT 50`, [`%${search}%`]);
                }
            } else {
                // Return latest tokens
                rows = await db.all(`SELECT * FROM tokens ORDER BY timestamp DESC LIMIT 100`);
            }

            if (isAddressSearch && rows.length === 0) {
                const newData = await indexTokenOnChain(search);
                if (newData.name !== 'Unknown') {
                    rows.push({ ...newData, mint: search, hasCommunityUpdate: false, kScore: 0, timestamp: Date.now() });
                }
            }

            return res.json({
                success: true,
                tokens: rows.map(r => ({
                    mint: r.mint, 
                    name: r.name, 
                    ticker: r.symbol, 
                    image: r.image,
                    marketCap: r.marketcap || r.marketCap || 0,
                    volume24h: r.volume24h || 0,
                    priceUsd: r.priceusd || r.priceUsd || 0,
                    change24h: r.change24h || 0,
                    change1h: r.change1h || 0,
                    change5m: r.change5m || 0,
                    hasCommunityUpdate: r.hasCommunityUpdate || r.hascommunityupdate || false,
                    timestamp: parseInt(r.timestamp),
                    kScore: r.k_score || 0
                }))
            });

        } catch (e) { res.status(500).json({ success: false, tokens: [], error: e.message }); }
    });

    router.get('/token/:mint', async (req, res) => {
        const { mint } = req.params;
        let token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
        let pairs = await db.all('SELECT * FROM pools WHERE mint = $1', [mint]);
        
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
        if (tokenData.symbol) tokenData.ticker = tokenData.symbol;

        res.json({ success: true, token: { ...tokenData, pairs } });
    });

    // --- ADMIN ROUTES ---
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
        if (typeof currentMeta === 'string') currentMeta = JSON.parse(currentMeta);
        const newMeta = { ...currentMeta, ...update }; 
        await db.run(`UPDATE tokens SET metadata = $1, hasCommunityUpdate = TRUE WHERE mint = $2`, [JSON.stringify(newMeta), update.mint]);
        await db.run("UPDATE token_updates SET status = 'approved' WHERE id = $1", [id]);
        res.json({success: true});
    });

    router.post('/admin/reject-update', requireAdmin, async (req, res) => {
        await db.run("UPDATE token_updates SET status = 'rejected' WHERE id = $1", [req.body.id]); 
        res.json({ success: true }); 
    });

    router.get('/admin/token/:mint', requireAdmin, async (req, res) => {
        const { mint } = req.params;
        const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
        if (!token) return res.json({ success: false, error: 'Token not found' });
        let meta = {};
        if (token.metadata) meta = typeof token.metadata === 'string' ? JSON.parse(token.metadata) : token.metadata;
        res.json({ success: true, token: { mint: token.mint, ticker: token.symbol, twitter: meta.twitter, website: meta.website, telegram: meta.telegram, banner: meta.banner, description: meta.description } });
    });

    router.post('/admin/update-token', requireAdmin, async (req, res) => {
        const { mint, twitter, website, telegram, banner, description } = req.body;
        const token = await db.get('SELECT metadata FROM tokens WHERE mint = $1', [mint]);
        if (!token) return res.status(404).json({success:false, error:'Token not found'});
        let currentMeta = token.metadata || {};
        if (typeof currentMeta === 'string') currentMeta = JSON.parse(currentMeta);
        const newMeta = { ...currentMeta, twitter, website, telegram, banner, description };
        await db.run(`UPDATE tokens SET metadata = $1, hasCommunityUpdate = TRUE WHERE mint = $2`, [JSON.stringify(newMeta), mint]);
        res.json({ success: true });
    });

    router.post('/admin/delete-token', requireAdmin, async (req, res) => {
        const { mint } = req.body;
        await db.run('DELETE FROM k_scores WHERE mint = $1', [mint]);
        await db.run('DELETE FROM token_updates WHERE mint = $1', [mint]);
        await db.run('DELETE FROM pools WHERE mint = $1', [mint]); 
        await db.run('DELETE FROM tokens WHERE mint = $1', [mint]);
        res.json({ success: true });
    });

    router.post('/admin/refresh-kscore', requireAdmin, async (req, res) => {
        const { mint } = req.body;
        const score = await kScoreUpdater.updateSingleToken({ db }, mint);
        res.json({ success: true, message: `K-Score recalculated: ${score}` });
    });

    return router;
}

module.exports = { init };
