const express = require('express');
const axios = require('axios');
const { PublicKey } = require('@solana/web3.js');
const { isValidPubkey } = require('../utils/solana');
const { smartCache, enableIndexing, aggregateAndSaveToken } = require('../services/database'); 
const { findPoolsOnChain } = require('../services/pool_finder');
const { fetchTokenMetadata } = require('../utils/metaplex');
const { getSolanaConnection } = require('../services/solana'); 
const config = require('../config/env');
const { updateSingleToken } = require('../tasks/kScoreUpdater'); 
const { getClient } = require('../services/redis'); 
const { enqueueTokenUpdate } = require('../services/queue'); 
const { snapshotPools } = require('../indexer/tasks/snapshotter'); 
const logger = require('../services/logger');
const cacheControl = require('../middleware/httpCache');

const router = express.Router();
const solanaConnection = getSolanaConnection();

// In-Memory Lock to prevent RPC spam on popular stale tokens
const pendingRefreshes = new Set();

// Middleware for Admin Routes
const requireAdmin = (req, res, next) => {
    const authHeader = req.headers['x-admin-auth'];
    if (!authHeader || authHeader !== config.ADMIN_PASSWORD) {
        return res.status(403).json({ success: false, error: 'Unauthorized' });
    }
    next();
};

function init(deps) {
    const { db } = deps;

    // --- HELPER FUNCTIONS ---
    async function fetchExternalCandles(poolAddress, resolution) {
        try {
            let timeframe = 'minute';
            let aggregate = 1;
            if (resolution === '5') aggregate = 5;
            else if (resolution === '15') aggregate = 15;
            else if (resolution === '60') { timeframe = 'hour'; aggregate = 1; }
            else if (resolution === '240') { timeframe = 'hour'; aggregate = 4; }
            else if (resolution === 'D') { timeframe = 'day'; aggregate = 1; }

            const url = `https://api.geckoterminal.com/api/v2/networks/solana/pools/${poolAddress}/ohlcv/${timeframe}?aggregate=${aggregate}&limit=100`;
            const response = await axios.get(url, { timeout: 5000 });
            const data = response.data.data.attributes.ohlcv_list;
            return data.map(c => ({ time: c[0], open: c[1], high: c[2], low: c[3], close: c[4], volume: c[5] })).reverse();
        } catch (e) { return []; }
    }

    async function fetchInitialMarketData(mint) {
        try {
            const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mint}`;
            const res = await axios.get(url, { timeout: 3000 });
            const attrs = res.data.data.attributes;
            return {
                priceUsd: parseFloat(attrs.price_usd || 0),
                volume24h: parseFloat(attrs.volume_usd?.h24 || 0),
                change24h: parseFloat(attrs.price_change_percentage?.h24 || 0),
                change1h: parseFloat(attrs.price_change_percentage?.h1 || 0),
                change5m: parseFloat(attrs.price_change_percentage?.m5 || 0),
                marketCap: parseFloat(attrs.fdv_usd || attrs.market_cap_usd || 0)
            };
        } catch (e) { return null; }
    }

    async function verifyPayment(signature, payerPubkey) {
        if (!signature) throw new Error("Payment signature required");
        const existing = await db.get('SELECT id FROM token_updates WHERE signature = $1', [signature]);
        if (existing) throw new Error("Transaction signature already used");
        return true; 
    }

    async function indexTokenOnChain(mint) {
        const meta = await fetchTokenMetadata(mint);
        let supply = '1000000000'; 
        let decimals = 9; 
        try {
            const supplyInfo = await solanaConnection.getTokenSupply(new PublicKey(mint));
            supply = supplyInfo.value.amount;
            decimals = supplyInfo.value.decimals;
        } catch (e) {}

        const marketData = await fetchInitialMarketData(mint);
        const baseData = { name: meta?.name || 'Unknown', ticker: meta?.symbol || 'UNKNOWN', image: meta?.image || null };
        const initialPrice = marketData?.priceUsd || 0;
        const initialVol = marketData?.volume24h || 0;
        const initialChange = marketData?.change24h || 0;
        const initialChange1h = marketData?.change1h || 0;
        const initialChange5m = marketData?.change5m || 0;
        const initialMcap = marketData?.marketCap || 0;

        // 1. CREATE TOKEN RECORD FIRST
        await db.run(`
            INSERT INTO tokens (mint, name, symbol, image, supply, decimals, priceUsd, liquidity, marketCap, volume24h, change24h, change1h, change5m, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT(mint) DO UPDATE SET
            name = EXCLUDED.name,
            symbol = EXCLUDED.symbol,
            image = EXCLUDED.image,
            decimals = EXCLUDED.decimals
        `, [
            mint, baseData.name, baseData.ticker, baseData.image, supply, decimals, 
            initialPrice, 0, initialMcap, initialVol, initialChange,
            initialChange1h, initialChange5m, Date.now()
        ]);

        // 2. THEN FIND POOLS
        const pools = await findPoolsOnChain(mint);
        const poolAddresses = [];

        for (const pool of pools) {
            poolAddresses.push(pool.pairAddress);
            await enableIndexing(db, mint, {
                pairAddress: pool.pairAddress,
                dexId: pool.dexId,
                liquidity: pool.liquidity || { usd: 0 },
                volume: pool.volume || { h24: 0 },
                priceUsd: pool.priceUsd || 0,
                baseToken: pool.baseToken,
                quoteToken: pool.quoteToken,
                reserve_a: pool.reserve_a, 
                reserve_b: pool.reserve_b
            });
        }

        await enqueueTokenUpdate(mint);
        if (poolAddresses.length > 0) {
            await snapshotPools(poolAddresses).catch(e => console.error("Snapshot Err:", e.message));
            await aggregateAndSaveToken(db, mint);
        }
        return { ...baseData, pairs: pools };
    }

    // --- PROXY ROUTES ---
    router.get('/proxy/blockhash', async (req, res) => {
        try {
            const { blockhash, lastValidBlockHeight } = await solanaConnection.getLatestBlockhash('confirmed');
            res.json({ success: true, blockhash, lastValidBlockHeight });
        } catch (e) { res.status(500).json({ success: false, error: "Network Busy" }); }
    });

    router.post('/proxy/send-tx', async (req, res) => {
        try {
            const { signedTx } = req.body; 
            if (!signedTx) return res.status(400).json({ success: false, error: "No transaction data" });
            const txBuffer = Buffer.from(signedTx, 'base64');
            const signature = await solanaConnection.sendRawTransaction(txBuffer, { skipPreflight: false, preflightCommitment: 'confirmed' });
            res.json({ success: true, signature });
        } catch (e) { res.status(500).json({ success: false, error: "Transaction Failed at RPC" }); }
    });

    router.get('/token/:mint/candles', cacheControl(30, 60), async (req, res) => {
        const { mint } = req.params;
        const { resolution = '5', from, to, poolAddress } = req.query; 
        try {
            const resMinutes = parseInt(resolution === 'D' ? 1440 : resolution);
            const resMs = resMinutes * 60 * 1000;
            const cacheKey = `chart:${mint}:${poolAddress || 'best'}:${resolution}:${Math.floor(Date.now() / 10000)}`; 

            const result = await smartCache(cacheKey, 10, async () => {
                let targetPoolAddress = poolAddress;
                if (!targetPoolAddress) {
                    const bestPool = await db.get(`SELECT address FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1`, [mint]);
                    if (!bestPool) return { success: false, error: "Token not indexed" };
                    targetPoolAddress = bestPool.address;
                }

                const fromMs = parseInt(from) * 1000 || (Date.now() - 24 * 60 * 60 * 1000);
                const toMs = parseInt(to) * 1000 || Date.now();

                const rows = await db.all(`
                    SELECT timestamp, open, high, low, close, volume FROM candles_1m 
                    WHERE pool_address = $1 AND timestamp >= $2 AND timestamp <= $3 
                    ORDER BY timestamp ASC
                `, [targetPoolAddress, fromMs, toMs]);
                
                if (!rows || rows.length < 5) {
                    const extCandles = await fetchExternalCandles(targetPoolAddress, resolution);
                    if (extCandles.length > 0) return { success: true, candles: extCandles, source: 'external' };
                }

                if (!rows || rows.length === 0) return { success: true, candles: [] };

                const candles = [];
                let currentCandle = null;
                for (const r of rows) {
                    const time = parseInt(String(r.timestamp));
                    const bucketStart = Math.floor(time / resMs) * resMs;
                    if (!currentCandle || currentCandle.timeMs !== bucketStart) {
                        if (currentCandle) {
                            currentCandle.time = Math.floor(currentCandle.timeMs / 1000); 
                            delete currentCandle.timeMs;
                            candles.push(currentCandle);
                        }
                        currentCandle = { timeMs: bucketStart, open: r.open, high: r.high, low: r.low, close: r.close, volume: 0 };
                    }
                    if (r.high > currentCandle.high) currentCandle.high = r.high;
                    if (r.low < currentCandle.low) currentCandle.low = r.low;
                    currentCandle.close = r.close;
                    if (r.volume) currentCandle.volume += r.volume;
                }
                if (currentCandle) {
                    currentCandle.time = Math.floor(currentCandle.timeMs / 1000);
                    delete currentCandle.timeMs;
                    candles.push(currentCandle);
                }
                return { success: true, candles, source: 'internal' };
            });
            res.json(result);
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

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
        const { mint, twitter, website, telegram, banner, description, signature, userPublicKey } = req.body;
        try {
            if (!mint || mint.length < 30) return res.status(400).json({ success: false, error: "Invalid Mint" });
            try { await verifyPayment(signature, userPublicKey); } catch (payErr) { return res.status(402).json({ success: false, error: payErr.message }); }
            await db.run(`
                INSERT INTO token_updates (mint, twitter, website, telegram, banner, description, submittedAt, status, signature, payer)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9)
            `, [mint, twitter, website, telegram, banner, description, Date.now(), signature, userPublicKey]);
            try { await indexTokenOnChain(mint); } catch (err) {}
            res.json({ success: true, message: "Update queued." });
        } catch (e) { res.status(500).json({ success: false, error: "Submission failed" }); }
    });

    // --- ADMIN ROUTES ---
    router.get('/admin/updates', requireAdmin, async (req, res) => { const { type } = req.query; try { let sql = `SELECT u.*, t.symbol as ticker, t.image FROM token_updates u LEFT JOIN tokens t ON u.mint = t.mint`; if (type === 'history') { sql += ` WHERE u.status != 'pending' ORDER BY u.submittedAt DESC LIMIT 50`; } else { sql += ` WHERE u.status = 'pending' ORDER BY u.submittedAt ASC`; } const updates = await db.all(sql); res.json({ success: true, updates }); } catch (e) { res.status(500).json({ success: false, error: e.message }); } });
    router.post('/admin/approve-update', requireAdmin, async (req, res) => { const { id } = req.body; try { const request = await db.get(`SELECT * FROM token_updates WHERE id = $1`, [id]); if (!request) return res.status(404).json({ success: false, error: "Request not found" }); const token = await db.get(`SELECT metadata FROM tokens WHERE mint = $1`, [request.mint]); let currentMeta = {}; if (token && token.metadata) { try { currentMeta = typeof token.metadata === 'string' ? JSON.parse(token.metadata) : token.metadata; } catch (e) {} } const newCommunity = { twitter: request.twitter, website: request.website, telegram: request.telegram, banner: request.banner, description: request.description }; currentMeta.community = { ...(currentMeta.community || {}), ...newCommunity }; const jsonStr = JSON.stringify(currentMeta); await db.run(`UPDATE tokens SET metadata = $1, hasCommunityUpdate = TRUE, updated_at = CURRENT_TIMESTAMP WHERE mint = $2`, [jsonStr, request.mint]); await db.run(`UPDATE token_updates SET status = 'approved' WHERE id = $1`, [id]); await updateSingleToken({ db }, request.mint); res.json({ success: true }); } catch (e) { res.status(500).json({ success: false, error: e.message }); } });
    router.post('/admin/reject-update', requireAdmin, async (req, res) => { const { id } = req.body; try { await db.run(`UPDATE token_updates SET status = 'rejected' WHERE id = $1`, [id]); res.json({ success: true }); } catch (e) { res.status(500).json({ success: false, error: e.message }); } });
    router.get('/admin/token/:mint', requireAdmin, async (req, res) => { const { mint } = req.params; try { const token = await db.get(`SELECT * FROM tokens WHERE mint = $1`, [mint]); if (!token) return res.status(404).json({ success: false, error: "Token not found" }); let meta = {}; try { if (typeof token.metadata === 'string') meta = JSON.parse(token.metadata); else meta = token.metadata || {}; } catch(e) {} const community = meta.community || {}; res.json({ success: true, token: { ...token, ticker: token.symbol, twitter: community.twitter || meta.twitter, website: community.website || meta.website, telegram: community.telegram || meta.telegram, banner: community.banner || meta.banner, description: community.description || meta.description } }); } catch (e) { res.status(500).json({ success: false, error: e.message }); } });
    router.post('/admin/update-token', requireAdmin, async (req, res) => { const { mint, twitter, website, telegram, banner, description } = req.body; try { const token = await db.get(`SELECT metadata FROM tokens WHERE mint = $1`, [mint]); let currentMeta = {}; if (token && token.metadata) { try { currentMeta = typeof token.metadata === 'string' ? JSON.parse(token.metadata) : token.metadata; } catch (e) {} } const newCommunity = { twitter, website, telegram, banner, description }; currentMeta.community = { ...(currentMeta.community || {}), ...newCommunity }; const jsonStr = JSON.stringify(currentMeta); await db.run(`UPDATE tokens SET metadata = $1, hasCommunityUpdate = TRUE WHERE mint = $2`, [jsonStr, mint]); await updateSingleToken({ db }, mint); res.json({ success: true }); } catch (e) { res.status(500).json({ success: false, error: e.message }); } });
    router.post('/admin/delete-token', requireAdmin, async (req, res) => { const { mint } = req.body; try { const pools = await db.all(`SELECT address FROM pools WHERE mint = $1`, [mint]); const poolAddresses = pools.map(p => p.address); if (poolAddresses.length > 0) { await db.run(`DELETE FROM candles_1m WHERE pool_address = ANY($1)`, [poolAddresses]); await db.run(`DELETE FROM active_trackers WHERE pool_address = ANY($1)`, [poolAddresses]); } await db.run(`DELETE FROM pools WHERE mint = $1`, [mint]); await db.run(`DELETE FROM k_scores WHERE mint = $1`, [mint]); await db.run(`DELETE FROM token_updates WHERE mint = $1`, [mint]); await db.run(`DELETE FROM holders_history WHERE mint = $1`, [mint]); await db.run(`DELETE FROM tokens WHERE mint = $1`, [mint]); const redis = getClient(); if (redis) { await redis.del(`token:detail:${mint}`); } res.json({ success: true, message: "Token and all history permanently deleted." }); } catch (e) { res.status(500).json({ success: false, error: e.message }); } });
    router.post('/admin/refresh-kscore', requireAdmin, async (req, res) => { const { mint } = req.body; try { const newScore = await updateSingleToken({ db }, mint); res.json({ success: true, message: `K-Score Updated: ${newScore}` }); } catch (e) { res.status(500).json({ success: false, error: e.message }); } });

    // --- NEW: BACKUP & RESTORE ROUTES ---
    
    // BACKUP: Dump all token_updates to a JSON file
    router.get('/admin/backup/updates', requireAdmin, async (req, res) => {
        try {
            const updates = await db.all('SELECT * FROM token_updates ORDER BY submittedAt DESC');
            res.setHeader('Content-Type', 'application/json');
            res.setHeader('Content-Disposition', `attachment; filename=holdex_updates_backup_${Date.now()}.json`);
            res.json({ success: true, count: updates.length, timestamp: Date.now(), data: updates });
        } catch (e) {
            res.status(500).json({ success: false, error: e.message });
        }
    });

    // RESTORE: Import token_updates from JSON (Merge Logic)
    router.post('/admin/restore/updates', requireAdmin, async (req, res) => {
        const { updates } = req.body;
        if (!Array.isArray(updates)) {
            return res.status(400).json({ success: false, error: "Invalid backup file format. Expected an array of updates." });
        }

        let restoredCount = 0;
        let skippedCount = 0;

        try {
            for (const u of updates) {
                // PG column names are often lowercase, handle both camelCase (JS) and lowercase (DB dump)
                const signature = u.signature;
                const mint = u.mint;
                
                // Use fallback for case-sensitivity issues
                const twitter = u.twitter;
                const website = u.website;
                const telegram = u.telegram;
                const banner = u.banner;
                const description = u.description;
                const submittedAt = u.submittedAt || u.submittedat || Date.now();
                const status = u.status || 'pending';
                const payer = u.payer;

                if (!signature || !mint) {
                    skippedCount++; 
                    continue; // Invalid record
                }

                // Check duplicate by signature
                const existing = await db.get('SELECT id FROM token_updates WHERE signature = $1', [signature]);
                
                if (existing) {
                    skippedCount++;
                    continue;
                }

                await db.run(`
                    INSERT INTO token_updates (mint, twitter, website, telegram, banner, description, submittedAt, status, signature, payer)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                `, [mint, twitter, website, telegram, banner, description, submittedAt, status, signature, payer]);
                
                restoredCount++;
            }

            res.json({ success: true, restored: restoredCount, skipped: skippedCount, message: `Restore Complete. Imported: ${restoredCount}, Skipped (Duplicates): ${skippedCount}` });
        } catch (e) {
            res.status(500).json({ success: false, error: e.message });
        }
    });

    // --- STANDARD PUBLIC ROUTES ---
    router.get('/token/:mint', cacheControl(3, 5), async (req, res) => {
        const { mint } = req.params;
        const cacheKey = `token:detail:${mint}`;
        try {
            const result = await smartCache(cacheKey, 5, async () => {
                let token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
                let pairs = await db.all('SELECT * FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC', [mint]);
                
                if (!token) {
                    try {
                        const indexed = await indexTokenOnChain(mint);
                        token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]); 
                        pairs = indexed.pairs || [];
                    } catch (e) {}
                }
                
                if (!token) return { success: false, error: "Token not found" };

                const now = Date.now();
                const isStale = !token.timestamp || (now - token.timestamp > 300000); 

                if (isStale && pairs.length > 0 && !pendingRefreshes.has(mint)) {
                    pendingRefreshes.add(mint);
                    try {
                        const poolAddresses = pairs.map(p => p.address);
                        await snapshotPools(poolAddresses);
                        await aggregateAndSaveToken(db, mint);

                        token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
                        pairs = await db.all('SELECT * FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC', [mint]);
                    } catch (err) {
                        logger.warn(`Lazy refresh failed for ${mint}: ${err.message}`);
                    } finally {
                        pendingRefreshes.delete(mint);
                    }
                }

                let tokenData = { ...token };
                tokenData.marketCap = tokenData.marketCap || tokenData.marketcap || 0;
                tokenData.priceUsd = tokenData.priceUsd || tokenData.priceusd || 0;
                tokenData.volume24h = tokenData.volume24h || 0;
                tokenData.holders = tokenData.holders || 0;
                tokenData.kScore = tokenData.k_score || tokenData.kScore || 0;
                
                if (tokenData.symbol) tokenData.ticker = tokenData.symbol;
                
                if (tokenData.metadata) {
                    try {
                        const meta = typeof tokenData.metadata === 'string' ? JSON.parse(tokenData.metadata) : tokenData.metadata;
                        const comm = meta.community || {};
                        tokenData.banner = comm.banner || meta.banner;
                        tokenData.description = comm.description || meta.description;
                        tokenData.twitter = comm.twitter || meta.twitter;
                        tokenData.telegram = comm.telegram || meta.telegram;
                        tokenData.website = comm.website || meta.website;
                    } catch (e) {}
                }

                if (pairs.length > 0) {
                    const mainPool = pairs[0];
                    if (mainPool.price_usd > 0) tokenData.priceUsd = mainPool.price_usd;
                }

                const holderHistory = await db.all(`
                    SELECT count, timestamp FROM holders_history 
                    WHERE mint = $1 
                    ORDER BY timestamp ASC 
                    LIMIT 100
                `, [mint]);

                return { success: true, token: { ...tokenData, pairs, holderHistory } };
            });
            res.json(result);
        } catch(e) { res.status(500).json({ success: false, error: e.message }); }
    });

    router.get('/tokens', cacheControl(2, 5), async (req, res) => {
        const { search = '', sort = 'kscore', page = 1 } = req.query;
        try {
            const isGenericView = !search;
            const cacheKey = `api:tokens:list:${sort}:${page}:${search}`;
            const redis = getClient(); 
            if (isGenericView && redis) {
                try { const cached = await redis.get(cacheKey); if (cached) { res.setHeader('X-Cache', 'HIT'); return res.json(JSON.parse(cached)); } } catch(e) {}
            }

            const isAddressSearch = isValidPubkey(search);
            let rows = [];

            if (search.length > 0) {
                if (isAddressSearch) {
                    rows = await db.all(`SELECT * FROM tokens WHERE mint = $1`, [search]);
                    if (rows.length === 0) { await indexTokenOnChain(search); rows = await db.all(`SELECT * FROM tokens WHERE mint = $1`, [search]); }
                } else {
                    rows = await db.all(`SELECT * FROM tokens WHERE (symbol ILIKE $1 OR name ILIKE $1) LIMIT 50`, [`%${search}%`]);
                }
            } else {
                let orderBy = 'k_score DESC';
                if (sort === 'newest') orderBy = 'timestamp DESC';
                else if (sort === 'age') orderBy = 'timestamp ASC'; 
                else if (sort === 'mcap') orderBy = 'marketCap DESC';
                else if (sort === 'volume') orderBy = 'volume24h DESC';
                else if (sort === '24h') orderBy = 'change24h DESC';
                else if (sort === 'liquidity') orderBy = 'liquidity DESC';
                else if (sort === '5m') orderBy = 'change5m DESC';
                else if (sort === '1h') orderBy = 'change1h DESC';
                else if (sort === 'holders') orderBy = 'holders DESC'; 

                const offset = (page - 1) * 100;
                rows = await db.all(`SELECT * FROM tokens ORDER BY ${orderBy} LIMIT 100 OFFSET ${offset}`);
            }

            const responsePayload = {
                success: true,
                lastUpdate: Date.now(),
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
                    liquidity: r.liquidity || 0,
                    holders: r.holders || 0, 
                    hasCommunityUpdate: r.hasCommunityUpdate || r.hascommunityupdate || false,
                    timestamp: parseInt(r.timestamp),
                    kScore: r.k_score || 0
                }))
            };

            if (isGenericView && redis) { try { await redis.set(cacheKey, JSON.stringify(responsePayload), 'EX', 3); } catch(e){} }
            res.setHeader('X-Cache', 'MISS');
            return res.json(responsePayload);

        } catch (e) { res.status(500).json({ success: false, tokens: [], error: e.message }); }
    });
    
    return router;
}

module.exports = { init };
