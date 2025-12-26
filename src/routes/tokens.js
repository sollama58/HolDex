const express = require('express');
const { PublicKey } = require('@solana/web3.js');
const { isValidPubkey } = require('../utils/solana');
const { smartCache, enableIndexing, aggregateAndSaveToken } = require('../services/database'); 
const { findPoolsOnChain } = require('../services/pool_finder');
const { fetchTokenMetadata } = require('../utils/metaplex');
const { getSolanaConnection } = require('../services/solana'); 
const config = require('../config/env');
const kScoreUpdater = require('../tasks/kScoreUpdater'); 
const { getClient } = require('../services/redis'); 
const { enqueueTokenUpdate } = require('../services/queue'); 
const { snapshotPools } = require('../indexer/tasks/snapshotter'); 
const logger = require('../services/logger');

const router = express.Router();
const solanaConnection = getSolanaConnection();

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
        // Logic Gap: This only checks if used. For production, you should verify tx on-chain.
        // For now, checking duplication prevents simple replay attacks on this DB.
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

        const baseData = {
            name: meta?.name || 'Unknown',
            ticker: meta?.symbol || 'UNKNOWN',
            image: meta?.image || null,
        };

        await db.run(`
            INSERT INTO tokens (mint, name, symbol, image, supply, decimals, priceUsd, liquidity, marketCap, volume24h, change24h, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT(mint) DO UPDATE SET
            name = EXCLUDED.name,
            symbol = EXCLUDED.symbol,
            image = EXCLUDED.image,
            decimals = EXCLUDED.decimals,
            timestamp = $12
        `, [
            mint, 
            baseData.name, 
            baseData.ticker, 
            baseData.image, 
            supply, 
            decimals, 
            0, 
            0, 
            0, 
            0, 
            0, 
            Date.now()
        ]);

        await enqueueTokenUpdate(mint);

        if (poolAddresses.length > 0) {
            await snapshotPools(poolAddresses).catch(e => console.error("Immediate snapshot error:", e.message));
            await aggregateAndSaveToken(db, mint);
        }

        return { ...baseData, pairs: pools };
    }

    // --- ROUTES ---

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
        const { mint, signature } = req.body;
        logger.info(`📝 Received Update Request for ${mint} (Sig: ${signature})`);
        
        try {
            const { mint, twitter, website, telegram, banner, description, signature, userPublicKey } = req.body;
            
            if (!mint || mint.length < 30) return res.status(400).json({ success: false, error: "Invalid Mint" });
            
            try { 
                await verifyPayment(signature, userPublicKey); 
            } catch (payErr) { 
                logger.warn(`Payment Verification Failed: ${payErr.message}`);
                return res.status(402).json({ success: false, error: payErr.message }); 
            }

            await db.run(`INSERT INTO token_updates (mint, twitter, website, telegram, banner, description, submittedAt, status, signature, payer) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9)`, [mint, twitter, website, telegram, banner, description, Date.now(), signature, userPublicKey]);
            
            // Auto Index if new
            try { await indexTokenOnChain(mint); } catch (err) { console.error("Auto-Index failed:", err.message); }
            
            logger.info(`✅ Update Queued for ${mint}`);
            res.json({ success: true, message: "Update queued. Indexing started." });
        } catch (e) { 
            logger.error(`❌ Submission failed: ${e.message}`);
            res.status(500).json({ success: false, error: "Submission failed: " + e.message }); 
        }
    });

    // UPDATED CANDLE ENDPOINT - SUPPORTS SPECIFIC POOL
    router.get('/token/:mint/candles', async (req, res) => {
        const { mint } = req.params;
        const { resolution = '5', from, to, poolAddress } = req.query; 
        const resMinutes = parseInt(resolution);
        const resMs = resMinutes * 60 * 1000;
        const fromMs = parseInt(from) * 1000 || (Date.now() - 24 * 60 * 60 * 1000);
        const toMs = parseInt(to) * 1000 || Date.now();
        
        // Include poolAddress in cache key if present
        const cacheKey = `chart:${mint}:${poolAddress || 'best'}:${resolution}:${Math.floor(Date.now() / 30000)}`; 

        try {
            const result = await smartCache(cacheKey, 30, async () => {
                let targetPoolAddress = poolAddress;

                // If no specific pool requested, find the best one
                if (!targetPoolAddress) {
                    const bestPool = await db.get(`SELECT address FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1`, [mint]);
                    if (!bestPool) return { success: false, error: "Token not indexed yet" };
                    targetPoolAddress = bestPool.address;
                }

                const rows = await db.all(`
                    SELECT timestamp, open, high, low, close, volume 
                    FROM candles_1m 
                    WHERE pool_address = $1 
                    AND timestamp >= $2 
                    AND timestamp <= $3 
                    ORDER BY timestamp ASC
                `, [targetPoolAddress, fromMs, toMs]);
                
                if (resMinutes === 1) {
                    return { success: true, candles: rows.map(r => ({
                        time: Math.floor(parseInt(r.timestamp) / 1000),
                        open: r.open, high: r.high, low: r.low, close: r.close
                    }))};
                }

                // Aggregate candles for higher resolutions
                const candles = [];
                let currentCandle = null;
                for (const r of rows) {
                    const time = parseInt(r.timestamp);
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
                return { success: true, candles };
            });
            res.json(result);
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    router.get('/token/:mint', async (req, res) => {
        const { mint } = req.params;
        const cacheKey = `token:detail:${mint}`;
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
            
            let tokenData = token || { mint, name: 'Unknown', ticker: 'Unknown' };
            if (tokenData.symbol) tokenData.ticker = tokenData.symbol;
            return { success: true, token: { ...tokenData, pairs } };
        });
        res.json(result);
    });

    router.get('/tokens', async (req, res) => {
        const { search = '', sort = 'kscore', page = 1 } = req.query;
        try {
            const isGenericView = !search;
            const cacheKey = `api:tokens:list:${sort}:${page}:${isGenericView ? 'generic' : search}`;
            const redis = getClient();
            
            if (isGenericView && redis) {
                const cached = await redis.get(cacheKey);
                if (cached) {
                    res.setHeader('X-Cache', 'HIT');
                    return res.json(JSON.parse(cached));
                }
            }

            const isAddressSearch = isValidPubkey(search);
            let rows = [];

            if (search.length > 0) {
                if (isAddressSearch) {
                    rows = await db.all(`SELECT * FROM tokens WHERE mint = $1`, [search]);
                    if (rows.length === 0) {
                         await indexTokenOnChain(search);
                         rows = await db.all(`SELECT * FROM tokens WHERE mint = $1`, [search]);
                    }
                } else {
                    rows = await db.all(`SELECT * FROM tokens WHERE (symbol ILIKE $1 OR name ILIKE $1) LIMIT 50`, [`%${search}%`]);
                }
            } else {
                let orderBy = 'k_score DESC';
                if (sort === 'newest') orderBy = 'timestamp DESC';
                else if (sort === 'mcap') orderBy = 'marketCap DESC';
                else if (sort === 'volume') orderBy = 'volume24h DESC';
                else if (sort === '24h') orderBy = 'change24h DESC';

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
                    hasCommunityUpdate: r.hasCommunityUpdate || r.hascommunityupdate || false,
                    timestamp: parseInt(r.timestamp),
                    kScore: r.k_score || 0
                }))
            };

            if (isGenericView && redis) await redis.set(cacheKey, JSON.stringify(responsePayload), 'EX', 3);
            res.setHeader('X-Cache', 'MISS');
            return res.json(responsePayload);

        } catch (e) { res.status(500).json({ success: false, tokens: [], error: e.message }); }
    });

    // ... (Admin routes unchanged)
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
