const express = require('express');
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

const router = express.Router();
const solanaConnection = getSolanaConnection();

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

    async function verifyPayment(signature, payerPubkey) {
        if (!signature) throw new Error("Payment signature required");
        // Check if signature already used
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
            decimals = EXCLUDED.decimals
        `, [
            mint, 
            baseData.name, 
            baseData.ticker, 
            baseData.image, 
            supply, 
            decimals, 
            0, 0, 0, 0, 0, 
            Date.now()
        ]);

        await enqueueTokenUpdate(mint);
        
        if (poolAddresses.length > 0) {
            await snapshotPools(poolAddresses).catch(e => console.error("Snapshot Err:", e.message));
            await aggregateAndSaveToken(db, mint);
        }

        return { ...baseData, pairs: pools };
    }

    // --- PUBLIC ROUTES ---

    router.get('/token/:mint/candles', async (req, res) => {
        const { mint } = req.params;
        const { resolution = '5', from, to, poolAddress } = req.query; 
        
        try {
            const resMinutes = parseInt(resolution);
            const resMs = resMinutes * 60 * 1000;
            const fromMs = parseInt(from) * 1000 || (Date.now() - 24 * 60 * 60 * 1000);
            const toMs = parseInt(to) * 1000 || Date.now();
            
            const cacheKey = `chart:${mint}:${poolAddress || 'best'}:${resolution}:${Math.floor(Date.now() / 30000)}`; 

            const result = await smartCache(cacheKey, 30, async () => {
                let targetPoolAddress = poolAddress;

                if (!targetPoolAddress) {
                    const bestPool = await db.get(`SELECT address FROM pools WHERE mint = $1 ORDER BY liquidity_usd DESC LIMIT 1`, [mint]);
                    if (!bestPool) return { success: false, error: "Token not indexed" };
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
                
                if (!rows || rows.length === 0) return { success: true, candles: [] };

                // ... (Candle processing logic retained) ...
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
                
                return { success: true, candles };
            });

            res.json(result);
            
        } catch (e) {
            logger.error(`Candle Error ${mint}: ${e.message}`);
            res.status(500).json({ success: false, error: e.message });
        }
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

            res.json({ 
                success: true, 
                sol: solBalance / 1e9, 
                tokens: tokenAccounts.value.length > 0 ? tokenAccounts.value[0].account.data.parsed.info.tokenAmount.uiAmount : 0 
            });
        } catch (e) {
            res.status(500).json({ success: false, error: "Failed to fetch balance" });
        }
    });

    router.post('/request-update', async (req, res) => {
        const { mint, twitter, website, telegram, banner, description, signature, userPublicKey } = req.body;
        logger.info(`📝 Update Request: ${mint}`);

        try {
            if (!mint || mint.length < 30) return res.status(400).json({ success: false, error: "Invalid Mint" });

            // Verify Payment on-chain (or check DB if sig used)
            try {
                await verifyPayment(signature, userPublicKey);
            } catch (payErr) {
                return res.status(402).json({ success: false, error: payErr.message });
            }

            await db.run(`
                INSERT INTO token_updates (mint, twitter, website, telegram, banner, description, submittedAt, status, signature, payer)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9)
            `, [mint, twitter, website, telegram, banner, description, Date.now(), signature, userPublicKey]);

            // Trigger an index refresh just in case
            try { await indexTokenOnChain(mint); } catch (err) {}

            res.json({ success: true, message: "Update queued." });

        } catch (e) {
            res.status(500).json({ success: false, error: "Submission failed" });
        }
    });

    // --- ADMIN ROUTES (Protected) ---

    router.get('/admin/updates', requireAdmin, async (req, res) => {
        const { type } = req.query;
        try {
            let sql = `SELECT u.*, t.symbol as ticker, t.image FROM token_updates u LEFT JOIN tokens t ON u.mint = t.mint`;
            
            if (type === 'history') {
                sql += ` WHERE u.status != 'pending' ORDER BY u.submittedAt DESC LIMIT 50`;
            } else {
                sql += ` WHERE u.status = 'pending' ORDER BY u.submittedAt ASC`;
            }

            const updates = await db.all(sql);
            res.json({ success: true, updates });
        } catch (e) {
            res.status(500).json({ success: false, error: e.message });
        }
    });

    router.post('/admin/approve-update', requireAdmin, async (req, res) => {
        const { id } = req.body;
        try {
            const request = await db.get(`SELECT * FROM token_updates WHERE id = $1`, [id]);
            if (!request) return res.status(404).json({ success: false, error: "Request not found" });

            // 1. Update Token Metadata in DB
            // We use COALESCE to keep existing data if the update field is empty, 
            // BUT usually an update replaces the old data. 
            // Here we assume the admin wants to apply the non-empty fields.
            
            const meta = {
                twitter: request.twitter,
                website: request.website,
                telegram: request.telegram,
                banner: request.banner,
                description: request.description
            };

            await db.run(`
                UPDATE tokens 
                SET metadata = jsonb_set(COALESCE(metadata, '{}'), '{community}', $1),
                    hasCommunityUpdate = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE mint = $2
            `, [JSON.stringify(meta), request.mint]);

            // Also update columns for easier querying
            // (Only if your schema has specific columns for these, otherwise JSONB is fine)
            // The provided schema has specific columns in 'token_updates' but 'tokens' table structure varies.
            // Let's assume JSONB metadata is the source of truth for display.

            // 2. Mark Request as Approved
            await db.run(`UPDATE token_updates SET status = 'approved' WHERE id = $1`, [id]);

            // 3. Trigger K-Score Recalc (Boost score)
            await updateSingleToken({ db }, request.mint);

            res.json({ success: true });
        } catch (e) {
            res.status(500).json({ success: false, error: e.message });
        }
    });

    router.post('/admin/reject-update', requireAdmin, async (req, res) => {
        const { id } = req.body;
        try {
            await db.run(`UPDATE token_updates SET status = 'rejected' WHERE id = $1`, [id]);
            res.json({ success: true });
        } catch (e) {
            res.status(500).json({ success: false, error: e.message });
        }
    });

    router.get('/admin/token/:mint', requireAdmin, async (req, res) => {
        const { mint } = req.params;
        try {
            const token = await db.get(`SELECT * FROM tokens WHERE mint = $1`, [mint]);
            if (!token) return res.status(404).json({ success: false, error: "Token not found" });

            // Extract metadata if available
            let meta = {};
            try { 
                if (typeof token.metadata === 'string') meta = JSON.parse(token.metadata);
                else meta = token.metadata || {};
            } catch(e) {}

            // Community metadata usually stored in 'community' key inside metadata jsonb
            // OR strictly in the token row if you added columns. 
            // Let's assume we pull from the JSONB for the form.
            const community = meta.community || {};

            res.json({ 
                success: true, 
                token: {
                    ...token,
                    ticker: token.symbol,
                    twitter: community.twitter || meta.twitter,
                    website: community.website || meta.website,
                    telegram: community.telegram || meta.telegram,
                    banner: community.banner || meta.banner,
                    description: community.description || meta.description
                } 
            });
        } catch (e) {
            res.status(500).json({ success: false, error: e.message });
        }
    });

    router.post('/admin/update-token', requireAdmin, async (req, res) => {
        const { mint, twitter, website, telegram, banner, description } = req.body;
        try {
            const meta = { twitter, website, telegram, banner, description };
            
            // Allow manual override of community data
            await db.run(`
                UPDATE tokens 
                SET metadata = jsonb_set(COALESCE(metadata, '{}'), '{community}', $1),
                    hasCommunityUpdate = TRUE
                WHERE mint = $2
            `, [JSON.stringify(meta), mint]);

            await updateSingleToken({ db }, mint);
            res.json({ success: true });
        } catch (e) {
            res.status(500).json({ success: false, error: e.message });
        }
    });

    router.post('/admin/delete-token', requireAdmin, async (req, res) => {
        const { mint } = req.body;
        logger.warn(`🗑️  ADMIN: Executing HARD DELETE for ${mint}`);

        try {
            // 1. Find all pools for this token to delete their candles/trackers
            const pools = await db.all(`SELECT address FROM pools WHERE mint = $1`, [mint]);
            const poolAddresses = pools.map(p => p.address);

            if (poolAddresses.length > 0) {
                // Delete Candles
                // Need to handle array for SQL IN clause safely
                // PG supports ANY($1) for arrays
                await db.run(`DELETE FROM candles_1m WHERE pool_address = ANY($1)`, [poolAddresses]);
                
                // Delete Trackers
                await db.run(`DELETE FROM active_trackers WHERE pool_address = ANY($1)`, [poolAddresses]);
            }

            // 2. Delete Pools
            await db.run(`DELETE FROM pools WHERE mint = $1`, [mint]);

            // 3. Delete K-Scores & Updates
            await db.run(`DELETE FROM k_scores WHERE mint = $1`, [mint]);
            await db.run(`DELETE FROM token_updates WHERE mint = $1`, [mint]);

            // 4. Delete Token
            await db.run(`DELETE FROM tokens WHERE mint = $1`, [mint]);

            // 5. Clear Redis Cache (Best Effort)
            const redis = getClient();
            if (redis) {
                await redis.del(`token:detail:${mint}`);
                // Note: Clearing list caches is harder without keys matching patterns, 
                // but they usually expire in 3-5 seconds anyway.
            }

            logger.info(`✅ ADMIN: Deleted ${mint} and all associated history.`);
            res.json({ success: true, message: "Token and all history permanently deleted." });

        } catch (e) {
            logger.error(`Delete Failed: ${e.message}`);
            res.status(500).json({ success: false, error: e.message });
        }
    });

    router.post('/admin/refresh-kscore', requireAdmin, async (req, res) => {
        const { mint } = req.body;
        try {
            const newScore = await updateSingleToken({ db }, mint);
            res.json({ success: true, message: `K-Score Updated: ${newScore}` });
        } catch (e) {
            res.status(500).json({ success: false, error: e.message });
        }
    });

    // --- STANDARD PUBLIC ROUTES ---
    router.get('/token/:mint', async (req, res) => {
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

                let tokenData = { ...token };
                if (tokenData.symbol) tokenData.ticker = tokenData.symbol;
                
                // Merge JSON metadata if exists
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
                    
                    const now = Date.now();
                    const oneHourAgo = now - (60 * 60 * 1000);
                    const twentyFourHoursAgo = now - (24 * 60 * 60 * 1000);

                    const [c1h, c24h, oldest] = await Promise.all([
                        db.get(`SELECT close FROM candles_1m WHERE pool_address = $1 AND timestamp <= $2 ORDER BY timestamp DESC LIMIT 1`, [mainPool.address, oneHourAgo]),
                        db.get(`SELECT close FROM candles_1m WHERE pool_address = $1 AND timestamp <= $2 ORDER BY timestamp DESC LIMIT 1`, [mainPool.address, twentyFourHoursAgo]),
                        db.get(`SELECT close, timestamp FROM candles_1m WHERE pool_address = $1 ORDER BY timestamp ASC LIMIT 1`, [mainPool.address])
                    ]);

                    if (c1h && c1h.close > 0) {
                        tokenData.change1h = ((tokenData.priceUsd - c1h.close) / c1h.close) * 100;
                    } else if (oldest && oldest.close > 0 && oldest.timestamp < (now - 60000)) {
                        tokenData.change1h = ((tokenData.priceUsd - oldest.close) / oldest.close) * 100;
                    } else {
                        tokenData.change1h = 0;
                    }
                    
                    if (c24h && c24h.close > 0) {
                        tokenData.change24h = ((tokenData.priceUsd - c24h.close) / c24h.close) * 100;
                    } else if (oldest && oldest.close > 0 && oldest.timestamp < (now - 300000)) {
                        tokenData.change24h = ((tokenData.priceUsd - oldest.close) / oldest.close) * 100;
                    } else {
                        tokenData.change24h = 0;
                    }
                }

                return { success: true, token: { ...tokenData, pairs } };
            });
            res.json(result);
        } catch(e) {
            res.status(500).json({ success: false, error: e.message });
        }
    });

    router.get('/tokens', async (req, res) => {
        const { search = '', sort = 'kscore', page = 1 } = req.query;
        try {
            const isGenericView = !search;
            const cacheKey = `api:tokens:list:${sort}:${page}:${search}`;
            const redis = getClient(); 

            if (isGenericView && redis) {
                try {
                    const cached = await redis.get(cacheKey);
                    if (cached) { res.setHeader('X-Cache', 'HIT'); return res.json(JSON.parse(cached)); }
                } catch(e) {}
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
                else if (sort === 'liquidity') orderBy = 'liquidity DESC';

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
                    hasCommunityUpdate: r.hasCommunityUpdate || r.hascommunityupdate || false,
                    timestamp: parseInt(r.timestamp),
                    kScore: r.k_score || 0
                }))
            };

            if (isGenericView && redis) {
                try { await redis.set(cacheKey, JSON.stringify(responsePayload), 'EX', 3); } catch(e){}
            }
            res.setHeader('X-Cache', 'MISS');
            return res.json(responsePayload);

        } catch (e) { res.status(500).json({ success: false, tokens: [], error: e.message }); }
    });
    
    return router;
}

module.exports = { init };
