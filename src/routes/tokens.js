/**
 * Token Routes
 * Platform: PostgreSQL
 * Optimization: Global Rate Limiting + Caching for External Search
 */
const express = require('express');
const axios = require('axios');
const { Connection, PublicKey } = require('@solana/web3.js');
const { isValidPubkey } = require('../utils/solana');
const { smartCache, saveTokenData } = require('../services/database');
const { getClient } = require('../services/redis'); // Direct Redis Access
const config = require('../config/env');
const kScoreUpdater = require('../tasks/kScoreUpdater'); 
const { syncTokenData } = require('../tasks/metadataUpdater'); 

const router = express.Router();
const solanaConnection = new Connection(config.SOLANA_RPC_URL, 'confirmed');

// --- HELPER: GLOBAL RATE LIMITER ---
// Returns TRUE if we are allowed to call DexScreener
async function checkExternalRateLimit() {
    try {
        const redis = getClient();
        if (!redis || redis.status !== 'ready') return true; // Fail open if Redis down

        const key = 'ratelimit:dexscreener:global';
        const current = await redis.incr(key);
        
        // If this is the first call, set expiry to 60 seconds
        if (current === 1) {
            await redis.expire(key, 60);
        }

        // Limit: 250 requests per minute (DexScreener limit is ~300)
        return current <= 250;
    } catch (e) {
        return true; // Fail open on error
    }
}

// Helper to get best pair
function getBestPairRoute(pairs, mint) {
    if (!pairs || pairs.length === 0) return null;
    const validPairs = pairs.filter(p => (p.liquidity?.usd || 0) > 100);
    const candidates = validPairs.length > 0 ? validPairs : pairs;
    const sortedPairs = candidates.sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0));
    return sortedPairs[0];
}

function getSocialLink(pair, type) {
    if (!pair.info || !pair.info.socials) return null;
    const social = pair.info.socials.find(s => s.type === type);
    return social ? social.url : null;
}

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

        let tx = null;
        let attempts = 0;
        const maxRetries = 5; 
        while (attempts < maxRetries) {
            try {
                tx = await solanaConnection.getParsedTransaction(signature, { commitment: 'confirmed', maxSupportedTransactionVersion: 0 });
                if (tx) break; 
            } catch (err) { }
            attempts++;
            if (attempts < maxRetries) await sleep(2500); 
        }
        
        if (!tx) throw new Error("Transaction propagation timed out.");
        if (tx.meta.err) throw new Error("Transaction failed on-chain.");
        
        return true; 
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
            res.json({ success: true, message: "Update queued." });
        } catch (e) { res.status(500).json({ success: false, error: "Submission failed: " + e.message }); }
    });

    // --- SEARCH / TOKENS LIST (OPTIMIZED) ---
    router.get('/tokens', async (req, res) => {
        const { sort = 'newest', limit = 100, page = 1, search = '', filter = '' } = req.query;
        const limitVal = Math.min(parseInt(limit) || 100, 100);
        const pageVal = Math.max(parseInt(page) || 1, 1);
        const offsetVal = (pageVal - 1) * limitVal;
        const searchTerm = search ? search.trim() : '';
        const cacheKey = `api:tokens:${sort}:${limitVal}:${pageVal}:${searchTerm || 'all'}:${filter}`;

        try {
            const result = await smartCache(cacheKey, 5, async () => {
                let orderByClause = 'ORDER BY timestamp DESC'; 
                switch (sort) {
                    case 'kscore': orderByClause = 'ORDER BY k_score DESC'; break;
                    case 'mcap': orderByClause = 'ORDER BY marketCap DESC'; break;
                    case 'volume': orderByClause = 'ORDER BY volume24h DESC'; break;
                    case 'gainers': case '24h': orderByClause = 'ORDER BY change24h DESC'; break;
                    case '1h': orderByClause = 'ORDER BY change1h DESC'; break;
                    case '5m': orderByClause = 'ORDER BY change5m DESC'; break;
                    case 'price': orderByClause = 'ORDER BY priceUsd DESC'; break;
                    case 'newest': case 'age': default: orderByClause = 'ORDER BY timestamp DESC'; break;
                }

                let rows = [];
                const isAddressSearch = isValidPubkey(searchTerm);

                // 1. LOCAL SEARCH
                if (searchTerm.length > 0) {
                    if (isAddressSearch) {
                        rows = await db.all(`SELECT * FROM tokens WHERE mint = $1`, [searchTerm]);
                    } else {
                        const searchPattern = `%${searchTerm}%`;
                        rows = await db.all(`SELECT * FROM tokens WHERE (ticker ILIKE $1 OR name ILIKE $1) ${filter === 'verified' ? 'AND hasCommunityUpdate = TRUE' : ''} ${orderByClause} LIMIT 50`, [searchPattern]);
                    }
                } else {
                    let query = `SELECT * FROM tokens`;
                    let where = [];
                    if (filter === 'verified') where.push(`hasCommunityUpdate = TRUE`);
                    if (where.length > 0) query += ` WHERE ${where.join(' AND ')}`;
                    query += ` ${orderByClause} LIMIT ${limitVal} OFFSET ${offsetVal}`;
                    rows = await db.all(query);
                }

                // 2. EXTERNAL SEARCH (OPTIMIZED)
                // Conditions: Search must be 3+ chars, and we must pass Global Rate Limit check
                const isTooShort = searchTerm.length < 3;
                
                let shouldFetchExternal = searchTerm.length > 0 && !isTooShort;

                if (shouldFetchExternal) {
                    // Check Global Rate Limit (Redis)
                    const canCallExternal = await checkExternalRateLimit();
                    
                    if (!canCallExternal) {
                        console.warn(`⚠️ Global Rate Limit Exceeded. Skipping external search for '${searchTerm}'`);
                        shouldFetchExternal = false;
                    } else {
                        // Check if we have this SPECIFIC search cached already (don't hit API if we just searched "pepe")
                        // Note: smartCache handles the *result* of this function, but we want to cache the *external fetch* specifically
                        // to reuse it across different page/sort views if possible.
                        // For simplicity, we implement logic here.
                    }
                }

                if (shouldFetchExternal) {
                    // Use a specific cache key for the external result to avoid hitting API for same term
                    const extCacheKey = `ext_search:${searchTerm.toLowerCase()}`;
                    
                    const externalTokens = await smartCache(extCacheKey, 300, async () => { // Cache for 5 minutes
                        try {
                            let dexRes;
                            if (isAddressSearch) {
                                dexRes = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${searchTerm}`, { timeout: 3500 });
                                if (!dexRes.data?.pairs) {
                                     dexRes = await axios.get(`https://api.dexscreener.com/latest/dex/pairs/solana/${searchTerm}`, { timeout: 3500 });
                                }
                            } else {
                                // GENERIC TEXT SEARCH ENABLED
                                dexRes = await axios.get(`https://api.dexscreener.com/latest/dex/search?q=${encodeURIComponent(searchTerm)}`, { timeout: 4500 });
                            }

                            if (dexRes.data?.pairs) {
                                const validPairs = dexRes.data.pairs.filter(p => p.chainId === 'solana');
                                
                                const foundTokens = validPairs.map(pair => ({
                                    mint: pair.baseToken.address, 
                                    name: pair.baseToken.name, 
                                    ticker: pair.baseToken.symbol, 
                                    image: pair.info?.imageUrl,
                                    marketCap: Number(pair.fdv || pair.marketCap || 0), 
                                    volume24h: Number(pair.volume?.h24 || 0), 
                                    priceUsd: Number(pair.priceUsd || 0), 
                                    timestamp: pair.pairCreatedAt || Date.now(),
                                    isExternal: true // Marker
                                }));
                                return foundTokens;
                            }
                            return [];
                        } catch (extErr) { 
                            console.error("External search failed:", extErr.message);
                            return [];
                        }
                    });

                    // Merge and Deduplicate
                    if (externalTokens && externalTokens.length > 0) {
                        // Save to DB (Fire & Forget)
                        externalTokens.forEach(t => saveTokenData(db, t.mint, t, t.timestamp).catch(console.error));

                        // Merge into rows (Avoid duplicates)
                        const existingMints = new Set(rows.map(r => r.mint));
                        externalTokens.forEach(t => {
                            if (!existingMints.has(t.mint)) {
                                rows.push({
                                    ...t,
                                    // Normalize for response format
                                    timestamp: t.timestamp,
                                    hasCommunityUpdate: false,
                                    k_score: 0
                                });
                            }
                        });
                    }
                }

                // 3. Final Sort
                if (searchTerm && rows.length > 0) {
                     // Prefer exact matches (Address or Ticker)
                     rows.sort((a, b) => {
                         const aExact = a.mint === searchTerm || a.ticker.toLowerCase() === searchTerm.toLowerCase();
                         const bExact = b.mint === searchTerm || b.ticker.toLowerCase() === searchTerm.toLowerCase();
                         if (aExact && !bExact) return -1;
                         if (bExact && !aExact) return 1;
                         return (b.marketCap || 0) - (a.marketCap || 0);
                     });
                }

                return {
                    success: true, page: pageVal, limit: limitVal,
                    tokens: rows.map(r => ({
                        mint: r.mint, name: r.name, ticker: r.ticker, image: r.image,
                        marketCap: r.marketcap || r.marketCap || 0, volume24h: r.volume24h || 0, priceUsd: r.priceusd || r.priceUsd || 0,
                        timestamp: parseInt(r.timestamp), change5m: r.change5m || 0, change1h: r.change1h || 0, change24h: r.change24h || 0,
                        hasCommunityUpdate: r.hascommunityupdate || r.hasCommunityUpdate || false, kScore: r.k_score || 0
                    })), lastUpdate: Date.now()
                };
            });
            res.json(result);
        } catch (e) { res.status(500).json({ success: false, tokens: [], error: e.message }); }
    });

    router.get('/token/:mint', async (req, res) => {
        const { mint } = req.params;
        const cacheKey = `api:token:${mint}`;
        const result = await smartCache(cacheKey, 30, async () => {
            const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
            let tokenData = token || { mint, name: 'Unknown', ticker: 'Unknown' };
            let pairs = [];
            
            try {
                // Individual token fetches are less strictly limited, but still cached
                const dexRes = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${mint}`, { timeout: 3000 });
                if (dexRes.data?.pairs) {
                    pairs = dexRes.data.pairs.sort((a,b) => (b.liquidity?.usd||0) - (a.liquidity?.usd||0));
                    if(!token) {
                         tokenData.name = pairs[0].baseToken.name;
                         tokenData.ticker = pairs[0].baseToken.symbol;
                         tokenData.image = pairs[0].info?.imageUrl;
                    }
                    syncTokenData(deps, mint, pairs).catch(()=>null); 
                }
            } catch(e) {}
            
            return { success: true, token: { ...tokenData, pairs } };
        });
        res.json(result);
    });

    router.get('/admin/updates', requireAdmin, async (req, res) => {
        const { type } = req.query;
        let sql = `SELECT u.*, t.name, t.ticker, t.image FROM token_updates u LEFT JOIN tokens t ON u.mint = t.mint`;
        sql += type === 'history' ? ` WHERE u.status != 'pending' ORDER BY u.submittedAt DESC LIMIT 100` : ` WHERE u.status = 'pending' ORDER BY u.submittedAt ASC`;
        res.json({ success: true, updates: await db.all(sql) });
    });

    router.post('/admin/approve-update', requireAdmin, async (req, res) => {
        const { id } = req.body;
        const update = await db.get('SELECT * FROM token_updates WHERE id = $1', [id]);
        if (!update) return res.status(404).json({error:'Not found'});
        
        const fields = []; const params = []; let idx = 1;
        if(update.twitter) { fields.push(`twitter=$${idx++}`); params.push(update.twitter); }
        if(update.website) { fields.push(`website=$${idx++}`); params.push(update.website); }
        if(update.telegram) { fields.push(`tweetUrl=$${idx++}`); params.push(update.telegram); } 
        if(update.banner) { fields.push(`banner=$${idx++}`); params.push(update.banner); }
        if(update.description) { fields.push(`description=$${idx++}`); params.push(update.description); }
        fields.push(`hasCommunityUpdate=$${idx++}`); params.push(true);
        params.push(update.mint);
        
        if(fields.length>0) await db.run(`UPDATE tokens SET ${fields.join(', ')} WHERE mint = $${idx}`, params);
        await db.run("UPDATE token_updates SET status = 'approved' WHERE id = $1", [id]);
        await kScoreUpdater.updateSingleToken(deps, update.mint);
        res.json({success: true});
    });

    router.post('/admin/reject-update', requireAdmin, async (req, res) => {
        await db.run("UPDATE token_updates SET status = 'rejected' WHERE id = $1", [req.body.id]); 
        res.json({ success: true }); 
    });

    return router;
}

module.exports = { init };
