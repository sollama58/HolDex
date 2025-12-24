/**
 * Token Routes
 * Updated: Robust "Live" Data for Individual Token View
 */
const express = require('express');
const axios = require('axios');
const { Connection, PublicKey } = require('@solana/web3.js');
const { isValidPubkey } = require('../utils/solana');
const { smartCache, saveTokenData } = require('../services/database');
const config = require('../config/env');
const { calculateTokenScore } = require('../tasks/kScoreUpdater'); 
// Import getBestPair helper to ensure consistent logic
// We need to export it from metadataUpdater.js or redefine it here. 
// Redefining simpler version for safety to avoid circular deps if structure is messy.
const { syncTokenData } = require('../tasks/metadataUpdater'); 

const router = express.Router();

const solanaConnection = new Connection(config.SOLANA_RPC_URL, 'confirmed');

// Helper to get best pair (Duplicated logic from metadataUpdater to ensure self-contained route safety)
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

    // --- PAYMENT VERIFICATION LOGIC ---
    async function verifyPayment(signature, payerPubkey) {
        if (!signature) throw new Error("Payment signature required");
        const existing = await db.get('SELECT id FROM token_updates WHERE signature = $1', [signature]);
        if (existing) throw new Error("Transaction signature already used");

        let tx = null;
        let attempts = 0;
        const maxRetries = 5; 

        while (attempts < maxRetries) {
            try {
                tx = await solanaConnection.getParsedTransaction(signature, { 
                    commitment: 'confirmed',
                    maxSupportedTransactionVersion: 0 
                });
                if (tx) break; 
            } catch (err) {
                console.log(`Attempt ${attempts + 1} failed to fetch tx: ${err.message}`);
            }
            attempts++;
            if (attempts < maxRetries) await sleep(2500); 
        }
        
        if (!tx) throw new Error("Transaction propagation timed out. Please try submitting again in 30 seconds.");
        if (tx.meta.err) throw new Error("Transaction failed on-chain.");

        const isSigner = tx.transaction.message.accountKeys.some(k => k.pubkey.toBase58() === payerPubkey && k.signer);
        if (!isSigner) throw new Error("Signer public key mismatch or signature missing.");

        const treasuryIndex = tx.transaction.message.accountKeys.findIndex(k => k.pubkey.toBase58() === config.TREASURY_WALLET);
        if (treasuryIndex === -1) throw new Error("Treasury wallet not found in transaction.");

        const preSol = tx.meta.preBalances[treasuryIndex];
        const postSol = tx.meta.postBalances[treasuryIndex];
        const solReceived = (postSol - preSol) / 1e9; 

        if (solReceived < config.FEE_SOL * 0.95) {
            throw new Error(`Insufficient SOL fee. Received: ${solReceived.toFixed(4)}, Required: ${config.FEE_SOL}`);
        }

        const treasuryTokenPre = tx.meta.preTokenBalances.find(b => b.owner === config.TREASURY_WALLET && b.mint === config.FEE_TOKEN_MINT);
        const treasuryTokenPost = tx.meta.postTokenBalances.find(b => b.owner === config.TREASURY_WALLET && b.mint === config.FEE_TOKEN_MINT);

        const preAmt = treasuryTokenPre ? (treasuryTokenPre.uiTokenAmount.uiAmount || 0) : 0;
        const postAmt = treasuryTokenPost ? (treasuryTokenPost.uiTokenAmount.uiAmount || 0) : 0;
        const tokensReceived = postAmt - preAmt;

        if (tokensReceived < config.FEE_TOKEN_AMOUNT * 0.95) {
            throw new Error(`Insufficient Token fee. Received: ${tokensReceived.toFixed(2)}, Required: ${config.FEE_TOKEN_AMOUNT}`);
        }
        return true;
    }

    // --- PROXY ENDPOINTS ---
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

            const sol = solBalance / 1e9;
            const tokens = tokenAccounts.value.length > 0 
                ? tokenAccounts.value[0].account.data.parsed.info.tokenAmount.uiAmount 
                : 0;

            res.json({ success: true, sol, tokens });
        } catch (e) {
            console.error("Proxy Balance Error:", e.message);
            res.status(500).json({ success: false, error: "Failed to fetch balance" });
        }
    });

    router.get('/config/fees', (req, res) => {
        res.json({
            success: true,
            solFee: config.FEE_SOL,
            tokenFee: config.FEE_TOKEN_AMOUNT,
            tokenMint: config.FEE_TOKEN_MINT,
            treasury: config.TREASURY_WALLET
        });
    });
    
    router.post('/request-update', async (req, res) => {
        try {
            const { mint, twitter, website, telegram, banner, description, signature, userPublicKey } = req.body;
            if (!mint || mint.length < 30) return res.status(400).json({ success: false, error: "Invalid Mint" });
            if (!signature || !userPublicKey) return res.status(400).json({ success: false, error: "Payment signature and wallet required" });

            let safeDesc = null;
            if (description) safeDesc = description.substring(0, 250).replace(/<[^>]*>?/gm, ''); 

            try { await verifyPayment(signature, userPublicKey); } 
            catch (payErr) { return res.status(402).json({ success: false, error: payErr.message }); }

            const hasData = (twitter && twitter.length > 0) || (website && website.length > 0) || (telegram && telegram.length > 0) || (banner && banner.length > 0) || (safeDesc && safeDesc.length > 0);
            if (!hasData) return res.status(400).json({ success: false, error: "No profile data provided" });

            await db.run(`INSERT INTO token_updates (mint, twitter, website, telegram, banner, description, submittedAt, status, signature, payer) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9)`, 
                [mint, twitter, website, telegram, banner, safeDesc, Date.now(), signature, userPublicKey]);

            res.json({ success: true, message: "Update queued successfully. Payment verified." });
        } catch (e) { res.status(500).json({ success: false, error: "Submission failed: " + e.message }); }
    });

    // --- SEARCH / TOKENS LIST ---
    router.get('/tokens', async (req, res) => {
        const { sort = 'newest', limit = 100, page = 1, search = '', filter = '' } = req.query;
        const limitVal = Math.min(parseInt(limit) || 100, 100);
        const pageVal = Math.max(parseInt(page) || 1, 1);
        const offsetVal = (pageVal - 1) * limitVal;
        const cacheKey = `api:tokens:${sort}:${limitVal}:${pageVal}:${search.trim() || 'all'}:${filter}`;

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

                let query = `SELECT * FROM tokens`;
                let params = [];
                let whereClauses = [];
                const searchTerm = search ? search.trim() : '';

                if (searchTerm.length > 0) {
                    if (isValidPubkey(searchTerm)) {
                        whereClauses.push(`mint = $${params.length + 1}`);
                        params.push(searchTerm);
                    } else {
                        whereClauses.push(`(ticker ILIKE $${params.length + 1} OR name ILIKE $${params.length + 1})`);
                        params.push(`%${searchTerm}%`);
                    }
                }

                if (filter === 'verified') whereClauses.push(`hasCommunityUpdate = TRUE`);
                if (whereClauses.length > 0) query += ` WHERE ${whereClauses.join(' AND ')}`;
                
                const dbLimit = searchTerm ? 20 : limitVal;
                query += ` ${orderByClause} LIMIT ${dbLimit} OFFSET ${offsetVal}`;

                let rows = params.length > 0 ? await db.all(query, params) : await db.all(query);

                const shouldFetchExternal = (searchTerm.length > 2 && filter !== 'verified');
                
                if (shouldFetchExternal) {
                    if (rows.length < 5) {
                        try {
                            let dexUrl = `https://api.dexscreener.com/latest/dex/search?q=${encodeURIComponent(searchTerm)}`;
                            if (isValidPubkey(searchTerm)) dexUrl = `https://api.dexscreener.com/latest/dex/tokens/${searchTerm}`;
                            
                            const dexRes = await axios.get(dexUrl, { timeout: 4000 });
                            
                            if (dexRes.data && dexRes.data.pairs) {
                                const validPairs = dexRes.data.pairs.filter(p => p.chainId === 'solana');
                                const externalTokens = validPairs.map(pair => {
                                    const metadata = {
                                        ticker: pair.baseToken.symbol, name: pair.baseToken.name, description: `Imported via Search: ${searchTerm}`,
                                        twitter: getSocialLink(pair, 'twitter'), website: pair.info?.websites?.[0]?.url || null, image: pair.info?.imageUrl,
                                        marketCap: Number(pair.fdv || pair.marketCap || 0), volume24h: Number(pair.volume?.h24 || 0), priceUsd: Number(pair.priceUsd || 0)
                                    };
                                    const createdAt = pair.pairCreatedAt || Date.now();
                                    return {
                                        mint: pair.baseToken.address, userPubkey: null, name: metadata.name, ticker: metadata.ticker, image: metadata.image,
                                        marketCap: metadata.marketCap, volume24h: metadata.volume24h, priceUsd: metadata.priceUsd, timestamp: createdAt,
                                        change5m: pair.priceChange?.m5 || 0, change1h: pair.priceChange?.h1 || 0, change24h: pair.priceChange?.h24 || 0,
                                        hasCommunityUpdate: false, k_score: 0, isExternal: true 
                                    };
                                });
                                const tokenMap = new Map();
                                rows.forEach(r => tokenMap.set(r.mint, r));
                                for (const extToken of externalTokens) {
                                    if (!tokenMap.has(extToken.mint)) {
                                        tokenMap.set(extToken.mint, extToken);
                                        saveTokenData(db, extToken.mint, extToken, extToken.timestamp).catch(err => console.error("Search Indexing Error:", err.message));
                                    }
                                }
                                rows = Array.from(tokenMap.values());
                            }
                        } catch (extErr) { console.error("External search failed:", extErr.message); }
                    }
                }

                if (searchTerm) {
                    const lowerSearch = searchTerm.toLowerCase();
                    if (!isValidPubkey(searchTerm)) {
                        rows = rows.filter(r => (r.ticker && r.ticker.toLowerCase().includes(lowerSearch)) || (r.name && r.name.toLowerCase().includes(lowerSearch)));
                    }
                    rows.sort((a, b) => (b.marketCap || 0) - (a.marketCap || 0));
                    rows = rows.slice(0, 5);
                }

                return {
                    success: true, page: pageVal, limit: limitVal,
                    tokens: rows.map(r => ({
                        mint: r.mint, userPubkey: r.userpubkey, name: r.name, ticker: r.ticker, image: r.image,
                        marketCap: r.marketcap || r.marketCap || 0, volume24h: r.volume24h || 0, priceUsd: r.priceusd || r.priceUsd || 0,
                        timestamp: parseInt(r.timestamp), change5m: r.change5m || 0, change1h: r.change1h || 0, change24h: r.change24h || 0,
                        hasCommunityUpdate: r.hascommunityupdate || r.hasCommunityUpdate || false, kScore: r.k_score || 0
                    })), lastUpdate: Date.now()
                };
            });
            res.json(result);
        } catch (e) { res.status(500).json({ success: false, tokens: [], error: e.message }); }
    });

    // --- INDIVIDUAL TOKEN VIEW (FIXED) ---
    router.get('/token/:mint', async (req, res) => {
        try {
            const { mint } = req.params;
            const cacheKey = `api:token:${mint}`;
            
            const result = await smartCache(cacheKey, 30, async () => {
                // 1. Get DB Data
                const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
                if (!token) return null;
                
                let pairs = [];
                let bestPair = null;

                // 2. Fetch Fresh Dex Data
                try {
                    const dexRes = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${mint}`, { timeout: 3000 });
                    if (dexRes.data && dexRes.data.pairs) {
                        // Sort by liquidity for UI
                        pairs = dexRes.data.pairs.map(p => ({
                            pairAddress: p.pairAddress, dexId: p.dexId, 
                            priceUsd: p.priceUsd, liquidity: p.liquidity?.usd || 0, 
                            url: p.url, fdv: p.fdv, marketCap: p.marketCap, 
                            volume: p.volume, priceChange: p.priceChange, 
                            pairCreatedAt: p.pairCreatedAt, info: p.info,
                            baseToken: p.baseToken
                        })).sort((a, b) => (b.liquidity || 0) - (a.liquidity || 0));

                        // Identify the absolute best pair for pricing
                        bestPair = getBestPairRoute(pairs, mint);

                        // Trigger DB Update (Side Effect)
                        if (pairs.length > 0) {
                            await syncTokenData(deps, mint, pairs);
                        }
                    }
                } catch (dexErr) { console.warn("Failed to fetch pairs:", dexErr.message); }
                
                // 3. Construct "Live" Response
                // Use fresh Best Pair data if available, otherwise fallback to DB
                
                let liveMcap = token.marketcap || token.marketCap || 0;
                let livePrice = token.priceusd || token.priceUsd || 0;
                let liveVol = token.volume24h || 0;
                let liveChange1h = token.change1h || 0;
                let liveChange24h = token.change24h || 0;
                let liveChange5m = token.change5m || 0;

                if (bestPair) {
                    liveMcap = Number(bestPair.fdv || bestPair.marketCap || 0);
                    livePrice = Number(bestPair.priceUsd || 0);
                    liveVol = pairs.reduce((sum, p) => sum + (Number(p.volume?.h24) || 0), 0);
                    liveChange1h = Number(bestPair.priceChange?.h1 || 0);
                    liveChange24h = Number(bestPair.priceChange?.h24 || 0);
                    liveChange5m = Number(bestPair.priceChange?.m5 || 0);
                }

                // Handle mixed-case DB columns
                const hasCommunityUpdate = token.hasCommunityUpdate || token.hascommunityupdate || false;
                const kScoreVal = token.k_score || 0;

                // Force K-Score Update if 0 and Verified
                let kScore = kScoreVal;
                if (kScore === 0 && hasCommunityUpdate) {
                    try { kScore = 50; } catch (e) {} // Fallback
                }

                return { 
                    success: true, 
                    token: {
                        ...token, 
                        // Explicitly override with "Live" values
                        marketCap: liveMcap, 
                        volume24h: liveVol, 
                        priceUsd: livePrice,
                        change1h: liveChange1h, 
                        change24h: liveChange24h, 
                        change5m: liveChange5m,
                        
                        userPubkey: token.userpubkey || token.userPubkey, 
                        timestamp: parseInt(token.timestamp), 
                        
                        // FIX: Ensure these accessors match DB Schema casing OR lowercase fallback
                        twitter: token.twitter,
                        website: token.website, 
                        // MAPPING FIX: Telegram is stored in 'tweetUrl' in the DB
                        telegram: token.tweetUrl || token.tweeturl || token.telegram, 
                        banner: token.banner, 
                        description: token.description,
                        
                        // Boolean Fix
                        hasCommunityUpdate: hasCommunityUpdate, 
                        kScore: kScore, 
                        pairs: pairs 
                    } 
                };
            });
            
            if (!result) return res.status(404).json({ success: false, error: "Not found" });
            res.json(result);
        } catch (e) { res.status(500).json({ success: false, error: "DB Error" }); }
    });

    // ... Public API & Admin endpoints (Unchanged) ...
    router.get('/public/token/:mint', async (req, res) => {
        try {
            const { mint } = req.params;
            const cacheKey = `api:public:${mint}`;
            const result = await smartCache(cacheKey, 30, async () => {
                const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
                if (!token) return null;

                const hasCommunityUpdate = token.hasCommunityUpdate || token.hascommunityupdate || false;

                return {
                    success: true,
                    data: {
                        name: token.name, ticker: token.ticker, mint: token.mint, description: token.description || "",
                        images: { icon: token.image, banner: token.banner || null },
                        socials: { 
                            twitter: token.twitter || null, 
                            telegram: token.tweetUrl || token.tweeturl || token.telegram || null, 
                            website: token.website || null 
                        },
                        stats: { 
                            kScore: token.k_score || 0, 
                            marketCap: token.marketCap || token.marketcap || 0, 
                            volume24h: token.volume24h || 0, 
                            updatedAt: parseInt(token.lastUpdated || Date.now()) 
                        },
                        verified: hasCommunityUpdate
                    }
                };
            });
            if (!result) return res.status(404).json({ success: false, error: "Token not found" });
            res.json(result);
        } catch (e) { console.error("Public API Error:", e); res.status(500).json({ success: false, error: "Internal Server Error" }); }
    });

    router.post('/admin/refresh-kscore', requireAdmin, async (req, res) => {
        const { mint } = req.body;
        try {
            await db.run(`UPDATE tokens SET last_k_calc = 0 WHERE mint = $1`, [mint]); 
            res.json({ success: true, message: `K-Score Refresh Queued for ${mint}` });
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    router.get('/admin/updates', requireAdmin, async (req, res) => {
        try {
            const updates = await db.all(`SELECT u.*, t.name, t.ticker, t.image FROM token_updates u LEFT JOIN tokens t ON u.mint = t.mint WHERE u.status = 'pending' ORDER BY u.submittedAt ASC`);
            res.json({ success: true, updates });
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    router.post('/admin/approve-update', requireAdmin, async (req, res) => {
        const { id } = req.body;
        try {
            const update = await db.get('SELECT * FROM token_updates WHERE id = $1', [id]);
            if (!update) return res.status(404).json({ success: false, error: 'Request not found' });
            
            const fields = []; const params = []; let idx = 1;
            
            if (update.twitter) { fields.push(`twitter = $${idx++}`); params.push(update.twitter); }
            if (update.website) { fields.push(`website = $${idx++}`); params.push(update.website); }
            if (update.telegram) { fields.push(`tweetUrl = $${idx++}`); params.push(update.telegram); } 
            if (update.banner) { fields.push(`banner = $${idx++}`); params.push(update.banner); }
            if (update.description) { fields.push(`description = $${idx++}`); params.push(update.description); }
            fields.push(`hasCommunityUpdate = $${idx++}`); params.push(true);
            
            if (fields.length > 0) { 
                params.push(update.mint); 
                await db.run(`UPDATE tokens SET ${fields.join(', ')} WHERE mint = $${idx}`, params); 
            }
            await db.run('DELETE FROM token_updates WHERE id = $1', [id]);
            await db.run(`UPDATE tokens SET last_k_calc = 0 WHERE mint = $1`, [update.mint]);
            res.json({ success: true, message: 'Approved & K-Score Queued' });
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    router.post('/admin/reject-update', requireAdmin, async (req, res) => {
        try { await db.run('DELETE FROM token_updates WHERE id = $1', [req.body.id]); res.json({ success: true, message: 'Rejected' }); } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    router.get('/admin/token/:mint', requireAdmin, async (req, res) => {
        try {
            const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [req.params.mint]);
            if (!token) return res.status(404).json({ success: false, error: "Not Found" });
            res.json({ success: true, token: { mint: token.mint, ticker: token.ticker, twitter: token.twitter, website: token.website, telegram: token.tweetUrl || token.tweeturl, banner: token.banner, description: token.description, kScore: token.k_score }});
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    router.post('/admin/update-token', requireAdmin, async (req, res) => {
        const { mint, twitter, website, telegram, banner, description } = req.body;
        let safeDesc = null;
        if(description) safeDesc = description.substring(0, 250).replace(/<[^>]*>?/gm, '');
        try { 
            await db.run(`UPDATE tokens SET twitter = $1, website = $2, tweetUrl = $3, banner = $4, description = $5, hasCommunityUpdate = TRUE WHERE mint = $6`, [twitter, website, telegram, banner, safeDesc, mint]); 
            await db.run(`UPDATE tokens SET last_k_calc = 0 WHERE mint = $1`, [mint]);
            res.json({ success: true, message: "Token Updated Successfully" });
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    router.post('/admin/delete-token', requireAdmin, async (req, res) => {
        try { await db.run('DELETE FROM tokens WHERE mint = $1', [req.body.mint]); res.json({ success: true, message: "Token Deleted Successfully" }); } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    return router;
}

module.exports = { init };
