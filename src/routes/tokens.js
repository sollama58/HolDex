/**
 * Token Routes
 * Updated: Payment Verification & Balance Proxy
 */
const express = require('express');
const axios = require('axios');
const { Connection, PublicKey } = require('@solana/web3.js');
const { isValidPubkey } = require('../utils/solana');
const { smartCache, saveTokenData } = require('../services/database');
const config = require('../config/env');
const { calculateTokenScore } = require('../tasks/kScoreUpdater'); 
const router = express.Router();

// Initialize backend connection
const solanaConnection = new Connection(config.SOLANA_RPC_URL, 'confirmed');

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

function init(deps) {
    const { db } = deps;

    // --- PAYMENT VERIFICATION LOGIC ---
    async function verifyPayment(signature, payerPubkey) {
        if (!signature) throw new Error("Payment signature required");
        
        // 1. Check Replay Attack
        const existing = await db.get('SELECT id FROM token_updates WHERE signature = $1', [signature]);
        if (existing) throw new Error("Transaction signature already used");

        // 2. Fetch Transaction from Chain
        const tx = await solanaConnection.getParsedTransaction(signature, { maxSupportedTransactionVersion: 0 });
        
        if (!tx) throw new Error("Transaction not found. Please wait 10s and try again.");
        if (tx.meta.err) throw new Error("Transaction failed on-chain.");

        // 3. Verify Signer
        const signer = tx.transaction.message.accountKeys.find(k => k.signer);
        if (!signer || signer.pubkey.toBase58() !== payerPubkey) throw new Error("Signer public key mismatch.");

        // 4. Verify SOL Transfer to Treasury
        const treasuryIndex = tx.transaction.message.accountKeys.findIndex(k => k.pubkey.toBase58() === config.TREASURY_WALLET);
        if (treasuryIndex === -1) throw new Error("Treasury wallet not found in transaction.");

        const preSol = tx.meta.preBalances[treasuryIndex];
        const postSol = tx.meta.postBalances[treasuryIndex];
        const solReceived = (postSol - preSol) / 1e9; // Lamports -> SOL

        if (solReceived < config.FEE_SOL * 0.99) {
            throw new Error(`Insufficient SOL fee. Received: ${solReceived.toFixed(4)}, Required: ${config.FEE_SOL}`);
        }

        // 5. Verify Token Transfer ($ASDFASDFA) to Treasury
        const treasuryTokenPre = tx.meta.preTokenBalances.find(b => b.owner === config.TREASURY_WALLET && b.mint === config.FEE_TOKEN_MINT);
        const treasuryTokenPost = tx.meta.postTokenBalances.find(b => b.owner === config.TREASURY_WALLET && b.mint === config.FEE_TOKEN_MINT);

        const preAmt = treasuryTokenPre ? (treasuryTokenPre.uiTokenAmount.uiAmount || 0) : 0;
        const postAmt = treasuryTokenPost ? (treasuryTokenPost.uiTokenAmount.uiAmount || 0) : 0;
        const tokensReceived = postAmt - preAmt;

        if (tokensReceived < config.FEE_TOKEN_AMOUNT * 0.99) {
            throw new Error(`Insufficient Token fee. Received: ${tokensReceived.toFixed(2)}, Required: ${config.FEE_TOKEN_AMOUNT}`);
        }

        return true;
    }

    // --- NEW: BACKEND PROXY FOR BALANCES ---
    // Fixes 403 Forbidden issues on frontend by routing requests through the server
    router.get('/proxy/balance/:wallet', async (req, res) => {
        try {
            const { wallet } = req.params;
            const tokenMint = req.query.tokenMint || config.FEE_TOKEN_MINT;

            if (!isValidPubkey(wallet)) return res.status(400).json({ success: false, error: "Invalid wallet" });

            const pubKey = new PublicKey(wallet);
            
            // Parallel fetch for speed
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

    // --- PUBLIC ENDPOINTS ---

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
            const { mint, twitter, website, telegram, banner, signature, userPublicKey } = req.body;
            
            if (!mint || mint.length < 30) return res.status(400).json({ success: false, error: "Invalid Mint" });
            if (!signature || !userPublicKey) return res.status(400).json({ success: false, error: "Payment signature and wallet required" });

            try {
                await verifyPayment(signature, userPublicKey);
            } catch (payErr) {
                console.warn(`Payment Verification Failed for ${mint}:`, payErr.message);
                return res.status(402).json({ success: false, error: payErr.message });
            }

            const hasData = (twitter && twitter.length > 0) || (website && website.length > 0) || (telegram && telegram.length > 0) || (banner && banner.length > 0);
            if (!hasData) return res.status(400).json({ success: false, error: "No profile data provided" });

            await db.run(`
                INSERT INTO token_updates (mint, twitter, website, telegram, banner, submittedAt, status, signature, payer) 
                VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8)
            `, [mint, twitter, website, telegram, banner, Date.now(), signature, userPublicKey]);

            res.json({ success: true, message: "Update queued successfully. Payment verified." });
        } catch (e) { 
            console.error(e);
            res.status(500).json({ success: false, error: "Submission failed: " + e.message }); 
        }
    });

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

                if (search && search.trim().length > 0) {
                    if (isValidPubkey(search.trim())) {
                        whereClauses.push(`mint = $${params.length + 1}`);
                        params.push(search.trim());
                    } else {
                        whereClauses.push(`(ticker ILIKE $${params.length + 1} OR name ILIKE $${params.length + 1})`);
                        params.push(`%${search.trim()}%`);
                    }
                }

                if (filter === 'verified') whereClauses.push(`hasCommunityUpdate = TRUE`);
                if (whereClauses.length > 0) query += ` WHERE ${whereClauses.join(' AND ')}`;
                query += ` ${orderByClause} LIMIT ${limitVal} OFFSET ${offsetVal}`;

                let rows = params.length > 0 ? await db.all(query, params) : await db.all(query);

                const needsExternalFetch = (filter !== 'verified' && pageVal === 1 && search && search.trim().length > 2 && (rows.length < 5 || isValidPubkey(search.trim())));
                if (needsExternalFetch) {
                    try {
                        let dexUrl = `https://api.dexscreener.com/latest/dex/search?q=${encodeURIComponent(search)}`;
                        if (isValidPubkey(search.trim())) dexUrl = `https://api.dexscreener.com/latest/dex/tokens/${search.trim()}`;
                        const dexRes = await axios.get(dexUrl, { timeout: 4000 });
                        if (dexRes.data && dexRes.data.pairs) {
                            const validPairs = dexRes.data.pairs.filter(p => p.chainId === 'solana');
                            const pairsProcess = isValidPubkey(search.trim()) ? validPairs.slice(0, 1) : validPairs.slice(0, 5);
                            for (const pair of pairsProcess) {
                                if (rows.some(r => r.mint === pair.baseToken.address)) continue;
                                const metadata = {
                                    ticker: pair.baseToken.symbol, name: pair.baseToken.name, description: `Imported via Search: ${search}`,
                                    twitter: getSocialLink(pair, 'twitter'), website: pair.info?.websites?.[0]?.url || null, metadataUri: null, image: pair.info?.imageUrl,
                                    isMayhemMode: false, marketCap: pair.fdv || pair.marketCap || 0, volume24h: pair.volume?.h24 || 0, priceUsd: pair.priceUsd
                                };
                                const createdAt = pair.pairCreatedAt || Date.now();
                                await saveTokenData(null, pair.baseToken.address, metadata, createdAt);
                                rows.push({
                                    mint: pair.baseToken.address, userPubkey: null, name: metadata.name, ticker: metadata.ticker, image: metadata.image,
                                    marketCap: metadata.marketCap, volume24h: metadata.volume24h, priceUsd: metadata.priceUsd, timestamp: createdAt,
                                    change5m: pair.priceChange?.m5 || 0, change1h: pair.priceChange?.h1 || 0, change24h: pair.priceChange?.h24 || 0,
                                    complete: false, hasCommunityUpdate: false, k_score: 0
                                });
                            }
                        }
                    } catch (extErr) { console.error("External search failed:", extErr.message); }
                }

                if (pageVal === 1 && rows.length > 0) {
                     if (sort === 'newest' || sort === 'age') rows.sort((a, b) => parseInt(b.timestamp) - parseInt(a.timestamp));
                     else if (sort === 'mcap') rows.sort((a, b) => (b.marketcap || b.marketCap) - (a.marketcap || a.marketCap));
                     else if (sort === 'kscore') rows.sort((a, b) => (b.k_score || 0) - (a.k_score || 0));
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

    router.get('/token/:mint', async (req, res) => {
        try {
            const { mint } = req.params;
            const cacheKey = `api:token:${mint}`;
            const result = await smartCache(cacheKey, 30, async () => {
                const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
                if (!token) return null;
                let pairs = [];
                try {
                    const dexRes = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${mint}`, { timeout: 3000 });
                    if (dexRes.data && dexRes.data.pairs) {
                        pairs = dexRes.data.pairs.map(p => ({
                            pairAddress: p.pairAddress, dexId: p.dexId, priceUsd: p.priceUsd, liquidity: p.liquidity?.usd || 0, url: p.url
                        })).sort((a, b) => b.liquidity - a.liquidity);
                    }
                } catch (dexErr) { console.warn("Failed to fetch pairs:", dexErr.message); }
                return { 
                    success: true, 
                    token: {
                        ...token, marketCap: token.marketcap || 0, volume24h: token.volume24h || 0, priceUsd: token.priceusd || 0,
                        change1h: token.change1h || 0, change24h: token.change24h || 0, change5m: token.change5m || 0,
                        userPubkey: token.userpubkey, timestamp: parseInt(token.timestamp), twitter: token.twitter,
                        website: token.website, telegram: token.tweeturl, banner: token.banner,
                        hasCommunityUpdate: token.hascommunityupdate || false, kScore: token.k_score || 0, pairs: pairs 
                    } 
                };
            });
            if (!result) return res.status(404).json({ success: false, error: "Not found" });
            res.json(result);
        } catch (e) { res.status(500).json({ success: false, error: "DB Error" }); }
    });

    // --- ADMIN ENDPOINTS (Require Password) ---
    
    router.post('/admin/refresh-kscore', requireAdmin, async (req, res) => {
        const { mint } = req.body;
        try {
            const score = await calculateTokenScore(mint);
            res.json({ success: true, message: `K-Score Updated: ${score}` });
        } catch (e) {
            res.status(500).json({ success: false, error: e.message });
        }
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
            fields.push(`hasCommunityUpdate = $${idx++}`); params.push(true);
            if (fields.length > 0) { params.push(update.mint); await db.run(`UPDATE tokens SET ${fields.join(', ')} WHERE mint = $${idx}`, params); }
            await db.run('DELETE FROM token_updates WHERE id = $1', [id]);
            res.json({ success: true, message: 'Approved' });
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    router.post('/admin/reject-update', requireAdmin, async (req, res) => {
        const { id } = req.body;
        try { await db.run('DELETE FROM token_updates WHERE id = $1', [id]); res.json({ success: true, message: 'Rejected' });
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    router.get('/admin/token/:mint', requireAdmin, async (req, res) => {
        try {
            const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [req.params.mint]);
            if (!token) return res.status(404).json({ success: false, error: "Not Found" });
            res.json({ success: true, token: { mint: token.mint, ticker: token.ticker, twitter: token.twitter, website: token.website, telegram: token.tweeturl, banner: token.banner, kScore: token.k_score } });
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    router.post('/admin/update-token', requireAdmin, async (req, res) => {
        const { mint, twitter, website, telegram, banner } = req.body;
        try { await db.run(`UPDATE tokens SET twitter = $1, website = $2, tweetUrl = $3, banner = $4, hasCommunityUpdate = TRUE WHERE mint = $5`, [twitter, website, telegram, banner, mint]); res.json({ success: true, message: "Token Updated Successfully" });
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    router.post('/admin/delete-token', requireAdmin, async (req, res) => {
        const { mint } = req.body;
        try { await db.run('DELETE FROM tokens WHERE mint = $1', [mint]); res.json({ success: true, message: "Token Deleted Successfully" });
        } catch (e) { res.status(500).json({ success: false, error: e.message }); }
    });

    return router;
}

module.exports = { init };
