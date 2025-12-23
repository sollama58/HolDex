/**
 * Token Routes
 * Fixed to match Frontend API calls
 */
const express = require('express');
const { isValidPubkey } = require('../utils/solana');

const router = express.Router();

function init(deps) {
    const { db, globalState } = deps;

    // MAIN ENDPOINT: /api/tokens
    // Handles sorting: 'newest', 'mcap', 'gainers'
    router.get('/tokens', async (req, res) => {
        try {
            const { sort = 'newest', limit = 100, search = '' } = req.query;
            const limitVal = Math.min(parseInt(limit) || 100, 100);
            
            let orderByClause = 'ORDER BY timestamp DESC'; // Default: Newest

            if (sort === 'mcap') {
                orderByClause = 'ORDER BY marketCap DESC';
            } else if (sort === 'gainers') {
                orderByClause = 'ORDER BY change24h DESC';
            } else if (sort === 'volume') {
                orderByClause = 'ORDER BY volume24h DESC';
            }

            let query = `SELECT * FROM tokens`;
            let params = [];

            // Simple search logic
            if (search && search.trim().length > 0) {
                query += ` WHERE ticker ILIKE $1 OR name ILIKE $1 OR mint = $1`;
                params.push(`%${search}%`);
            }

            query += ` ${orderByClause} LIMIT ${limitVal}`; // Note: strictly use param binding for LIMIT in prod, but logic here is simple

            // If search is used, params has 1 element, otherwise 0.
            // PostgreSQL param binding requires careful index management if mixing dynamic strings.
            // For safety with simple string concat for ORDER BY (which can't be bound), we execute:
            
            let rows;
            if (params.length > 0) {
                // If using $1 for search
                 // We need to re-verify the query construction logic for the wrapper
                 // Assuming db.all handles standard PG queries
                 rows = await db.all(query, params);
            } else {
                 rows = await db.all(query);
            }
            
            const tokens = rows.map(r => ({
                mint: r.mint,
                userPubkey: r.userpubkey,
                name: r.name,
                ticker: r.ticker,
                image: r.image,
                metadataUri: r.metadatauri,
                marketCap: r.marketcap || 0,
                volume24h: r.volume24h || 0,
                priceUsd: r.priceusd || 0,
                timestamp: parseInt(r.timestamp),
                change5m: r.change5m || 0,
                change1h: r.change1h || 0,
                change24h: r.change24h || 0,
                complete: !!r.complete
            }));
            
            res.json({ success: true, tokens, lastUpdate: Date.now() });
        } catch (e) {
            console.error("Fetch Tokens Error:", e);
            res.status(500).json({ success: false, tokens: [], error: e.message });
        }
    });

    // Get single token
    router.get('/token/:mint', async (req, res) => {
        try {
            const { mint } = req.params;
            const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
            
            if (!token) return res.status(404).json({ success: false, error: "Not found" });

            res.json({ 
                success: true, 
                token: {
                    ...token,
                    marketCap: token.marketcap,
                    volume24h: token.volume24h,
                    priceUsd: token.priceusd,
                    change1h: token.change1h,
                    change24h: token.change24h
                } 
            });
        } catch (e) {
            res.status(500).json({ success: false, error: "DB Error" });
        }
    });

    // King of the Pill (KOTH)
    router.get('/koth', async (req, res) => {
        try {
            const koth = await db.get(`
                SELECT * FROM tokens 
                WHERE marketCap > 0 
                ORDER BY marketCap DESC 
                LIMIT 1
            `);
            
            if (koth) {
                res.json({ 
                    found: true,
                    token: koth
                });
            } else {
                res.json({ found: false });
            }
        } catch (e) {
            res.status(500).json({ error: "DB Error" });
        }
    });

    // Check holder status
    router.get('/check-holder', async (req, res) => {
        const { userPubkey } = req.query;
        if (!userPubkey || !isValidPubkey(userPubkey)) {
            return res.status(400).json({ isHolder: false, error: "Invalid address" });
        }

        try {
            // Get Top 10 Tokens by Volume
            const top10 = await db.all('SELECT mint FROM tokens ORDER BY volume24h DESC LIMIT 10');
            const top10Mints = top10.map(t => t.mint);

            let heldPositionsCount = 0;

            if (top10Mints.length > 0) {
                // Construct $1, $2, $3... for the IN clause
                // Param $1 is userPubkey, so mints start at $2
                const placeholders = top10Mints.map((_, i) => `$${i + 2}`).join(',');
                
                const query = `SELECT COUNT(*) as count FROM token_holders WHERE holderPubkey = $1 AND mint IN (${placeholders})`;
                const result = await db.get(query, [userPubkey, ...top10Mints]);
                heldPositionsCount = parseInt(result?.count || 0);
            }

            res.json({
                isHolder: heldPositionsCount > 0,
                heldPositionsCount,
                checkedAgainst: top10Mints.length
            });
        } catch (e) {
            console.error(e);
            res.status(500).json({ error: "DB Error" });
        }
    });

    return router;
}

module.exports = { init };
