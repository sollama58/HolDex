const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const { logger } = require('../services');

const solanaConnection = new Connection(config.SOLANA_RPC_URL, 'confirmed');

// Helper sleep function
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function getHolderCount(mintAddress) {
    try {
        // Strategy: Use getTokenLargestAccounts to check top 20 holders.
        const largestAccounts = await solanaConnection.getTokenLargestAccounts(new PublicKey(mintAddress));
        return largestAccounts.value || [];
    } catch (e) {
        return null;
    }
}

/**
 * Shared Scoring Logic
 * Used by both the batch updater and the single-token API
 */
async function computeScoreInternal(mint, dbData = null) {
    let score = 0;
    
    try {
        // 1. Verification / Community Update (Max 50 pts)
        // If we have DB data, check verification status
        if (dbData && (dbData.hasCommunityUpdate || dbData.hascommunityupdate)) {
            score += 50;
        } else {
            // If no DB data passed, we assume base score or 0
            // (The API route calls this, usually without knowing if it's verified yet unless passed)
            // We give a small "Discovery" points base
            score += 10;
        }

        // 2. Volume (Max 20 pts)
        if (dbData) {
            const vol = dbData.volume24h || 0;
            if (vol > 100000) score += 20;
            else if (vol > 10000) score += 10;
        }

        // 3. Holder Analysis (RPC) - Max 20 pts
        // We only do this if it's a critical update, as it eats RPC credits
        const holders = await getHolderCount(mint);
        if (holders && holders.length > 0) {
            // Basic check: If we can fetch holders, the chain data is alive
            score += 20; 
            
            // Advanced: Check concentration (placeholder logic for future expansion)
            // const topHolder = holders[0];
            // if (topHolder.uiAmount > supply * 0.5) score -= 10; 
        }

        // 4. Age/Market Cap Boost (Max 10 pts)
        if (dbData) {
             const mcap = dbData.marketCap || dbData.marketcap || 0;
             if (mcap > 100000) score += 10;
        }

        return Math.min(score, 100);
        
    } catch (e) {
        console.error(`Score Calc Error ${mint}:`, e.message);
        return 10; // Default low score on error
    }
}

/**
 * Updates a single token's score immediately in the DB.
 * Used by Admin Approval route.
 */
async function updateSingleToken(deps, mint) {
    const { db } = deps;
    try {
        const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
        if (!token) return;

        logger.info(`⚡ Immediate K-Score Calc triggered for ${token.ticker}`);
        const score = await computeScoreInternal(mint, token);

        await db.run(`
            UPDATE tokens 
            SET k_score = $1, last_k_calc = $2 
            WHERE mint = $3
        `, [score, Date.now(), mint]);
        
        return score;
    } catch (e) {
        logger.error(`Failed single update for ${mint}:`, e);
        return 0;
    }
}

async function updateKScores(deps) {
    const { db } = deps;
    
    logger.info("虫 K-Score Updater: Starting cycle...");

    try {
        const tokens = await db.all(`
            SELECT * FROM tokens 
            WHERE hasCommunityUpdate = 1 
            OR volume24h > 5000
        `);

        if (!tokens || tokens.length === 0) {
            logger.info("虫 K-Score: No eligible tokens found.");
            return;
        }

        logger.info(`虫 K-Score: Updating ${tokens.length} tokens...`);

        for (const t of tokens) {
            try {
                // Use the shared scoring logic
                const score = await computeScoreInternal(t.mint, t);

                // Update DB
                await db.run(`
                    UPDATE tokens 
                    SET k_score = $1, last_k_calc = $2 
                    WHERE mint = $3
                `, [score, Date.now(), t.mint]);

            } catch (err) {
                console.warn(`Failed K-Score for ${t.mint}: ${err.message}`);
            }
            
            await sleep(50); 
        }
        
        logger.info("虫 K-Score Updater: Cycle complete.");

    } catch (e) {
        logger.error("K-Score Cycle Error", e);
    }
}

function start(deps) {
    // Run every 10 minutes
    setInterval(() => updateKScores(deps), 600000);
    // Run once immediately after startup (delay 10s)
    setTimeout(() => updateKScores(deps), 10000);
}

module.exports = { 
    start, 
    updateSingleToken, // Exported for Routes
    calculateTokenScore: async (mint) => {
        return await computeScoreInternal(mint, null);
    }
};
