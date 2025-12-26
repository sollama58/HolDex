const { logger } = require('../services');

const INTERVAL_MS = 6 * 60 * 60 * 1000; // 6 Hours

async function updateKScore(deps) {
    const { db } = deps;
    logger.info("🧠 K-Score Updater: Starting Cycle...");

    try {
        // FIX: Replaced "hasCommunityUpdate = 1" with "hasCommunityUpdate = TRUE"
        // In PostgreSQL, booleans must be compared with boolean literals
        const tokens = await db.all(`
            SELECT * FROM tokens 
            WHERE hasCommunityUpdate = TRUE 
            OR volume24h > 5000
        `);

        if (!tokens || tokens.length === 0) {
            logger.info("🧠 K-Score: No eligible tokens found.");
            return;
        }

        logger.info(`🧠 K-Score: Analyzing ${tokens.length} tokens...`);

        for (const token of tokens) {
            try {
                // Simple heuristic score calculation since Helius dependency was removed/simplified
                let score = 50; // Base Score

                // Volume Boost
                if (token.volume24h > 100000) score += 20;
                else if (token.volume24h > 10000) score += 10;

                // Liquidity Boost
                if (token.liquidity > 50000) score += 20;
                else if (token.liquidity > 5000) score += 10;

                // Community Update Boost
                // FIX: Check for boolean true, not integer 1
                if (token.hascommunityupdate === true || token.hasCommunityUpdate === true) score += 10;

                // Cap at 99
                score = Math.min(score, 99);

                await db.run(
                    `UPDATE tokens SET k_score = $1 WHERE mint = $2`, 
                    [score, token.mint]
                );
            } catch (err) {
                logger.warn(`Failed to update K-Score for ${token.mint}: ${err.message}`);
            }
        }
        
        logger.info("🧠 K-Score Updater: Cycle Complete.");

    } catch (err) {
        logger.error(`K-Score Cycle Error: ${err.message}`, { stack: err.stack });
    }
}

async function updateSingleToken(deps, mint) {
    // Helper for immediate updates (e.g. via Admin API)
    const { db } = deps;
    try {
        // Recalculate based on current DB stats
        const token = await db.get(`SELECT * FROM tokens WHERE mint = $1`, [mint]);
        if (!token) return 0;

        let score = 50;
        if (token.volume24h > 100000) score += 20;
        else if (token.volume24h > 10000) score += 10;

        if (token.liquidity > 50000) score += 20;
        else if (token.liquidity > 5000) score += 10;

        if (token.hascommunityupdate === true) score += 10;

        score = Math.min(score, 99);

        await db.run(`UPDATE tokens SET k_score = $1 WHERE mint = $2`, [score, mint]);
        return score;
    } catch (e) {
        return 0;
    }
}

function start(deps) {
    updateKScore(deps); // Run immediately on start
    setInterval(() => updateKScore(deps), INTERVAL_MS);
}

module.exports = { start, updateSingleToken };
