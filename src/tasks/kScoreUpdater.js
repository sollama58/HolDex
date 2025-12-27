const { analyzeTokenHolders } = require('../services/solana');

/**
 * PURE FUNCTION: Calculates K-Score
 * * Logic:
 * 0. PREREQUISITE: Token MUST have a community update.
 * 1. Volume & Liquidity Baseline
 * 2. Deep Analysis: Top 20 Holder behavior (Hold Time)
 * 3. Trend Analysis: Holder count growth over 24h
 */
async function calculateDeepScore(db, token) {
    // --- 0. ELIGIBILITY CHECK ---
    const hasUpdate = token.hascommunityupdate === true || token.hasCommunityUpdate === true;

    if (!hasUpdate) {
        // Not eligible for a score calculation. Return base skepticism.
        return 10; 
    }

    let score = 10; // Base Score
    const now = Date.now();

    // 1. Get LP Addresses to exclude from "Holder" analysis
    const pools = await db.all(`SELECT address, reserve_a, reserve_b FROM pools WHERE mint = $1`, [token.mint]);
    const excludeList = [];
    pools.forEach(p => {
        if (p.address) excludeList.push(p.address);
        if (p.reserve_a) excludeList.push(p.reserve_a);
        if (p.reserve_b) excludeList.push(p.reserve_b);
    });

    // 2. Heavy Analysis (RPC Call)
    let avgHoldHours = 0;
    if (excludeList.length > 0) {
        const analysis = await analyzeTokenHolders(token.mint, excludeList);
        avgHoldHours = analysis.avgHoldHours || 0;
    }

    // 3. Holder Trend (SQL Only)
    let holderGrowthPct = 0;
    const yesterday = now - (24 * 60 * 60 * 1000);
    const historyRow = await db.get(`
        SELECT count FROM holders_history 
        WHERE mint = $1 AND timestamp <= $2 
        ORDER BY timestamp DESC LIMIT 1
    `, [token.mint, yesterday]);

    if (historyRow && historyRow.count > 0 && token.holders > 0) {
        holderGrowthPct = ((token.holders - historyRow.count) / historyRow.count) * 100;
    }

    // --- SCORING RULES ---
    
    // A. Hold Time
    if (avgHoldHours > 168) score += 40;     
    else if (avgHoldHours > 72) score += 30; 
    else if (avgHoldHours > 24) score += 20; 
    else if (avgHoldHours > 6) score += 10;  
    else if (avgHoldHours < 1) score -= 5;   

    // B. Trend
    if (holderGrowthPct > 20) score += 30;     
    else if (holderGrowthPct > 5) score += 20; 
    else if (holderGrowthPct > 0) score += 5;  
    else if (holderGrowthPct === 0 && token.holders > 100) score += 5; 
    else if (holderGrowthPct < -5) score -= 15;

    // C. Liquidity / Volume
    if (token.liquidity > 50000) score += 10;
    else if (token.liquidity < 1000) score -= 20;

    if (token.volume24h > 1000000 && avgHoldHours < 1) score -= 20;

    // D. Community (Bonus)
    score += 15;

    return Math.min(Math.max(Math.floor(score), 1), 99);
}

/**
 * Helper function for API Routes to manually refresh a single token
 */
async function updateSingleToken(deps, mint) {
    const { db } = deps;
    try {
        const token = await db.get(`SELECT * FROM tokens WHERE mint = $1`, [mint]);
        if (!token) throw new Error("Token not found");

        // Perform calculation
        const score = await calculateDeepScore(db, token);

        await db.run(`
            UPDATE tokens 
            SET k_score = $1, last_k_score_update = $2 
            WHERE mint = $3
        `, [score, Date.now(), mint]);
        
        return score;
    } catch (e) {
        // Allow the route to catch the error
        throw e;
    }
}

module.exports = { calculateDeepScore, updateSingleToken };
