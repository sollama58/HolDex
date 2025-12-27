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
    // If checking casing, 'pg' driver usually returns lowercase column names.
    const hasUpdate = token.hascommunityupdate === true || token.hasCommunityUpdate === true;

    if (!hasUpdate) {
        // Not eligible for a score calculation. Return base skepticism.
        // We return 10 (instead of 0) to avoid "broken" looking UI, but it's a low score.
        return 10; 
    }

    let score = 10; // Base Score
    const now = Date.now();

    // 1. Get LP Addresses to exclude from "Holder" analysis
    // We don't want to calculate the 'hold time' of the Raydium Pool itself.
    const pools = await db.all(`SELECT address, reserve_a, reserve_b FROM pools WHERE mint = $1`, [token.mint]);
    const excludeList = [];
    pools.forEach(p => {
        if (p.address) excludeList.push(p.address);
        if (p.reserve_a) excludeList.push(p.reserve_a);
        if (p.reserve_b) excludeList.push(p.reserve_b);
    });

    // 2. Heavy Analysis (RPC Call)
    // Only run if we have valid pools to exclude (implies token is somewhat valid)
    let avgHoldHours = 0;
    if (excludeList.length > 0) {
        // analyzeTokenHolders checks the ATAs, so filtering is handled by the account type.
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

    // Trend Logic:
    // - If we have history > 0, calculate growth.
    // - If history is missing, assume 0% growth (Neutral).
    if (historyRow && historyRow.count > 0 && token.holders > 0) {
        holderGrowthPct = ((token.holders - historyRow.count) / historyRow.count) * 100;
    }

    // --- SCORING RULES ---
    
    // A. Hold Time (The "Diamond Hand" Factor)
    if (avgHoldHours > 168) score += 40;     // > 1 Week
    else if (avgHoldHours > 72) score += 30; // > 3 Days
    else if (avgHoldHours > 24) score += 20; // > 1 Day
    else if (avgHoldHours > 6) score += 10;  // > 6 Hours
    else if (avgHoldHours < 1) score -= 5;   // < 1 Hour (Flipper/Bot)

    // B. Trend (The "Viral" Factor)
    if (holderGrowthPct > 20) score += 30;     
    else if (holderGrowthPct > 5) score += 20; 
    else if (holderGrowthPct > 0) score += 5;  
    else if (holderGrowthPct === 0 && token.holders > 100) score += 5; 
    else if (holderGrowthPct < -5) score -= 15;

    // C. Liquidity / Volume Sanity
    if (token.liquidity > 50000) score += 10;
    else if (token.liquidity < 1000) score -= 20;

    // Wash Trading Penalty: High Volume + Zero Hold Time = Bot
    if (token.volume24h > 1000000 && avgHoldHours < 1) score -= 20;

    // D. Community Verification (Guaranteed true if we got here, but adding bonus)
    score += 15;

    // Final Clamp 1-99
    return Math.min(Math.max(Math.floor(score), 1), 99);
}

module.exports = { calculateDeepScore };
