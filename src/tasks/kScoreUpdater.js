const { analyzeTokenHolders } = require('../services/solana');
const logger = require('../services/logger');

/**
 * PURE FUNCTION: Calculates K-Score (Balanced for Differentiation)
 * * Logic:
 * 0. PREREQUISITE: Community Update (Eligibility Filter only).
 * 1. Hold Time (45%): Harder scale. 1 Month = ~40pts. 3 Months = 45pts.
 * 2. Age (15%): Reduced impact. Mostly a baseline trust metric.
 * 3. Volume/Liquidity Health (20%): NEW. Penalizes dormant tokens.
 * 4. Trend (20%): Rewards active growth.
 */
async function calculateDeepScore(db, token) {
    // --- 0. ELIGIBILITY CHECK ---
    const hasUpdate = token.hascommunityupdate === true || token.hasCommunityUpdate === true;

    if (!hasUpdate) {
        return 0; 
    }

    let score = 0;
    
    const now = Date.now();
    const vol = parseFloat(token.volume24h || 0);
    const liq = parseFloat(token.liquidity || 0);

    // Calculate Token Age
    const createdAt = parseInt(token.timestamp) || now;
    const ageMs = Math.max(0, now - createdAt);
    const ageHours = ageMs / (1000 * 60 * 60);

    // 1. Get LP Addresses to exclude
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

    // --- SCORING BREAKDOWN ---
    let logMsg = `K-Score [${token.symbol}]:`;

    // A. DIAMOND HANDS (Log Scale) - Max 45 pts
    // Previous: 20 * log10(hours+1) -> Too easy.
    // New: 15 * log10(hours+1)
    // 1 week (168h) -> 15 * 2.22 = 33 pts
    // 1 month (720h) -> 15 * 2.85 = 42 pts
    // 6 months (4380h) -> 15 * 3.6 = 54 (Capped at 45)
    const holdScore = 15 * Math.log10(avgHoldHours + 1);
    const cappedHold = Math.min(Math.max(holdScore, 0), 45);
    score += cappedHold;
    logMsg += ` Hold(${avgHoldHours.toFixed(1)}h->+${cappedHold.toFixed(1)})`;

    // B. TOKEN AGE (Log Scale) - Max 15 pts (Reduced from 30)
    // Survival is good, but not the only metric.
    // 1 week -> 16 pts (Capped at 15)
    // Basically, if it survives a week, it gets the full "Age" bonus.
    const ageScore = 7 * Math.log10(ageHours + 1);
    const cappedAge = Math.min(Math.max(ageScore, 0), 15);
    score += cappedAge;
    logMsg += ` Age(${ageHours.toFixed(1)}h->+${cappedAge.toFixed(1)})`;

    // C. ACTIVITY (Volume & Liquidity) - Max 20 pts (NEW)
    // Differentiates "Old & Dead" from "Old & Active".
    // Dormant tokens (low vol) will miss these points, dropping them from 80s to 60s.
    let activityScore = 0;
    if (vol > 100000) activityScore += 10;
    else if (vol > 10000) activityScore += 5;
    
    if (liq > 100000) activityScore += 10;
    else if (liq > 20000) activityScore += 5;
    
    score += activityScore;
    logMsg += ` Active(Vol/Liq->+${activityScore})`;

    // D. VIRALITY (Trend) - Max 20 pts
    if (holderGrowthPct > 20) { score += 20; logMsg += ' Trend(>20%->+20)'; }
    else if (holderGrowthPct > 5) { score += 10; logMsg += ' Trend(>5%->+10)'; }
    else if (holderGrowthPct > 0) { score += 2; logMsg += ' Trend(>0%->+2)'; }
    else if (holderGrowthPct < -10) { score -= 10; logMsg += ' Trend(Dump->-10)'; }
    
    // E. PENALTIES
    // Bot volume detection
    if (vol > 1000000 && avgHoldHours < 1) {
        score -= 40;
        logMsg += ' BotPenalty(-40)';
    }
    
    // Removed Zombie Penalty as requested - old tokens with diamond hands should score well.

    // Final Clamp 0-99
    const finalScore = Math.min(Math.max(Math.floor(score), 0), 99);
    
    logger.info(`${logMsg} = ${finalScore}`);

    return finalScore;
}

async function updateSingleToken(deps, mint) {
    const { db } = deps;
    try {
        const token = await db.get(`SELECT * FROM tokens WHERE mint = $1`, [mint]);
        if (!token) throw new Error("Token not found");

        const score = await calculateDeepScore(db, token);

        await db.run(`
            UPDATE tokens 
            SET k_score = $1, last_k_score_update = $2 
            WHERE mint = $3
        `, [score, Date.now(), mint]);
        
        return score;
    } catch (e) {
        throw e;
    }
}

module.exports = { calculateDeepScore, updateSingleToken };
