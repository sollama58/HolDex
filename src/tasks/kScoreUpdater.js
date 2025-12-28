const { analyzeTokenHolders } = require('../services/solana');
const logger = require('../services/logger');

/**
 * PURE FUNCTION: Calculates K-Score (Diamond Hands + Longevity)
 * * Logic:
 * 0. PREREQUISITE: Community Update (Eligibility Filter only).
 * 1. Hold Time: DOMINANT FACTOR (Log Scale). Rewards conviction of top holders.
 * 2. Trend: Rewards viral growth.
 * 3. Age: NEW FACTOR (Log Scale). Rewards token longevity.
 */
async function calculateDeepScore(db, token) {
    // --- 0. ELIGIBILITY CHECK ---
    // We only calculate scores for updated tokens to save RPC resources.
    const hasUpdate = token.hascommunityupdate === true || token.hasCommunityUpdate === true;

    if (!hasUpdate) {
        // If not verified, score is strictly 0.
        // This overwrites any previous score the token might have had.
        return 0; 
    }

    // --- 1. RESET SCORE ---
    // We start at 0. We do NOT look at token.k_score.
    // This ensures we are SETTING a fresh score, not accumulating.
    let score = 0;
    
    const now = Date.now();
    const vol = parseFloat(token.volume24h || 0);

    // Calculate Token Age
    const createdAt = parseInt(token.timestamp) || now;
    const ageMs = Math.max(0, now - createdAt);
    const ageHours = ageMs / (1000 * 60 * 60);

    // Get LP Addresses to exclude from "Holder" analysis
    const pools = await db.all(`SELECT address, reserve_a, reserve_b FROM pools WHERE mint = $1`, [token.mint]);
    const excludeList = [];
    pools.forEach(p => {
        if (p.address) excludeList.push(p.address);
        if (p.reserve_a) excludeList.push(p.reserve_a);
        if (p.reserve_b) excludeList.push(p.reserve_b);
    });

    // --- 2. DIAMOND HANDS (Heavy Analysis) ---
    let avgHoldHours = 0;
    if (excludeList.length > 0) {
        const analysis = await analyzeTokenHolders(token.mint, excludeList);
        avgHoldHours = analysis.avgHoldHours || 0;
    }

    // --- 3. VIRALITY (Trend) ---
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

    // --- SCORING CALCULATION ---
    let logMsg = `K-Score [${token.symbol}]:`;

    // A. DIAMOND HANDS (Log Scale) - Max ~55 pts (DOMINANT)
    // Formula: 20 * log10(hours + 1)
    // 1h -> 6 pts | 24h -> 28 pts | 1 week -> 44 pts | 1 month -> 57 pts
    const holdScore = 20 * Math.log10(avgHoldHours + 1);
    const cappedHold = Math.min(Math.max(holdScore, 0), 55);
    score += cappedHold;
    logMsg += ` Hold(${avgHoldHours.toFixed(1)}h->+${cappedHold.toFixed(1)})`;

    // B. TOKEN AGE (Log Scale) - Max ~30 pts
    // Formula: 10 * log10(hours + 1)
    // 24h -> 14 pts | 1 week -> 22 pts | 1 month -> 28 pts
    const ageScore = 10 * Math.log10(ageHours + 1);
    const cappedAge = Math.min(Math.max(ageScore, 0), 30);
    score += cappedAge;
    logMsg += ` Age(${ageHours.toFixed(1)}h->+${cappedAge.toFixed(1)})`;

    // C. VIRALITY (Trend) - Max 20 pts
    if (holderGrowthPct > 50) { score += 20; logMsg += ' Trend(>50%->+20)'; }
    else if (holderGrowthPct > 20) { score += 15; logMsg += ' Trend(>20%->+15)'; }
    else if (holderGrowthPct > 5) { score += 5; logMsg += ' Trend(>5%->+5)'; }
    else if (holderGrowthPct < -10) { score -= 15; logMsg += ' Trend(Dump->-15)'; }
    else { logMsg += ' Trend(Stable->0)'; }

    // D. PENALTIES
    // Bot volume detection: Huge volume but zero hold time
    if (vol > 1000000 && avgHoldHours < 1) {
        score -= 40;
        logMsg += ' BotPenalty(-40)';
    }

    // Final Clamp 0-99
    const finalScore = Math.min(Math.max(Math.floor(score), 0), 99);
    
    logger.info(`${logMsg} = ${finalScore}`);

    return finalScore;
}

// Helper for Admin API
async function updateSingleToken(deps, mint) {
    const { db } = deps;
    try {
        const token = await db.get(`SELECT * FROM tokens WHERE mint = $1`, [mint]);
        if (!token) throw new Error("Token not found");

        const score = await calculateDeepScore(db, token);

        // SQL: "SET k_score = $1" ensures we overwrite, not add.
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
