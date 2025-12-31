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

    // 2. Heavy Analysis (RPC Call) - Hold time + Conviction
    let avgHoldHours = 0;
    let conviction = { score: 0, accumulators: 0, holders: 0, reducers: 0, extractors: 0, analyzed: 0 };

    const analysis = await analyzeTokenHolders(token.mint, excludeList);
    avgHoldHours = analysis.avgHoldHours || 0;
    conviction = analysis.conviction || conviction;

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

    // --- SCORING BREAKDOWN (Rebalanced with Conviction) ---
    // A. CONVICTION (35 pts) - Top 20 holders' behavior
    // B. HOLD TIME (25 pts) - Average hold duration
    // C. AGE (10 pts) - Token survival
    // D. ACTIVITY (15 pts) - Volume & Liquidity
    // E. TREND (15 pts) - Holder growth
    // Total: 100 pts max

    let logMsg = `K-Score [${token.symbol}]:`;

    // A. CONVICTION (Top 20 Holders) - Max 35 pts
    // conviction.score = % of accumulators + holders (diamond hands)
    const convictionPts = (conviction.score / 100) * 35;
    score += convictionPts;
    logMsg += ` Conv(${conviction.score}%->+${convictionPts.toFixed(1)})`;
    if (conviction.analyzed > 0) {
        logMsg += `[${conviction.accumulators}A/${conviction.holders}H/${conviction.reducers}R/${conviction.extractors}E]`;
    }

    // B. HOLD TIME (Log Scale) - Max 25 pts
    // 1 week (168h) -> ~15 pts, 1 month (720h) -> ~21 pts
    const holdScore = 11 * Math.log10(avgHoldHours + 1);
    const cappedHold = Math.min(Math.max(holdScore, 0), 25);
    score += cappedHold;
    logMsg += ` Hold(${avgHoldHours.toFixed(0)}h->+${cappedHold.toFixed(1)})`;

    // C. TOKEN AGE (Log Scale) - Max 10 pts
    const ageScore = 5 * Math.log10(ageHours + 1);
    const cappedAge = Math.min(Math.max(ageScore, 0), 10);
    score += cappedAge;
    logMsg += ` Age(${(ageHours/24).toFixed(0)}d->+${cappedAge.toFixed(1)})`;

    // D. ACTIVITY (Volume & Liquidity) - Max 15 pts
    let activityScore = 0;
    if (vol > 100000) activityScore += 7;
    else if (vol > 10000) activityScore += 4;

    if (liq > 100000) activityScore += 8;
    else if (liq > 20000) activityScore += 4;

    score += activityScore;
    logMsg += ` Active(+${activityScore})`;

    // E. TREND (Holder Growth) - Max 15 pts
    if (holderGrowthPct > 20) { score += 15; logMsg += ' Trend(+15)'; }
    else if (holderGrowthPct > 5) { score += 8; logMsg += ' Trend(+8)'; }
    else if (holderGrowthPct > 0) { score += 2; logMsg += ' Trend(+2)'; }
    else if (holderGrowthPct < -10) { score -= 10; logMsg += ' Trend(-10)'; }

    // F. PENALTIES
    if (vol > 1000000 && avgHoldHours < 1) {
        score -= 40;
        logMsg += ' BotPenalty(-40)';
    }

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
