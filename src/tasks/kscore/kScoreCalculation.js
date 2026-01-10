/**
 * K-Score Calculation Module
 *
 * Core K-Score algorithm (v10):
 * K = 100 × ∛(D × O × L)
 *
 * Where:
 * - D (Diamond Hands) = √(C × R × F) - Conviction from holder behavior
 * - O (Organic Growth) = √(H × T) - Distribution quality
 * - L (Longevity) = A × S - Survival factor
 *
 * This module contains the normalization functions and core scoring logic.
 */

const logger = require('../../services/logger');

// ============================================
// CONSTANTS
// ============================================

// EMA smoothing factor for score stability
const EMA_ALPHA = 0.3;

// Score thresholds
const SCORE_MIN = 0;
const SCORE_MAX = 100;

// ============================================
// NORMALIZATION FUNCTIONS
// ============================================

/**
 * Normalize holder count using sigmoid function
 * Higher holder count → higher score (asymptotic to 1)
 *
 * @param {number} holders - Total holder count
 * @param {number} kappa - Scaling factor (default 100)
 * @returns {number} Normalized value 0-1
 */
function normalizeHolders(holders, kappa = 100) {
    if (!holders || holders <= 0) return 0;
    // Sigmoid: H / (H + κ)
    // 100 holders = 0.5, 1000 holders = 0.91
    return holders / (holders + kappa);
}

/**
 * Normalize token age using exponential decay
 * Older tokens get higher scores (asymptotic to 1)
 *
 * @param {number} ageDays - Age in days
 * @param {number} tau - Half-life parameter (default 21 days)
 * @returns {number} Normalized value 0-1
 */
function normalizeAge(ageDays, tau = 21) {
    if (!ageDays || ageDays <= 0) return 0;
    // Exponential: 1 - e^(-age/τ)
    // 21 days = 0.63, 42 days = 0.86, 90 days = 0.99
    return 1 - Math.exp(-ageDays / tau);
}

/**
 * Normalize top 20% concentration
 * Lower concentration = higher score (better distribution)
 *
 * @param {number} concentrationPct - Top 20 holders' percentage of supply
 * @returns {number} Normalized value 0-1
 */
function normalizeTop20(concentrationPct) {
    if (concentrationPct === undefined || concentrationPct === null) return 0.5;
    // Invert: high concentration = low score
    // 100% concentration = 0, 0% concentration = 1
    const clamped = Math.max(0, Math.min(100, concentrationPct));
    return 1 - (clamped / 100);
}

/**
 * Normalize accumulator/extractor ratio
 * More accumulators than extractors = higher score
 *
 * @param {number} ratio - accumulators / (accumulators + extractors)
 * @returns {number} Normalized value 0-1
 */
function normalizeAccExtRatio(ratio) {
    if (!ratio || ratio < 0) return 0.5;
    return Math.min(1, Math.max(0, ratio));
}

/**
 * Normalize conviction score
 * Maps raw conviction to 0-1 range
 *
 * @param {number} score - Raw conviction score (0-100)
 * @returns {number} Normalized value 0-1
 */
function normalizeConviction(score) {
    if (!score || score <= 0) return 0;
    // Already on 0-100 scale, convert to 0-1
    return Math.min(1, Math.max(0, score / 100));
}

/**
 * Normalize activity freshness
 * Recent activity = higher score
 *
 * @param {number} activityDays - Days since last activity
 * @param {number} tau - Decay parameter (default 21 days)
 * @returns {number} Normalized value 0-1
 */
function normalizeActivityFreshness(activityDays, tau = 21) {
    if (activityDays === undefined || activityDays === null) return 0.5;
    if (activityDays <= 0) return 1; // Very recent activity
    // Exponential decay: e^(-days/τ)
    return Math.exp(-activityDays / tau);
}

/**
 * Normalize survival factor
 * Tokens that survive longer get higher scores
 *
 * @param {number} activityDays - Days since last activity
 * @param {number} tau - Threshold for "dead" token (default 30 days)
 * @returns {number} Normalized value 0-1
 */
function normalizeSurvival(activityDays, tau = 30) {
    if (activityDays === undefined || activityDays === null) return 0.5;
    // If no activity in tau days, considered "dead"
    if (activityDays > tau) return 0.1; // Not zero, but heavily penalized
    return 1 - (activityDays / tau) * 0.5; // Gradual decline
}

/**
 * Geometric mean of two values
 * Used for combining scores fairly
 *
 * @param {number} a - First value
 * @param {number} b - Second value
 * @param {number} epsilon - Small value to prevent zero (default 0.001)
 * @returns {number} Geometric mean
 */
function geometricMean2(a, b, epsilon = 0.001) {
    const safeA = Math.max(epsilon, a);
    const safeB = Math.max(epsilon, b);
    return Math.sqrt(safeA * safeB);
}

/**
 * Geometric mean of three values (cube root)
 *
 * @param {number} a - First value
 * @param {number} b - Second value
 * @param {number} c - Third value
 * @param {number} epsilon - Small value to prevent zero
 * @returns {number} Cubic root of product
 */
function geometricMean3(a, b, c, epsilon = 0.001) {
    const safeA = Math.max(epsilon, a);
    const safeB = Math.max(epsilon, b);
    const safeC = Math.max(epsilon, c);
    return Math.cbrt(safeA * safeB * safeC);
}

/**
 * Apply Exponential Moving Average smoothing
 * Prevents wild score swings between updates
 *
 * @param {number} calculated - Newly calculated score
 * @param {number} previous - Previous score
 * @param {number} alpha - Smoothing factor (default 0.3)
 * @returns {number} Smoothed score
 */
function applyEMA(calculated, previous, alpha = EMA_ALPHA) {
    if (!previous || previous === 0) return calculated;
    // EMA: new = α × calculated + (1-α) × previous
    return alpha * calculated + (1 - alpha) * previous;
}

/**
 * Calculate Diamond Hands score (D)
 * Measures conviction strength from holder behavior
 *
 * D = √(C × R × F)
 * - C = Conviction (accumulators + holders in top 20)
 * - R = Accumulator/Extractor ratio
 * - F = Activity freshness
 *
 * @param {Object} conviction - Conviction analysis results
 * @returns {number} Diamond hands score 0-1
 */
function calculateDiamondHands(conviction) {
    if (!conviction) return 0;

    const C = normalizeConviction(conviction.score || 0);
    const R = normalizeAccExtRatio(conviction.accExtRatio || 0.5);
    const F = normalizeActivityFreshness(conviction.daysSinceActivity || 0);

    return geometricMean3(C, R, F);
}

/**
 * Calculate Organic Growth score (O)
 * Measures distribution quality
 *
 * O = √(H × T)
 * - H = Holder count (normalized)
 * - T = Distribution quality (1 - top20 concentration)
 *
 * @param {number} holders - Total holder count
 * @param {number} top20Pct - Top 20 holders' percentage
 * @returns {number} Organic growth score 0-1
 */
function calculateOrganicGrowth(holders, top20Pct) {
    const H = normalizeHolders(holders || 0);
    const T = normalizeTop20(top20Pct || 50);

    return geometricMean2(H, T);
}

/**
 * Calculate Longevity score (L)
 * Measures survival factor over time
 *
 * L = A × S
 * - A = Age factor (normalized)
 * - S = Survival factor (recent activity)
 *
 * @param {number} ageDays - Token age in days
 * @param {number} daysSinceActivity - Days since last activity
 * @returns {number} Longevity score 0-1
 */
function calculateLongevity(ageDays, daysSinceActivity) {
    const A = normalizeAge(ageDays || 0);
    const S = normalizeSurvival(daysSinceActivity || 0);

    return A * S;
}

/**
 * Calculate final K-Score
 *
 * K = 100 × ∛(D × O × L)
 *
 * @param {Object} params - Score parameters
 * @param {Object} params.conviction - Conviction analysis results
 * @param {number} params.holders - Total holder count
 * @param {number} params.top20Pct - Top 20 concentration percentage
 * @param {number} params.ageDays - Token age in days
 * @param {number} params.daysSinceActivity - Days since last activity
 * @returns {Object} { score, breakdown }
 */
function calculateKScore(params) {
    const {
        conviction = {},
        holders = 0,
        top20Pct = 50,
        ageDays = 0,
        daysSinceActivity = 0
    } = params;

    // Calculate component scores
    const D = calculateDiamondHands(conviction);
    const O = calculateOrganicGrowth(holders, top20Pct);
    const L = calculateLongevity(ageDays, daysSinceActivity);

    // Final K-Score: 100 × ∛(D × O × L)
    const rawScore = 100 * geometricMean3(D, O, L);

    // Clamp to valid range
    const score = Math.round(Math.max(SCORE_MIN, Math.min(SCORE_MAX, rawScore)));

    return {
        score,
        breakdown: {
            diamond: Math.round(D * 100) / 100,
            organic: Math.round(O * 100) / 100,
            longevity: Math.round(L * 100) / 100
        }
    };
}

module.exports = {
    // Core calculation
    calculateKScore,
    applyEMA,

    // Component calculations
    calculateDiamondHands,
    calculateOrganicGrowth,
    calculateLongevity,

    // Normalization functions
    normalizeHolders,
    normalizeAge,
    normalizeTop20,
    normalizeAccExtRatio,
    normalizeConviction,
    normalizeActivityFreshness,
    normalizeSurvival,

    // Utilities
    geometricMean2,
    geometricMean3,

    // Constants
    EMA_ALPHA,
    SCORE_MIN,
    SCORE_MAX
};
