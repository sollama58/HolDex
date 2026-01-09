/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * HARMONY MODULE v2.0 - $ASDFASDFA ECOSYSTEM ECONOMIC ENGINE
 * ═══════════════════════════════════════════════════════════════════════════════
 *
 * "Don't Trust, Verify. Don't Extract, Burn."
 *
 * This is the SINGLE SOURCE OF TRUTH for all φ-based calculations across
 * the entire $asdfasdfa ecosystem: HolDex, GASdf, Brain, and future projects.
 *
 * USAGE:
 *   const harmony = require('./shared/harmony');
 *   const discount = harmony.calculateDiscount(eScore);
 *   const distribution = harmony.distributeFee(totalFee);
 *
 * DESIGN PRINCIPLES:
 *   1. PURE - No side effects, no external dependencies
 *   2. IMMUTABLE - All constants are frozen
 *   3. UNIVERSAL - Works in Node.js, browser, and edge
 *   4. DOCUMENTED - Every formula has mathematical explanation
 *
 * MATHEMATICAL FOUNDATION:
 *   φ (Golden Ratio) = (1 + √5) / 2 ≈ 1.618033988749895
 *
 *   Properties:
 *   - φ² = φ + 1 (self-similarity)
 *   - 1/φ = φ - 1 (reciprocal beauty)
 *   - φⁿ = φⁿ⁻¹ + φⁿ⁻² (Fibonacci connection)
 *
 * SEFIROT MAPPING:
 *   Keter (Crown)     → Philosophy (asdf-manifesto)
 *   Daat (Knowledge)  → AI Superlayer (asdf-brain)
 *   Hod (Splendor)    → K-Score Analytics (HolDex)
 *   Yesod (Foundation)→ Infrastructure (GASdf)
 *   Malkhuth (Kingdom)→ The Token ($asdfasdfa)
 *
 * @version 2.0.0
 * @license MIT
 * @author $asdfasdfa ecosystem contributors
 */

'use strict';

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 1: UNIVERSAL CONSTANTS (φ - THE GOLDEN RATIO)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * The Golden Ratio (φ) - Nature's most beautiful proportion
 *
 * Calculated as: (1 + √5) / 2
 * This constant appears in: spirals, flowers, galaxies, DNA, and now crypto.
 */
const PHI = 1.618033988749895;

/**
 * Pre-calculated powers of φ for efficiency and precision
 * These are used throughout all calculations
 */
const PHI_POWERS = Object.freeze({
    PHI_SQ: PHI * PHI,                    // φ² = 2.618033988749895
    PHI_CUBED: PHI * PHI * PHI,           // φ³ = 4.236067977499790
    PHI_INV: 1 / PHI,                     // φ⁻¹ = 0.618033988749895
    PHI_INV_SQ: 1 / (PHI * PHI),          // φ⁻² = 0.381966011250105
    PHI_INV_CUBED: 1 / (PHI * PHI * PHI), // φ⁻³ = 0.236067977499790
    PHI_INV_4: 1 / (PHI ** 4),            // φ⁻⁴ = 0.145898033750315
    PHI_INV_5: 1 / (PHI ** 5),            // φ⁻⁵ = 0.090169943749474
});

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 2: FEE DISTRIBUTION RATIOS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fee Distribution Ratios
 *
 * When fees are collected, they are split according to φ:
 *
 *   BURN (38.2%)     = φ⁻² → Permanent deflation, rewards all holders
 *   REWARDS (38.2%)  = φ⁻² → Distribution to E-Score participants
 *   TREASURY (23.6%) = φ⁻³ → Operational costs, development
 *
 * Mathematical elegance: BURN + REWARDS + TREASURY = 1.0
 * Verification: 0.382 + 0.382 + 0.236 = 1.000
 */
const FEE_RATIOS = Object.freeze({
    BURN: PHI_POWERS.PHI_INV_SQ,        // 38.2% - Deflation
    REWARDS: PHI_POWERS.PHI_INV_SQ,     // 38.2% - Distribution
    TREASURY: PHI_POWERS.PHI_INV_CUBED, // 23.6% - Operations
});

/**
 * Reward Pool Distribution (within the 38.2% REWARDS)
 *
 *   NODES (61.8%) = φ⁻¹ → Infrastructure operators
 *   USERS (23.6%) = φ⁻³ → E-Score participants
 *   DEVS (14.6%)  = remainder → Developers
 *
 * Nodes get the largest share because they provide infrastructure.
 * "Run" dimension has φ² multiplier in E-Score for the same reason.
 */
const REWARD_SPLITS = Object.freeze({
    NODES: PHI_POWERS.PHI_INV,          // 61.8% of rewards
    USERS: PHI_POWERS.PHI_INV_CUBED,    // 23.6% of rewards
    DEVS: 1 - PHI_POWERS.PHI_INV - PHI_POWERS.PHI_INV_CUBED, // ~14.6%
});

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 3: E-SCORE SYSTEM
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * E-Score Dimension Multipliers
 *
 * Each dimension has a multiplier based on its value to the ecosystem:
 *
 *   HOLD (1.0)   → Capital at risk, but temporary
 *   BURN (φ)    → Permanent commitment, irreversible
 *   USE (1.0)   → Activity, but no skin in game
 *   BUILD (φ²)  → Creating value, highest contribution
 *   RUN (φ²)    → Infrastructure, highest contribution
 *   REFER (φ)   → Growth, organic expansion
 *   TIME (1.0)  → Loyalty, but passive
 *
 * BUILD and RUN have φ² because they create permanent value.
 * BURN and REFER have φ because they're irreversible contributions.
 */
const MULTIPLIERS = Object.freeze({
    HOLD: 1.0,                    // Temporary capital
    BURN: PHI,                    // 1.618 - Permanent commitment
    USE: 1.0,                     // Activity
    BUILD: PHI_POWERS.PHI_SQ,     // 2.618 - Value creation
    RUN: PHI_POWERS.PHI_SQ,       // 2.618 - Infrastructure
    REFER: PHI,                   // 1.618 - Growth
    TIME: 1.0,                    // Loyalty
});

/**
 * E-Score Tiers
 *
 * Progression follows φ-based thresholds for natural growth:
 *
 *   Observer (0)     → Entry level, no participation yet
 *   Seedling (0.1)   → First contribution
 *   Sprout (1)       → Growing engagement
 *   Sapling (5)      → Establishing roots
 *   Tree (15)        → Stable contributor
 *   Grove (30)       → Significant presence
 *   Forest (50)      → Major ecosystem participant
 *   Ecosystem (75)   → Top tier, ecosystem pillar
 */
const TIERS = Object.freeze([
    { name: 'Observer', threshold: 0, icon: '👁️', color: '#888888' },
    { name: 'Seedling', threshold: 0.1, icon: '🌱', color: '#90EE90' },
    { name: 'Sprout', threshold: 1, icon: '🌿', color: '#32CD32' },
    { name: 'Sapling', threshold: 5, icon: '🌳', color: '#228B22' },
    { name: 'Tree', threshold: 15, icon: '🌲', color: '#006400' },
    { name: 'Grove', threshold: 30, icon: '🌴', color: '#2E8B57' },
    { name: 'Forest', threshold: 50, icon: '🌲🌳🌲', color: '#1B4D3E' },
    { name: 'Ecosystem', threshold: 75, icon: '🏔️🌲🌳', color: '#0D2818' },
]);

/**
 * E-Score PHI Unit for discount calculation
 *
 * At E = 25 → discount = 1 - φ⁻¹ = 38.2%
 * At E = 50 → discount = 1 - φ⁻² = 61.8%
 * At E = 75 → discount = 1 - φ⁻³ = 76.4%
 *
 * 25 = 5² where 5 is a Fibonacci number (φ connection)
 */
const E_SCORE_PHI_UNIT = 25;

/**
 * Maximum discount cap (φ-based safety limit)
 *
 * Even with infinite E-Score, discount cannot exceed 1 - φ⁻⁵
 * This ensures minimum φ⁻⁵ ≈ 9.02% of base fee is always captured.
 *
 * Mathematical derivation:
 *   MAX_DISCOUNT = 1 - φ⁻⁵ = 1 - 0.09017 = 0.90983
 *
 * Why φ⁻⁵:
 *   - φ⁻¹ = 61.8% → too restrictive
 *   - φ⁻² = 38.2% → still too restrictive
 *   - φ⁻³ = 23.6% → reasonable but conservative
 *   - φ⁻⁴ = 14.6% → good balance
 *   - φ⁻⁵ = 9.02% → minimal capture, maximum reward for engagement
 */
const MAX_DISCOUNT_CAP = 1 - PHI_POWERS.PHI_INV_5; // ≈ 0.90983

/**
 * Safety margin for efficiency floor (φ-based)
 *
 * Ensures φ⁻³ ≈ 23.6% buffer above actual costs.
 *
 * Mathematical derivation:
 *   SAFETY_MARGIN = 1 + φ⁻³ = 1.236
 *
 * This means: minFee = actualCost × 1.236
 * The 23.6% margin covers operational variance while staying φ-aligned.
 */
const SAFETY_MARGIN = 1 + PHI_POWERS.PHI_INV_CUBED; // ≈ 1.236

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 4: K-SCORE SYSTEM (Token Quality)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * K-Score Formula: K = 100 × ∛(D × O × L)
 *
 *   D = Diamond Hands (conviction, holding patterns)
 *   O = Organic Growth (natural distribution, not bots)
 *   L = Longevity (survival, time-tested)
 *
 * Geometric mean ensures all three factors matter equally.
 * A token with D=100, O=0, L=100 still gets K=0.
 */
const K_SCORE = Object.freeze({
    MAX: 100,
    MIN: 0,
    FORMULA: 'K = 100 × ∛(D × O × L)',
});

/**
 * K-Score Consensus Rules
 *
 * Multiple nodes must verify a token's K-Score.
 * Consensus is reached when φ⁻¹ (61.8%) of nodes agree.
 */
const CONSENSUS_RULES = Object.freeze({
    MIN_VERIFICATIONS: 2,
    AGREEMENT_THRESHOLD: PHI_POWERS.PHI_INV, // 61.8%
    KSCORE_TOLERANCE: 5, // Fibonacci number (1,1,2,3,5,8...) - φ-aligned tolerance
    CONSENSUS_WINDOW_MS: 24 * 60 * 60 * 1000, // 24 hours

    /**
     * Calculate required approvals based on total nodes
     */
    getRequiredApprovals(totalNodes) {
        return Math.max(2, Math.ceil(totalNodes * PHI_POWERS.PHI_INV));
    },

    /**
     * Check if consensus is reached
     */
    isConsensusReached(agreeing, total) {
        if (total < 2) return false;
        return agreeing / total >= PHI_POWERS.PHI_INV;
    },
});

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 5: I-SCORE SYSTEM (Infrastructure Integrity)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * I-Score Formula: I = 100 × ∛(D × O × L)
 *
 * For infrastructure tokens (SOL, USDC, etc.):
 *   D = Depth (liquidity availability)
 *   O = Oracle (price feed freshness/accuracy)
 *   L = Reliability (uptime, historical stability)
 */
const I_SCORE = Object.freeze({
    FORMULA: 'I_infra = 100 × ∛(D_liquidity × O_oracle × L_reliability)',

    // Health thresholds (φ-based)
    THRESHOLDS: Object.freeze({
        CRITICAL: PHI_POWERS.PHI_INV_SQ * 100,  // < 38.2%
        WARNING: PHI_POWERS.PHI_INV * 100,      // < 61.8%
        HEALTHY: PHI_POWERS.PHI_INV * 100,      // >= 61.8%
    }),

    // Token weights for aggregate score
    WEIGHTS: Object.freeze({
        SOL: PHI_POWERS.PHI_SQ,     // 2.618 - Foundation
        USDC: PHI,                   // 1.618 - Primary stable
        USDT: 1.0,                   // Secondary stable
        wSOL: PHI,                   // 1.618 - Wrapped native
        JitoSOL: 1.0,                // LST
        mSOL: 1.0,                   // LST
        bSOL: 1.0,                   // LST
    }),
});

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 6: Φ-SCORE SYSTEM (Meta Score)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Φ-Score: The unified meta-score combining K, E, and I
 *
 * Formula: Φ = 100 × ∛(K̄^φ × Ē^1 × Ī^φ²)
 *
 * Where:
 *   K̄ = Normalized average K-Score of ecosystem
 *   Ē = Normalized average E-Score of participants
 *   Ī = Normalized average I-Score of infrastructure
 *
 * The exponents reflect importance:
 *   K gets φ weight (token quality matters)
 *   E gets 1 weight (participation is baseline)
 *   I gets φ² weight (infrastructure is critical)
 */
const PHI_SCORE = Object.freeze({
    FORMULA: 'Φ = 100 × ∛(K̄^φ × Ē^1 × Ī^φ²)',
    WEIGHTS: Object.freeze({
        K: PHI,                      // Token quality
        E: 1.0,                      // Participation
        I: PHI_POWERS.PHI_SQ,        // Infrastructure
    }),
});

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 7: CALCULATION FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Calculate E-Score from contributions
 *
 * Uses geometric mean with φ multipliers for natural weighting.
 * Active dimensions get diversity bonus (encourages balanced participation).
 *
 * @param {Object} contributions - Raw contribution data
 * @returns {Object} { score, breakdown, activeDimensions, diversityBonus }
 */
function calculateEScore(contributions) {
    const {
        holdings = 0,
        burned = 0,
        apiCalls30d = 0,
        appsLive = 0,
        nodesActive = 0,
        referralsActive = 0,
        daysActive = 0,
    } = contributions;

    // Normalize each dimension to 0-100 range with log scaling
    const normalize = (value, scale) => {
        if (value <= 0) return 0;
        return Math.min(100, Math.log10(1 + value / scale) * 100);
    };

    // Calculate each dimension score
    const dimensions = {
        hold: normalize(holdings, 1000000),           // Scale: 1M tokens
        burn: normalize(burned, 100000),              // Scale: 100K tokens
        use: normalize(apiCalls30d, 1000),            // Scale: 1K calls
        build: normalize(appsLive, 1),                // Scale: 1 app
        run: normalize(nodesActive, 1),               // Scale: 1 node
        refer: normalize(referralsActive, 10),        // Scale: 10 referrals
        time: normalize(daysActive, 365),             // Scale: 1 year
    };

    // Apply multipliers
    const weighted = {
        hold: dimensions.hold * MULTIPLIERS.HOLD,
        burn: dimensions.burn * MULTIPLIERS.BURN,
        use: dimensions.use * MULTIPLIERS.USE,
        build: dimensions.build * MULTIPLIERS.BUILD,
        run: dimensions.run * MULTIPLIERS.RUN,
        refer: dimensions.refer * MULTIPLIERS.REFER,
        time: dimensions.time * MULTIPLIERS.TIME,
    };

    // Count active dimensions (for diversity bonus)
    const activeDimensions = Object.values(dimensions).filter(v => v > 0).length;

    // Diversity bonus: φ^(active-1) / φ^6 (max 7 dimensions)
    // Encourages spreading contributions across multiple dimensions
    const diversityBonus = activeDimensions > 1
        ? Math.pow(PHI, activeDimensions - 1) / Math.pow(PHI, 6)
        : 0;

    // Calculate weighted sum
    const totalWeight = Object.values(MULTIPLIERS).reduce((a, b) => a + b, 0);
    const weightedSum = Object.values(weighted).reduce((a, b) => a + b, 0);

    // Base score (0-100)
    const baseScore = (weightedSum / totalWeight) * (1 + diversityBonus);

    // Final score capped at 100
    const score = Math.min(100, Math.round(baseScore * 100) / 100);

    return {
        score,
        breakdown: weighted,
        rawDimensions: dimensions,
        activeDimensions,
        diversityBonus: Math.round(diversityBonus * 1000) / 1000,
    };
}

/**
 * Get tier for an E-Score
 *
 * @param {number} score - E-Score value
 * @returns {Object} Tier object { name, threshold, icon, color }
 */
function getTier(score) {
    for (let i = TIERS.length - 1; i >= 0; i--) {
        if (score >= TIERS[i].threshold) {
            return { ...TIERS[i] };
        }
    }
    return { ...TIERS[0] };
}

/**
 * Calculate progress to next tier
 *
 * @param {number} score - Current E-Score
 * @returns {Object} Progress information
 */
function getProgressToNextTier(score) {
    const currentTier = getTier(score);
    const currentIndex = TIERS.findIndex(t => t.name === currentTier.name);
    const nextTier = TIERS[currentIndex + 1];

    if (!nextTier) {
        return {
            currentTier,
            nextTier: null,
            progress: 1,
            remaining: 0,
            isMaxTier: true,
        };
    }

    const rangeSize = nextTier.threshold - currentTier.threshold;
    const progress = (score - currentTier.threshold) / rangeSize;
    const remaining = nextTier.threshold - score;

    return {
        currentTier,
        nextTier,
        progress: Math.round(progress * 1000) / 1000,
        remaining: Math.round(remaining * 100) / 100,
        isMaxTier: false,
    };
}

/**
 * Calculate discount from E-Score
 *
 * Uses golden ratio curve: discount = 1 - φ^(-E/25)
 *
 * At E = 25 → discount = 38.2% (1 - φ⁻¹)
 * At E = 50 → discount = 61.8% (1 - φ⁻²)
 * At E = 75 → discount = 76.4% (1 - φ⁻³)
 *
 * @param {number} eScore - E-Score value (0-100)
 * @returns {number} Discount percentage (0 to MAX_DISCOUNT_CAP)
 */
function calculateDiscount(eScore) {
    if (!eScore || eScore <= 0) return 0;

    const discount = 1 - Math.pow(PHI, -eScore / E_SCORE_PHI_UNIT);
    return Math.min(MAX_DISCOUNT_CAP, Math.max(0, discount));
}

/**
 * Calculate benefits for an E-Score
 *
 * @param {number} eScore - E-Score value
 * @returns {Object} Benefits package
 */
function calculateBenefits(eScore) {
    const discount = calculateDiscount(eScore);
    const tier = getTier(eScore);

    // Additional benefits scale with tier
    const tierIndex = TIERS.findIndex(t => t.name === tier.name);
    const tierMultiplier = 1 + (tierIndex * PHI_POWERS.PHI_INV_SQ);

    return {
        benefits: {
            theoreticalDiscount: discount,
            freeCalls: Math.floor(tierIndex * 10 * tierMultiplier),
            rateLimit: Math.floor(100 * tierMultiplier),
            priority: tierIndex,
        },
        display: {
            discount: `${(discount * 100).toFixed(1)}%`,
            tier: tier.name,
            icon: tier.icon,
        },
    };
}

/**
 * Calculate minimum fee (efficiency floor)
 *
 * Ensures fee never drops below cost + safety margin.
 *
 * @param {number} actualCost - Real operational cost
 * @returns {number} Minimum acceptable fee
 */
function calculateMinimumFee(actualCost) {
    return actualCost * SAFETY_MARGIN;
}

/**
 * Calculate maximum possible discount for an operation
 *
 * @param {number} baseFee - Advertised base fee
 * @param {number} actualCost - Real operational cost
 * @returns {number} Maximum discount (0-1)
 */
function calculateMaxDiscount(baseFee, actualCost) {
    const minFee = calculateMinimumFee(actualCost);
    if (baseFee <= minFee) return 0;
    return (baseFee - minFee) / baseFee;
}

/**
 * Calculate final fee with efficiency floor
 *
 * @param {number} eScore - User's E-Score
 * @param {number} baseFee - Advertised base fee
 * @param {number} actualCost - Real operational cost
 * @returns {Object} Fee calculation result
 */
function calculateFinalFee(eScore, baseFee, actualCost) {
    const theoreticalDiscount = calculateDiscount(eScore);
    const maxDiscount = calculateMaxDiscount(baseFee, actualCost);
    const effectiveDiscount = Math.min(theoreticalDiscount, maxDiscount);
    const minFee = calculateMinimumFee(actualCost);

    const discountedFee = baseFee * (1 - effectiveDiscount);
    const finalFee = Math.max(minFee, discountedFee);

    return {
        baseFee,
        actualCost,
        minFee,
        eScore,
        discounts: {
            theoretical: Math.round(theoreticalDiscount * 10000) / 100,
            max: Math.round(maxDiscount * 10000) / 100,
            effective: Math.round(effectiveDiscount * 10000) / 100,
            limited: effectiveDiscount < theoreticalDiscount,
        },
        finalFee: Math.round(finalFee * 100) / 100,
        savings: Math.round((baseFee - finalFee) * 100) / 100,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 8: FEE DISTRIBUTION FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Distribute a fee according to φ ratios
 *
 * @param {number} totalFee - Total fee collected
 * @returns {Object} Distribution breakdown
 */
function distributeFee(totalFee) {
    const burn = Math.floor(totalFee * FEE_RATIOS.BURN);
    const rewards = Math.floor(totalFee * FEE_RATIOS.REWARDS);
    const treasury = totalFee - burn - rewards; // Remainder to avoid rounding loss

    return {
        total: totalFee,
        burn,
        rewards,
        treasury,
        ratios: {
            burn: FEE_RATIOS.BURN,
            rewards: FEE_RATIOS.REWARDS,
            treasury: FEE_RATIOS.TREASURY,
        },
    };
}

/**
 * Split rewards pool among recipient types
 *
 * @param {number} rewardsPool - Total rewards to distribute
 * @returns {Object} Split amounts
 */
function splitRewardsPool(rewardsPool) {
    const nodes = Math.floor(rewardsPool * REWARD_SPLITS.NODES);
    const users = Math.floor(rewardsPool * REWARD_SPLITS.USERS);
    const devs = rewardsPool - nodes - users; // Remainder

    return {
        total: rewardsPool,
        nodes,
        users,
        devs,
        ratios: REWARD_SPLITS,
    };
}

/**
 * Calculate individual reward share
 *
 * @param {number} participantScore - Participant's E-Score
 * @param {number} totalScore - Sum of all participants' E-Scores
 * @param {number} pool - Total pool to distribute
 * @returns {number} Participant's share
 */
function calculateRewardShare(participantScore, totalScore, pool) {
    if (totalScore <= 0 || participantScore <= 0) return 0;
    return (participantScore / totalScore) * pool;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 9: K-SCORE FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Calculate K-Score from components
 *
 * K = 100 × ∛(D × O × L)
 *
 * @param {number} d - Diamond hands score (0-1)
 * @param {number} o - Organic growth score (0-1)
 * @param {number} l - Longevity score (0-1)
 * @returns {number} K-Score (0-100)
 */
function calculateKScore(d, o, l) {
    // Clamp inputs to 0-1
    const D = Math.min(1, Math.max(0, d));
    const O = Math.min(1, Math.max(0, o));
    const L = Math.min(1, Math.max(0, l));

    // Geometric mean
    return Math.round(100 * Math.cbrt(D * O * L));
}

/**
 * Calculate I-Score for infrastructure token
 *
 * I = 100 × ∛(D × O × L)
 *
 * @param {number} liquidity - Liquidity depth (0-1)
 * @param {number} oracle - Oracle accuracy (0-1)
 * @param {number} reliability - Historical reliability (0-1)
 * @returns {Object} I-Score with components
 */
function calculateIScore(liquidity, oracle, reliability) {
    const D = Math.min(1, Math.max(0, liquidity));
    const O = Math.min(1, Math.max(0, oracle));
    const L = Math.min(1, Math.max(0, reliability));

    const score = 100 * Math.cbrt(D * O * L);

    return {
        score: Math.round(score * 100) / 100,
        components: { D, O, L },
        health: score >= I_SCORE.THRESHOLDS.HEALTHY ? 'healthy' :
            score >= I_SCORE.THRESHOLDS.CRITICAL ? 'warning' : 'critical',
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 10: UTILITY FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Format a number with φ-based notation
 *
 * @param {number} value - Value to format
 * @returns {string} Formatted string
 */
function formatPhiNumber(value) {
    if (value === PHI) return 'φ';
    if (value === PHI_POWERS.PHI_SQ) return 'φ²';
    if (value === PHI_POWERS.PHI_INV) return 'φ⁻¹';
    if (value === PHI_POWERS.PHI_INV_SQ) return 'φ⁻²';
    if (value === PHI_POWERS.PHI_INV_CUBED) return 'φ⁻³';
    return value.toFixed(4);
}

/**
 * Verify φ relationships (for testing/validation)
 *
 * @returns {Object} Verification results
 */
function verifyPhiRelationships() {
    const checks = {
        // φ² = φ + 1
        phiSquaredIdentity: Math.abs(PHI_POWERS.PHI_SQ - (PHI + 1)) < 1e-10,

        // 1/φ = φ - 1
        phiInverseIdentity: Math.abs(PHI_POWERS.PHI_INV - (PHI - 1)) < 1e-10,

        // φ × φ⁻¹ = 1
        phiProduct: Math.abs(PHI * PHI_POWERS.PHI_INV - 1) < 1e-10,

        // Fee ratios sum to ~1
        feeRatioSum: Math.abs(
            FEE_RATIOS.BURN + FEE_RATIOS.REWARDS + FEE_RATIOS.TREASURY - 1
        ) < 1e-10,

        // Reward splits sum to 1
        rewardSplitSum: Math.abs(
            REWARD_SPLITS.NODES + REWARD_SPLITS.USERS + REWARD_SPLITS.DEVS - 1
        ) < 1e-10,
    };

    return {
        allValid: Object.values(checks).every(v => v),
        checks,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
    // Constants
    PHI,
    PHI_POWERS,
    FEE_RATIOS,
    REWARD_SPLITS,
    MULTIPLIERS,
    TIERS,
    E_SCORE_PHI_UNIT,
    MAX_DISCOUNT_CAP,
    SAFETY_MARGIN,
    K_SCORE,
    CONSENSUS_RULES,
    I_SCORE,
    PHI_SCORE,

    // E-Score functions
    calculateEScore,
    getTier,
    getProgressToNextTier,
    calculateDiscount,
    calculateBenefits,

    // Fee functions
    calculateMinimumFee,
    calculateMaxDiscount,
    calculateFinalFee,
    distributeFee,
    splitRewardsPool,
    calculateRewardShare,

    // K-Score / I-Score functions
    calculateKScore,
    calculateIScore,

    // Utilities
    formatPhiNumber,
    verifyPhiRelationships,

    // Legacy aliases (backward compatibility)
    RATIOS: FEE_RATIOS,
};
