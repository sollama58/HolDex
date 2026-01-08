/**
 * HARMONY MODULE v1.0
 *
 * $ASDFASDFA Ecosystem - Unified Economic Engine
 *
 * Single source of truth pour HolDex et GASdf:
 * - Constantes φ (Golden Ratio)
 * - Formule E-Score
 * - Calcul discounts avec plancher d'efficience
 * - Distribution des fees
 * - Définition des tiers
 *
 * Ce fichier DOIT être identique dans HolDex et GASdf
 * Toute modification doit être synchronisée entre les deux repos
 *
 * @version 1.0.0
 * @license MIT
 */

'use strict';

// ═══════════════════════════════════════════════════════════════
// CONSTANTES UNIVERSELLES (φ - Golden Ratio)
// ═══════════════════════════════════════════════════════════════

const PHI = 1.618033988749895;

/**
 * Ratios de distribution des fees basés sur φ
 * Ces ratios garantissent une harmonie mathématique naturelle
 */
const RATIOS = Object.freeze({
    BURN: 1 / (PHI ** 2),        // 0.38196... (38.2%) → Deflation
    REWARDS: 1 / (PHI ** 2),     // 0.38196... (38.2%) → Distribution
    TREASURY: 1 / (PHI ** 3),    // 0.23606... (23.6%) → Operations
});

/**
 * Multiplicateurs par type de contribution
 * Récompense les engagements permanents (burn) et créateurs (build/run)
 */
const MULTIPLIERS = Object.freeze({
    HOLD: 1.0,                   // Capital temporaire
    BURN: PHI,                   // 1.618 - Engagement permanent
    USE: 1.0,                    // Activité standard
    BUILD: PHI ** 2,             // 2.618 - Création de valeur
    RUN: PHI ** 2,               // 2.618 - Infrastructure
    REFER: PHI,                  // 1.618 - Croissance organique
    TIME: 1.0,                   // Loyauté
});

/**
 * Marge de sécurité pour le plancher d'efficience
 * Garantit 20% de buffer sur les coûts réels
 */
const SAFETY_MARGIN = 1.2;

/**
 * Discount maximum absolu (cap de sécurité)
 * Même avec E-Score infini, ne peut dépasser 95%
 */
const MAX_DISCOUNT_CAP = 0.95;

/**
 * E-Score unit for PHI-based discount curve
 *
 * At E=25 → discount = 1 - φ^(-1) = 38.2% (1/φ²)
 * At E=50 → discount = 1 - φ^(-2) = 61.8% (1/φ)
 * At E=75 → discount = 1 - φ^(-3) = 76.4% (1-1/φ³)
 *
 * 25 = 5² where 5 is a Fibonacci number (phi connection)
 * Each unit of 25 E-Score = one power of φ in discount
 */
const E_SCORE_PHI_UNIT = 25;

// ═══════════════════════════════════════════════════════════════
// E-SCORE CALCULATION
// ═══════════════════════════════════════════════════════════════

/**
 * Calcule le E-Score (Engagement Score) d'un participant
 *
 * Le E-Score mesure la valeur totale apportée à l'écosystème
 * via une moyenne géométrique pondérée des contributions.
 *
 * Propriétés:
 * - Logarithmique: empêche la domination par les whales
 * - Géométrique: récompense la diversité de contribution
 * - Multiplicateurs φ: valorise l'engagement permanent
 *
 * @param {Object} contributions - Les contributions du participant
 * @param {number} contributions.holdings - Tokens détenus
 * @param {number} contributions.burned - Tokens brûlés (lifetime)
 * @param {number} contributions.apiCalls30d - Appels API (30 jours)
 * @param {number} contributions.appsLive - Apps actives
 * @param {number} contributions.nodesActive - Nodes infra actifs
 * @param {number} contributions.referralsActive - Referrals actifs
 * @param {number} contributions.daysActive - Jours d'ancienneté
 *
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
        daysActive = 0
    } = contributions || {};

    // Validation des inputs
    const safeHoldings = Math.max(0, Number(holdings) || 0);
    const safeBurned = Math.max(0, Number(burned) || 0);
    const safeApiCalls = Math.max(0, Number(apiCalls30d) || 0);
    const safeApps = Math.max(0, Number(appsLive) || 0);
    const safeNodes = Math.max(0, Number(nodesActive) || 0);
    const safeReferrals = Math.max(0, Number(referralsActive) || 0);
    const safeDays = Math.max(0, Number(daysActive) || 0);

    // Calcul des scores par dimension (log scale + multiplicateur)
    const scores = {
        hold:  Math.log(1 + safeHoldings) * MULTIPLIERS.HOLD,
        burn:  Math.log(1 + safeBurned) * MULTIPLIERS.BURN,
        use:   Math.log(1 + safeApiCalls) * MULTIPLIERS.USE,
        build: Math.log(1 + safeApps) * MULTIPLIERS.BUILD,
        run:   Math.log(1 + safeNodes) * MULTIPLIERS.RUN,
        refer: Math.log(1 + safeReferrals) * MULTIPLIERS.REFER,
        time:  Math.log(1 + safeDays / 30) * MULTIPLIERS.TIME  // En mois
    };

    // Filtrer les dimensions actives (score > 0)
    const activeDimensions = Object.entries(scores)
        .filter(([_, value]) => value > 0);

    // Cas edge: aucune contribution
    if (activeDimensions.length === 0) {
        return {
            score: 0,
            breakdown: scores,
            activeDimensions: 0,
            diversityBonus: 1,
            raw: { holdings: safeHoldings, burned: safeBurned, apiCalls30d: safeApiCalls,
                   appsLive: safeApps, nodesActive: safeNodes, referralsActive: safeReferrals,
                   daysActive: safeDays }
        };
    }

    // Moyenne géométrique des dimensions actives
    const product = activeDimensions.reduce((acc, [_, value]) => acc * value, 1);
    const geometricMean = Math.pow(product, 1 / activeDimensions.length);

    // Bonus de diversité: +10% par dimension additionnelle
    // 1 dim = x1.0, 2 dims = x1.1, 3 dims = x1.2, etc.
    const diversityBonus = 1 + (activeDimensions.length - 1) * 0.1;

    // Score final
    const finalScore = geometricMean * diversityBonus;

    return {
        score: Math.round(finalScore * 100) / 100,
        breakdown: scores,
        activeDimensions: activeDimensions.length,
        activeDimensionNames: activeDimensions.map(([name]) => name),
        diversityBonus: Math.round(diversityBonus * 100) / 100,
        raw: { holdings: safeHoldings, burned: safeBurned, apiCalls30d: safeApiCalls,
               appsLive: safeApps, nodesActive: safeNodes, referralsActive: safeReferrals,
               daysActive: safeDays }
    };
}

// ═══════════════════════════════════════════════════════════════
// EFFICIENCY FLOOR (Plancher d'Efficience)
// ═══════════════════════════════════════════════════════════════

/**
 * Calcule le fee MINIMUM pour une opération
 *
 * Garantit que le treasury reçoit toujours assez pour couvrir les coûts.
 *
 * Formule: minFee = (cost / TREASURY_RATIO) * SAFETY_MARGIN
 *
 * @param {number} operationCost - Coût réel de l'opération en $ASDF
 * @returns {number} Fee minimum (arrondi supérieur)
 */
function calculateMinimumFee(operationCost) {
    if (operationCost <= 0) return 0;
    return Math.ceil((operationCost / RATIOS.TREASURY) * SAFETY_MARGIN);
}

/**
 * Calcule le discount MAXIMUM autorisé pour une opération
 *
 * Basé sur le ratio entre le fee minimum et le base fee.
 * Si baseFee <= minFee, aucun discount n'est possible.
 *
 * @param {number} baseFee - Fee de base pour l'opération
 * @param {number} operationCost - Coût réel de l'opération
 * @returns {number} Discount maximum (0-0.95)
 */
function calculateMaxDiscount(baseFee, operationCost) {
    if (baseFee <= 0) return 0;

    const minFee = calculateMinimumFee(operationCost);

    if (baseFee <= minFee) {
        return 0; // Pas de discount possible
    }

    const maxDiscount = 1 - (minFee / baseFee);
    return Math.min(MAX_DISCOUNT_CAP, maxDiscount);
}

/**
 * Calcule le discount THÉORIQUE basé uniquement sur le E-Score
 *
 * Pure PHI formula - no magic numbers
 *
 * Formule: discount = min(95%, 1 - φ^(-eScore/25))
 *
 * Milestones (exact phi ratios):
 * - E-Score 0   → 0%
 * - E-Score 25  → 38.2% (1/φ²) - same as burn rate
 * - E-Score 50  → 61.8% (1/φ)  - golden cut
 * - E-Score 75  → 76.4% (1-1/φ³)
 * - E-Score 100 → 85.4% (1-1/φ⁴)
 * - E-Score ∞   → 95% (capped)
 *
 * @param {number} eScore - E-Score du participant
 * @returns {number} Discount théorique (0-0.95)
 */
function calculateTheoreticalDiscount(eScore) {
    if (eScore <= 0) return 0;
    const discount = 1 - Math.pow(PHI, -eScore / E_SCORE_PHI_UNIT);
    return Math.min(MAX_DISCOUNT_CAP, discount);
}

/**
 * Calcule le discount EFFECTIF (minimum entre théorique et maximum autorisé)
 *
 * C'est LE discount réellement appliqué, prenant en compte:
 * 1. Le E-Score du participant (théorique)
 * 2. Le plancher d'efficience (maximum autorisé)
 *
 * @param {number} eScore - E-Score du participant
 * @param {number} baseFee - Fee de base pour l'opération
 * @param {number} operationCost - Coût réel de l'opération
 * @returns {number} Discount effectif
 */
function calculateEffectiveDiscount(eScore, baseFee, operationCost) {
    const theoretical = calculateTheoreticalDiscount(eScore);
    const maxAllowed = calculateMaxDiscount(baseFee, operationCost);
    return Math.min(theoretical, maxAllowed);
}

/**
 * Calcule le fee FINAL après application du discount
 *
 * @param {number} eScore - E-Score du participant
 * @param {number} baseFee - Fee de base pour l'opération
 * @param {number} operationCost - Coût réel de l'opération
 * @returns {Object} Détails complets du calcul
 */
function calculateFinalFee(eScore, baseFee, operationCost) {
    const theoreticalDiscount = calculateTheoreticalDiscount(eScore);
    const maxDiscount = calculateMaxDiscount(baseFee, operationCost);
    const effectiveDiscount = Math.min(theoreticalDiscount, maxDiscount);
    const minFee = calculateMinimumFee(operationCost);

    // Fee finale = max(minFee, baseFee * (1 - discount))
    const calculatedFee = baseFee * (1 - effectiveDiscount);
    const finalFee = Math.max(minFee, calculatedFee);

    // Distribution de la fee
    const distribution = distributeFee(finalFee);

    // Vérification de viabilité
    const isViable = distribution.treasury >= operationCost;
    const profit = distribution.treasury - operationCost;

    return {
        // Inputs
        eScore,
        baseFee,
        operationCost,

        // Discounts
        discounts: {
            theoretical: Math.round(theoreticalDiscount * 10000) / 100, // En %
            maxAllowed: Math.round(maxDiscount * 10000) / 100,
            effective: Math.round(effectiveDiscount * 10000) / 100,
            limited: theoreticalDiscount > maxDiscount
        },

        // Fees
        minFee,
        finalFee: Math.round(finalFee * 100) / 100,
        savings: Math.round((baseFee - finalFee) * 100) / 100,

        // Distribution
        distribution: {
            burn: Math.round(distribution.burn * 100) / 100,
            rewards: Math.round(distribution.rewards * 100) / 100,
            treasury: Math.round(distribution.treasury * 100) / 100
        },

        // Viability
        efficiency: {
            isViable,
            profit: Math.round(profit * 100) / 100,
            margin: operationCost > 0
                ? Math.round((profit / operationCost) * 10000) / 100
                : 100
        }
    };
}

// ═══════════════════════════════════════════════════════════════
// FEE DISTRIBUTION
// ═══════════════════════════════════════════════════════════════

/**
 * Distribue une fee selon les ratios φ
 *
 * - 38.2% → Burn (deflation permanente)
 * - 38.2% → Rewards (redistribution aux participants)
 * - 23.6% → Treasury (coûts opérationnels)
 *
 * @param {number} feeAmount - Montant total de la fee
 * @returns {Object} Distribution { burn, rewards, treasury, total }
 */
function distributeFee(feeAmount) {
    if (feeAmount <= 0) {
        return { burn: 0, rewards: 0, treasury: 0, total: 0 };
    }

    return {
        burn: feeAmount * RATIOS.BURN,
        rewards: feeAmount * RATIOS.REWARDS,
        treasury: feeAmount * RATIOS.TREASURY,
        total: feeAmount
    };
}

// ═══════════════════════════════════════════════════════════════
// TIERS (Cosmétiques - pas de gatekeeping)
// ═══════════════════════════════════════════════════════════════

/**
 * Définition des tiers
 *
 * Les tiers sont COSMÉTIQUES uniquement.
 * Ils donnent un sentiment de progression sans bloquer personne.
 * Tous les bénéfices sont calculés sur une courbe continue.
 */
const TIERS = Object.freeze([
    { name: 'Legendary', icon: '👑', minScore: 100, color: '#FFD700', level: 9 },
    { name: 'Mythic',    icon: '🔮', minScore: 50,  color: '#9B59B6', level: 8 },
    { name: 'Epic',      icon: '⚡', minScore: 30,  color: '#8E44AD', level: 7 },
    { name: 'Rare',      icon: '💎', minScore: 15,  color: '#3498DB', level: 6 },
    { name: 'Uncommon',  icon: '🌟', minScore: 7,   color: '#2ECC71', level: 5 },
    { name: 'Common',    icon: '✨', minScore: 3,   color: '#27AE60', level: 4 },
    { name: 'Starter',   icon: '🌱', minScore: 1,   color: '#95A5A6', level: 3 },
    { name: 'Seedling',  icon: '🫘', minScore: 0.1, color: '#BDC3C7', level: 2 },
    { name: 'Observer',  icon: '👁️', minScore: 0,   color: '#7F8C8D', level: 1 }
]);

/**
 * Retourne le tier correspondant à un E-Score
 *
 * @param {number} eScore - E-Score du participant
 * @returns {Object} Tier { name, icon, minScore, color, level }
 */
function getTier(eScore) {
    const score = Number(eScore) || 0;

    for (const tier of TIERS) {
        if (score >= tier.minScore) {
            return { ...tier };
        }
    }

    return { ...TIERS[TIERS.length - 1] };
}

/**
 * Calcule la progression vers le prochain tier
 *
 * @param {number} eScore - E-Score actuel
 * @returns {Object|null} { nextTier, progress, remaining } ou null si max
 */
function getProgressToNextTier(eScore) {
    const score = Number(eScore) || 0;
    const currentTier = getTier(score);
    const currentIndex = TIERS.findIndex(t => t.name === currentTier.name);

    // Déjà au max
    if (currentIndex === 0) {
        return null;
    }

    const nextTier = TIERS[currentIndex - 1];
    const currentMin = currentTier.minScore;
    const nextMin = nextTier.minScore;

    const progress = (score - currentMin) / (nextMin - currentMin);
    const remaining = nextMin - score;

    return {
        currentTier: currentTier.name,
        nextTier: nextTier.name,
        nextIcon: nextTier.icon,
        progress: Math.min(1, Math.max(0, progress)),
        progressPercent: Math.round(Math.min(100, Math.max(0, progress * 100))),
        remaining: Math.max(0, remaining),
        currentScore: score
    };
}

// ═══════════════════════════════════════════════════════════════
// BENEFITS CALCULATION
// ═══════════════════════════════════════════════════════════════

/**
 * Calcule tous les bénéfices pour un E-Score donné
 *
 * @param {number} eScore - E-Score du participant
 * @returns {Object} Tous les bénéfices calculés
 */
function calculateBenefits(eScore) {
    const score = Number(eScore) || 0;
    const tier = getTier(score);
    const progress = getProgressToNextTier(score);

    // Discount théorique (sans considération des coûts)
    const theoreticalDiscount = calculateTheoreticalDiscount(score);

    // Free calls: base 10 + 20 par point E-Score
    const freeCalls = Math.floor(10 + score * 20);

    // Rate limit: base 1 + 5 par point, cap 1000
    const rateLimit = Math.min(1000, Math.floor(1 + score * 5));

    // Priority: logarithmique, 0-100
    const priority = Math.min(100, Math.floor(20 * Math.log(1 + score)));

    return {
        eScore: score,
        tier,
        progress,

        benefits: {
            theoreticalDiscount: Math.round(theoreticalDiscount * 10000) / 100,
            freeCalls,
            rateLimit,
            priority
        },

        // Pour affichage
        display: {
            discount: `${Math.round(theoreticalDiscount * 100)}%`,
            freeCalls: freeCalls.toLocaleString(),
            rateLimit: `${rateLimit}/sec`,
            priority: `${priority}/100`
        }
    };
}

// ═══════════════════════════════════════════════════════════════
// REWARD DISTRIBUTION HELPERS
// ═══════════════════════════════════════════════════════════════

/**
 * Calcule la part de rewards pour un participant
 *
 * @param {number} participantEScore - E-Score du participant
 * @param {number} totalEScore - Somme de tous les E-Scores
 * @param {number} rewardPool - Montant total à distribuer
 * @returns {number} Part du participant
 */
function calculateRewardShare(participantEScore, totalEScore, rewardPool) {
    if (totalEScore <= 0 || rewardPool <= 0 || participantEScore <= 0) {
        return 0;
    }

    const share = (participantEScore / totalEScore) * rewardPool;
    return Math.round(share * 100000000) / 100000000; // 8 decimals precision
}

/**
 * Calcule le pool de rewards à partir des fees collectées
 *
 * @param {number} totalFees - Total des fees collectées
 * @returns {Object} { burn, rewards, treasury }
 */
function calculateRewardPool(totalFees) {
    return distributeFee(totalFees);
}

// ═══════════════════════════════════════════════════════════════
// VALIDATION HELPERS
// ═══════════════════════════════════════════════════════════════

/**
 * Vérifie si une opération est viable économiquement
 *
 * @param {number} baseFee - Fee de base
 * @param {number} operationCost - Coût réel
 * @returns {Object} { viable, minFee, maxDiscount, issue }
 */
function validateOperationViability(baseFee, operationCost) {
    const minFee = calculateMinimumFee(operationCost);
    const maxDiscount = calculateMaxDiscount(baseFee, operationCost);
    const viable = baseFee >= minFee;

    return {
        viable,
        baseFee,
        operationCost,
        minFee,
        maxDiscount: Math.round(maxDiscount * 10000) / 100,
        treasuryShare: Math.round(baseFee * RATIOS.TREASURY * 100) / 100,
        issue: !viable
            ? `Base fee (${baseFee}) < minimum (${minFee}). Increase base fee or reduce cost.`
            : null
    };
}

/**
 * Valide un tableau de coûts d'opérations
 *
 * @param {Array} operations - [{ type, baseFee, cost }, ...]
 * @returns {Array} Résultats de validation
 */
function validateOperationCosts(operations) {
    return operations.map(op => ({
        type: op.type,
        ...validateOperationViability(op.baseFee, op.cost)
    }));
}

// ═══════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════

module.exports = {
    // Constants
    PHI,
    RATIOS,
    MULTIPLIERS,
    SAFETY_MARGIN,
    MAX_DISCOUNT_CAP,
    E_SCORE_PHI_UNIT,
    TIERS,

    // E-Score
    calculateEScore,

    // Efficiency Floor
    calculateMinimumFee,
    calculateMaxDiscount,
    calculateTheoreticalDiscount,
    calculateEffectiveDiscount,
    calculateFinalFee,

    // Distribution
    distributeFee,
    calculateRewardShare,
    calculateRewardPool,

    // Tiers & Benefits
    getTier,
    getProgressToNextTier,
    calculateBenefits,

    // Validation
    validateOperationViability,
    validateOperationCosts
};
