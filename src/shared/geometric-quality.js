/**
 * Geometric Quality - Unified D×O×L Quality Measurement
 * Based on $asdfasdfa Philosophy: "Don't Trust, Verify"
 *
 * Abstracts the geometric mean quality formula used by:
 * - K-Score: Token health (Diamond × Organic × Longevity)
 * - Session Quality: Claude context health (Efficiency × Completion × Freshness)
 *
 * Formula: Q = scale × ∛(D × O × L)
 * Property: If ANY dimension is 0, quality collapses to 0
 */

'use strict';

const { PHI, PHI_INVERSE, PHI_INVERSE_SQUARED } = require('./claude-phi');

// =============================================================================
// CORE GEOMETRIC MEAN
// =============================================================================

/**
 * Calculate geometric mean of N values
 * @param {number[]} values - Array of values (0-1 range expected)
 * @returns {number} Geometric mean (0-1)
 */
function geometricMean(values) {
  if (!values || values.length === 0) return 0;

  // If any value is 0, result is 0 (core principle)
  if (values.some(v => v <= 0)) return 0;

  // Nth root of product
  const product = values.reduce((acc, v) => acc * v, 1);
  return Math.pow(product, 1 / values.length);
}

/**
 * Calculate quality score using D×O×L formula
 * Q = scale × ∛(D × O × L)
 *
 * @param {Object} dimensions - The three quality dimensions
 * @param {number} dimensions.D - First dimension (0-1)
 * @param {number} dimensions.O - Second dimension (0-1)
 * @param {number} dimensions.L - Third dimension (0-1)
 * @param {number} [scale=100] - Output scale (default 0-100)
 * @returns {number} Quality score
 */
function calculateQuality({ D, O, L }, scale = 100) {
  const mean = geometricMean([D, O, L]);
  return Math.round(scale * mean);
}

// =============================================================================
// QUALITY PROFILES
// =============================================================================

/**
 * Predefined quality profiles for different use cases
 */
const QUALITY_PROFILES = Object.freeze({
  /**
   * K-Score: Token health metrics
   * D = Diamond Hands (conviction strength)
   * O = Organic Growth (distribution quality)
   * L = Longevity (survival factor)
   */
  KSCORE: {
    name: 'K-Score',
    scale: 100,
    dimensions: {
      D: { name: 'Diamond', description: 'Conviction strength from holder behavior' },
      O: { name: 'Organic', description: 'Distribution quality (anti-sniper)' },
      L: { name: 'Longevity', description: 'Survival factor over time' },
    },
    thresholds: {
      excellent: 80,  // Strong token
      good: 60,       // Healthy token
      warning: 40,    // Caution
      critical: 20,   // High risk
    },
  },

  /**
   * Session Quality: Claude context health
   * D = Efficiency (token conviction ratio)
   * O = Completion (task completion rate)
   * L = Freshness (context decay)
   */
  SESSION: {
    name: 'Session Quality',
    scale: 100,
    dimensions: {
      D: { name: 'Efficiency', description: 'Token conviction (Accumulator vs Extractor)' },
      O: { name: 'Completion', description: 'Task completion rate (todos done/created)' },
      L: { name: 'Freshness', description: 'Context decay over messages' },
    },
    thresholds: {
      excellent: 80,  // Continue freely
      good: 60,       // Monitor
      warning: 40,    // /compact recommended
      critical: 25,   // /rewind recommended
    },
  },

  /**
   * Integrity Score: Data integrity health
   * D = Coverage (signature completeness)
   * O = Consistency (cross-signature agreement)
   * L = Recency (time since last verification)
   */
  INTEGRITY: {
    name: 'Integrity Score',
    scale: 100,
    dimensions: {
      D: { name: 'Coverage', description: 'Signature completeness (8/8 categories)' },
      O: { name: 'Consistency', description: 'Cross-signature agreement' },
      L: { name: 'Recency', description: 'Time since last verification' },
    },
    thresholds: {
      excellent: 95,  // Fully verified
      good: 80,       // Minor gaps
      warning: 60,    // Needs attention
      critical: 40,   // Integrity breach
    },
  },
});

// =============================================================================
// THRESHOLD EVALUATION
// =============================================================================

/**
 * Quality levels with actions
 */
const QUALITY_LEVELS = Object.freeze({
  EXCELLENT: 'excellent',
  GOOD: 'good',
  WARNING: 'warning',
  CRITICAL: 'critical',
  FAILED: 'failed',
});

/**
 * Evaluate quality against thresholds
 * @param {number} score - Quality score
 * @param {Object} thresholds - Threshold configuration
 * @returns {Object} Level assessment with recommendation
 */
function evaluateQuality(score, thresholds) {
  const { excellent, good, warning, critical } = thresholds;

  if (score >= excellent) {
    return {
      level: QUALITY_LEVELS.EXCELLENT,
      emoji: '🟢',
      action: null,
      description: 'Excellent quality - no action needed',
    };
  }
  if (score >= good) {
    return {
      level: QUALITY_LEVELS.GOOD,
      emoji: '🟡',
      action: null,
      description: 'Good quality - monitor',
    };
  }
  if (score >= warning) {
    return {
      level: QUALITY_LEVELS.WARNING,
      emoji: '🟠',
      action: 'review',
      description: 'Warning - review recommended',
    };
  }
  if (score >= critical) {
    return {
      level: QUALITY_LEVELS.CRITICAL,
      emoji: '🔴',
      action: 'intervene',
      description: 'Critical - intervention required',
    };
  }
  return {
    level: QUALITY_LEVELS.FAILED,
    emoji: '⛔',
    action: 'reset',
    description: 'Failed - reset required',
  };
}

// =============================================================================
// DECAY FUNCTIONS
// =============================================================================

/**
 * Calculate exponential decay (for Longevity/Freshness dimension)
 * @param {number} age - Age in units (messages, hours, days)
 * @param {number} halfLife - Half-life in same units
 * @param {number} [floor=0.1] - Minimum value (never decay below)
 * @returns {number} Decay factor (0-1)
 */
function exponentialDecay(age, halfLife, floor = 0.1) {
  const decay = Math.pow(0.5, age / halfLife);
  return Math.max(floor, decay);
}

/**
 * Calculate phi-based decay tiers
 * Uses golden ratio for natural-feeling decay
 * @param {number} value - Current value
 * @param {number} tier - Decay tier (1, 2, 3...)
 * @returns {number} Decayed value
 */
function phiDecay(value, tier) {
  return value * Math.pow(PHI_INVERSE, tier);
}

// =============================================================================
// CONVICTION CLASSIFICATION
// =============================================================================

/**
 * Conviction multipliers based on φ
 * Used for weighting contributions to quality
 */
const CONVICTION_MULTIPLIERS = Object.freeze({
  // Positive contribution (φ multiplier)
  ACCUMULATOR: PHI,           // 1.618x - adds significant value

  // Neutral (1.0 multiplier)
  HOLDER: 1.0,                // 1.0x - maintains value

  // Negative contribution (1/φ multipliers)
  REDUCER: PHI_INVERSE,       // 0.618x - slight negative
  EXTRACTOR: PHI_INVERSE_SQUARED,  // 0.382x - significant negative
});

/**
 * Calculate weighted efficiency from conviction-classified items
 * @param {Array<{weight: number, conviction: string}>} items
 * @returns {number} Efficiency score (0-1)
 */
function calculateConvictionEfficiency(items) {
  if (!items || items.length === 0) return 1.0;

  let weightedSum = 0;
  let totalWeight = 0;

  for (const item of items) {
    const multiplier = CONVICTION_MULTIPLIERS[item.conviction] || CONVICTION_MULTIPLIERS.HOLDER;
    weightedSum += (item.weight || 1) * multiplier;
    totalWeight += item.weight || 1;
  }

  if (totalWeight === 0) return 1.0;

  // Normalize to 0-1 (max possible is PHI for all accumulators)
  return Math.min(1, weightedSum / (totalWeight * PHI));
}

// =============================================================================
// QUALITY BUILDER (Fluent API)
// =============================================================================

/**
 * Fluent quality calculator
 * @example
 * const q = new QualityBuilder('SESSION')
 *   .setD(0.8)
 *   .setO(0.9)
 *   .setL(0.7)
 *   .calculate();
 */
class QualityBuilder {
  constructor(profileName = 'SESSION') {
    this.profile = QUALITY_PROFILES[profileName] || QUALITY_PROFILES.SESSION;
    this.dimensions = { D: 1, O: 1, L: 1 };
  }

  setD(value) {
    this.dimensions.D = Math.max(0, Math.min(1, value));
    return this;
  }

  setO(value) {
    this.dimensions.O = Math.max(0, Math.min(1, value));
    return this;
  }

  setL(value) {
    this.dimensions.L = Math.max(0, Math.min(1, value));
    return this;
  }

  setDimensions({ D, O, L }) {
    if (D !== undefined) this.setD(D);
    if (O !== undefined) this.setO(O);
    if (L !== undefined) this.setL(L);
    return this;
  }

  calculate() {
    const score = calculateQuality(this.dimensions, this.profile.scale);
    const evaluation = evaluateQuality(score, this.profile.thresholds);

    return {
      profile: this.profile.name,
      score,
      dimensions: {
        D: { value: this.dimensions.D, ...this.profile.dimensions.D },
        O: { value: this.dimensions.O, ...this.profile.dimensions.O },
        L: { value: this.dimensions.L, ...this.profile.dimensions.L },
      },
      ...evaluation,
    };
  }

  /**
   * Get formatted string for display
   */
  toString() {
    const result = this.calculate();
    return `${result.emoji} ${result.profile}: ${result.score}/100 (D:${Math.round(this.dimensions.D * 100)}% O:${Math.round(this.dimensions.O * 100)}% L:${Math.round(this.dimensions.L * 100)}%)`;
  }
}

// =============================================================================
// EXPORTS
// =============================================================================

module.exports = {
  // Core functions
  geometricMean,
  calculateQuality,
  evaluateQuality,

  // Profiles and constants
  QUALITY_PROFILES,
  QUALITY_LEVELS,

  // Decay functions
  exponentialDecay,
  phiDecay,

  // Conviction system
  CONVICTION_MULTIPLIERS,
  calculateConvictionEfficiency,

  // Builder
  QualityBuilder,
};
