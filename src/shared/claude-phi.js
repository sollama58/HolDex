/**
 * Claude Phi - Token Economics & Context Management
 * Based on $asdfasdfa Philosophy: "Don't Trust, Verify"
 *
 * All ratios derived from φ (Golden Ratio) - no magic numbers.
 * Geometric mean ensures balanced optimization.
 */

'use strict';

// =============================================================================
// FUNDAMENTAL CONSTANTS
// =============================================================================

const PHI = 1.618033988749895;
const PHI_INVERSE = 1 / PHI;                    // 0.618033988749895
const PHI_SQUARED = PHI * PHI;                  // 2.618033988749895
const PHI_CUBED = PHI * PHI * PHI;              // 4.236067977499790
const PHI_INVERSE_SQUARED = 1 / PHI_SQUARED;    // 0.381966011250105
const PHI_INVERSE_CUBED = 1 / PHI_CUBED;        // 0.236067977499790

// =============================================================================
// CONTEXT BUDGET RATIOS
// =============================================================================

/**
 * Token allocation ratios based on φ
 * Uses golden ratio property: 1 = φ⁻¹ + φ⁻² (exactly 1.0)
 * Overhead is carved from Research, not added separately
 */
const CONTEXT_RATIOS = Object.freeze({
  // Primary work: code, debugging, implementation
  ACTIVE_WORK: PHI_INVERSE,                     // 61.8%

  // Research: subagents, context7, docs lookup (φ⁻² total)
  // Split into pure research + overhead using φ ratio internally
  RESEARCH: PHI_INVERSE_SQUARED * PHI_INVERSE,  // 23.6% (φ⁻² × φ⁻¹)

  // Overhead: system prompts, MCP, planning
  OVERHEAD: PHI_INVERSE_SQUARED * PHI_INVERSE_SQUARED,  // 14.6% (φ⁻² × φ⁻²)
});

// Verification: PHI_INVERSE + (PHI_INVERSE_SQUARED * PHI_INVERSE) + (PHI_INVERSE_SQUARED * PHI_INVERSE_SQUARED)
// = 0.618 + 0.236 + 0.146 = 1.0 ✓

/**
 * Calculate token budget for each category
 * @param {number} totalTokens - Total available context tokens
 * @returns {Object} Budget allocation per category
 */
function calculateContextBudget(totalTokens) {
  return {
    activeWork: Math.floor(totalTokens * CONTEXT_RATIOS.ACTIVE_WORK),
    research: Math.floor(totalTokens * CONTEXT_RATIOS.RESEARCH),
    overhead: Math.floor(totalTokens * CONTEXT_RATIOS.OVERHEAD),
    total: totalTokens,
  };
}

// =============================================================================
// PROGRESSIVE DISCLOSURE (D × O × L)
// =============================================================================

/**
 * Context disclosure levels matching K-Score components
 * D (Diamond) = Index - minimal tokens, high conviction filter
 * O (Organic) = Timeline - connection context
 * L (Longevity) = Full - complete data only if justified
 */
const DISCLOSURE_LEVELS = Object.freeze({
  // INDEX: IDs + metadata only (~50 tokens/result)
  INDEX: {
    name: 'Diamond',
    maxTokensPerResult: 50,
    purpose: 'Filter relevance - is this pertinent?',
    threshold: 0,  // Always allowed
  },

  // TIMELINE: Context around results (~500 tokens)
  TIMELINE: {
    name: 'Organic',
    maxTokensPerResult: 500,
    purpose: 'Connection mapping - how does it relate?',
    threshold: PHI_INVERSE,  // Require 61.8% relevance from INDEX
  },

  // FULL: Complete data (variable tokens)
  FULL: {
    name: 'Longevity',
    maxTokensPerResult: -1,  // Unlimited
    purpose: 'Complete understanding - worth the cost?',
    threshold: PHI_INVERSE_SQUARED,  // Require 38.2% combined D×O score
  },
});

/**
 * Calculate disclosure score (geometric mean)
 * @param {number} indexRelevance - 0-1 relevance from index search
 * @param {number} timelineConnection - 0-1 connection strength
 * @returns {number} Combined score determining if FULL fetch is justified
 */
function calculateDisclosureScore(indexRelevance, timelineConnection) {
  // Geometric mean - if either is 0, result is 0
  return Math.sqrt(indexRelevance * timelineConnection);
}

/**
 * Determine if full fetch is justified
 * @param {number} disclosureScore - From calculateDisclosureScore
 * @returns {boolean} Whether to proceed with FULL disclosure
 */
function shouldFetchFull(disclosureScore) {
  return disclosureScore >= DISCLOSURE_LEVELS.FULL.threshold;
}

// =============================================================================
// TOKEN CONVICTION CLASSES
// =============================================================================

/**
 * Token conviction classification (mirrors holder behavior)
 * ACCUMULATOR: Adds net value to context
 * HOLDER: Neutral, necessary context
 * REDUCER: Slight negative (partial repetition)
 * EXTRACTOR: Strong negative (errors, hallucinations)
 */
const TOKEN_CONVICTION = Object.freeze({
  ACCUMULATOR: {
    name: 'Accumulator',
    multiplier: PHI,              // 1.618x value
    examples: [
      'Working code',
      'Validated decision',
      'Bug fix',
      'New understanding',
    ],
    color: '#00ff00',
  },

  HOLDER: {
    name: 'Holder',
    multiplier: 1.0,              // 1.0x value
    examples: [
      'Necessary context',
      'Docs read once',
      'Setup/boilerplate',
    ],
    color: '#888888',
  },

  REDUCER: {
    name: 'Reducer',
    multiplier: PHI_INVERSE,      // 0.618x value
    examples: [
      'Partial repetition',
      'Exploration without result',
      'Redundant verification',
    ],
    color: '#ffaa00',
  },

  EXTRACTOR: {
    name: 'Extractor',
    multiplier: PHI_INVERSE_SQUARED,  // 0.382x value
    examples: [
      'Same error 3x',
      'Hallucination',
      'Off-topic tangent',
      'Rejected approach',
    ],
    color: '#ff0000',
  },
});

/**
 * Calculate token efficiency for a session
 * @param {Array<{tokens: number, conviction: string}>} tokenUsage - Array of token usage records
 * @returns {number} Efficiency score 0-1
 */
function calculateTokenEfficiency(tokenUsage) {
  if (!tokenUsage || tokenUsage.length === 0) return 1.0;

  let weightedSum = 0;
  let totalTokens = 0;

  for (const usage of tokenUsage) {
    const conviction = TOKEN_CONVICTION[usage.conviction] || TOKEN_CONVICTION.HOLDER;
    weightedSum += usage.tokens * conviction.multiplier;
    totalTokens += usage.tokens;
  }

  if (totalTokens === 0) return 1.0;

  // Normalize to 0-1 range (max possible is PHI)
  return Math.min(1, weightedSum / (totalTokens * PHI));
}

// =============================================================================
// EFFICIENCY FLOOR (Quality Preservation)
// =============================================================================

/**
 * Minimum context threshold - never compress below this
 * Based on: minFee = (cost / TREASURY) × SAFETY_MARGIN
 */
const EFFICIENCY_FLOOR = Object.freeze({
  TREASURY_RATIO: PHI_INVERSE_CUBED,  // 0.236
  SAFETY_MARGIN: 1.2,

  // Absolute minimums per task type
  MINIMUMS: {
    trivial: 1000,      // Simple lookup
    simple: 2500,       // Single file edit
    moderate: 5000,     // Multi-file change
    complex: 10000,     // Architecture change
    critical: 20000,    // System-wide refactor
  },
});

/**
 * Calculate minimum context required for a task
 * @param {number} taskComplexity - Estimated tokens needed
 * @param {string} taskType - One of EFFICIENCY_FLOOR.MINIMUMS keys
 * @returns {number} Minimum context tokens (never compress below)
 */
function calculateMinContext(taskComplexity, taskType = 'moderate') {
  const floorMinimum = EFFICIENCY_FLOOR.MINIMUMS[taskType] || EFFICIENCY_FLOOR.MINIMUMS.moderate;
  const calculatedMinimum = Math.ceil(
    (taskComplexity / EFFICIENCY_FLOOR.TREASURY_RATIO) * EFFICIENCY_FLOOR.SAFETY_MARGIN
  );

  return Math.max(floorMinimum, calculatedMinimum);
}

// =============================================================================
// SESSION QUALITY (Geometric Mean)
// =============================================================================

/**
 * Session quality thresholds
 */
const SESSION_QUALITY = Object.freeze({
  EXCELLENT: 80,    // Continue freely
  GOOD: 60,         // Minor optimization needed
  WARNING: 40,      // Consider /compact
  CRITICAL: 25,     // Recommend /rewind
  FAILED: 0,        // Force /new
});

/**
 * Calculate session quality score using geometric mean
 * Mirrors K-Score: K = 100 × ∛(D × O × L)
 *
 * @param {Object} metrics - Session metrics
 * @param {number} metrics.tokenEfficiency - 0-1 (D: Accumulators vs Extractors)
 * @param {number} metrics.taskCompletionRate - 0-1 (O: todos done / created)
 * @param {number} metrics.contextFreshness - 0-1 (L: decay over time)
 * @returns {number} Quality score 0-100
 */
function calculateSessionQuality(metrics) {
  const { tokenEfficiency = 1, taskCompletionRate = 1, contextFreshness = 1 } = metrics;

  // Geometric mean - if any dimension is 0, quality is 0
  const geometricMean = Math.cbrt(tokenEfficiency * taskCompletionRate * contextFreshness);

  return Math.round(100 * geometricMean);
}

/**
 * Get recommended action based on session quality
 * @param {number} quality - From calculateSessionQuality
 * @returns {Object} Recommendation with action and reason
 */
function getQualityRecommendation(quality) {
  if (quality >= SESSION_QUALITY.EXCELLENT) {
    return { action: 'continue', reason: 'Session quality excellent', urgency: 'none' };
  }
  if (quality >= SESSION_QUALITY.GOOD) {
    return { action: 'monitor', reason: 'Minor efficiency loss', urgency: 'low' };
  }
  if (quality >= SESSION_QUALITY.WARNING) {
    return { action: '/compact', reason: 'Context degradation detected', urgency: 'medium' };
  }
  if (quality >= SESSION_QUALITY.CRITICAL) {
    return { action: '/rewind', reason: 'Quality below threshold', urgency: 'high' };
  }
  return { action: '/new', reason: 'Session failed - geometric mean collapsed', urgency: 'critical' };
}

// =============================================================================
// CONTEXT FRESHNESS DECAY
// =============================================================================

/**
 * Context freshness decay function
 * Mirrors Longevity component: relevance decreases over time/messages
 */
const FRESHNESS_DECAY = Object.freeze({
  HALF_LIFE_MESSAGES: 20,  // Context 50% relevant after 20 messages
  MIN_FRESHNESS: 0.1,      // Never decay below 10%
});

/**
 * Calculate context freshness based on message age
 * @param {number} messageAge - Messages since context was added
 * @returns {number} Freshness 0-1
 */
function calculateFreshness(messageAge) {
  const decay = Math.pow(0.5, messageAge / FRESHNESS_DECAY.HALF_LIFE_MESSAGES);
  return Math.max(FRESHNESS_DECAY.MIN_FRESHNESS, decay);
}

// =============================================================================
// SUBAGENT TOKEN BUDGETS
// =============================================================================

/**
 * Subagent token budgets using normalized φ ratios
 * Uses same partition as CONTEXT_RATIOS: φ⁻¹ + (φ⁻² × φ⁻¹) + (φ⁻² × φ⁻²) = 1.0
 */
const SUBAGENT_BUDGETS = Object.freeze({
  // PRIMARY: Gets the golden ratio share
  librarian: {
    ratio: PHI_INVERSE,                           // 61.8% of research budget
    model: 'sonnet',
    maxOutputTokens: 500,
    priority: 'PRIMARY',
    purpose: 'Documentation lookup, context7, external research',
  },

  // SECONDARY: Gets φ⁻² × φ⁻¹ share
  'integrity-auditor': {
    ratio: PHI_INVERSE_SQUARED * PHI_INVERSE,     // 23.6% of research budget
    model: 'haiku',
    maxOutputTokens: 300,
    priority: 'SECONDARY',
    purpose: 'Signature verification, HMAC audit',
  },

  // TERTIARY: Gets φ⁻² × φ⁻² share
  'commit-analyzer': {
    ratio: PHI_INVERSE_SQUARED * PHI_INVERSE_SQUARED,  // 14.6% of research budget
    model: 'sonnet',
    maxOutputTokens: 400,
    priority: 'TERTIARY',
    purpose: 'Git history, pattern analysis',
  },
});

// Verification: 0.618 + 0.236 + 0.146 = 1.0 ✓

/**
 * Calculate token budget for a specific subagent
 * @param {number} totalResearchBudget - Total tokens for research category
 * @param {string} agentName - Subagent name
 * @returns {number} Token budget for this agent
 */
function getSubagentBudget(totalResearchBudget, agentName) {
  const agent = SUBAGENT_BUDGETS[agentName];
  if (!agent) return Math.floor(totalResearchBudget * PHI_INVERSE_CUBED);

  return Math.floor(totalResearchBudget * agent.ratio);
}

// =============================================================================
// WATCHDOG THRESHOLDS
// =============================================================================

/**
 * Context watchdog configuration (mirrors integrityWatchdog)
 */
const WATCHDOG_CONFIG = Object.freeze({
  // Tamper detection thresholds
  TAMPER_THRESHOLDS: {
    sameError: 3,           // Same error N times = TAMPERED
    contextUsage: 0.9,      // Context > 90% = TAMPERED
    subagentTimeout: 3,     // Subagent timeout N times = TAMPERED
  },

  // Snapshot triggers
  SNAPSHOT_TRIGGERS: [
    'todo_completed',       // After task marked done
    'commit_success',       // After successful commit
    'before_exploration',   // Before risky operation
  ],

  // Auto-heal actions
  HEAL_ACTIONS: {
    minor: '/compact',
    moderate: '/rewind',
    severe: '/new',
  },
});

// =============================================================================
// MCP CONSENSUS
// =============================================================================

/**
 * Multi-node verification for context (mirrors distributed integrity)
 */
const MCP_CONSENSUS = Object.freeze({
  // Minimum nodes agreeing for high confidence
  MIN_CONSENSUS: 2,

  // Node weights (trust levels)
  NODE_WEIGHTS: {
    'context7': PHI_INVERSE,          // 0.618 - external docs
    'claude-mem': PHI_INVERSE,        // 0.618 - historical memory
    'render': PHI_INVERSE_SQUARED,    // 0.382 - deployment state
    'ide': PHI_INVERSE_CUBED,         // 0.236 - local diagnostics
  },
});

/**
 * Calculate consensus score from multiple MCP sources
 * @param {Array<{source: string, confirms: boolean}>} verifications
 * @returns {number} Consensus score 0-1
 */
function calculateConsensus(verifications) {
  if (!verifications || verifications.length === 0) return 0;

  let weightedConfirms = 0;
  let totalWeight = 0;

  for (const v of verifications) {
    const weight = MCP_CONSENSUS.NODE_WEIGHTS[v.source] || PHI_INVERSE_CUBED;
    if (v.confirms) weightedConfirms += weight;
    totalWeight += weight;
  }

  return totalWeight > 0 ? weightedConfirms / totalWeight : 0;
}

// =============================================================================
// HELIUS OPTIMIZATION (Phi-Aligned)
// =============================================================================

/**
 * Helius-specific optimizations following φ ratios
 */
const HELIUS_PHI = Object.freeze({
  // Conviction thresholds for holder classification
  CONVICTION_THRESHOLDS: {
    accumulator: PHI,         // buy/sell > 1.618
    holder: 1.0,              // buy/sell ≈ 1.0
    reducer: PHI_INVERSE,     // buy/sell < 0.618
    extractor: PHI_INVERSE_SQUARED,  // buy/sell < 0.382
  },

  // Rate limit tiers (example alignment)
  RATE_TIERS: {
    free: Math.floor(10 / PHI),           // 6 rps
    starter: Math.floor(10 * PHI),        // 16 rps
    growth: Math.floor(10 * PHI_SQUARED), // 26 rps
    business: Math.floor(10 * PHI_CUBED), // 42 rps
  },

  // Webhook batch windows (phi progression)
  BATCH_WINDOWS: {
    realtime: 1000,                       // 1s
    fast: Math.floor(1000 * PHI),         // 1.6s
    normal: Math.floor(1000 * PHI_SQUARED), // 2.6s
    relaxed: Math.floor(1000 * PHI_CUBED),  // 4.2s
  },
});

// =============================================================================
// EXPORTS
// =============================================================================

module.exports = {
  // Fundamental constants
  PHI,
  PHI_INVERSE,
  PHI_SQUARED,
  PHI_CUBED,
  PHI_INVERSE_SQUARED,
  PHI_INVERSE_CUBED,

  // Context management
  CONTEXT_RATIOS,
  calculateContextBudget,

  // Progressive disclosure
  DISCLOSURE_LEVELS,
  calculateDisclosureScore,
  shouldFetchFull,

  // Token conviction
  TOKEN_CONVICTION,
  calculateTokenEfficiency,

  // Efficiency floor
  EFFICIENCY_FLOOR,
  calculateMinContext,

  // Session quality
  SESSION_QUALITY,
  calculateSessionQuality,
  getQualityRecommendation,

  // Freshness decay
  FRESHNESS_DECAY,
  calculateFreshness,

  // Subagent budgets
  SUBAGENT_BUDGETS,
  getSubagentBudget,

  // Watchdog
  WATCHDOG_CONFIG,

  // MCP consensus
  MCP_CONSENSUS,
  calculateConsensus,

  // Helius optimization
  HELIUS_PHI,
};
