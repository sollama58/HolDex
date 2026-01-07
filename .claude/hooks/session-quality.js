#!/usr/bin/env node
/**
 * Session Quality Hook
 * Based on $asdfasdfa Philosophy & geometric-quality.js
 *
 * Monitors session health using geometric mean: Q = 100 × ∛(D × O × L)
 * - D (Diamond/Efficiency): Token conviction ratio
 * - O (Organic/Completion): Task completion rate
 * - L (Longevity/Freshness): Context decay over time
 *
 * Usage: Called by Claude Code hooks on PostToolUse events
 */

'use strict';

const fs = require('fs');
const path = require('path');
const os = require('os');

// =============================================================================
// SHARED MODULE IMPORTS
// =============================================================================

// Dynamic project root detection (works in any codespace location)
const PROJECT_ROOT = process.env.CLAUDE_PROJECT_DIR ||
                     process.cwd() ||
                     path.dirname(path.dirname(__dirname));

let geometricQuality;
let PHI, PHI_INVERSE, PHI_INVERSE_SQUARED;

// Inline constants (always available, no external dependency)
PHI = 1.618033988749895;
PHI_INVERSE = 1 / PHI;
PHI_INVERSE_SQUARED = 1 / (PHI * PHI);

// Try to load shared modules for enhanced features
try {
  geometricQuality = require(path.join(PROJECT_ROOT, 'src/shared/geometric-quality.js'));
  const claudePhi = require(path.join(PROJECT_ROOT, 'src/shared/claude-phi.js'));
  PHI = claudePhi.PHI;
  PHI_INVERSE = claudePhi.PHI_INVERSE;
  PHI_INVERSE_SQUARED = claudePhi.PHI_INVERSE_SQUARED;
} catch (e) {
  // Shared modules not critical - inline constants work fine
  geometricQuality = null;
}

// =============================================================================
// CONFIGURATION
// =============================================================================

// Persistent storage path (survives container rebuild)
// Priority: env var > codespace shared > project local > temp
const PERSISTENT_CANDIDATES = [
  process.env.CLAUDE_SESSION_STATE_DIR,
  '/workspaces/.claude-mem-data',
  path.join(PROJECT_ROOT, '.claude', 'state'),
].filter(Boolean);

const PERSISTENT_DIR = PERSISTENT_CANDIDATES.find(dir => {
  try {
    if (fs.existsSync(dir)) return true;
    // Try to create if parent exists
    const parent = path.dirname(dir);
    if (fs.existsSync(parent)) {
      fs.mkdirSync(dir, { recursive: true });
      return true;
    }
  } catch (e) { /* ignore */ }
  return false;
}) || os.tmpdir();

const SESSION_STATE_FILE = path.join(PERSISTENT_DIR, 'claude-session-quality.json');

const CONFIG = {
  // State file location (persistent if available)
  stateFile: SESSION_STATE_FILE,

  // Quality thresholds
  thresholds: {
    excellent: 80,
    good: 60,
    warning: 40,
    critical: 25,
  },

  // Freshness decay
  halfLifeMessages: 20,
  minFreshness: 0.1,

  // Token conviction multipliers
  conviction: {
    accumulator: PHI,           // 1.618x
    holder: 1.0,
    reducer: PHI_INVERSE,       // 0.618x
    extractor: PHI_INVERSE_SQUARED,  // 0.382x
  },

  // Tool classifications (which tools produce value vs consume)
  toolClassification: {
    // ACCUMULATOR: Produces lasting value
    accumulator: [
      'Edit', 'Write', 'NotebookEdit',
      'TodoWrite', 'Bash:git commit', 'Bash:npm test',
    ],
    // HOLDER: Necessary context gathering
    holder: [
      'Read', 'Glob', 'Grep', 'LSP',
      'WebFetch', 'WebSearch', 'Task',
    ],
    // REDUCER: Potentially redundant
    reducer: [
      'AskUserQuestion',  // If asked same thing twice
    ],
    // EXTRACTOR: Errors, retries
    extractor: [
      // Detected dynamically via error patterns
    ],
  },

  // Patterns indicating extraction (negative value)
  extractorPatterns: [
    /error/i,
    /failed/i,
    /retry/i,
    /permission denied/i,
    /not found/i,
  ],
};

// =============================================================================
// STATE MANAGEMENT
// =============================================================================

function loadState() {
  try {
    if (fs.existsSync(CONFIG.stateFile)) {
      const data = fs.readFileSync(CONFIG.stateFile, 'utf8');
      return JSON.parse(data);
    }
  } catch (e) {
    // Ignore read errors, start fresh
  }

  return {
    sessionStart: Date.now(),
    messageCount: 0,
    toolCalls: [],
    todosCreated: 0,
    todosCompleted: 0,
    lastQuality: 100,
    warnings: [],
  };
}

function saveState(state) {
  try {
    fs.writeFileSync(CONFIG.stateFile, JSON.stringify(state, null, 2));
  } catch (e) {
    // Ignore write errors
  }
}

function resetState() {
  const freshState = {
    sessionStart: Date.now(),
    messageCount: 0,
    toolCalls: [],
    todosCreated: 0,
    todosCompleted: 0,
    lastQuality: 100,
    warnings: [],
  };
  saveState(freshState);
  return freshState;
}

// =============================================================================
// METRIC CALCULATIONS
// =============================================================================

/**
 * Calculate token efficiency (D component)
 * Based on ratio of accumulator vs extractor tool calls
 */
function calculateTokenEfficiency(toolCalls) {
  if (toolCalls.length === 0) return 1.0;

  let weightedSum = 0;
  let count = 0;

  for (const call of toolCalls) {
    const multiplier = CONFIG.conviction[call.conviction] || 1.0;
    weightedSum += multiplier;
    count++;
  }

  // Normalize: max possible is PHI (all accumulators)
  return Math.min(1, weightedSum / (count * PHI));
}

/**
 * Calculate task completion rate (O component)
 * Based on todos completed vs created
 */
function calculateTaskCompletion(state) {
  if (state.todosCreated === 0) return 1.0;  // No tasks = 100% complete

  return Math.min(1, state.todosCompleted / state.todosCreated);
}

/**
 * Calculate context freshness (L component)
 * Decays over message count
 */
function calculateFreshness(messageCount) {
  const decay = Math.pow(0.5, messageCount / CONFIG.halfLifeMessages);
  return Math.max(CONFIG.minFreshness, decay);
}

/**
 * Calculate overall session quality using geometric mean
 * Q = 100 × ∛(D × O × L)
 * Uses shared geometric-quality module when available
 */
function calculateSessionQuality(state) {
  const D = calculateTokenEfficiency(state.toolCalls);
  const O = calculateTaskCompletion(state);
  const L = calculateFreshness(state.messageCount);

  // Use shared module if available
  if (geometricQuality) {
    const result = new geometricQuality.QualityBuilder('SESSION')
      .setDimensions({ D, O, L })
      .calculate();

    return {
      score: result.score,
      components: {
        efficiency: Math.round(D * 100),
        completion: Math.round(O * 100),
        freshness: Math.round(L * 100),
      },
    };
  }

  // Fallback: inline geometric mean
  const mean = Math.cbrt(D * O * L);

  return {
    score: Math.round(100 * mean),
    components: {
      efficiency: Math.round(D * 100),
      completion: Math.round(O * 100),
      freshness: Math.round(L * 100),
    },
  };
}

/**
 * Get recommendation based on quality score
 * Uses shared geometric-quality module when available
 */
function getRecommendation(score) {
  // Use shared module if available
  if (geometricQuality) {
    const evaluation = geometricQuality.evaluateQuality(
      score,
      geometricQuality.QUALITY_PROFILES.SESSION.thresholds
    );
    // Map generic actions to session-specific commands
    const actionMap = {
      review: '/compact',
      intervene: '/rewind',
      reset: '/new',
    };
    return {
      level: evaluation.level,
      action: actionMap[evaluation.action] || evaluation.action,
      emoji: evaluation.emoji,
    };
  }

  // Fallback: inline thresholds
  if (score >= CONFIG.thresholds.excellent) {
    return { level: 'excellent', action: null, emoji: '🟢' };
  }
  if (score >= CONFIG.thresholds.good) {
    return { level: 'good', action: null, emoji: '🟡' };
  }
  if (score >= CONFIG.thresholds.warning) {
    return { level: 'warning', action: '/compact', emoji: '🟠' };
  }
  if (score >= CONFIG.thresholds.critical) {
    return { level: 'critical', action: '/rewind', emoji: '🔴' };
  }
  return { level: 'failed', action: '/new', emoji: '⛔' };
}

// =============================================================================
// TOOL CLASSIFICATION
// =============================================================================

function classifyTool(toolName, toolInput, toolResult) {
  // Check for error patterns in result (extractor)
  if (toolResult) {
    const resultStr = typeof toolResult === 'string' ? toolResult : JSON.stringify(toolResult);
    for (const pattern of CONFIG.extractorPatterns) {
      if (pattern.test(resultStr)) {
        return 'extractor';
      }
    }
  }

  // Check tool name against classifications
  for (const [conviction, tools] of Object.entries(CONFIG.toolClassification)) {
    for (const tool of tools) {
      if (toolName.startsWith(tool.split(':')[0])) {
        // Check for specific subcommand if specified
        if (tool.includes(':')) {
          const subCommand = tool.split(':')[1];
          const inputStr = typeof toolInput === 'string' ? toolInput : JSON.stringify(toolInput);
          if (inputStr.includes(subCommand)) {
            return conviction;
          }
        } else {
          return conviction;
        }
      }
    }
  }

  return 'holder';  // Default
}

// =============================================================================
// HOOK HANDLERS
// =============================================================================

function handlePostToolUse(toolName, toolInput, toolResult) {
  const state = loadState();

  // Classify and record tool call
  const conviction = classifyTool(toolName, toolInput, toolResult);
  state.toolCalls.push({
    tool: toolName,
    conviction,
    timestamp: Date.now(),
  });

  // Keep only last 100 tool calls for efficiency
  if (state.toolCalls.length > 100) {
    state.toolCalls = state.toolCalls.slice(-100);
  }

  // Track todo changes
  if (toolName === 'TodoWrite') {
    try {
      const todos = JSON.parse(toolInput).todos || [];
      const completed = todos.filter(t => t.status === 'completed').length;
      const total = todos.length;

      state.todosCreated = Math.max(state.todosCreated, total);
      state.todosCompleted = completed;
    } catch (e) {
      // Ignore parse errors
    }
  }

  state.messageCount++;

  // Calculate quality
  const quality = calculateSessionQuality(state);
  const recommendation = getRecommendation(quality.score);

  // Track degradation (before updating lastQuality)
  const previousQuality = state.lastQuality || 100;
  const degradation = previousQuality - quality.score;
  state.lastQuality = quality.score;

  saveState(state);

  // Output warning if quality is concerning OR rapid degradation detected
  const rapidDegradation = degradation > 10; // More than 10 points drop
  if (recommendation.level === 'warning' || recommendation.level === 'critical' || recommendation.level === 'failed' || rapidDegradation) {
    const output = {
      type: 'session_quality_warning',
      quality: quality.score,
      components: quality.components,
      recommendation: recommendation.action,
      degradation: rapidDegradation ? degradation : undefined,
      message: `Session quality ${recommendation.emoji} ${quality.score}/100 (D:${quality.components.efficiency}% O:${quality.components.completion}% L:${quality.components.freshness}%)${rapidDegradation ? ` ⚡-${degradation}pts` : ''}`,
    };

    // Output to stderr so it shows as hook feedback
    console.error(JSON.stringify(output));
  }

  return quality;
}

function handleNotification() {
  const state = loadState();
  const quality = calculateSessionQuality(state);
  const recommendation = getRecommendation(quality.score);

  return {
    score: quality.score,
    components: quality.components,
    level: recommendation.level,
    action: recommendation.action,
    emoji: recommendation.emoji,
    messageCount: state.messageCount,
    toolCalls: state.toolCalls.length,
  };
}

function handleStop() {
  const state = loadState();
  const quality = calculateSessionQuality(state);

  // Log final session stats
  const summary = {
    type: 'session_summary',
    duration: Date.now() - state.sessionStart,
    messageCount: state.messageCount,
    toolCalls: state.toolCalls.length,
    finalQuality: quality.score,
    components: quality.components,
    todosCreated: state.todosCreated,
    todosCompleted: state.todosCompleted,
  };

  console.log(JSON.stringify(summary, null, 2));

  // Reset for next session
  resetState();
}

// =============================================================================
// CLI INTERFACE
// =============================================================================

/**
 * Read from stdin with timeout (Claude Code sends JSON via stdin)
 * @returns {Promise<string>} stdin content
 */
function readStdin(timeoutMs = 1000) {
  return new Promise((resolve) => {
    let input = '';
    let resolved = false;

    const timeout = setTimeout(() => {
      if (!resolved) {
        resolved = true;
        resolve(input);
      }
    }, timeoutMs);

    process.stdin.setEncoding('utf8');
    process.stdin.on('readable', () => {
      let chunk;
      while ((chunk = process.stdin.read()) !== null) {
        input += chunk;
      }
    });
    process.stdin.on('end', () => {
      if (!resolved) {
        resolved = true;
        clearTimeout(timeout);
        resolve(input);
      }
    });

    // Handle case where stdin is empty/closed immediately
    if (process.stdin.readableEnded) {
      resolved = true;
      clearTimeout(timeout);
      resolve(input);
    }
  });
}

async function main() {
  const args = process.argv.slice(2);
  const command = args[0];

  switch (command) {
    case 'status': {
      // Get current session status
      const status = handleNotification();
      console.log(JSON.stringify(status, null, 2));
      break;
    }

    case 'reset': {
      // Reset session state
      resetState();
      console.log('Session state reset');
      break;
    }

    case 'stop': {
      // Called on session end
      handleStop();
      break;
    }

    case 'quality': {
      // Quick quality check for statusline
      const state = loadState();
      const quality = calculateSessionQuality(state);
      const rec = getRecommendation(quality.score);
      console.log(`${rec.emoji} Q:${quality.score}`);
      break;
    }

    default: {
      // Default: read from stdin (Claude Code hook format)
      // Claude Code sends JSON: {tool_name, tool_input, tool_response, ...}
      try {
        const input = await readStdin();
        if (!input.trim()) {
          // No stdin = probably manual invocation, show help
          console.log('Session Quality Hook - Commands:');
          console.log('  status  - Show current session quality');
          console.log('  reset   - Reset session state');
          console.log('  stop    - End session and show summary');
          console.log('  quality - Quick quality check for statusline');
          console.log('\nWhen called as hook, reads JSON from stdin');
          return;
        }

        const data = JSON.parse(input);
        if (data.tool_name) {
          handlePostToolUse(
            data.tool_name,
            JSON.stringify(data.tool_input || {}),
            JSON.stringify(data.tool_response || data.tool_result || {})
          );
        }
      } catch (e) {
        // Silent fail for hooks - don't disrupt Claude Code
        if (process.env.DEBUG_HOOK) {
          console.error('Hook error:', e.message);
        }
      }
    }
  }
}

// Export for testing
module.exports = {
  calculateSessionQuality,
  calculateTokenEfficiency,
  calculateFreshness,
  classifyTool,
  getRecommendation,
  loadState,
  resetState,
  CONFIG,
};

// Run if called directly
if (require.main === module) {
  main().catch(() => {});
}
