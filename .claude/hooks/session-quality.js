#!/usr/bin/env node
/**
 * Session Quality Hook
 * Based on $asdfasdfa Philosophy & claude-phi.js
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
// PHI CONSTANTS (inline to avoid require path issues in hook context)
// =============================================================================

const PHI = 1.618033988749895;
const PHI_INVERSE = 1 / PHI;
const PHI_INVERSE_SQUARED = 1 / (PHI * PHI);

// =============================================================================
// CONFIGURATION
// =============================================================================

const CONFIG = {
  // State file location
  stateFile: path.join(os.tmpdir(), 'claude-session-quality.json'),

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
 */
function calculateSessionQuality(state) {
  const D = calculateTokenEfficiency(state.toolCalls);
  const O = calculateTaskCompletion(state);
  const L = calculateFreshness(state.messageCount);

  // Geometric mean - if any is 0, quality is 0
  const geometricMean = Math.cbrt(D * O * L);

  return {
    score: Math.round(100 * geometricMean),
    components: {
      efficiency: Math.round(D * 100),
      completion: Math.round(O * 100),
      freshness: Math.round(L * 100),
    },
  };
}

/**
 * Get recommendation based on quality score
 */
function getRecommendation(score) {
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

  state.lastQuality = quality.score;

  // Check for quality degradation
  const previousQuality = state.lastQuality || 100;
  const degradation = previousQuality - quality.score;

  saveState(state);

  // Output warning if quality is concerning
  if (recommendation.level === 'warning' || recommendation.level === 'critical' || recommendation.level === 'failed') {
    const output = {
      type: 'session_quality_warning',
      quality: quality.score,
      components: quality.components,
      recommendation: recommendation.action,
      message: `Session quality ${recommendation.emoji} ${quality.score}/100 (D:${quality.components.efficiency}% O:${quality.components.completion}% L:${quality.components.freshness}%)`,
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

function main() {
  const args = process.argv.slice(2);
  const command = args[0];

  switch (command) {
    case 'post-tool': {
      // Called after tool use: post-tool <toolName> <toolInput> <toolResult>
      const toolName = args[1] || 'Unknown';
      const toolInput = args[2] || '{}';
      const toolResult = args[3] || '';
      handlePostToolUse(toolName, toolInput, toolResult);
      break;
    }

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
      // When called as hook, read from stdin
      let input = '';
      process.stdin.setEncoding('utf8');
      process.stdin.on('readable', () => {
        let chunk;
        while ((chunk = process.stdin.read()) !== null) {
          input += chunk;
        }
      });
      process.stdin.on('end', () => {
        try {
          const data = JSON.parse(input);
          if (data.tool_name) {
            handlePostToolUse(
              data.tool_name,
              JSON.stringify(data.tool_input || {}),
              JSON.stringify(data.tool_result || {})
            );
          }
        } catch (e) {
          // Ignore parse errors for non-JSON input
        }
      });
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
  main();
}
