/**
 * K-Score Updater Module Index
 *
 * This module has been refactored from a single 3800+ line file into
 * organized submodules for better maintainability:
 *
 * - ./circuitBreaker.js  - Helius API circuit breaker
 * - ./holderSnapshots.js - Holder snapshot management
 * - ./convictionAnalysis.js - Conviction score calculation
 * - ./tokenSecurity.js   - Security checks (mint/freeze authority)
 * - ./burnAnalysis.js    - Burn calculations
 * - ./lpAnalysis.js      - LP status checks
 * - ./kScoreCalculation.js - Core K-Score algorithm
 *
 * The main kScoreUpdater.js remains as the orchestrator that imports
 * these modules and runs the update cycle.
 */

// Re-export all submodules for backwards compatibility
module.exports = {
    // The main entry point remains kScoreUpdater.js
    // These exports allow direct access to submodule functions if needed
    get circuitBreaker() { return require('./circuitBreaker'); },
    get holderSnapshots() { return require('./holderSnapshots'); },
    get kScoreCalculation() { return require('./kScoreCalculation'); }
};
