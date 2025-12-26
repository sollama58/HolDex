/**
 * New Token Listener
 * Disabled external polling to remove DexScreener dependency.
 * Future: Implement Helius Webhooks or LogsSubscribe here.
 */
const { logger } = require('../services');

function start(deps) {
    logger.info("ℹ️ New Token Listener is DISABLED (DexScreener Dependency Removed).");
    logger.info("   Tokens must be added via Search or API Request.");
}

module.exports = { start };
