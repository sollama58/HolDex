/**
 * New Token Listener
 * * NOTE: The "Sniper" functionality that relied on DexScreener has been removed.
 * Tokens are now indexed "On-Demand" when:
 * 1. A user searches for the Mint Address in the UI.
 * 2. A user submits a "Community Update".
 * * This ensures we strictly use on-chain data and avoid third-party API dependencies.
 */
const logger = require('../services/logger');

function start(deps) {
    logger.info("ℹ️  Indexing Mode: On-Demand Only.");
    logger.info("    To index a new token, search for its Mint Address in the API/UI.");
}

module.exports = { start };
