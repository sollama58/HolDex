/**
 * SOLSCAN SERVICE - DISABLED
 * * As per request, we are disabling Solscan lookups to rely purely on 
 * GeckoTerminal and direct Helius/RPC indexing for better reliability and
 * to avoid 403 Forbidden / Rate Limit issues.
 */

const _logger = require('./logger');

async function fetchSolscanData(_mint) {
    // Return null immediately to bypass Solscan completely
    return null;
}

module.exports = { fetchSolscanData };
