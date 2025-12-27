const axios = require('axios');
const logger = require('./logger');

/**
 * Fetches data from Solscan's Public API.
 * Uses the 'token/meta' endpoint which is often more reliable for holder counts.
 */
async function fetchSolscanData(mint) {
    try {
        const agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
            'Mozilla/5.0 (Linux; Android 10; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.181 Mobile Safari/537.36'
        ];
        const ua = agents[Math.floor(Math.random() * agents.length)];

        // Primary: Token Meta Endpoint (Better for holders)
        const url = `https://public-api.solscan.io/token/meta?tokenAddress=${mint}`;
        
        const res = await axios.get(url, { 
            timeout: 5000,
            headers: { 'User-Agent': ua }
        });
        
        if (res.data) {
            return {
                holders: parseInt(res.data.holder || 0),
                marketCap: parseFloat(res.data.marketCap || res.data.fdv || 0),
                supply: res.data.supply || '0'
            };
        }
    } catch (e) {
        // Fallback: Market Endpoint
        try {
            if (e.response && e.response.status !== 404) {
                const url2 = `https://public-api.solscan.io/market/token/${mint}`;
                const res2 = await axios.get(url2, { timeout: 3000, headers: { 'User-Agent': ua } });
                if (res2.data) {
                    return {
                        holders: parseInt(res2.data.holderCount || res2.data.holder || 0),
                        marketCap: parseFloat(res2.data.marketCapFD || res2.data.marketCap || 0),
                        priceUsd: parseFloat(res2.data.priceUsd || 0)
                    };
                }
            }
        } catch (err2) {
            // silent fail
        }
    }
    return null;
}

// --- HYBRID EXPORT PATTERN ---
// 1. Assign the function to module.exports so require('...')() works (Backward Compatibility)
module.exports = fetchSolscanData;

// 2. Assign the named property so { fetchSolscanData } = require('...') works (New Logic)
module.exports.fetchSolscanData = fetchSolscanData;
