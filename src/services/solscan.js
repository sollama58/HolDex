const axios = require('axios');
const logger = require('./logger');

/**
 * Fetches market data from Solscan's Public API.
 * Note: Subject to rate limits.
 */
async function fetchSolscanData(mint) {
    try {
        // Using the public market endpoint
        const url = `https://public-api.solscan.io/market/token/${mint}`;
        
        // Short timeout to prevent hanging if API is slow
        const res = await axios.get(url, { 
            timeout: 3000,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        });
        
        if (res.data) {
            return {
                priceUsd: parseFloat(res.data.priceUsd || 0),
                volume24h: parseFloat(res.data.volumeUsd24h || 0),
                marketCap: parseFloat(res.data.marketCap || res.data.marketCapFD || 0)
            };
        }
    } catch (e) {
        // Suppress generic 429/403 errors to keep logs clean, as these are expected on public APIs
        if (e.response && (e.response.status === 429 || e.response.status === 403)) {
            // logger.debug(`Solscan Rate Limit for ${mint}`);
        } else {
            // logger.warn(`Solscan fetch error for ${mint}: ${e.message}`);
        }
    }
    return null;
}

module.exports = { fetchSolscanData };
