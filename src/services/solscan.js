const axios = require('axios');
const logger = require('./logger');

/**
 * Fetches market data from Solscan's Public API.
 * Returns normalized object or null.
 */
async function fetchSolscanData(mint) {
    try {
        // Randomize User-Agent to avoid simple blocking
        const agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
            'Mozilla/5.0 (Linux; Android 10; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.181 Mobile Safari/537.36'
        ];
        const ua = agents[Math.floor(Math.random() * agents.length)];

        const url = `https://public-api.solscan.io/market/token/${mint}`;
        
        const res = await axios.get(url, { 
            timeout: 5000,
            headers: { 'User-Agent': ua }
        });
        
        if (res.data) {
            return {
                priceUsd: parseFloat(res.data.priceUsd || 0),
                volume24h: parseFloat(res.data.volumeUsd24h || 0),
                // Prefer Fully Diluted, fall back to standard marketCap
                marketCap: parseFloat(res.data.marketCapFD || res.data.marketCap || 0),
                change24h: parseFloat(res.data.priceChange24h || 0),
                // Parse holder count (supports 'holder' or 'holderCount' fields)
                holders: parseInt(res.data.holder || res.data.holderCount || 0)
            };
        }
    } catch (e) {
        // 429 = Rate Limit, 403 = Blocked. 
        // We fail silently so the system falls back to internal data.
        if (e.response && (e.response.status === 429 || e.response.status === 403)) {
            // logger.debug(`Solscan Rate Limit/Block for ${mint}`);
        }
    }
    return null;
}

module.exports = { fetchSolscanData };
