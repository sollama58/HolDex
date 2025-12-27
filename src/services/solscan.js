const axios = require('axios');
const logger = require('./logger');

/**
 * Fetches data from Solscan's APIs.
 * Includes multiple fallbacks to handle rate limits and endpoint changes.
 */
async function fetchSolscanData(mint) {
    const agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
        'Mozilla/5.0 (Linux; Android 10; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.181 Mobile Safari/537.36'
    ];
    
    const getHeaders = () => ({
        'User-Agent': agents[Math.floor(Math.random() * agents.length)],
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://solscan.io/'
    });

    // 1. Try Internal API (Often more reliable for Holder Counts)
    try {
        const urlInternal = `https://api.solscan.io/v1/token/meta?token=${mint}`;
        const res = await axios.get(urlInternal, { 
            timeout: 3000, 
            headers: getHeaders() 
        });

        if (res.data && res.data.data) {
            const d = res.data.data;
            // Internal API often returns holder count in 'holder'
            const holders = parseInt(d.holder || d.holderCount || 0);
            if (holders > 0) {
                return {
                    holders: holders,
                    marketCap: parseFloat(d.marketCap || d.fdv || 0),
                    supply: d.supply || '0'
                };
            }
        }
    } catch (e) {
        // Silently fail to next fallback
    }

    // 2. Try Public API: Token Meta Endpoint
    try {
        const urlPublic = `https://public-api.solscan.io/token/meta?tokenAddress=${mint}`;
        const res = await axios.get(urlPublic, { 
            timeout: 3000,
            headers: getHeaders()
        });
        
        if (res.data) {
            return {
                holders: parseInt(res.data.holder || 0),
                marketCap: parseFloat(res.data.marketCap || res.data.fdv || 0),
                supply: res.data.supply || '0'
            };
        }
    } catch (e) {
        // Fallback continues
    }

    // 3. Try Public API: Market Endpoint
    try {
        const urlMarket = `https://public-api.solscan.io/market/token/${mint}`;
        const res2 = await axios.get(urlMarket, { 
            timeout: 3000, 
            headers: getHeaders() 
        });
        if (res2.data) {
            return {
                holders: parseInt(res2.data.holderCount || res2.data.holder || 0),
                marketCap: parseFloat(res2.data.marketCapFD || res2.data.marketCap || 0),
                priceUsd: parseFloat(res2.data.priceUsd || 0)
            };
        }
    } catch (err2) {
        // All lookups failed
        // logger.debug(`Solscan Metadata Fetch Failed for ${mint}: ${err2.message}`);
    }
    
    return null;
}

module.exports = { fetchSolscanData };
