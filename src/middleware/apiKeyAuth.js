const { getDB } = require('../services/database');
const logger = require('../services/logger');

const apiKeyAuth = (required = true) => {
    return async (req, res, next) => {
        // Allow OPTIONS requests (CORS preflight)
        if (req.method === 'OPTIONS') return next();

        // 1. Check for Key in Headers or Query
        const apiKey = req.headers['x-api-key'] || req.query.api_key;

        // If not required and no key, proceed (rate limits might be handled by IP elsewhere)
        if (!required && !apiKey) return next();

        if (required && !apiKey) {
            return res.status(401).json({ success: false, error: 'API Key Required. Header: x-api-key' });
        }

        try {
            const db = getDB();
            
            // 2. Validate Key
            const keyRecord = await db.get('SELECT * FROM api_keys WHERE key = $1', [apiKey]);

            if (!keyRecord) {
                return res.status(403).json({ success: false, error: 'Invalid API Key' });
            }

            if (!keyRecord.is_active) {
                return res.status(403).json({ success: false, error: 'API Key Revoked' });
            }

            // 3. Check Daily Limit
            const now = Date.now();
            const oneDay = 24 * 60 * 60 * 1000;
            let currentUsage = parseInt(keyRecord.requests_today || 0);
            const lastReset = parseInt(keyRecord.last_reset || 0);

            // Reset if day changed
            if (now - lastReset > oneDay) {
                currentUsage = 0;
                await db.run('UPDATE api_keys SET requests_today = 0, last_reset = $1 WHERE key = $2', [now, apiKey]);
            }

            if (currentUsage >= keyRecord.requests_limit) {
                return res.status(429).json({ success: false, error: 'Daily API Limit Exceeded' });
            }

            // 4. Increment Usage (Async - don't block response)
            // We use SQL increment for atomicity
            db.run('UPDATE api_keys SET requests_today = requests_today + 1 WHERE key = $1', [apiKey]).catch(e => logger.error(`API Usage Update Fail: ${e.message}`));

            // Attach user info to request for downstream use
            req.apiUser = { owner: keyRecord.owner, tier: keyRecord.tier };
            
            next();

        } catch (e) {
            logger.error(`Auth Middleware Error: ${e.message}`);
            return res.status(500).json({ success: false, error: 'Internal Auth Error' });
        }
    };
};

module.exports = apiKeyAuth;
