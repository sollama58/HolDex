/**
 * Applies Cache-Control headers to responses.
 * * @param {number} seconds - How long the browser/CDN should cache the content (max-age).
 * @param {number} staleSeconds - (Optional) How long a CDN can serve stale content while revalidating (stale-while-revalidate).
 * @returns {Function} Express middleware.
 */
const cacheControl = (seconds, staleSeconds = 0) => {
    return (req, res, next) => {
        // GET requests only. Don't cache POST/PUT/DELETE.
        if (req.method !== 'GET') {
            return next();
        }

        // 'public' = CDNs and browsers can cache this
        // 'max-age' = Browser cache time
        // 's-maxage' = CDN/Proxy cache time (often set slightly higher)
        // 'stale-while-revalidate' = Allow serving old content while fetching new in background (High Availability)
        let headerVal = `public, max-age=${seconds}, s-maxage=${seconds}`;
        
        if (staleSeconds > 0) {
            headerVal += `, stale-while-revalidate=${staleSeconds}`;
        }

        res.setHeader('Cache-Control', headerVal);
        next();
    };
};

module.exports = cacheControl;
