/**
 * Correlation ID Middleware
 *
 * Generates or propagates correlation IDs for request tracing across services.
 * The correlation ID is:
 * - Read from incoming `x-correlation-id` header (if present)
 * - Generated if not present
 * - Attached to the request object
 * - Added to the response header
 * - Used in all log entries within the request context
 *
 * Usage:
 *   app.use(correlationIdMiddleware);
 */

const logger = require('../services/logger');

/**
 * Middleware that manages correlation IDs for request tracing
 */
function correlationIdMiddleware(req, res, next) {
    // Get or generate correlation ID
    const correlationId = req.headers['x-correlation-id'] ||
                          req.headers['x-request-id'] ||
                          logger.generateCorrelationId();

    // Attach to request
    req.correlationId = correlationId;

    // Add to response headers
    res.setHeader('x-correlation-id', correlationId);

    // Run the rest of the request within the correlation ID context
    // This ensures all log entries within this request have the ID
    logger.withCorrelationId(correlationId, () => {
        next();
    });
}

/**
 * Request timing middleware
 * Logs request start and completion with timing
 */
function requestTimingMiddleware(req, res, next) {
    const startTime = process.hrtime.bigint();

    // Skip logging for health checks and static assets
    const skipPaths = ['/live', '/ready', '/health', '/favicon.ico'];
    const shouldSkip = skipPaths.some(p => req.path.startsWith(p)) ||
                       req.path.match(/\.(js|css|png|jpg|svg|ico)$/);

    if (!shouldSkip) {
        logger.debug(`Request started: ${req.method} ${req.path}`);
    }

    // Capture response finish
    res.on('finish', () => {
        if (!shouldSkip) {
            const endTime = process.hrtime.bigint();
            const durationMs = Number(endTime - startTime) / 1_000_000;

            logger.response(req, res, durationMs, {
                contentLength: res.get('content-length'),
                correlationId: req.correlationId
            });
        }
    });

    next();
}

module.exports = {
    correlationIdMiddleware,
    requestTimingMiddleware
};
