/**
 * Structured Logger Service
 *
 * Provides consistent logging format across all services with support for:
 * - JSON structured output (for log aggregation: ELK, Datadog, etc.)
 * - Correlation IDs for request tracing
 * - Performance metrics
 * - Context metadata
 *
 * Output Modes:
 * - JSON mode (production): {"timestamp":"...","level":"INFO","message":"...","meta":{}}
 * - Pretty mode (development): [timestamp] [LEVEL] message
 *
 * Environment Variables:
 * - LOG_FORMAT: 'json' | 'pretty' (default: json in production, pretty in dev)
 * - SERVICE_NAME: Service identifier (default: 'holdex-api')
 * - DEBUG: 'true' to enable debug logs in production
 */

const isDev = process.env.NODE_ENV !== 'production';
const isJsonMode = process.env.LOG_FORMAT === 'json' || (!isDev && process.env.LOG_FORMAT !== 'pretty');

// Service identification
const SERVICE_NAME = process.env.SERVICE_NAME || 'holdex-api';
const SERVICE_VERSION = process.env.npm_package_version || '1.0.0';
const NODE_ID = process.env.NODE_ID || null;

// Async local storage for correlation ID tracking
const { AsyncLocalStorage } = require('async_hooks');
const asyncLocalStorage = new AsyncLocalStorage();

/**
 * Get current correlation ID from async context
 */
function getCorrelationId() {
    const store = asyncLocalStorage.getStore();
    return store?.correlationId || null;
}

/**
 * Run function with correlation ID context
 */
function withCorrelationId(correlationId, fn) {
    return asyncLocalStorage.run({ correlationId }, fn);
}

/**
 * Generate a new correlation ID
 */
function generateCorrelationId() {
    return `${Date.now().toString(36)}-${Math.random().toString(36).substring(2, 9)}`;
}

/**
 * Format log entry as JSON (for log aggregation)
 */
function formatJson(level, message, meta = {}) {
    const entry = {
        timestamp: new Date().toISOString(),
        level,
        service: SERVICE_NAME,
        version: SERVICE_VERSION,
        message: typeof message === 'string' ? message : JSON.stringify(message),
        correlationId: getCorrelationId(),
        nodeId: NODE_ID,
        ...meta
    };

    // Remove null/undefined fields for cleaner output
    Object.keys(entry).forEach(key => {
        if (entry[key] === null || entry[key] === undefined) {
            delete entry[key];
        }
    });

    return JSON.stringify(entry);
}

/**
 * Format log entry as pretty text (for development)
 */
function formatPretty(level, message, meta = {}) {
    const timestamp = new Date().toISOString();
    const correlationId = getCorrelationId();
    const corrStr = correlationId ? ` [${correlationId}]` : '';
    const metaStr = Object.keys(meta).length > 0 ? ` ${JSON.stringify(meta)}` : '';
    return `[${timestamp}] [${level}]${corrStr} ${message}${metaStr}`;
}

/**
 * Core log function
 */
function log(level, message, meta = {}) {
    // Format and output
    if (isJsonMode) {
        const output = formatJson(level, message, meta);
        if (level === 'ERROR') {
            console.error(output);
        } else if (level === 'WARN') {
            console.warn(output);
        } else {
            console.log(output);
        }
    } else {
        const output = formatPretty(level, message, meta);
        if (level === 'ERROR') {
            console.error(output);
        } else if (level === 'WARN') {
            console.warn(output);
        } else {
            console.log(output);
        }
    }
}

const logger = {
    info: (message, meta = {}) => {
        log('INFO', message, meta);
    },
    warn: (message, meta = {}) => {
        log('WARN', message, meta);
    },
    error: (message, meta = {}) => {
        log('ERROR', message, meta);
    },
    debug: (message, meta = {}) => {
        if (isDev || process.env.DEBUG === 'true') {
            log('DEBUG', message, meta);
        }
    },

    // Performance timing helper
    time: (label) => {
        const start = process.hrtime.bigint();
        return {
            end: (meta = {}) => {
                const end = process.hrtime.bigint();
                const durationMs = Number(end - start) / 1_000_000;
                log('INFO', `${label} completed`, { ...meta, durationMs: Math.round(durationMs * 100) / 100 });
                return durationMs;
            }
        };
    },

    // Request logging helper
    request: (req, meta = {}) => {
        const correlationId = req.headers['x-correlation-id'] || getCorrelationId() || generateCorrelationId();
        log('INFO', `${req.method} ${req.path}`, {
            method: req.method,
            path: req.path,
            ip: req.ip || req.headers['x-forwarded-for'],
            userAgent: req.headers['user-agent'],
            correlationId,
            ...meta
        });
    },

    // Response logging helper
    response: (req, res, durationMs, meta = {}) => {
        log('INFO', `${req.method} ${req.path} -> ${res.statusCode}`, {
            method: req.method,
            path: req.path,
            statusCode: res.statusCode,
            durationMs: Math.round(durationMs * 100) / 100,
            ...meta
        });
    },

    // Correlation ID utilities
    getCorrelationId,
    withCorrelationId,
    generateCorrelationId,

    // Child logger with preset context
    child: (context) => {
        return {
            info: (message, meta = {}) => logger.info(message, { ...context, ...meta }),
            warn: (message, meta = {}) => logger.warn(message, { ...context, ...meta }),
            error: (message, meta = {}) => logger.error(message, { ...context, ...meta }),
            debug: (message, meta = {}) => logger.debug(message, { ...context, ...meta })
        };
    }
};

module.exports = logger;
