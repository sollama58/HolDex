/**
 * SECURITY EVENT LOGGER
 *
 * Centralized security event logging for HolDex
 * Provides structured audit trails for security-sensitive operations
 *
 * Features:
 * - Structured security event logging
 * - Rate-limited alert detection
 * - IP tracking and pattern detection
 * - Severity levels for prioritization
 */

'use strict';

const logger = require('../services/logger');
const { getClient } = require('../services/redis');

// ═══════════════════════════════════════════════════════════════
// CONSTANTS
// ═══════════════════════════════════════════════════════════════

const SECURITY_EVENT_TYPES = {
    // Authentication events
    AUTH_SUCCESS: 'auth.success',
    AUTH_FAILED: 'auth.failed',
    AUTH_LOCKOUT: 'auth.lockout',
    AUTH_BRUTE_FORCE: 'auth.brute_force',

    // Authorization events
    ACCESS_DENIED: 'access.denied',
    GRANT_CREATED: 'grant.created',
    GRANT_REVOKED: 'grant.revoked',
    PRIVILEGE_ESCALATION: 'privilege.escalation',

    // Input validation events
    INVALID_INPUT: 'input.invalid',
    INJECTION_ATTEMPT: 'input.injection',
    OVERFLOW_DETECTED: 'input.overflow',

    // Rate limiting events
    RATE_LIMITED: 'rate.limited',
    RATE_DEGRADED: 'rate.degraded',

    // Webhook events
    WEBHOOK_INVALID_SIG: 'webhook.invalid_signature',
    WEBHOOK_REPLAY: 'webhook.replay_attack',
    WEBHOOK_EXPIRED: 'webhook.expired_timestamp',

    // Integrity events
    SIGNATURE_INVALID: 'integrity.signature_invalid',
    DATA_TAMPERED: 'integrity.tampered',

    // System events
    CONFIG_ERROR: 'system.config_error',
    REDIS_OUTAGE: 'system.redis_outage',
    SERVICE_DEGRADED: 'system.degraded'
};

const SEVERITY = {
    CRITICAL: 'critical',
    HIGH: 'high',
    MEDIUM: 'medium',
    LOW: 'low',
    INFO: 'info'
};

// Alert thresholds for pattern detection
const ALERT_THRESHOLDS = {
    AUTH_FAILED: { count: 10, windowMs: 60000 },      // 10 failures in 1 minute
    RATE_LIMITED: { count: 50, windowMs: 60000 },     // 50 rate limits in 1 minute
    WEBHOOK_INVALID: { count: 5, windowMs: 60000 },   // 5 invalid webhooks in 1 minute
    INJECTION_ATTEMPT: { count: 3, windowMs: 300000 } // 3 injection attempts in 5 minutes
};

// In-memory event counters for pattern detection
const eventCounters = new Map();

// Cleanup old counters every 5 minutes
setInterval(() => {
    const now = Date.now();
    for (const [key, data] of eventCounters) {
        if (now - data.windowStart > 300000) { // 5 minute max retention
            eventCounters.delete(key);
        }
    }
}, 5 * 60 * 1000);

// ═══════════════════════════════════════════════════════════════
// HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════

/**
 * Get client IP address from request
 */
function getClientIP(req) {
    if (!req) return 'unknown';
    return req.headers?.['x-forwarded-for']?.split(',')[0]?.trim() ||
           req.headers?.['x-real-ip'] ||
           req.ip ||
           req.connection?.remoteAddress ||
           'unknown';
}

/**
 * Mask sensitive data for logging
 */
function maskSensitive(data) {
    if (!data) return data;
    if (typeof data === 'string') {
        // Mask long strings (potential tokens/keys)
        if (data.length > 20) {
            return `${data.slice(0, 8)}...${data.slice(-4)}`;
        }
        return data;
    }
    if (typeof data === 'object') {
        const masked = {};
        const sensitiveKeys = ['password', 'secret', 'token', 'key', 'signature', 'auth'];
        for (const [k, v] of Object.entries(data)) {
            if (sensitiveKeys.some(sk => k.toLowerCase().includes(sk))) {
                masked[k] = typeof v === 'string' ? `${v.slice(0, 4)}***` : '[REDACTED]';
            } else {
                masked[k] = v;
            }
        }
        return masked;
    }
    return data;
}

/**
 * Check if event count exceeds threshold (pattern detection)
 */
function checkThreshold(eventType, ip) {
    const threshold = ALERT_THRESHOLDS[eventType];
    if (!threshold) return { exceeded: false };

    const key = `${eventType}:${ip}`;
    const now = Date.now();

    let counter = eventCounters.get(key);
    if (!counter || now - counter.windowStart > threshold.windowMs) {
        counter = { count: 1, windowStart: now };
    } else {
        counter.count++;
    }
    eventCounters.set(key, counter);

    return {
        exceeded: counter.count >= threshold.count,
        count: counter.count,
        threshold: threshold.count
    };
}

// ═══════════════════════════════════════════════════════════════
// MAIN LOGGING FUNCTION
// ═══════════════════════════════════════════════════════════════

/**
 * Log a security event
 *
 * @param {Object} options - Event options
 * @param {string} options.type - Event type (from SECURITY_EVENT_TYPES)
 * @param {string} options.severity - Severity level (from SEVERITY)
 * @param {string} options.message - Human-readable message
 * @param {Object} options.request - Express request object (for IP extraction)
 * @param {string} options.ip - IP address (if not from request)
 * @param {string} options.wallet - Associated wallet address
 * @param {string} options.endpoint - API endpoint
 * @param {Object} options.details - Additional details
 * @param {boolean} options.alert - Force alert even if below threshold
 */
async function logSecurityEvent(options) {
    const {
        type,
        severity = SEVERITY.MEDIUM,
        message,
        request,
        ip: providedIP,
        wallet,
        endpoint,
        details = {},
        alert: forceAlert = false
    } = options;

    const ip = providedIP || getClientIP(request);
    const timestamp = new Date().toISOString();

    // Build structured log entry
    const logEntry = {
        timestamp,
        type,
        severity,
        message,
        ip,
        wallet: wallet ? maskSensitive(wallet) : undefined,
        endpoint: endpoint || request?.originalUrl,
        method: request?.method,
        userAgent: request?.headers?.['user-agent']?.slice(0, 100),
        details: maskSensitive(details)
    };

    // Check for pattern-based alerting
    let patternAlert = false;
    if (type === SECURITY_EVENT_TYPES.AUTH_FAILED) {
        const check = checkThreshold('AUTH_FAILED', ip);
        if (check.exceeded) {
            patternAlert = true;
            logEntry.patternAlert = `Threshold exceeded: ${check.count}/${check.threshold} in window`;
        }
    }

    // Log based on severity
    const logMessage = `[SECURITY] ${JSON.stringify(logEntry)}`;

    if (severity === SEVERITY.CRITICAL || severity === SEVERITY.HIGH || forceAlert || patternAlert) {
        logger.error(logMessage);

        // Store critical events in Redis for alerting dashboard
        const redis = getClient();
        if (redis) {
            try {
                const alertKey = `security:alerts:${Date.now()}`;
                await redis.set(alertKey, JSON.stringify(logEntry), 'EX', 86400); // 24 hour TTL
                await redis.lpush('security:alerts:recent', JSON.stringify(logEntry));
                await redis.ltrim('security:alerts:recent', 0, 99); // Keep last 100 alerts
            } catch (_e) {
                // Ignore Redis errors for logging
            }
        }
    } else if (severity === SEVERITY.MEDIUM) {
        logger.warn(logMessage);
    } else {
        logger.info(logMessage);
    }

    return logEntry;
}

// ═══════════════════════════════════════════════════════════════
// CONVENIENCE FUNCTIONS
// ═══════════════════════════════════════════════════════════════

/**
 * Log authentication failure
 */
function logAuthFailure(request, { wallet, reason, attempts } = {}) {
    return logSecurityEvent({
        type: SECURITY_EVENT_TYPES.AUTH_FAILED,
        severity: attempts >= 5 ? SEVERITY.HIGH : SEVERITY.MEDIUM,
        message: `Authentication failed: ${reason}`,
        request,
        wallet,
        details: { reason, attempts }
    });
}

/**
 * Log access denied
 */
function logAccessDenied(request, { wallet, resource, requiredGrant } = {}) {
    return logSecurityEvent({
        type: SECURITY_EVENT_TYPES.ACCESS_DENIED,
        severity: SEVERITY.MEDIUM,
        message: `Access denied to ${resource}`,
        request,
        wallet,
        details: { resource, requiredGrant }
    });
}

/**
 * Log webhook security event
 */
function logWebhookSecurity(request, { type, txSignature, reason } = {}) {
    const eventType = type === 'replay'
        ? SECURITY_EVENT_TYPES.WEBHOOK_REPLAY
        : type === 'expired'
            ? SECURITY_EVENT_TYPES.WEBHOOK_EXPIRED
            : SECURITY_EVENT_TYPES.WEBHOOK_INVALID_SIG;

    return logSecurityEvent({
        type: eventType,
        severity: SEVERITY.HIGH,
        message: `Webhook security: ${reason}`,
        request,
        details: { txSignature: txSignature?.slice(0, 16), reason }
    });
}

/**
 * Log rate limiting event
 */
function logRateLimited(request, { keyHash, limit, usage, fallback } = {}) {
    return logSecurityEvent({
        type: fallback ? SECURITY_EVENT_TYPES.RATE_DEGRADED : SECURITY_EVENT_TYPES.RATE_LIMITED,
        severity: SEVERITY.LOW,
        message: `Rate limit ${fallback ? '(fallback)' : ''}: ${usage}/${limit}`,
        request,
        details: { keyHash: keyHash?.slice(0, 8), limit, usage, fallback }
    });
}

/**
 * Log injection attempt
 */
function logInjectionAttempt(request, { field, value, pattern } = {}) {
    return logSecurityEvent({
        type: SECURITY_EVENT_TYPES.INJECTION_ATTEMPT,
        severity: SEVERITY.CRITICAL,
        message: `Potential injection attempt in ${field}`,
        request,
        details: { field, valuePreview: String(value).slice(0, 50), pattern },
        alert: true
    });
}

/**
 * Log system/config error
 */
function logSystemError(type, message, details = {}) {
    return logSecurityEvent({
        type: type === 'redis' ? SECURITY_EVENT_TYPES.REDIS_OUTAGE : SECURITY_EVENT_TYPES.CONFIG_ERROR,
        severity: SEVERITY.HIGH,
        message,
        details
    });
}

// ═══════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════

module.exports = {
    // Main function
    logSecurityEvent,

    // Convenience functions
    logAuthFailure,
    logAccessDenied,
    logWebhookSecurity,
    logRateLimited,
    logInjectionAttempt,
    logSystemError,

    // Constants
    SECURITY_EVENT_TYPES,
    SEVERITY,

    // Helpers
    getClientIP,
    maskSensitive
};
