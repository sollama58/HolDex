/**
 * ADMIN AUTHENTICATION MIDDLEWARE
 *
 * Secure admin authentication with:
 * - Brute-force protection (exponential backoff)
 * - Account lockout after repeated failures
 * - Comprehensive audit logging
 * - Timing-safe comparison
 *
 * SECURITY: This middleware should be used for all admin endpoints
 */

'use strict';

const crypto = require('crypto');
const logger = require('../services/logger');
const config = require('../config/env');
const { getClient } = require('../services/redis');
const securityLogger = require('../utils/securityLogger');

// ═══════════════════════════════════════════════════════════════
// CONSTANTS
// ═══════════════════════════════════════════════════════════════

const MAX_FAILED_ATTEMPTS = 5;           // Lock after 5 failed attempts
const LOCKOUT_DURATION_MS = 15 * 60 * 1000; // 15 minute lockout
const ATTEMPT_WINDOW_MS = 60 * 60 * 1000;   // Track attempts for 1 hour
const BACKOFF_BASE_MS = 1000;            // 1 second base delay

// In-memory fallback when Redis unavailable
const failedAttempts = new Map();
const lockedIPs = new Map();

// Cleanup interval (every 5 minutes)
setInterval(() => {
    const now = Date.now();
    for (const [ip, data] of failedAttempts) {
        if (now - data.firstAttempt > ATTEMPT_WINDOW_MS) {
            failedAttempts.delete(ip);
        }
    }
    for (const [ip, lockUntil] of lockedIPs) {
        if (now > lockUntil) {
            lockedIPs.delete(ip);
        }
    }
}, 5 * 60 * 1000);

// ═══════════════════════════════════════════════════════════════
// HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════

/**
 * Get client IP address (handles proxies)
 */
function getClientIP(req) {
    return req.headers['x-forwarded-for']?.split(',')[0]?.trim() ||
           req.headers['x-real-ip'] ||
           req.ip ||
           req.connection?.remoteAddress ||
           'unknown';
}

/**
 * Calculate exponential backoff delay
 */
function calculateBackoff(attempts) {
    // 1s, 2s, 4s, 8s, 16s...
    return Math.min(BACKOFF_BASE_MS * Math.pow(2, attempts - 1), 60000);
}

/**
 * Record failed authentication attempt
 */
async function recordFailedAttempt(ip) {
    const redis = getClient();
    const now = Date.now();

    if (redis) {
        try {
            const key = `admin:auth:failed:${ip}`;
            const current = await redis.get(key);
            const data = current ? JSON.parse(current) : { count: 0, firstAttempt: now };
            data.count++;
            data.lastAttempt = now;

            await redis.set(key, JSON.stringify(data), 'EX', Math.ceil(ATTEMPT_WINDOW_MS / 1000));

            // Check for lockout
            if (data.count >= MAX_FAILED_ATTEMPTS) {
                const lockKey = `admin:auth:locked:${ip}`;
                await redis.set(lockKey, '1', 'EX', Math.ceil(LOCKOUT_DURATION_MS / 1000));
            }

            return data.count;
        } catch (e) {
            logger.error(`[AdminAuth] Redis error: ${e.message}`);
        }
    }

    // Fallback to in-memory
    let data = failedAttempts.get(ip);
    if (!data || now - data.firstAttempt > ATTEMPT_WINDOW_MS) {
        data = { count: 0, firstAttempt: now };
    }
    data.count++;
    data.lastAttempt = now;
    failedAttempts.set(ip, data);

    if (data.count >= MAX_FAILED_ATTEMPTS) {
        lockedIPs.set(ip, now + LOCKOUT_DURATION_MS);
    }

    return data.count;
}

/**
 * Check if IP is locked out
 */
async function isLockedOut(ip) {
    const redis = getClient();

    if (redis) {
        try {
            const locked = await redis.get(`admin:auth:locked:${ip}`);
            return !!locked;
        } catch (_e) {
            // Fall through to in-memory
        }
    }

    const lockUntil = lockedIPs.get(ip);
    if (lockUntil && Date.now() < lockUntil) {
        return true;
    }
    return false;
}

/**
 * Get current failed attempt count
 */
async function getFailedAttemptCount(ip) {
    const redis = getClient();

    if (redis) {
        try {
            const data = await redis.get(`admin:auth:failed:${ip}`);
            if (data) {
                return JSON.parse(data).count;
            }
            return 0;
        } catch (_e) {
            // Fall through to in-memory
        }
    }

    const data = failedAttempts.get(ip);
    return data?.count || 0;
}

/**
 * Clear failed attempts on successful auth
 */
async function clearFailedAttempts(ip) {
    const redis = getClient();

    if (redis) {
        try {
            await redis.del(`admin:auth:failed:${ip}`);
            await redis.del(`admin:auth:locked:${ip}`);
        } catch (_e) {
            // Continue
        }
    }

    failedAttempts.delete(ip);
    lockedIPs.delete(ip);
}

/**
 * Log security event using centralized security logger
 */
function logSecurityEvent(event, details) {
    // Use centralized security logger for structured events
    if (event.includes('FAILED') || event.includes('LOCKED') || event.includes('BLOCKED')) {
        securityLogger.logSecurityEvent({
            type: securityLogger.SECURITY_EVENT_TYPES.AUTH_FAILED,
            severity: event.includes('LOCKED') ? securityLogger.SEVERITY.HIGH : securityLogger.SEVERITY.MEDIUM,
            message: event,
            ip: details.ip,
            endpoint: details.endpoint,
            details
        });
    } else if (event === 'AUTH_SUCCESS') {
        securityLogger.logSecurityEvent({
            type: securityLogger.SECURITY_EVENT_TYPES.AUTH_SUCCESS,
            severity: securityLogger.SEVERITY.INFO,
            message: 'Admin authentication successful',
            ip: details.ip,
            endpoint: details.endpoint,
            details
        });
    } else if (event === 'CONFIG_ERROR') {
        securityLogger.logSystemError('config', details.error, details);
    }
}

// ═══════════════════════════════════════════════════════════════
// MIDDLEWARE
// ═══════════════════════════════════════════════════════════════

/**
 * Admin password authentication middleware
 *
 * Features:
 * - Brute-force protection with exponential backoff
 * - Account lockout after repeated failures
 * - Timing-safe password comparison
 * - Comprehensive audit logging
 *
 * @param {Object} options - Configuration options
 * @param {string} options.headerName - Header containing password (default: 'x-admin-auth')
 * @param {boolean} options.setWallet - Set req.wallet to 'ADMIN_PASSWORD' (default: true)
 */
function requireAdminPassword(options = {}) {
    const {
        headerName = 'x-admin-auth',
        setWallet = true
    } = options;

    return async (req, res, next) => {
        const ip = getClientIP(req);
        const endpoint = `${req.method} ${req.originalUrl}`;

        // Check if admin password is configured
        if (!config.ADMIN_PASSWORD) {
            logSecurityEvent('CONFIG_ERROR', {
                ip,
                endpoint,
                error: 'ADMIN_PASSWORD not configured'
            });
            return res.status(503).json({
                success: false,
                error: 'Admin authentication not configured',
                code: 'ADMIN_NOT_CONFIGURED'
            });
        }

        // Check lockout status FIRST
        if (await isLockedOut(ip)) {
            logSecurityEvent('AUTH_BLOCKED_LOCKOUT', { ip, endpoint });
            return res.status(429).json({
                success: false,
                error: 'Too many failed attempts. Try again later.',
                code: 'ACCOUNT_LOCKED',
                retryAfter: Math.ceil(LOCKOUT_DURATION_MS / 1000)
            });
        }

        const authHeader = req.headers[headerName];

        // Check for missing auth header
        if (!authHeader) {
            await recordFailedAttempt(ip);
            logSecurityEvent('AUTH_FAILED_NO_HEADER', { ip, endpoint });
            return res.status(401).json({
                success: false,
                error: 'Admin password required',
                code: 'NO_AUTH'
            });
        }

        // Timing-safe comparison
        const passwordBuffer = Buffer.from(config.ADMIN_PASSWORD);
        const authBuffer = Buffer.from(authHeader);

        // Length check first (required for timingSafeEqual)
        const lengthMatch = passwordBuffer.length === authBuffer.length;

        // Always perform comparison to prevent timing attacks
        let match = false;
        if (lengthMatch) {
            match = crypto.timingSafeEqual(passwordBuffer, authBuffer);
        }

        if (!match) {
            const attempts = await recordFailedAttempt(ip);
            const backoffMs = calculateBackoff(attempts);

            logSecurityEvent('AUTH_FAILED_INVALID_PASSWORD', {
                ip,
                endpoint,
                attempts,
                backoffMs,
                willLockAt: MAX_FAILED_ATTEMPTS
            });

            // Add delay to slow down brute force
            await new Promise(resolve => setTimeout(resolve, backoffMs));

            if (attempts >= MAX_FAILED_ATTEMPTS) {
                return res.status(429).json({
                    success: false,
                    error: 'Too many failed attempts. Account locked.',
                    code: 'ACCOUNT_LOCKED',
                    retryAfter: Math.ceil(LOCKOUT_DURATION_MS / 1000)
                });
            }

            return res.status(403).json({
                success: false,
                error: 'Invalid admin password',
                code: 'INVALID_PASSWORD',
                attemptsRemaining: MAX_FAILED_ATTEMPTS - attempts
            });
        }

        // Success - clear failed attempts
        await clearFailedAttempts(ip);

        logSecurityEvent('AUTH_SUCCESS', { ip, endpoint });

        if (setWallet) {
            req.wallet = 'ADMIN_PASSWORD';
        }
        req.isAdmin = true;

        next();
    };
}

/**
 * Admin rate limiter (stricter than normal endpoints)
 * 10 requests per minute for admin endpoints
 */
const adminRateLimiter = require('express-rate-limit')({
    windowMs: 60 * 1000,
    max: 10,
    standardHeaders: true,
    legacyHeaders: false,
    message: {
        success: false,
        error: 'Admin rate limit exceeded',
        code: 'ADMIN_RATE_LIMITED'
    },
    keyGenerator: (req) => getClientIP(req)
});

// ═══════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════

module.exports = {
    requireAdminPassword,
    adminRateLimiter,
    getClientIP,
    isLockedOut,
    clearFailedAttempts,
    recordFailedAttempt,
    getFailedAttemptCount,
    // Constants for testing
    MAX_FAILED_ATTEMPTS,
    LOCKOUT_DURATION_MS,
    ATTEMPT_WINDOW_MS
};
