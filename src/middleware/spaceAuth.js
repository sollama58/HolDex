/**
 * SPACE ACCESS AUTHENTICATION MIDDLEWARE
 *
 * φ-Aligned wallet-based authentication for HolDex Spaces
 * "Hold to enter. Grants unlock features. E-Score = benefits."
 *
 * Two modes:
 * 1. Session-based (for reads): Session token in header/cookie
 * 2. Signature-based (for writes): Fresh wallet signature per action
 */

const crypto = require('crypto');
const nacl = require('tweetnacl');
const { PublicKey } = require('@solana/web3.js');
const { getDB } = require('../services/database');
const { getClient } = require('../services/redis');
const logger = require('../services/logger');
const config = require('../config/env');

// ═══════════════════════════════════════════════════════════════
// CONSTANTS
// ═══════════════════════════════════════════════════════════════

const SESSION_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days
const SIGNATURE_MAX_AGE_MS = 5 * 60 * 1000;      // 5 minutes (replay prevention)
const GRANT_SECRET = config.DATA_SIGNING_SECRET || 'holdex-grants-secret';

// Valid grants (whitelist)
const VALID_GRANTS = [
    'space_access',       // Access to /space
    'card_generator',     // Generate K-Score cards
    'gasdf_analytics',    // View burns/rewards analytics
    'direct_submit',      // Submit tokens without review
    'priority_queue',     // Priority K-Score refresh
    'grants_admin',       // Manage other grants
];

// Grant → Required grants mapping
const GRANT_REQUIREMENTS = {
    'card_generator': ['space_access'],
    'gasdf_analytics': ['space_access'],
    'direct_submit': ['space_access'],
    'priority_queue': ['space_access'],
    'grants_admin': ['space_access'],
};

// Memory cache for grants (reduce DB hits)
const GRANTS_CACHE = new Map();
const GRANTS_CACHE_TTL = 60000; // 1 minute

// ═══════════════════════════════════════════════════════════════
// SIGNATURE UTILITIES
// ═══════════════════════════════════════════════════════════════

/**
 * Generate HMAC signature for grant integrity
 */
function signGrant(wallet, grants, grantedBy, createdAt) {
    const data = `${wallet}:${grants.sort().join(',')}:${grantedBy}:${createdAt}`;
    return crypto.createHmac('sha256', GRANT_SECRET).update(data).digest('hex');
}

/**
 * Verify grant signature integrity
 */
function verifyGrantSignature(grant) {
    const expected = signGrant(
        grant.wallet,
        grant.grants,
        grant.granted_by,
        grant.created_at
    );
    return grant.signature === expected;
}

/**
 * Verify Solana wallet signature
 * @param {string} wallet - Wallet address (base58)
 * @param {string} message - Original message that was signed
 * @param {string} signature - Base64-encoded signature
 * @returns {boolean}
 */
function verifyWalletSignature(wallet, message, signature) {
    try {
        const msgBytes = new TextEncoder().encode(message);
        const sigBytes = Buffer.from(signature, 'base64');
        const pubBytes = new PublicKey(wallet).toBytes();
        return nacl.sign.detached.verify(msgBytes, sigBytes, pubBytes);
    } catch (e) {
        logger.debug(`[SpaceAuth] Signature verification failed: ${e.message}`);
        return false;
    }
}

/**
 * Generate secure session token
 */
function generateSessionToken() {
    return crypto.randomBytes(32).toString('hex');
}

/**
 * Validate Solana address format
 */
function isValidWallet(wallet) {
    if (!wallet || typeof wallet !== 'string') return false;
    if (wallet.length < 32 || wallet.length > 44) return false;
    try {
        new PublicKey(wallet);
        return true;
    } catch {
        return false;
    }
}

// ═══════════════════════════════════════════════════════════════
// GRANT MANAGEMENT
// ═══════════════════════════════════════════════════════════════

/**
 * Get grants for a wallet (with caching)
 */
async function getGrants(wallet) {
    const cacheKey = `grants:${wallet}`;
    const cached = GRANTS_CACHE.get(cacheKey);

    if (cached && Date.now() < cached.expiry) {
        return cached.data;
    }

    const db = getDB();
    const grant = await db.get(`
        SELECT * FROM access_grants
        WHERE wallet = $1
        AND is_active = TRUE
        AND (expires_at IS NULL OR expires_at > NOW())
    `, [wallet]);

    if (!grant) {
        GRANTS_CACHE.set(cacheKey, { data: null, expiry: Date.now() + GRANTS_CACHE_TTL });
        return null;
    }

    // Verify signature integrity
    if (!verifyGrantSignature(grant)) {
        logger.warn(`[SpaceAuth] SECURITY: Invalid grant signature for ${wallet}`);
        return null;
    }

    GRANTS_CACHE.set(cacheKey, { data: grant, expiry: Date.now() + GRANTS_CACHE_TTL });
    return grant;
}

/**
 * Check if wallet has specific grant
 */
async function hasGrant(wallet, grantType) {
    const grants = await getGrants(wallet);
    if (!grants) return false;

    // Check main grant
    if (!grants.grants.includes(grantType)) return false;

    // Check required dependencies
    const required = GRANT_REQUIREMENTS[grantType] || [];
    for (const req of required) {
        if (!grants.grants.includes(req)) return false;
    }

    return true;
}

/**
 * Create new grant (admin only)
 */
async function createGrant(wallet, grants, grantedBy, reason = null) {
    // Validate grants
    for (const g of grants) {
        if (!VALID_GRANTS.includes(g)) {
            throw new Error(`Invalid grant type: ${g}`);
        }
    }

    const db = getDB();
    const createdAt = new Date().toISOString();
    const signature = signGrant(wallet, grants, grantedBy, createdAt);

    await db.run(`
        INSERT INTO access_grants (wallet, grants, granted_by, grant_reason, signature, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $6)
        ON CONFLICT (wallet) DO UPDATE SET
            grants = $2,
            granted_by = $3,
            grant_reason = $4,
            signature = $5,
            updated_at = $6,
            is_active = TRUE,
            revoked_at = NULL,
            revoked_by = NULL
    `, [wallet, grants, grantedBy, reason, signature, createdAt]);

    // Invalidate cache
    GRANTS_CACHE.delete(`grants:${wallet}`);

    return { wallet, grants, grantedBy, createdAt, signature };
}

/**
 * Revoke grant
 */
async function revokeGrant(wallet, revokedBy) {
    const db = getDB();
    await db.run(`
        UPDATE access_grants
        SET is_active = FALSE, revoked_at = NOW(), revoked_by = $2, updated_at = NOW()
        WHERE wallet = $1
    `, [wallet, revokedBy]);

    // Invalidate cache
    GRANTS_CACHE.delete(`grants:${wallet}`);
}

// ═══════════════════════════════════════════════════════════════
// SESSION MANAGEMENT
// ═══════════════════════════════════════════════════════════════

/**
 * Create session for wallet
 */
async function createSession(wallet, signedMessage, signature) {
    // Verify wallet signature
    if (!verifyWalletSignature(wallet, signedMessage, signature)) {
        throw new Error('Invalid wallet signature');
    }

    // Parse timestamp from message (format: "Access HolDex Space: [timestamp]")
    const match = signedMessage.match(/: (\d+)$/);
    if (!match) {
        throw new Error('Invalid message format');
    }

    const timestamp = parseInt(match[1]);
    const now = Date.now();

    // Check message freshness (prevent replay)
    if (now - timestamp > SIGNATURE_MAX_AGE_MS) {
        throw new Error('Signature expired');
    }

    const db = getDB();
    const sessionToken = generateSessionToken();
    const expiresAt = new Date(now + SESSION_TTL_MS);

    await db.run(`
        INSERT INTO wallet_sessions (wallet, session_token, signed_message, signature, expires_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (wallet) DO UPDATE SET
            session_token = $2,
            signed_message = $3,
            signature = $4,
            expires_at = $5,
            last_used_at = NOW()
    `, [wallet, sessionToken, signedMessage, signature, expiresAt]);

    return { sessionToken, expiresAt };
}

/**
 * Validate session token
 */
async function validateSession(sessionToken) {
    if (!sessionToken) return null;

    // Try Redis first
    const redis = getClient();
    if (redis) {
        const cached = await redis.get(`session:${sessionToken}`);
        if (cached) {
            return JSON.parse(cached);
        }
    }

    const db = getDB();
    const session = await db.get(`
        SELECT wallet, expires_at
        FROM wallet_sessions
        WHERE session_token = $1 AND expires_at > NOW()
    `, [sessionToken]);

    if (!session) return null;

    // Update last_used_at
    db.run('UPDATE wallet_sessions SET last_used_at = NOW() WHERE session_token = $1', [sessionToken])
        .catch(() => {}); // Fire and forget

    // Cache in Redis
    if (redis) {
        const ttl = Math.floor((new Date(session.expires_at) - Date.now()) / 1000);
        await redis.setex(`session:${sessionToken}`, Math.min(ttl, 3600), JSON.stringify(session));
    }

    return session;
}

/**
 * Destroy session
 */
async function destroySession(sessionToken) {
    const db = getDB();
    await db.run('DELETE FROM wallet_sessions WHERE session_token = $1', [sessionToken]);

    const redis = getClient();
    if (redis) {
        await redis.del(`session:${sessionToken}`);
    }
}

// ═══════════════════════════════════════════════════════════════
// ACTION LOGGING
// ═══════════════════════════════════════════════════════════════

/**
 * Log space action for audit trail
 */
async function logAction(wallet, action, grantUsed, metadata = {}, signature = null, success = true, error = null) {
    const db = getDB();
    await db.run(`
        INSERT INTO space_actions (wallet, action, grant_used, metadata, signature, success, error)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
    `, [wallet, action, grantUsed, JSON.stringify(metadata), signature, success, error]);
}

// ═══════════════════════════════════════════════════════════════
// MIDDLEWARE FACTORIES
// ═══════════════════════════════════════════════════════════════

/**
 * Middleware: Require session (for read operations)
 * Extracts wallet from session token
 */
function requireSession() {
    return async (req, res, next) => {
        const sessionToken = req.headers['x-session-token'] || req.cookies?.session;

        if (!sessionToken) {
            return res.status(401).json({
                success: false,
                error: 'Session required',
                code: 'NO_SESSION'
            });
        }

        const session = await validateSession(sessionToken);
        if (!session) {
            return res.status(401).json({
                success: false,
                error: 'Invalid or expired session',
                code: 'INVALID_SESSION'
            });
        }

        // Attach wallet to request
        req.wallet = session.wallet;
        req.session = session;
        next();
    };
}

/**
 * Middleware: Require specific grant
 * Must be used after requireSession()
 */
function requireGrant(grantType) {
    return async (req, res, next) => {
        const wallet = req.wallet;

        if (!wallet) {
            return res.status(401).json({
                success: false,
                error: 'Authentication required',
                code: 'NO_WALLET'
            });
        }

        const granted = await hasGrant(wallet, grantType);
        if (!granted) {
            await logAction(wallet, `access_denied:${grantType}`, null, {}, null, false, 'Grant not found');
            return res.status(403).json({
                success: false,
                error: `Grant required: ${grantType}`,
                code: 'MISSING_GRANT'
            });
        }

        req.grantType = grantType;
        next();
    };
}

/**
 * Middleware: Require fresh signature (for write operations)
 * Verifies wallet owns the address and action is intentional
 */
function requireSignature(actionTemplate) {
    return async (req, res, next) => {
        const { wallet, signature, timestamp } = req.body;

        // Validate inputs
        if (!wallet || !signature || !timestamp) {
            return res.status(400).json({
                success: false,
                error: 'Wallet, signature, and timestamp required',
                code: 'MISSING_PARAMS'
            });
        }

        if (!isValidWallet(wallet)) {
            return res.status(400).json({
                success: false,
                error: 'Invalid wallet address',
                code: 'INVALID_WALLET'
            });
        }

        // Build expected message
        const params = req.body.params || {};
        const message = typeof actionTemplate === 'function'
            ? actionTemplate(params, timestamp)
            : `${actionTemplate}: ${timestamp}`;

        // Verify signature
        if (!verifyWalletSignature(wallet, message, signature)) {
            await logAction(wallet, 'signature_failed', null, { message }, null, false, 'Invalid signature');
            return res.status(403).json({
                success: false,
                error: 'Invalid signature',
                code: 'INVALID_SIGNATURE'
            });
        }

        // Check timestamp freshness
        const now = Date.now();
        const ts = parseInt(timestamp);
        if (isNaN(ts) || now - ts > SIGNATURE_MAX_AGE_MS) {
            return res.status(403).json({
                success: false,
                error: 'Signature expired',
                code: 'EXPIRED_SIGNATURE'
            });
        }

        // Attach to request
        req.wallet = wallet;
        req.signedMessage = message;
        req.walletSignature = signature;
        next();
    };
}

/**
 * Middleware: Optional session (attach wallet if present, but don't require)
 */
function optionalSession() {
    return async (req, res, next) => {
        const sessionToken = req.headers['x-session-token'] || req.cookies?.session;

        if (sessionToken) {
            const session = await validateSession(sessionToken);
            if (session) {
                req.wallet = session.wallet;
                req.session = session;
            }
        }

        next();
    };
}

// ═══════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════

module.exports = {
    // Middleware
    requireSession,
    requireGrant,
    requireSignature,
    optionalSession,

    // Grant management
    getGrants,
    hasGrant,
    createGrant,
    revokeGrant,
    VALID_GRANTS,

    // Session management
    createSession,
    validateSession,
    destroySession,

    // Utilities
    verifyWalletSignature,
    isValidWallet,
    logAction,
    signGrant,

    // Constants
    SESSION_TTL_MS,
    SIGNATURE_MAX_AGE_MS,
};
