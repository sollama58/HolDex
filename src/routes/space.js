/**
 * SPACE ROUTES
 *
 * Authenticated dashboard endpoints for the $ASDFASDFA cult members.
 * "Hold to enter. Grants unlock features. E-Score = benefits."
 *
 * Features by grant:
 * - space_access: Basic dashboard access
 * - card_generator: Generate K-Score cards for sharing
 * - gasdf_analytics: View burn/rewards analytics
 * - direct_submit: Submit tokens without review
 * - grants_admin: Manage other users' grants
 */

'use strict';

const express = require('express');
const rateLimit = require('express-rate-limit');
const router = express.Router();
const {
    requireSession,
    requireGrant,
    requireSignature,
    createSession,
    destroySession,
    getGrants,
    createGrant,
    revokeGrant,
    logAction,
    isValidWallet,
    VALID_GRANTS,
} = require('../middleware/spaceAuth');
const { requireAdminPassword, adminRateLimiter } = require('../middleware/adminAuth');
const { getHarmonyEngine, harmony } = require('../services/harmonyEngine');
const config = require('../config/env');

let db = null;
let logger = null;

// ═══════════════════════════════════════════════════════════════
// SECURITY: URL Validation
// ═══════════════════════════════════════════════════════════════

// Allowed domains for API URLs (prevents open redirect/SSRF)
const ALLOWED_API_DOMAINS = new Set([
    'holdex-api.onrender.com',
    'api.holdex.io',
    'holdex.io',
    'localhost',
    '127.0.0.1'
]);

/**
 * Validate and sanitize API URL
 * Prevents open redirect and SSRF vulnerabilities
 */
function validateApiUrl(url) {
    if (!url || typeof url !== 'string') {
        return { valid: false, url: null };
    }

    try {
        const parsed = new URL(url);

        // Only allow HTTPS in production
        if (config.NODE_ENV === 'production' && parsed.protocol !== 'https:') {
            return { valid: false, url: null, reason: 'HTTPS required in production' };
        }

        // Check if domain is whitelisted
        const hostname = parsed.hostname.toLowerCase();
        if (!ALLOWED_API_DOMAINS.has(hostname)) {
            // Allow subdomains of whitelisted domains
            const isSubdomain = Array.from(ALLOWED_API_DOMAINS).some(domain =>
                hostname.endsWith(`.${domain}`)
            );
            if (!isSubdomain) {
                return { valid: false, url: null, reason: 'Domain not in whitelist' };
            }
        }

        // Return sanitized URL (removes any path traversal attempts)
        return { valid: true, url: parsed.origin };
    } catch (_e) {
        return { valid: false, url: null, reason: 'Invalid URL format' };
    }
}

/**
 * Get validated base URL for card generation
 */
function getSecureBaseUrl() {
    const configUrl = config.API_URL;

    if (configUrl) {
        const validation = validateApiUrl(configUrl);
        if (validation.valid) {
            return validation.url;
        }
        // Log warning but don't expose to user
        logger?.warn(`[Space] SECURITY: Invalid API_URL configured: ${validation.reason}`);
    }

    // Fallback to default secure URL
    return 'https://holdex-api.onrender.com';
}

// ═══════════════════════════════════════════════════════════════
// SECURITY: Safe JSON Parsing with Size Limits
// ═══════════════════════════════════════════════════════════════

const MAX_METADATA_SIZE = 4096; // 4KB max for metadata JSON
const MAX_JSON_DEPTH = 5;       // Max nesting depth

/**
 * Safely parse JSON with size and depth limits
 * Prevents DoS via large or deeply nested payloads
 */
function safeParseJSON(str, options = {}) {
    const { maxSize = MAX_METADATA_SIZE, maxDepth = MAX_JSON_DEPTH } = options;

    if (typeof str !== 'string') {
        return { success: false, error: 'Not a string' };
    }

    // Check size limit
    if (str.length > maxSize) {
        return { success: false, error: `JSON too large: ${str.length} bytes (max ${maxSize})` };
    }

    try {
        const parsed = JSON.parse(str);

        // Check depth
        if (!checkDepth(parsed, maxDepth)) {
            return { success: false, error: `JSON nesting too deep (max ${maxDepth} levels)` };
        }

        return { success: true, data: parsed };
    } catch (_e) {
        return { success: false, error: 'Invalid JSON' };
    }
}

/**
 * Check if object depth exceeds limit
 */
function checkDepth(obj, maxDepth, currentDepth = 0) {
    if (currentDepth > maxDepth) return false;
    if (obj === null || typeof obj !== 'object') return true;

    if (Array.isArray(obj)) {
        for (const item of obj) {
            if (!checkDepth(item, maxDepth, currentDepth + 1)) return false;
        }
    } else {
        for (const value of Object.values(obj)) {
            if (!checkDepth(value, maxDepth, currentDepth + 1)) return false;
        }
    }
    return true;
}

/**
 * Parse metadata safely, returning empty object on failure
 */
function parseMetadataSafe(metadata) {
    if (!metadata) return {};
    if (typeof metadata === 'object') {
        // Already parsed, but check depth
        if (!checkDepth(metadata, MAX_JSON_DEPTH)) {
            return {};
        }
        return metadata;
    }
    if (typeof metadata === 'string') {
        const result = safeParseJSON(metadata);
        return result.success ? result.data : {};
    }
    return {};
}

// ═══════════════════════════════════════════════════════════════
// RATE LIMITING
// ═══════════════════════════════════════════════════════════════

const spaceRateLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 60, // 60 requests per minute
    standardHeaders: true,
    legacyHeaders: false,
    message: { success: false, error: 'Too many requests', code: 'RATE_LIMITED' },
    keyGenerator: (req) => req.wallet || req.ip
});

const writeRateLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 10, // 10 writes per minute
    standardHeaders: true,
    legacyHeaders: false,
    message: { success: false, error: 'Too many write requests', code: 'WRITE_RATE_LIMITED' },
    keyGenerator: (req) => req.wallet || req.ip
});

// ═══════════════════════════════════════════════════════════════
// AUTHENTICATION ENDPOINTS
// ═══════════════════════════════════════════════════════════════

/**
 * POST /space/auth/login
 * Authenticate with wallet signature and get session token
 *
 * Body: { wallet, message, signature }
 * Message format: "Access HolDex Space: [timestamp]"
 */
router.post('/auth/login', writeRateLimiter, async (req, res) => {
    const { wallet, message, signature } = req.body;

    if (!wallet || !message || !signature) {
        return res.status(400).json({
            success: false,
            error: 'Wallet, message, and signature required',
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

    try {
        // Check if wallet has any grants
        const grants = await getGrants(wallet);
        if (!grants || !grants.grants.includes('space_access')) {
            await logAction(wallet, 'login_denied', null, {}, null, false, 'No space_access grant');
            return res.status(403).json({
                success: false,
                error: 'Access not granted. Contact an ambassador.',
                code: 'NO_ACCESS'
            });
        }

        // Create session
        const session = await createSession(wallet, message, signature);

        await logAction(wallet, 'login', 'space_access', { grants: grants.grants });

        res.json({
            success: true,
            data: {
                sessionToken: session.sessionToken,
                expiresAt: session.expiresAt,
                wallet,
                grants: grants.grants,
                eScoreBoost: grants.e_score_boost || 0
            }
        });

    } catch (e) {
        logger?.error(`[Space] Login error: ${e.message}`);
        return res.status(401).json({
            success: false,
            error: e.message,
            code: 'AUTH_FAILED'
        });
    }
});

/**
 * POST /space/auth/logout
 * Destroy session
 */
router.post('/auth/logout', requireSession(), async (req, res) => {
    const sessionToken = req.headers['x-session-token'] || req.cookies?.session;

    try {
        await destroySession(sessionToken);
        await logAction(req.wallet, 'logout', null, {});

        res.json({ success: true });
    } catch (_e) {
        res.status(500).json({ success: false, error: 'Logout failed' });
    }
});

/**
 * GET /space/auth/me
 * Get current session info and grants
 */
router.get('/auth/me', requireSession(), spaceRateLimiter, async (req, res) => {
    try {
        const grants = await getGrants(req.wallet);
        const harmonyEngine = getHarmonyEngine();

        // Get E-Score for benefits calculation
        let eScore = null;
        let tier = null;
        if (harmonyEngine) {
            const participant = await harmonyEngine.getParticipant(req.wallet);
            if (participant) {
                eScore = participant.cached_escore || 0;
                tier = { name: participant.cached_tier, icon: participant.cached_tier_icon };

                // Apply E-Score boost from grants
                if (grants?.e_score_boost) {
                    eScore += grants.e_score_boost;
                    tier = harmony.getTier(eScore);
                }
            }
        }

        res.json({
            success: true,
            data: {
                wallet: req.wallet,
                grants: grants?.grants || [],
                eScore,
                tier,
                benefits: eScore ? harmony.calculateBenefits(eScore) : null
            }
        });
    } catch (e) {
        logger?.error(`[Space] /me error: ${e.message}`);
        res.status(500).json({ success: false, error: 'Failed to fetch profile' });
    }
});

// ═══════════════════════════════════════════════════════════════
// DASHBOARD ENDPOINTS
// ═══════════════════════════════════════════════════════════════

/**
 * GET /space/dashboard
 * Main dashboard data
 */
router.get('/dashboard', requireSession(), requireGrant('space_access'), spaceRateLimiter, async (req, res) => {
    try {
        const harmonyEngine = getHarmonyEngine();
        const grants = await getGrants(req.wallet);

        // Get participant data
        let participant = null;
        let leaderboardPosition = null;

        if (harmonyEngine) {
            participant = await harmonyEngine.getParticipant(req.wallet);

            // Get leaderboard position
            const leaderboard = await db.all(`
                SELECT wallet, cached_escore
                FROM participants
                WHERE cached_escore > 0
                ORDER BY cached_escore DESC
                LIMIT 100
            `);
            const position = leaderboard.findIndex(p => p.wallet === req.wallet);
            if (position >= 0) {
                leaderboardPosition = {
                    rank: position + 1,
                    total: leaderboard.length,
                    percentile: Math.round((1 - position / leaderboard.length) * 100)
                };
            }
        }

        // Get recent actions
        const recentActions = await db.all(`
            SELECT action, metadata, created_at, success
            FROM space_actions
            WHERE wallet = $1
            ORDER BY created_at DESC
            LIMIT 10
        `, [req.wallet]);

        await logAction(req.wallet, 'view_dashboard', 'space_access', {});

        res.json({
            success: true,
            data: {
                wallet: req.wallet,
                grants: grants?.grants || [],
                participant: participant ? {
                    eScore: participant.cached_escore || 0,
                    tier: { name: participant.cached_tier, icon: participant.cached_tier_icon },
                    totalBurned: participant.total_burned || 0,
                    holdings: participant.holdings || 0,
                    apiCalls30d: participant.api_calls_30d || 0
                } : null,
                leaderboardPosition,
                recentActions: recentActions.map(a => ({
                    action: a.action,
                    metadata: parseMetadataSafe(a.metadata),
                    createdAt: a.created_at,
                    success: a.success
                }))
            }
        });
    } catch (e) {
        logger?.error(`[Space] Dashboard error: ${e.message}`);
        res.status(500).json({ success: false, error: 'Failed to load dashboard' });
    }
});

// ═══════════════════════════════════════════════════════════════
// K-SCORE CARD GENERATION
// ═══════════════════════════════════════════════════════════════

/**
 * POST /space/card/generate
 * Generate K-Score card for a token (requires signature for write action)
 *
 * Body: { wallet, signature, timestamp, params: { mint, mode } }
 */
router.post('/card/generate',
    requireSignature((params, ts) => `Generate K-Score card for ${params.mint}: ${ts}`),
    requireGrant('card_generator'),
    writeRateLimiter,
    async (req, res) => {
        const { params } = req.body;
        const { mint, mode = 'full' } = params || {};

        if (!mint || typeof mint !== 'string' || mint.length < 32) {
            return res.status(400).json({
                success: false,
                error: 'Valid mint address required',
                code: 'INVALID_MINT'
            });
        }

        try {
            // Check token exists and has K-Score
            const token = await db.get(`
                SELECT name, symbol, k_score, hasCommunityUpdate,
                       conviction_accumulators, conviction_holders, conviction_reducers, conviction_extractors
                FROM tokens WHERE mint = $1
            `, [mint]);

            if (!token) {
                return res.status(404).json({
                    success: false,
                    error: 'Token not found',
                    code: 'TOKEN_NOT_FOUND'
                });
            }

            const isVerified = token.hascommunityupdate || token.hasCommunityUpdate;

            // SECURITY: Use validated base URL to prevent open redirects
            const baseUrl = getSecureBaseUrl();
            // Sanitize mode parameter (only allow alphanumeric)
            const safeMode = /^[a-z0-9]+$/i.test(mode) ? mode : 'full';
            const cardUrl = `${baseUrl}/api/token/${encodeURIComponent(mint)}/card.png?mode=${safeMode}`;
            const shareUrl = `${baseUrl}/k/${encodeURIComponent(mint)}?mode=${safeMode}`;

            await logAction(req.wallet, 'generate_card', 'card_generator', {
                mint,
                mode,
                symbol: token.symbol,
                kScore: token.k_score
            }, req.walletSignature);

            res.json({
                success: true,
                data: {
                    mint,
                    symbol: token.symbol,
                    name: token.name,
                    kScore: isVerified ? token.k_score : null,
                    isVerified,
                    cardUrl,
                    shareUrl,
                    twitterShareUrl: `https://twitter.com/intent/tweet?url=${encodeURIComponent(shareUrl)}&text=${encodeURIComponent(`Check out $${token.symbol} K-Score on HolDex!`)}`,
                    generatedBy: req.wallet
                }
            });
        } catch (e) {
            logger?.error(`[Space] Card generation error: ${e.message}`);
            res.status(500).json({ success: false, error: 'Card generation failed' });
        }
    }
);

/**
 * GET /space/card/history
 * Get card generation history for current user
 */
router.get('/card/history', requireSession(), requireGrant('card_generator'), spaceRateLimiter, async (req, res) => {
    try {
        const history = await db.all(`
            SELECT metadata, created_at
            FROM space_actions
            WHERE wallet = $1 AND action = 'generate_card' AND success = TRUE
            ORDER BY created_at DESC
            LIMIT 50
        `, [req.wallet]);

        res.json({
            success: true,
            data: history.map(h => ({
                ...parseMetadataSafe(h.metadata),
                createdAt: h.created_at
            }))
        });
    } catch (_e) {
        res.status(500).json({ success: false, error: 'Failed to fetch history' });
    }
});

// ═══════════════════════════════════════════════════════════════
// GASDF ANALYTICS
// ═══════════════════════════════════════════════════════════════

/**
 * GET /space/analytics/burns
 * Get burn analytics for current user
 */
router.get('/analytics/burns', requireSession(), requireGrant('gasdf_analytics'), spaceRateLimiter, async (req, res) => {
    try {
        const harmonyEngine = getHarmonyEngine();

        // Get participant burn data
        const participant = await harmonyEngine?.getParticipant(req.wallet);

        // Get burn history from contributions
        const burnHistory = await db.all(`
            SELECT amount, source, tx_signature, created_at, verified
            FROM contributions
            WHERE wallet = $1 AND type = 'burn'
            ORDER BY created_at DESC
            LIMIT 100
        `, [req.wallet]);

        // Get ecosystem totals
        const ecosystemStats = await db.get(`
            SELECT
                SUM(total_burned) as total_ecosystem_burns,
                COUNT(*) as total_burners
            FROM participants
            WHERE total_burned > 0
        `);

        // Calculate user's % of ecosystem burns
        const userBurns = participant?.total_burned || 0;
        const ecosystemBurns = ecosystemStats?.total_ecosystem_burns || 1;
        const burnShare = (userBurns / ecosystemBurns) * 100;

        await logAction(req.wallet, 'view_burn_analytics', 'gasdf_analytics', {});

        res.json({
            success: true,
            data: {
                user: {
                    totalBurned: userBurns,
                    burnCount: burnHistory.length,
                    burnShare: Math.round(burnShare * 1000) / 1000, // 3 decimals
                    eScoreFromBurns: Math.log(1 + userBurns) * harmony.MULTIPLIERS.BURN
                },
                history: burnHistory.map(b => ({
                    amount: b.amount,
                    source: b.source,
                    txSignature: b.tx_signature,
                    createdAt: new Date(parseInt(b.created_at, 10)).toISOString(),
                    verified: b.verified
                })),
                ecosystem: {
                    totalBurned: ecosystemBurns,
                    totalBurners: ecosystemStats?.total_burners || 0
                }
            }
        });
    } catch (e) {
        logger?.error(`[Space] Burns analytics error: ${e.message}`);
        res.status(500).json({ success: false, error: 'Failed to fetch burn analytics' });
    }
});

/**
 * GET /space/analytics/leaderboard
 * Get E-Score leaderboard
 */
router.get('/analytics/leaderboard', requireSession(), requireGrant('gasdf_analytics'), spaceRateLimiter, async (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit, 10) || 50, 100);

        const leaderboard = await db.all(`
            SELECT
                wallet,
                cached_escore as e_score,
                cached_tier as tier,
                cached_tier_icon as tier_icon,
                total_burned,
                holdings
            FROM participants
            WHERE cached_escore > 0
            ORDER BY cached_escore DESC
            LIMIT $1
        `, [limit]);

        // Find current user position
        const userPosition = leaderboard.findIndex(p => p.wallet === req.wallet);

        res.json({
            success: true,
            data: {
                leaderboard: leaderboard.map((p, i) => ({
                    rank: i + 1,
                    wallet: p.wallet,
                    eScore: Math.round(p.e_score * 100) / 100,
                    tier: { name: p.tier, icon: p.tier_icon },
                    totalBurned: p.total_burned,
                    isYou: p.wallet === req.wallet
                })),
                yourPosition: userPosition >= 0 ? userPosition + 1 : null
            }
        });
    } catch (e) {
        logger?.error(`[Space] Leaderboard error: ${e.message}`);
        res.status(500).json({ success: false, error: 'Failed to fetch leaderboard' });
    }
});

// ═══════════════════════════════════════════════════════════════
// ADMIN: GRANTS MANAGEMENT
// ═══════════════════════════════════════════════════════════════

/**
 * GET /space/admin/grants
 * List all grants (admin only)
 */
router.get('/admin/grants', requireSession(), requireGrant('grants_admin'), spaceRateLimiter, async (req, res) => {
    try {
        const grants = await db.all(`
            SELECT wallet, grants, e_score_boost, granted_by, grant_reason, is_active, created_at, revoked_at
            FROM access_grants
            ORDER BY created_at DESC
            LIMIT 100
        `);

        await logAction(req.wallet, 'view_grants', 'grants_admin', {});

        res.json({
            success: true,
            data: grants
        });
    } catch (_e) {
        res.status(500).json({ success: false, error: 'Failed to fetch grants' });
    }
});

/**
 * GET /space/admin/grants/password
 * List all grants using password auth
 * SECURITY: Uses secure admin auth with brute-force protection
 */
router.get('/admin/grants/password', adminRateLimiter, requireAdminPassword(), async (req, res) => {
    try {
        const grants = await db.all(`
            SELECT wallet, grants, e_score_boost, granted_by, grant_reason, is_active, created_at, revoked_at
            FROM access_grants
            ORDER BY created_at DESC
            LIMIT 100
        `);

        await logAction('ADMIN_PASSWORD', 'view_grants_password', 'grants_admin', {});

        res.json({
            success: true,
            data: grants
        });
    } catch (_e) {
        res.status(500).json({ success: false, error: 'Failed to fetch grants' });
    }
});

/**
 * POST /space/admin/grants
 * Create or update grant (admin only, requires signature)
 *
 * Body: { wallet, signature, timestamp, params: { targetWallet, grants, reason, eScoreBoost } }
 */
router.post('/admin/grants',
    requireSignature((params, ts) => `Grant access to ${params.targetWallet}: ${ts}`),
    requireGrant('grants_admin'),
    writeRateLimiter,
    async (req, res) => {
        const { params } = req.body;
        const { targetWallet, grants, reason, eScoreBoost = 0 } = params || {};

        if (!targetWallet || !isValidWallet(targetWallet)) {
            return res.status(400).json({
                success: false,
                error: 'Valid target wallet required',
                code: 'INVALID_TARGET'
            });
        }

        if (!grants || !Array.isArray(grants) || grants.length === 0) {
            return res.status(400).json({
                success: false,
                error: 'At least one grant required',
                code: 'NO_GRANTS'
            });
        }

        // Validate grants
        for (const g of grants) {
            if (!VALID_GRANTS.includes(g)) {
                return res.status(400).json({
                    success: false,
                    error: `Invalid grant: ${g}`,
                    code: 'INVALID_GRANT'
                });
            }
        }

        try {
            const result = await createGrant(targetWallet, grants, req.wallet, reason);

            // Update e_score_boost if provided
            if (eScoreBoost > 0) {
                await db.run(
                    'UPDATE access_grants SET e_score_boost = $1 WHERE wallet = $2',
                    [eScoreBoost, targetWallet]
                );
            }

            await logAction(req.wallet, 'create_grant', 'grants_admin', {
                targetWallet,
                grants,
                reason,
                eScoreBoost
            }, req.walletSignature);

            res.json({
                success: true,
                data: {
                    ...result,
                    eScoreBoost
                }
            });
        } catch (e) {
            logger?.error(`[Space] Create grant error: ${e.message}`);
            res.status(500).json({ success: false, error: e.message });
        }
    }
);

/**
 * POST /space/admin/grants/password
 * Create or update grant using admin password auth
 * SECURITY: Uses secure admin auth with brute-force protection
 *
 * Body: { targetWallet, grants, reason, eScoreBoost }
 */
router.post('/admin/grants/password',
    adminRateLimiter,
    requireAdminPassword(),
    writeRateLimiter,
    async (req, res) => {
        const { targetWallet, grants, reason, eScoreBoost = 0 } = req.body;

        if (!targetWallet || !isValidWallet(targetWallet)) {
            return res.status(400).json({
                success: false,
                error: 'Valid target wallet required',
                code: 'INVALID_TARGET'
            });
        }

        if (!grants || !Array.isArray(grants) || grants.length === 0) {
            return res.status(400).json({
                success: false,
                error: 'At least one grant required',
                code: 'NO_GRANTS'
            });
        }

        // Validate grants
        for (const g of grants) {
            if (!VALID_GRANTS.includes(g)) {
                return res.status(400).json({
                    success: false,
                    error: `Invalid grant: ${g}`,
                    code: 'INVALID_GRANT'
                });
            }
        }

        try {
            const result = await createGrant(targetWallet, grants, 'ADMIN_PASSWORD', reason);

            if (eScoreBoost > 0) {
                await db.run(
                    'UPDATE access_grants SET e_score_boost = $1 WHERE wallet = $2',
                    [eScoreBoost, targetWallet]
                );
            }

            await logAction('ADMIN_PASSWORD', 'create_grant', 'grants_admin', {
                targetWallet,
                grants,
                reason,
                eScoreBoost
            }, null);

            res.json({
                success: true,
                data: {
                    ...result,
                    eScoreBoost
                }
            });
        } catch (e) {
            logger?.error(`[Space] Create grant (password) error: ${e.message}`);
            res.status(500).json({ success: false, error: e.message });
        }
    }
);

/**
 * DELETE /space/admin/grants/password/:wallet
 * Revoke grant using admin password auth
 * SECURITY: Uses secure admin auth with brute-force protection
 */
router.delete('/admin/grants/password/:targetWallet',
    adminRateLimiter,
    requireAdminPassword(),
    writeRateLimiter,
    async (req, res) => {
        const { targetWallet } = req.params;

        if (!isValidWallet(targetWallet)) {
            return res.status(400).json({
                success: false,
                error: 'Invalid wallet address',
                code: 'INVALID_WALLET'
            });
        }

        try {
            await revokeGrant(targetWallet, 'ADMIN_PASSWORD');
            await logAction('ADMIN_PASSWORD', 'revoke_grant', 'grants_admin', { targetWallet }, null);
            res.json({ success: true });
        } catch (_e) {
            res.status(500).json({ success: false, error: 'Failed to revoke grant' });
        }
    }
);

/**
 * DELETE /space/admin/grants/:wallet
 * Revoke grant (admin only, requires signature)
 */
router.delete('/admin/grants/:targetWallet',
    requireSignature((params, ts) => `Revoke access for ${params.targetWallet}: ${ts}`),
    requireGrant('grants_admin'),
    writeRateLimiter,
    async (req, res) => {
        const { targetWallet } = req.params;

        if (!isValidWallet(targetWallet)) {
            return res.status(400).json({
                success: false,
                error: 'Invalid wallet address',
                code: 'INVALID_WALLET'
            });
        }

        try {
            await revokeGrant(targetWallet, req.wallet);

            await logAction(req.wallet, 'revoke_grant', 'grants_admin', {
                targetWallet
            }, req.walletSignature);

            res.json({ success: true });
        } catch (_e) {
            res.status(500).json({ success: false, error: 'Failed to revoke grant' });
        }
    }
);

/**
 * GET /space/admin/actions
 * Get recent space actions (admin only)
 */
router.get('/admin/actions', requireSession(), requireGrant('grants_admin'), spaceRateLimiter, async (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit, 10) || 100, 500);

        const actions = await db.all(`
            SELECT id, wallet, action, grant_used, metadata, success, error, created_at
            FROM space_actions
            ORDER BY created_at DESC
            LIMIT $1
        `, [limit]);

        res.json({
            success: true,
            data: actions.map(a => ({
                id: a.id,
                wallet: a.wallet,
                action: a.action,
                grantUsed: a.grant_used,
                metadata: parseMetadataSafe(a.metadata),
                success: a.success,
                error: a.error,
                createdAt: a.created_at
            }))
        });
    } catch (e) {
        logger?.error(`[Space] Actions fetch error: ${e.message}`);
        res.status(500).json({ success: false, error: 'Failed to fetch actions' });
    }
});

// ═══════════════════════════════════════════════════════════════
// INITIALIZATION
// ═══════════════════════════════════════════════════════════════

function init({ db: database, logger: log }) {
    db = database;
    logger = log;
    return router;
}

module.exports = { init, router };
