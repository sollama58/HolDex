/**
 * Data Signature Utility - "Don't Trust, Verify"
 *
 * 8-Category Signature System + Chaos Nonce (42 colonnes)
 *
 * Categories:
 * 1. sig_identity  → name|symbol|image|decimals
 * 2. sig_security  → mint_auth|freeze_auth|is_mutable|verified
 * 3. sig_lp        → lp_burn|lp_locked|lp_status
 * 4. sig_supply    → supply|initial|burned_amt|burned_pct
 * 5. sig_kscore    → k_score|conviction_*|holders
 * 6. sig_market    → price|mcap|liquidity
 * 7. sig_origin    → is_pump|bonding_complete|timestamp
 * 8. sig_full      → HMAC(1-7 + chaos_nonce)
 *
 * $asdfasdfa philosophy: Controlled Chaos
 */

const crypto = require('crypto');
const config = require('../config/env');
const logger = require('../services/logger');

// ============================================
// KEY ROTATION SUPPORT
// ============================================
// To rotate keys with zero downtime:
// 1. Set DATA_SIGNING_SECRET_PREVIOUS = current key
// 2. Set DATA_SIGNING_SECRET = new key
// 3. After 24h (all signatures refreshed), remove PREVIOUS
//
const SIGNING_SECRET = config.DATA_SIGNING_SECRET || process.env.DATA_SIGNING_SECRET;
const SIGNING_SECRET_PREVIOUS = process.env.DATA_SIGNING_SECRET_PREVIOUS || null;

// Signature version prefix (allows future algorithm changes)
const SIG_VERSION = 'v1:';

if (!SIGNING_SECRET && process.env.NODE_ENV === 'production') {
    logger.error('CRITICAL: DATA_SIGNING_SECRET not set in production!');
}

// DEBUG: Log secret fingerprint to verify both services use same key
if (SIGNING_SECRET) {
    const fingerprint = crypto.createHash('sha256').update(SIGNING_SECRET).digest('hex').slice(0, 16);
    logger.info(`[Signature] Secret fingerprint: ${fingerprint} (len=${SIGNING_SECRET.length})`);
}

if (SIGNING_SECRET_PREVIOUS) {
    logger.info('[Signature] Key rotation mode active - verifying with current + previous keys');
}

/**
 * Generate HMAC-SHA256 signature with version prefix
 * @param {string} data - Data to sign
 * @returns {string} Versioned signature (v1:base64...)
 */
function hmacSign(data) {
    if (!SIGNING_SECRET) return 'dev_unsigned';
    const hmac = crypto.createHmac('sha256', SIGNING_SECRET);
    hmac.update(data);
    return SIG_VERSION + hmac.digest('base64');
}

/**
 * Generate HMAC with a specific key (for verification)
 * @param {string} data - Data to sign
 * @param {string} key - Signing key
 * @returns {string} Raw base64 signature (no version prefix)
 */
function hmacSignWithKey(data, key) {
    const hmac = crypto.createHmac('sha256', key);
    hmac.update(data);
    return hmac.digest('base64');
}

/**
 * Verify signature with key rotation support
 * @param {string} data - Original data
 * @param {string} expectedSig - Expected signature (may have version prefix)
 * @returns {{valid: boolean, keyUsed: string}} Verification result
 */
function verifyWithRotation(data, expectedSig) {
    if (!SIGNING_SECRET) return { valid: true, keyUsed: 'dev_mode' };
    if (!expectedSig) return { valid: false, keyUsed: 'none' };
    if (expectedSig === 'dev_unsigned') return { valid: false, keyUsed: 'dev' };

    // Strip version prefix if present
    const sig = expectedSig.startsWith(SIG_VERSION)
        ? expectedSig.slice(SIG_VERSION.length)
        : expectedSig;

    // Try current key first
    const currentSig = hmacSignWithKey(data, SIGNING_SECRET);
    try {
        const sigBuffer = Buffer.from(sig, 'base64');
        const currentBuffer = Buffer.from(currentSig, 'base64');

        if (sigBuffer.length === currentBuffer.length &&
            crypto.timingSafeEqual(sigBuffer, currentBuffer)) {
            return { valid: true, keyUsed: 'current' };
        }
    } catch (_e) { /* Invalid base64, continue */ }

    // Try previous key if available (rotation in progress)
    if (SIGNING_SECRET_PREVIOUS) {
        const previousSig = hmacSignWithKey(data, SIGNING_SECRET_PREVIOUS);
        try {
            const sigBuffer = Buffer.from(sig, 'base64');
            const previousBuffer = Buffer.from(previousSig, 'base64');

            if (sigBuffer.length === previousBuffer.length &&
                crypto.timingSafeEqual(sigBuffer, previousBuffer)) {
                logger.debug('[Signature] Verified with previous key (rotation in progress)');
                return { valid: true, keyUsed: 'previous' };
            }
        } catch (_e) { /* Invalid base64, continue */ }
    }

    return { valid: false, keyUsed: 'none' };
}

/**
 * Generate chaos nonce (random entropy)
 */
function generateChaosNonce() {
    return crypto.randomBytes(16).toString('hex');
}

// ============================================
// CATEGORY SIGNATURES
// ============================================

/**
 * 1. Identity Signature
 * Protects: name, symbol, image, decimals
 */
function signIdentity(token) {
    const data = [
        token.mint,
        token.name || '',
        token.symbol || '',
        token.image || '',
        token.decimals || 9
    ].join('|');
    return hmacSign(data);
}

/**
 * 2. Security Signature
 * Protects: mint_authority_revoked, freeze_authority_revoked, is_mutable_supply, hasCommunityUpdate
 */
function signSecurity(token) {
    const data = [
        token.mint,
        token.mint_authority_revoked ? '1' : '0',
        token.freeze_authority_revoked ? '1' : '0',
        token.is_mutable_supply ? '1' : '0',
        (token.hasCommunityUpdate || token.hascommunityupdate) ? '1' : '0'
    ].join('|');
    return hmacSign(data);
}

/**
 * 3. LP Signature
 * Protects: lp_burn_pct, lp_locked_pct, lp_status
 */
function signLP(token) {
    // PostgreSQL returns NUMERIC as strings - must parseFloat before .toFixed()
    const data = [
        token.mint,
        parseFloat(token.lp_burn_pct || 0).toFixed(4),
        parseFloat(token.lp_locked_pct || 0).toFixed(4),
        token.lp_status || 'unknown'
    ].join('|');
    return hmacSign(data);
}

/**
 * 4. Supply Signature
 * Protects: supply, initial_supply, burned_amount, burned_percent
 */
function signSupply(token) {
    // PostgreSQL returns NUMERIC as strings - must parseFloat before .toFixed()
    const data = [
        token.mint,
        token.supply || '0',
        token.initial_supply || token.supply || '0',
        (token.burned_amount || 0).toString(),
        parseFloat(token.burned_percent || 0).toFixed(4)
    ].join('|');
    return hmacSign(data);
}

/**
 * 5. K-Score Signature
 * Protects: k_score, conviction_*, holders
 */
function signKScore(token) {
    // PostgreSQL returns NUMERIC as strings - must parseFloat before Math.round()
    const data = [
        token.mint,
        Math.round(parseFloat(token.k_score) || 0),
        Math.round(parseFloat(token.conviction_score) || 0),
        token.conviction_accumulators || 0,
        token.conviction_holders || 0,
        token.conviction_reducers || 0,
        token.conviction_extractors || 0,
        token.conviction_analyzed || 0,
        token.holders || 0,
        token.last_k_score_update || 0
    ].join('|');
    return hmacSign(data);
}

/**
 * 6. Market Signature
 * Protects: price, mcap, liquidity with full provenance chain
 * Each field has source + timestamp for "Don't Trust, Verify"
 */
function signMarket(token) {
    // PostgreSQL returns BIGINT/NUMERIC as strings - must parseFloat before .toFixed()
    const data = [
        token.mint,
        // Price with provenance
        parseFloat(token.priceusd || token.priceUsd || 0).toFixed(12),
        token.price_source || 'unknown',
        token.price_timestamp || 0,
        token.price_pool || '',
        // MCap with provenance
        parseFloat(token.marketcap || token.marketCap || 0).toFixed(2),
        token.mcap_calculated ? '1' : '0',
        // Liquidity with provenance
        parseFloat(token.liquidity || 0).toFixed(2),
        token.liquidity_source || 'unknown',
        token.liquidity_timestamp || 0,
        // Holders with provenance
        token.holders_source || 'unknown',
        token.holders_timestamp || 0,
        // Age
        parseFloat(token.age_days || 0).toFixed(2)
    ].join('|');
    return hmacSign(data);
}

/**
 * 7. Origin Signature
 * Protects: is_pump_fun, bonding_curve_complete, timestamp, metadata
 */
function signOrigin(token) {
    const data = [
        token.mint,
        token.is_pump_fun ? '1' : '0',
        token.bonding_curve_complete ? '1' : '0',
        token.timestamp || 0,
        token.metadata || ''
    ].join('|');
    return hmacSign(data);
}

/**
 * 8. Holders Signature
 * Protects: holder_snapshots integrity (top 20 balances hash)
 * @param {string} mint - Token mint address
 * @param {Array} snapshots - Array of {holder, balance} from holder_snapshots
 * @returns {string} HMAC signature
 */
function signHolders(mint, snapshots) {
    // Sort by balance desc, take top 20, create deterministic hash
    const sorted = (snapshots || [])
        // BigInt-safe comparison (sort() expects Number, not BigInt)
        .sort((a, b) => {
            const bBal = BigInt(b.balance || 0);
            const aBal = BigInt(a.balance || 0);
            return bBal > aBal ? 1 : bBal < aBal ? -1 : 0;
        })
        .slice(0, 20);

    const data = [
        mint,
        sorted.length,
        // Hash of top 20: holder|balance pairs
        ...sorted.map(s => `${s.holder}:${s.balance}`)
    ].join('|');
    return hmacSign(data);
}

/**
 * 9. Full Signature (Chaos Mode)
 * Signs all category signatures + chaos_nonce
 */
function signFull(signatures, chaosNonce) {
    const data = [
        signatures.sig_identity,
        signatures.sig_security,
        signatures.sig_lp,
        signatures.sig_supply,
        signatures.sig_kscore,
        signatures.sig_market,
        signatures.sig_origin,
        chaosNonce
    ].join('|');
    return hmacSign(data);
}

// ============================================
// MAIN SIGNING FUNCTION
// ============================================

/**
 * Sign all token data categories
 * @param {Object} token - Token data from DB
 * @returns {Object} All 8 signatures + chaos_nonce
 */
function signAllCategories(token) {
    const chaosNonce = generateChaosNonce();

    const signatures = {
        sig_identity: signIdentity(token),
        sig_security: signSecurity(token),
        sig_lp: signLP(token),
        sig_supply: signSupply(token),
        sig_kscore: signKScore(token),
        sig_market: signMarket(token),
        sig_origin: signOrigin(token),
        chaos_nonce: chaosNonce
    };

    signatures.sig_full = signFull(signatures, chaosNonce);

    return signatures;
}

// ============================================
// VERIFICATION FUNCTIONS
// ============================================

/**
 * Verify a single category signature (with key rotation support)
 */
function verifyCategory(token, category, expectedSig) {
    if (!SIGNING_SECRET) return { valid: true, reason: 'dev_mode' };
    if (!expectedSig) return { valid: false, reason: 'missing_signature' };
    if (expectedSig === 'dev_unsigned') return { valid: false, reason: 'dev_signature_in_prod' };

    // Build the data string for this category
    let data;
    switch (category) {
        case 'identity':
            data = [token.mint, token.name || '', token.symbol || '', token.image || '', token.decimals || 9].join('|');
            break;
        case 'security':
            data = [token.mint, token.mint_authority_revoked ? '1' : '0', token.freeze_authority_revoked ? '1' : '0',
                    token.is_mutable_supply ? '1' : '0', (token.hasCommunityUpdate || token.hascommunityupdate) ? '1' : '0'].join('|');
            break;
        case 'lp':
            // PostgreSQL returns NUMERIC as strings - must parseFloat before .toFixed()
            data = [token.mint, parseFloat(token.lp_burn_pct || 0).toFixed(4), parseFloat(token.lp_locked_pct || 0).toFixed(4), token.lp_status || 'unknown'].join('|');
            break;
        case 'supply':
            data = [token.mint, token.supply || '0', token.initial_supply || token.supply || '0',
                    (token.burned_amount || 0).toString(), parseFloat(token.burned_percent || 0).toFixed(4)].join('|');
            break;
        case 'kscore':
            data = [token.mint, Math.round(parseFloat(token.k_score) || 0), Math.round(parseFloat(token.conviction_score) || 0),
                    token.conviction_accumulators || 0, token.conviction_holders || 0, token.conviction_reducers || 0,
                    token.conviction_extractors || 0, token.conviction_analyzed || 0, token.holders || 0,
                    token.last_k_score_update || 0].join('|');
            break;
        case 'market':
            // PostgreSQL returns BIGINT/NUMERIC as strings - must parseFloat before .toFixed()
            data = [token.mint, parseFloat(token.priceusd || token.priceUsd || 0).toFixed(12), token.price_source || 'unknown',
                    token.price_timestamp || 0, token.price_pool || '', parseFloat(token.marketcap || token.marketCap || 0).toFixed(2),
                    token.mcap_calculated ? '1' : '0', parseFloat(token.liquidity || 0).toFixed(2), token.liquidity_source || 'unknown',
                    token.liquidity_timestamp || 0, token.holders_source || 'unknown', token.holders_timestamp || 0,
                    parseFloat(token.age_days || 0).toFixed(2)].join('|');
            break;
        case 'origin':
            data = [token.mint, token.is_pump_fun ? '1' : '0', token.bonding_curve_complete ? '1' : '0',
                    token.timestamp || 0, token.metadata || ''].join('|');
            break;
        default:
            return { valid: false, reason: 'unknown_category' };
    }

    // Use key rotation verification
    const result = verifyWithRotation(data, expectedSig);
    if (result.valid) {
        return { valid: true, reason: 'verified', keyUsed: result.keyUsed };
    }

    // DEBUG: Log data string for failed verifications to diagnose mismatch
    if (category === 'identity') { // Only log identity to reduce noise but see actual data
        const computedSig = hmacSign(data);
        logger.warn(`[Signature] MISMATCH ${category} for ${token.mint?.slice(0, 8)}: expected=${expectedSig?.slice(0, 16)}... computed=${computedSig?.slice(0, 16)}...`);
        logger.warn(`[Signature] ${category} data: ${data.slice(0, 200)}${data.length > 200 ? '...' : ''}`);
    }
    return { valid: false, reason: 'signature_mismatch' };
}

/**
 * Verify holders signature against holder_snapshots data
 * @param {string} mint - Token mint address
 * @param {string} expectedSig - Expected sig_holders from token
 * @param {Array} snapshots - Array of {holder, balance} from holder_snapshots table
 * @returns {{valid: boolean, reason: string}} Verification result
 */
function verifyHolders(mint, expectedSig, snapshots) {
    if (!SIGNING_SECRET) return { valid: true, reason: 'dev_mode' };
    if (!expectedSig) return { valid: false, reason: 'missing_signature' };
    if (expectedSig === 'dev_unsigned') return { valid: false, reason: 'dev_signature_in_prod' };
    if (!snapshots || snapshots.length === 0) return { valid: false, reason: 'no_snapshots' };

    // Rebuild the data string that was signed
    const sorted = (snapshots || [])
        .sort((a, b) => {
            const bBal = BigInt(b.balance || 0);
            const aBal = BigInt(a.balance || 0);
            return bBal > aBal ? 1 : bBal < aBal ? -1 : 0;
        })
        .slice(0, 20);

    const data = [
        mint,
        sorted.length,
        ...sorted.map(s => `${s.holder}:${s.balance}`)
    ].join('|');

    const result = verifyWithRotation(data, expectedSig);
    if (result.valid) {
        return { valid: true, reason: 'verified', keyUsed: result.keyUsed };
    }
    return { valid: false, reason: 'signature_mismatch' };
}

/**
 * Verify all token signatures
 * @param {Object} token - Token with all signatures
 * @param {Object} options - Optional: { holderSnapshots: [] } for sig_holders verification
 * @returns {Object} { valid, tampered: [], unsigned: [], details }
 */
function verifyAllSignatures(token, options = {}) {
    if (!SIGNING_SECRET) {
        return { valid: true, tampered: [], unsigned: [], reason: 'dev_mode' };
    }

    const categories = ['identity', 'security', 'lp', 'supply', 'kscore', 'market', 'origin'];
    const tampered = [];
    const unsigned = [];
    const details = {};

    for (const cat of categories) {
        const sigField = `sig_${cat}`;
        const sig = token[sigField];

        if (!sig || sig === 'dev_unsigned') {
            unsigned.push(cat);
            details[cat] = 'unsigned';
        } else {
            const result = verifyCategory(token, cat, sig);
            details[cat] = result.reason;
            if (!result.valid && result.reason === 'signature_mismatch') {
                tampered.push(cat);
            }
        }
    }

    // Verify sig_holders if snapshots are provided
    if (options.holderSnapshots && options.holderSnapshots.length > 0) {
        const holdersResult = verifyHolders(token.mint, token.sig_holders, options.holderSnapshots);
        details['holders'] = holdersResult.reason;
        if (!holdersResult.valid && holdersResult.reason === 'signature_mismatch') {
            tampered.push('holders');
        } else if (!token.sig_holders || token.sig_holders === 'dev_unsigned') {
            unsigned.push('holders');
        }
    }

    // Verify full signature if all categories are signed
    if (tampered.length === 0 && unsigned.length === 0 && token.sig_full && token.chaos_nonce) {
        const sigs = {
            sig_identity: token.sig_identity,
            sig_security: token.sig_security,
            sig_lp: token.sig_lp,
            sig_supply: token.sig_supply,
            sig_kscore: token.sig_kscore,
            sig_market: token.sig_market,
            sig_origin: token.sig_origin
        };
        const expectedFull = signFull(sigs, token.chaos_nonce);

        const fullBuffer = Buffer.from(token.sig_full, 'base64');
        const expectedBuffer = Buffer.from(expectedFull, 'base64');

        if (fullBuffer.length !== expectedBuffer.length ||
            !crypto.timingSafeEqual(fullBuffer, expectedBuffer)) {
            tampered.push('full');
            details['full'] = 'signature_mismatch';
        } else {
            details['full'] = 'verified';
        }
    }

    return {
        valid: tampered.length === 0,
        tampered,
        unsigned,
        details,
        chaosVerified: details['full'] === 'verified'
    };
}

// ============================================
// LEGACY COMPATIBILITY
// ============================================

/**
 * Legacy: Sign just K-Score data (backward compatible)
 */
function signData(data) {
    return signKScore(data);
}

/**
 * Legacy: Verify single signature
 */
function verifySignature(data, signature) {
    return verifyCategory(data, 'kscore', signature);
}

// ============================================
// NODE SIGNATURES - Defense against DB attacks
// ============================================
// Nodes in the decentralized network need integrity protection too.
// Attackers can INSERT fake nodes in the public DB, but without valid
// signatures they will be rejected by the verification layer.

/**
 * Sign Node Identity
 * Protects: node_id, name, operator, public_key fingerprint
 * This is the core identity - cannot be forged without signing key
 */
function signNodeIdentity(node) {
    const data = [
        node.node_id,
        node.name || '',
        node.operator || '',
        node.node_key_fingerprint || '',
        node.region || ''
    ].join('|');
    return hmacSign(data);
}

/**
 * Sign Node Status
 * Protects: status, heartbeat, version, stats
 * Changes over time but must stay consistent
 */
function signNodeStatus(node) {
    // IMPORTANT: Ensure consistent types for signature computation
    // PostgreSQL BIGINT may return as string, must normalize
    const data = [
        String(node.node_id || ''),
        String(node.status || 'pending'),
        String(parseInt(node.last_heartbeat, 10) || 0),
        String(node.version || '1.0.0'),
        String(parseInt(node.tokens_verified, 10) || 0),
        String(parseInt(node.verifications_24h, 10) || 0)
    ].join('|');
    return hmacSign(data);
}

/**
 * Sign all node categories
 * @param {Object} node - Node object
 * @returns {Object} { sig_node_identity, sig_node_status, node_chaos_nonce }
 */
function signNodeAllCategories(node) {
    const chaosNonce = generateChaosNonce();
    return {
        sig_node_identity: signNodeIdentity(node),
        sig_node_status: signNodeStatus(node),
        node_chaos_nonce: chaosNonce
    };
}

/**
 * Verify Node Identity signature
 * @param {Object} node - Node object
 * @returns {{valid: boolean, reason: string}}
 */
function verifyNodeIdentity(node) {
    if (!SIGNING_SECRET) return { valid: true, reason: 'dev_mode' };
    if (!node.sig_node_identity) return { valid: false, reason: 'unsigned' };

    const data = [
        node.node_id,
        node.name || '',
        node.operator || '',
        node.node_key_fingerprint || '',
        node.region || ''
    ].join('|');

    const result = verifyWithRotation(data, node.sig_node_identity);
    return result.valid
        ? { valid: true, reason: 'verified' }
        : { valid: false, reason: 'signature_mismatch' };
}

/**
 * Verify Node Status signature
 * @param {Object} node - Node object
 * @returns {{valid: boolean, reason: string}}
 */
function verifyNodeStatus(node) {
    if (!SIGNING_SECRET) return { valid: true, reason: 'dev_mode' };
    if (!node.sig_node_status) return { valid: false, reason: 'unsigned' };

    // IMPORTANT: Must match signNodeStatus() exactly - normalize types
    const data = [
        String(node.node_id || ''),
        String(node.status || 'pending'),
        String(parseInt(node.last_heartbeat, 10) || 0),
        String(node.version || '1.0.0'),
        String(parseInt(node.tokens_verified, 10) || 0),
        String(parseInt(node.verifications_24h, 10) || 0)
    ].join('|');

    const result = verifyWithRotation(data, node.sig_node_status);
    return result.valid
        ? { valid: true, reason: 'verified' }
        : { valid: false, reason: 'signature_mismatch' };
}

/**
 * Verify all node signatures
 * @param {Object} node - Node with signatures
 * @returns {{valid: boolean, tampered: string[], unsigned: string[]}}
 */
function verifyNodeSignatures(node) {
    if (!SIGNING_SECRET) {
        return { valid: true, tampered: [], unsigned: [], reason: 'dev_mode' };
    }

    const tampered = [];
    const unsigned = [];

    // Check identity signature
    const identityResult = verifyNodeIdentity(node);
    if (identityResult.reason === 'unsigned') {
        unsigned.push('identity');
    } else if (!identityResult.valid) {
        tampered.push('identity');
    }

    // Check status signature
    const statusResult = verifyNodeStatus(node);
    if (statusResult.reason === 'unsigned') {
        unsigned.push('status');
    } else if (!statusResult.valid) {
        tampered.push('status');
    }

    return {
        valid: tampered.length === 0 && unsigned.length === 0,
        tampered,
        unsigned
    };
}

module.exports = {
    // New 8-category system
    signAllCategories,
    verifyAllSignatures,
    generateChaosNonce,

    // Individual signers
    signIdentity,
    signSecurity,
    signLP,
    signSupply,
    signKScore,
    signMarket,
    signOrigin,
    signHolders,
    signFull,

    // Verification (with key rotation support)
    verifyCategory,
    verifyHolders,
    verifyWithRotation,

    // Legacy compatibility
    signData,
    verifySignature,

    // Node signatures (defense against DB attacks)
    signNodeIdentity,
    signNodeStatus,
    signNodeAllCategories,
    verifyNodeIdentity,
    verifyNodeStatus,
    verifyNodeSignatures,

    // Constants (for external use)
    SIG_VERSION
};
