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

const SIGNING_SECRET = config.DATA_SIGNING_SECRET || process.env.DATA_SIGNING_SECRET;

if (!SIGNING_SECRET && process.env.NODE_ENV === 'production') {
    logger.error('CRITICAL: DATA_SIGNING_SECRET not set in production!');
}

/**
 * Generate HMAC-SHA256 signature
 */
function hmacSign(data) {
    if (!SIGNING_SECRET) return 'dev_unsigned';
    const hmac = crypto.createHmac('sha256', SIGNING_SECRET);
    hmac.update(data);
    return hmac.digest('base64');
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
    const data = [
        token.mint,
        (token.lp_burn_pct || 0).toFixed(4),
        (token.lp_locked_pct || 0).toFixed(4),
        token.lp_status || 'unknown'
    ].join('|');
    return hmacSign(data);
}

/**
 * 4. Supply Signature
 * Protects: supply, initial_supply, burned_amount, burned_percent
 */
function signSupply(token) {
    const data = [
        token.mint,
        token.supply || '0',
        token.initial_supply || token.supply || '0',
        (token.burned_amount || 0).toString(),
        (token.burned_percent || 0).toFixed(4)
    ].join('|');
    return hmacSign(data);
}

/**
 * 5. K-Score Signature
 * Protects: k_score, conviction_*, holders
 */
function signKScore(token) {
    const data = [
        token.mint,
        Math.round(token.k_score || 0),
        Math.round(token.conviction_score || 0),
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
 * Protects: priceusd, marketcap, liquidity, price_source, price_timestamp, mcap_calculated
 * Includes proof of on-chain derivation with timestamp
 */
function signMarket(token) {
    const data = [
        token.mint,
        (token.priceusd || token.priceUsd || 0).toFixed(12),
        (token.marketcap || token.marketCap || 0).toFixed(2),
        (token.liquidity || 0).toFixed(2),
        token.price_source || 'unknown',
        token.price_timestamp || 0,
        token.price_pool || '',
        token.mcap_calculated ? '1' : '0'
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
 * 8. Full Signature (Chaos Mode)
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
 * Verify a single category signature
 */
function verifyCategory(token, category, expectedSig) {
    if (!SIGNING_SECRET) return { valid: true, reason: 'dev_mode' };
    if (!expectedSig) return { valid: false, reason: 'missing_signature' };
    if (expectedSig === 'dev_unsigned') return { valid: false, reason: 'dev_signature_in_prod' };

    let actualSig;
    switch (category) {
        case 'identity': actualSig = signIdentity(token); break;
        case 'security': actualSig = signSecurity(token); break;
        case 'lp': actualSig = signLP(token); break;
        case 'supply': actualSig = signSupply(token); break;
        case 'kscore': actualSig = signKScore(token); break;
        case 'market': actualSig = signMarket(token); break;
        case 'origin': actualSig = signOrigin(token); break;
        default: return { valid: false, reason: 'unknown_category' };
    }

    // Constant-time comparison
    const sigBuffer = Buffer.from(expectedSig, 'base64');
    const actualBuffer = Buffer.from(actualSig, 'base64');

    if (sigBuffer.length !== actualBuffer.length) {
        return { valid: false, reason: 'length_mismatch' };
    }

    const isValid = crypto.timingSafeEqual(sigBuffer, actualBuffer);
    return { valid: isValid, reason: isValid ? 'verified' : 'signature_mismatch' };
}

/**
 * Verify all token signatures
 * @param {Object} token - Token with all signatures
 * @returns {Object} { valid, tampered: [], unsigned: [], details }
 */
function verifyAllSignatures(token) {
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
    signFull,

    // Verification
    verifyCategory,

    // Legacy compatibility
    signData,
    verifySignature
};
