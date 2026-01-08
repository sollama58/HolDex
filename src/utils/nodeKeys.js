/**
 * Node Keys - Per-Node Cryptographic Identity
 *
 * Philosophy: "Don't trust. Verify."
 * Each node has a unique Ed25519 keypair that signs all its verifications.
 * Anyone can verify which node produced which data.
 *
 * Key Management:
 * - Private key: Stored in NODE_PRIVATE_KEY env var (base64)
 * - Public key: Registered in database, publicly verifiable
 * - Fingerprint: SHA256(public_key)[0:16] for quick identification
 *
 * Usage:
 *   const { generateKeyPair, signData, verifySignature } = require('./nodeKeys');
 *
 *   // On first startup (one-time):
 *   const { publicKey, privateKey } = generateKeyPair();
 *   // Save privateKey to NODE_PRIVATE_KEY env var
 *   // Register publicKey in database
 *
 *   // On every verification:
 *   const signature = signData(verificationData, privateKey);
 *
 *   // Anyone can verify:
 *   const valid = verifySignature(verificationData, signature, publicKey);
 */

const crypto = require('crypto');
const logger = require('../services/logger');

// Signature version prefix for future algorithm upgrades
const SIG_VERSION = 'ed25519v1:';

/**
 * Generate a new Ed25519 keypair for a node
 * @returns {{ publicKey: string, privateKey: string, fingerprint: string }}
 */
function generateKeyPair() {
    const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519', {
        publicKeyEncoding: { type: 'spki', format: 'der' },
        privateKeyEncoding: { type: 'pkcs8', format: 'der' }
    });

    const publicKeyBase64 = publicKey.toString('base64');
    const privateKeyBase64 = privateKey.toString('base64');
    const fingerprint = computeFingerprint(publicKeyBase64);

    return {
        publicKey: publicKeyBase64,
        privateKey: privateKeyBase64,
        fingerprint
    };
}

/**
 * Compute SHA256 fingerprint of a public key (first 16 hex chars)
 * @param {string} publicKeyBase64 - Base64 encoded public key
 * @returns {string} First 16 characters of SHA256 hash
 */
function computeFingerprint(publicKeyBase64) {
    return crypto
        .createHash('sha256')
        .update(publicKeyBase64)
        .digest('hex')
        .slice(0, 16);
}

/**
 * Sign data with node's private key
 * @param {string|object} data - Data to sign (will be JSON stringified if object)
 * @param {string} privateKeyBase64 - Base64 encoded private key
 * @returns {string} Versioned signature (ed25519v1:base64signature)
 */
function signData(data, privateKeyBase64) {
    if (!privateKeyBase64) {
        logger.warn('[NodeKeys] No private key provided, returning unsigned');
        return null;
    }

    try {
        const dataString = typeof data === 'string' ? data : JSON.stringify(data);
        const privateKeyDer = Buffer.from(privateKeyBase64, 'base64');
        const privateKey = crypto.createPrivateKey({
            key: privateKeyDer,
            format: 'der',
            type: 'pkcs8'
        });

        const signature = crypto.sign(null, Buffer.from(dataString), privateKey);
        return SIG_VERSION + signature.toString('base64');
    } catch (e) {
        logger.error(`[NodeKeys] Sign failed: ${e.message}`);
        return null;
    }
}

/**
 * Verify a signature against data and public key
 * @param {string|object} data - Original data that was signed
 * @param {string} signature - Versioned signature (ed25519v1:base64signature)
 * @param {string} publicKeyBase64 - Base64 encoded public key
 * @returns {{ valid: boolean, error?: string }}
 */
function verifySignature(data, signature, publicKeyBase64) {
    if (!signature || !publicKeyBase64) {
        return { valid: false, error: 'Missing signature or public key' };
    }

    try {
        // Strip version prefix
        let sigBase64 = signature;
        if (signature.startsWith(SIG_VERSION)) {
            sigBase64 = signature.slice(SIG_VERSION.length);
        } else if (signature.startsWith('ed25519')) {
            // Handle potential version mismatch
            const colonIndex = signature.indexOf(':');
            if (colonIndex > 0) {
                sigBase64 = signature.slice(colonIndex + 1);
            }
        }

        const dataString = typeof data === 'string' ? data : JSON.stringify(data);
        const publicKeyDer = Buffer.from(publicKeyBase64, 'base64');
        const publicKey = crypto.createPublicKey({
            key: publicKeyDer,
            format: 'der',
            type: 'spki'
        });

        const sigBuffer = Buffer.from(sigBase64, 'base64');
        const valid = crypto.verify(null, Buffer.from(dataString), publicKey, sigBuffer);

        return { valid, error: valid ? null : 'Signature mismatch' };
    } catch (e) {
        return { valid: false, error: e.message };
    }
}

/**
 * Create canonical verification data string for signing
 * This ensures deterministic signing across all nodes
 * @param {object} verification - Verification object
 * @returns {string} Canonical string representation
 */
function createVerificationData(verification) {
    // Canonical fields in strict order
    const canonical = {
        mint: verification.mint,
        node_id: verification.node_id,
        verified_at: verification.verified_at,
        k_score: verification.k_score,
        signatures_valid: verification.signatures_valid
    };

    // Sort keys for determinism (though object above is already ordered)
    return JSON.stringify(canonical, Object.keys(canonical).sort());
}

/**
 * Sign a token verification with node's private key
 * @param {object} verification - { mint, node_id, verified_at, k_score, signatures_valid }
 * @param {string} privateKeyBase64 - Node's private key
 * @returns {string|null} Signature or null if signing failed
 */
function signVerification(verification, privateKeyBase64) {
    const data = createVerificationData(verification);
    return signData(data, privateKeyBase64);
}

/**
 * Verify a token verification signature
 * @param {object} verification - { mint, node_id, verified_at, k_score, signatures_valid }
 * @param {string} signature - The signature to verify
 * @param {string} publicKeyBase64 - Node's public key
 * @returns {{ valid: boolean, error?: string }}
 */
function verifyVerificationSignature(verification, signature, publicKeyBase64) {
    const data = createVerificationData(verification);
    return verifySignature(data, signature, publicKeyBase64);
}

/**
 * Load node private key from environment
 * @returns {string|null} Private key or null if not set
 */
function getNodePrivateKey() {
    return process.env.NODE_PRIVATE_KEY || null;
}

/**
 * Check if node has a registered key
 * @returns {boolean}
 */
function hasNodeKey() {
    return !!process.env.NODE_PRIVATE_KEY;
}

/**
 * Extract public key from private key
 * Useful for deriving public key when only private key is in env
 * @param {string} privateKeyBase64 - Base64 encoded private key
 * @returns {{ publicKey: string, fingerprint: string }|null}
 */
function derivePublicKey(privateKeyBase64) {
    if (!privateKeyBase64) return null;

    try {
        const privateKeyDer = Buffer.from(privateKeyBase64, 'base64');
        const privateKey = crypto.createPrivateKey({
            key: privateKeyDer,
            format: 'der',
            type: 'pkcs8'
        });

        const publicKey = crypto.createPublicKey(privateKey);
        const publicKeyDer = publicKey.export({ type: 'spki', format: 'der' });
        const publicKeyBase64 = publicKeyDer.toString('base64');
        const fingerprint = computeFingerprint(publicKeyBase64);

        return { publicKey: publicKeyBase64, fingerprint };
    } catch (e) {
        logger.error(`[NodeKeys] Failed to derive public key: ${e.message}`);
        return null;
    }
}

/**
 * Generate and log a new keypair for initial setup
 * Called via: node -e "require('./src/utils/nodeKeys').generateAndPrintKeyPair()"
 */
function generateAndPrintKeyPair() {
    const keys = generateKeyPair();
    console.log('\n=== NEW NODE KEYPAIR ===');
    console.log('Add these to your environment:\n');
    console.log(`NODE_PRIVATE_KEY=${keys.privateKey}\n`);
    console.log('Register this public key in the database:');
    console.log(`Public Key: ${keys.publicKey}`);
    console.log(`Fingerprint: ${keys.fingerprint}`);
    console.log('\n========================\n');
    return keys;
}

module.exports = {
    generateKeyPair,
    computeFingerprint,
    signData,
    verifySignature,
    createVerificationData,
    signVerification,
    verifyVerificationSignature,
    getNodePrivateKey,
    hasNodeKey,
    derivePublicKey,
    generateAndPrintKeyPair,
    SIG_VERSION
};
