/**
 * Node Keys Tests
 *
 * "Don't Trust, Verify" - even our own cryptographic code
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
    generateKeyPair,
    computeFingerprint,
    signData,
    verifySignature,
    signVerification,
    verifyVerificationSignature,
    derivePublicKey,
    SIG_VERSION
} = require('../src/utils/nodeKeys');

describe('nodeKeys', () => {
    describe('generateKeyPair', () => {
        it('should generate valid Ed25519 keypair', () => {
            const keys = generateKeyPair();

            assert.ok(keys.publicKey, 'should have publicKey');
            assert.ok(keys.privateKey, 'should have privateKey');
            assert.ok(keys.fingerprint, 'should have fingerprint');

            // Base64 encoded DER keys
            assert.ok(keys.publicKey.length > 40, 'publicKey should be substantial');
            assert.ok(keys.privateKey.length > 40, 'privateKey should be substantial');

            // Fingerprint is 16 hex chars
            assert.equal(keys.fingerprint.length, 16, 'fingerprint should be 16 chars');
            assert.match(keys.fingerprint, /^[0-9a-f]{16}$/, 'fingerprint should be hex');
        });

        it('should generate unique keypairs', () => {
            const keys1 = generateKeyPair();
            const keys2 = generateKeyPair();

            assert.notEqual(keys1.publicKey, keys2.publicKey, 'public keys should differ');
            assert.notEqual(keys1.privateKey, keys2.privateKey, 'private keys should differ');
            assert.notEqual(keys1.fingerprint, keys2.fingerprint, 'fingerprints should differ');
        });
    });

    describe('computeFingerprint', () => {
        it('should compute deterministic fingerprint', () => {
            const keys = generateKeyPair();
            const fp1 = computeFingerprint(keys.publicKey);
            const fp2 = computeFingerprint(keys.publicKey);

            assert.equal(fp1, fp2, 'same key should produce same fingerprint');
            assert.equal(fp1, keys.fingerprint, 'should match generated fingerprint');
        });

        it('should produce different fingerprints for different keys', () => {
            const keys1 = generateKeyPair();
            const keys2 = generateKeyPair();

            assert.notEqual(
                computeFingerprint(keys1.publicKey),
                computeFingerprint(keys2.publicKey)
            );
        });
    });

    describe('signData / verifySignature', () => {
        it('should sign and verify string data', () => {
            const keys = generateKeyPair();
            const data = 'test message';

            const signature = signData(data, keys.privateKey);
            assert.ok(signature, 'should produce signature');
            assert.ok(signature.startsWith(SIG_VERSION), 'should have version prefix');

            const result = verifySignature(data, signature, keys.publicKey);
            assert.equal(result.valid, true, 'signature should verify');
            assert.equal(result.error, null, 'should have no error');
        });

        it('should sign and verify object data', () => {
            const keys = generateKeyPair();
            const data = { mint: 'abc123', k_score: 75 };

            const signature = signData(data, keys.privateKey);
            const result = verifySignature(data, signature, keys.publicKey);

            assert.equal(result.valid, true);
        });

        it('should reject tampered data', () => {
            const keys = generateKeyPair();
            const original = { mint: 'abc123', k_score: 75 };
            const tampered = { mint: 'abc123', k_score: 99 };

            const signature = signData(original, keys.privateKey);
            const result = verifySignature(tampered, signature, keys.publicKey);

            assert.equal(result.valid, false, 'tampered data should fail verification');
        });

        it('should reject wrong public key', () => {
            const keys1 = generateKeyPair();
            const keys2 = generateKeyPair();
            const data = 'test message';

            const signature = signData(data, keys1.privateKey);
            const result = verifySignature(data, signature, keys2.publicKey);

            assert.equal(result.valid, false, 'wrong key should fail verification');
        });

        it('should return null for missing private key', () => {
            const signature = signData('test', null);
            assert.equal(signature, null);
        });

        it('should fail for missing signature or public key', () => {
            const result1 = verifySignature('test', null, 'key');
            const result2 = verifySignature('test', 'sig', null);

            assert.equal(result1.valid, false);
            assert.equal(result2.valid, false);
        });
    });

    describe('signVerification / verifyVerificationSignature', () => {
        it('should sign and verify token verification', () => {
            const keys = generateKeyPair();
            const verification = {
                mint: 'So11111111111111111111111111111111111111112',
                node_id: 'test-node',
                verified_at: Date.now(),
                k_score: 85,
                signatures_valid: true
            };

            const signature = signVerification(verification, keys.privateKey);
            assert.ok(signature, 'should produce signature');

            const result = verifyVerificationSignature(verification, signature, keys.publicKey);
            assert.equal(result.valid, true, 'verification should be valid');
        });

        it('should be deterministic (canonical data)', () => {
            const keys = generateKeyPair();
            const timestamp = Date.now();

            // Create verification with fields in different order
            const v1 = {
                mint: 'abc', node_id: 'n1', verified_at: timestamp, k_score: 50, signatures_valid: true
            };
            const v2 = {
                signatures_valid: true, k_score: 50, verified_at: timestamp, node_id: 'n1', mint: 'abc'
            };

            const sig1 = signVerification(v1, keys.privateKey);
            const sig2 = signVerification(v2, keys.privateKey);

            // Both should produce same signature (canonical ordering)
            assert.equal(sig1, sig2, 'canonical ordering should produce same signature');
        });
    });

    describe('derivePublicKey', () => {
        it('should derive public key from private key', () => {
            const keys = generateKeyPair();
            const derived = derivePublicKey(keys.privateKey);

            assert.ok(derived, 'should derive successfully');
            assert.equal(derived.publicKey, keys.publicKey, 'should match original public key');
            assert.equal(derived.fingerprint, keys.fingerprint, 'should match original fingerprint');
        });

        it('should return null for invalid private key', () => {
            const result = derivePublicKey('invalid-key');
            assert.equal(result, null);
        });

        it('should return null for null input', () => {
            const result = derivePublicKey(null);
            assert.equal(result, null);
        });
    });
});
