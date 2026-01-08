/**
 * Genesis Configuration Tests
 *
 * "The Code is the Constitution"
 * These tests verify the foundational trust anchors.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
    PHI,
    PHI_INV,
    PHI_INV_SQ,
    GENESIS_NODES,
    isGenesisNode,
    getGenesisNode,
    getGenesisNodeIds,
    verifyGenesisPublicKey,
    CONSENSUS_RULES,
    APPROVAL_RULES,
    SECURITY_RULES,
    getConsensusThreshold
} = require('../src/config/genesis');

describe('genesis', () => {
    describe('φ Constants', () => {
        it('should have correct golden ratio value', () => {
            // φ = (1 + √5) / 2
            const expectedPhi = (1 + Math.sqrt(5)) / 2;
            assert.ok(Math.abs(PHI - expectedPhi) < 0.0000001, 'PHI should be golden ratio');
            assert.ok(Math.abs(PHI - 1.618033988749895) < 0.0000001);
        });

        it('should have correct φ⁻¹ (inverse)', () => {
            // φ⁻¹ = 1/φ ≈ 0.618
            assert.ok(Math.abs(PHI_INV - (1 / PHI)) < 0.0000001);
            assert.ok(Math.abs(PHI_INV - 0.618033988749895) < 0.0000001);
        });

        it('should have correct φ⁻² (inverse squared)', () => {
            // φ⁻² = 1/φ² ≈ 0.382
            assert.ok(Math.abs(PHI_INV_SQ - (1 / (PHI * PHI))) < 0.0000001);
            assert.ok(Math.abs(PHI_INV_SQ - 0.381966011250105) < 0.0000001);
        });

        it('should satisfy φ property: φ = 1 + 1/φ', () => {
            // Golden ratio property: φ = 1 + φ⁻¹
            assert.ok(Math.abs(PHI - (1 + PHI_INV)) < 0.0000001);
        });
    });

    describe('Genesis Nodes', () => {
        it('should have exactly 2 genesis nodes', () => {
            const ids = getGenesisNodeIds();
            assert.equal(ids.length, 2, 'should have 2 genesis nodes');
        });

        it('should have asdfasdfa and gcrtrd nodes', () => {
            assert.ok(isGenesisNode('asdfasdfa'), 'asdfasdfa should be genesis');
            assert.ok(isGenesisNode('gcrtrd'), 'gcrtrd should be genesis');
        });

        it('should have frozen (immutable) genesis nodes', () => {
            assert.ok(Object.isFrozen(GENESIS_NODES), 'GENESIS_NODES should be frozen');
            assert.ok(Object.isFrozen(GENESIS_NODES.asdfasdfa), 'asdfasdfa should be frozen');
            assert.ok(Object.isFrozen(GENESIS_NODES.gcrtrd), 'gcrtrd should be frozen');
        });

        it('should have required fields for each genesis node', () => {
            const requiredFields = ['id', 'name', 'operator', 'publicKey', 'fingerprint', 'role', 'capabilities'];

            for (const nodeId of getGenesisNodeIds()) {
                const node = getGenesisNode(nodeId);
                for (const field of requiredFields) {
                    assert.ok(node[field] !== undefined, `${nodeId} should have ${field}`);
                }
            }
        });

        it('should have genesis role for all genesis nodes', () => {
            for (const nodeId of getGenesisNodeIds()) {
                const node = getGenesisNode(nodeId);
                assert.equal(node.role, 'genesis');
            }
        });

        it('should have valid base64 public keys', () => {
            for (const nodeId of getGenesisNodeIds()) {
                const node = getGenesisNode(nodeId);
                // Base64 should be decodable
                const decoded = Buffer.from(node.publicKey, 'base64');
                assert.ok(decoded.length > 0, `${nodeId} publicKey should decode`);
            }
        });

        it('should return null for non-genesis node', () => {
            assert.equal(getGenesisNode('fake-node'), null);
            assert.equal(isGenesisNode('fake-node'), false);
        });
    });

    describe('verifyGenesisPublicKey', () => {
        it('should verify correct public key', () => {
            const node = getGenesisNode('asdfasdfa');
            assert.equal(
                verifyGenesisPublicKey('asdfasdfa', node.publicKey),
                true
            );
        });

        it('should reject wrong public key', () => {
            const wrongKey = 'MCowBQYDK2VwAyEAwrongkeyhere12345678901234567890=';
            assert.equal(
                verifyGenesisPublicKey('asdfasdfa', wrongKey),
                false
            );
        });

        it('should reject non-existent node', () => {
            assert.equal(
                verifyGenesisPublicKey('fake-node', 'anykey'),
                false
            );
        });
    });

    describe('Consensus Rules', () => {
        it('should use φ⁻¹ (61.8%) threshold', () => {
            assert.equal(CONSENSUS_RULES.THRESHOLD_RATIO, PHI_INV);
        });

        it('should calculate correct required approvals', () => {
            // For 2 nodes: ceil(2 × 0.618) = ceil(1.236) = 2
            assert.equal(CONSENSUS_RULES.getRequiredApprovals(2), 2);

            // For 3 nodes: ceil(3 × 0.618) = ceil(1.854) = 2
            assert.equal(CONSENSUS_RULES.getRequiredApprovals(3), 2);

            // For 5 nodes: ceil(5 × 0.618) = ceil(3.09) = 4
            assert.equal(CONSENSUS_RULES.getRequiredApprovals(5), 4);

            // For 10 nodes: ceil(10 × 0.618) = ceil(6.18) = 7
            assert.equal(CONSENSUS_RULES.getRequiredApprovals(10), 7);
        });

        it('should require at least 1 approval', () => {
            assert.equal(CONSENSUS_RULES.getRequiredApprovals(0), 1);
            assert.equal(CONSENSUS_RULES.getRequiredApprovals(1), 1);
        });

        it('should check consensus correctly', () => {
            // Single node = self-consensus
            assert.equal(CONSENSUS_RULES.isConsensusReached(1, 1), true);

            // 2 nodes need 2 agreeing (ceil(2 × 0.618) = 2)
            assert.equal(CONSENSUS_RULES.isConsensusReached(2, 2), true);
            assert.equal(CONSENSUS_RULES.isConsensusReached(1, 2), false);

            // 3 nodes need 2 agreeing
            assert.equal(CONSENSUS_RULES.isConsensusReached(2, 3), true);
            assert.equal(CONSENSUS_RULES.isConsensusReached(1, 3), false);
        });

        it('should have 5-point K-Score tolerance', () => {
            assert.equal(CONSENSUS_RULES.KSCORE_TOLERANCE, 5);
        });

        it('should require minimum 2 verifications', () => {
            assert.equal(CONSENSUS_RULES.MIN_VERIFICATIONS, 2);
        });
    });

    describe('Approval Rules', () => {
        it('should be in Phase 1 (Human Approval)', () => {
            assert.equal(APPROVAL_RULES.CURRENT_PHASE, 1);
            assert.equal(APPROVAL_RULES.PHASE_1.name, 'Human Approval');
        });

        it('should allow genesis nodes to approve', () => {
            assert.equal(APPROVAL_RULES.PHASE_1.genesisCanApprove, true);
        });

        it('should have 7-day approval expiry', () => {
            assert.equal(APPROVAL_RULES.PHASE_1.approvalExpiryDays, 7);
        });

        it('should not have Phase 2 enabled yet', () => {
            assert.equal(APPROVAL_RULES.PHASE_2.enabled, false);
        });
    });

    describe('Security Rules', () => {
        it('should require Ed25519 signatures', () => {
            assert.equal(SECURITY_RULES.REQUIRE_SIGNATURES, true);
            assert.equal(SECURITY_RULES.SIGNATURE_ALGORITHM, 'Ed25519');
        });

        it('should require nonces for replay protection', () => {
            assert.equal(SECURITY_RULES.REQUIRE_NONCE, true);
        });

        it('should have 5-minute message age limit', () => {
            assert.equal(SECURITY_RULES.MAX_MESSAGE_AGE_MS, 5 * 60 * 1000);
        });

        it('should have reasonable rate limits', () => {
            assert.ok(SECURITY_RULES.RATE_LIMITS.TASKS_PER_MINUTE > 0);
            assert.ok(SECURITY_RULES.RATE_LIMITS.VERIFICATIONS_PER_HOUR > 0);
            assert.ok(SECURITY_RULES.RATE_LIMITS.APPROVALS_PER_DAY > 0);
        });
    });

    describe('getConsensusThreshold', () => {
        it('should be alias for CONSENSUS_RULES.getRequiredApprovals', () => {
            for (const n of [1, 2, 3, 5, 10, 20]) {
                assert.equal(
                    getConsensusThreshold(n),
                    CONSENSUS_RULES.getRequiredApprovals(n)
                );
            }
        });
    });
});
