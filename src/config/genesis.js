/**
 * HolDex Genesis Configuration
 *
 * "The Code is the Constitution"
 *
 * This file defines the foundational trust anchors for the HolDex
 * verification network. Changes require git commit + deploy.
 *
 * Philosophy: "Don't Trust, Verify" - but someone must be first.
 * These genesis nodes bootstrapped the network.
 */

'use strict';

// φ constants - guide all ratios
const PHI = 1.618033988749895;
const PHI_INV = 1 / PHI;                    // 0.618033988749895
const PHI_INV_SQ = 1 / (PHI * PHI);          // 0.381966011250105

/**
 * GENESIS NODES
 *
 * The two founding nodes of the HolDex verification network.
 * Their public keys are immutably recorded here.
 *
 * Both nodes are EQUAL - no hierarchy, no "primary".
 * Trust is established by being in this file (code = constitution).
 */
const GENESIS_NODES = Object.freeze({
    // Node 1: asdfasdfa - Architect, Dev
    asdfasdfa: Object.freeze({
        id: 'asdfasdfa',
        name: 'Genesis Dev',
        operator: 'asdfasdfa',
        publicKey: 'MCowBQYDK2VwAyEAAJzth4oouA3ZvRKu2LdCMkgUXPWAZzb/6vJB+xAAoGM=',
        fingerprint: '866bceeafc29729d',
        role: 'genesis',
        genesisBlock: 0,
        capabilities: ['polling', 'webhooks', 'verification', 'consensus', 'approval'],
        // Private key: NEVER in code, held by operator
    }),

    // Node 2: gcrtrd - Production, Official Site
    gcrtrd: Object.freeze({
        id: 'gcrtrd',
        name: 'Genesis Prod',
        operator: 'sollama58',
        publicKey: 'MCowBQYDK2VwAyEA9bvnTdmwUc3x7nEsayr83CsFN8O4zutTLgfBp3ANP0Q=',
        fingerprint: '173693ac6d4e08cb',
        role: 'genesis',
        genesisBlock: 0,
        capabilities: ['polling', 'webhooks', 'verification', 'consensus', 'approval'],
        // Private key: transmitted securely to sollama58
    })
});

/**
 * CONSENSUS RULES
 *
 * φ-based thresholds for network consensus.
 */
const CONSENSUS_RULES = Object.freeze({
    // Minimum nodes required for consensus (φ⁻¹ = 61.8%)
    THRESHOLD_RATIO: PHI_INV,

    // K-Score tolerance for agreement (±5 points)
    KSCORE_TOLERANCE: 5,

    // Minimum verifications before K-Score is "final"
    MIN_VERIFICATIONS: 2,

    // Time window for verification consensus (24 hours)
    CONSENSUS_WINDOW_MS: 24 * 60 * 60 * 1000,

    // Calculate required approvals for N nodes
    getRequiredApprovals: (activeNodes) => {
        return Math.max(1, Math.ceil(activeNodes * PHI_INV));
    },

    // Check if consensus is reached
    isConsensusReached: (agreeing, total) => {
        if (total < 2) return agreeing >= 1; // Single node = self-consensus
        return agreeing >= Math.ceil(total * PHI_INV);
    }
});

/**
 * NODE APPROVAL RULES
 *
 * How new nodes join the network.
 * Phase 1: Human approval by existing nodes
 * Phase 2: Economic stake (future)
 */
const APPROVAL_RULES = Object.freeze({
    // Current phase
    CURRENT_PHASE: 1,

    // Phase 1: Human Approval
    PHASE_1: Object.freeze({
        name: 'Human Approval',
        description: 'New nodes must be approved by φ⁻¹ of existing nodes',

        // Required approvals = ceil(active_nodes × φ⁻¹)
        getRequiredApprovals: (activeNodes) => {
            return Math.max(1, Math.ceil(activeNodes * PHI_INV));
        },

        // Genesis nodes can approve immediately (bootstrap)
        genesisCanApprove: true,

        // Approval expires after 7 days if not enough signatures
        approvalExpiryDays: 7
    }),

    // Phase 2: Economic Stake (future)
    PHASE_2: Object.freeze({
        name: 'Economic Stake',
        description: 'New nodes must stake SOL or burn tokens',
        enabled: false,  // Not yet implemented

        // Minimum stake in SOL (future)
        minStakeSOL: null,

        // Or burn ASDF tokens (future)
        minBurnASDF: null
    })
});

/**
 * SECURITY RULES
 *
 * Fundamental security constraints.
 */
const SECURITY_RULES = Object.freeze({
    // All writes must be signed
    REQUIRE_SIGNATURES: true,

    // Signature algorithm
    SIGNATURE_ALGORITHM: 'Ed25519',

    // Nonce required for replay protection
    REQUIRE_NONCE: true,

    // Maximum age of a signed message (5 minutes)
    MAX_MESSAGE_AGE_MS: 5 * 60 * 1000,

    // Heartbeat timeout (node considered dead)
    HEARTBEAT_TIMEOUT_MS: 5 * 60 * 1000,

    // Rate limits per node
    RATE_LIMITS: Object.freeze({
        TASKS_PER_MINUTE: 60,
        VERIFICATIONS_PER_HOUR: 1000,
        APPROVALS_PER_DAY: 10
    })
});

/**
 * Helper: Check if a node is a genesis node
 */
function isGenesisNode(nodeId) {
    return nodeId in GENESIS_NODES;
}

/**
 * Helper: Get genesis node by ID
 */
function getGenesisNode(nodeId) {
    return GENESIS_NODES[nodeId] || null;
}

/**
 * Helper: Get all genesis node IDs
 */
function getGenesisNodeIds() {
    return Object.keys(GENESIS_NODES);
}

/**
 * Helper: Verify a public key belongs to a genesis node
 */
function verifyGenesisPublicKey(nodeId, publicKey) {
    const genesis = GENESIS_NODES[nodeId];
    if (!genesis) return false;
    return genesis.publicKey === publicKey;
}

/**
 * Helper: Get consensus threshold for N nodes
 */
function getConsensusThreshold(activeNodes) {
    return CONSENSUS_RULES.getRequiredApprovals(activeNodes);
}

module.exports = {
    // Constants
    PHI,
    PHI_INV,
    PHI_INV_SQ,

    // Genesis
    GENESIS_NODES,
    isGenesisNode,
    getGenesisNode,
    getGenesisNodeIds,
    verifyGenesisPublicKey,

    // Rules
    CONSENSUS_RULES,
    APPROVAL_RULES,
    SECURITY_RULES,

    // Helpers
    getConsensusThreshold
};
