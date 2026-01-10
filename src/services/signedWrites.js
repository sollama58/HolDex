/**
 * Signed Writes Service
 *
 * "Don't Trust, Verify" - Every write must be signed.
 *
 * This is the core security layer for the HolDex verification network.
 * All writes to K-Score and verification data must be Ed25519 signed
 * by an approved node, creating an immutable audit trail.
 *
 * Architecture:
 * - Every K-Score write is signed with the node's Ed25519 private key
 * - Signatures are stored alongside data for verification
 * - Other nodes can verify using public keys from genesis/nodes table
 * - Creates "blockchain on PostgreSQL" - tamper-evident writes
 */

'use strict';

const crypto = require('crypto');
const logger = require('./logger');
const nodeKeys = require('../utils/nodeKeys');
const genesis = require('../config/genesis');

// Write types that require signatures
const WRITE_TYPE = {
    KSCORE_UPDATE: 'kscore_update',
    VERIFICATION: 'verification',
    APPROVAL: 'approval',
    NODE_REGISTRATION: 'node_registration',
    CONSENSUS: 'consensus'
};

/**
 * Sign a K-Score update before writing to database
 * @param {Object} data - The K-Score data to sign
 * @param {string} nodeId - The signing node's ID
 * @param {string} privateKey - The node's Ed25519 private key (base64)
 * @returns {Object} Signed write package
 */
function signKScoreUpdate(data, nodeId, privateKey) {
    const { mint, k_score, d_score, o_score, l_score, conviction_score } = data;

    const payload = {
        type: WRITE_TYPE.KSCORE_UPDATE,
        mint,
        k_score,
        d_score,
        o_score,
        l_score,
        conviction_score,
        node_id: nodeId,
        timestamp: Date.now(),
        nonce: crypto.randomBytes(16).toString('hex')
    };

    const signature = nodeKeys.signData(payload, privateKey);

    return {
        ...payload,
        signature,
        write_type: WRITE_TYPE.KSCORE_UPDATE
    };
}

/**
 * Sign a verification record
 * @param {Object} data - Verification data
 * @param {string} nodeId - The signing node's ID
 * @param {string} privateKey - The node's Ed25519 private key
 * @returns {Object} Signed verification package
 */
function signVerification(data, nodeId, privateKey) {
    const { mint, k_score, verification_hash } = data;

    const payload = {
        type: WRITE_TYPE.VERIFICATION,
        mint,
        k_score,
        verification_hash,
        node_id: nodeId,
        timestamp: Date.now(),
        nonce: crypto.randomBytes(16).toString('hex')
    };

    const signature = nodeKeys.signData(payload, privateKey);

    return {
        ...payload,
        signature,
        write_type: WRITE_TYPE.VERIFICATION
    };
}

/**
 * Sign a node approval
 * @param {Object} data - Approval data
 * @param {string} nodeId - The approving node's ID
 * @param {string} privateKey - The approving node's private key
 * @returns {Object} Signed approval package
 */
function signApproval(data, nodeId, privateKey) {
    const { target_node_id, target_fingerprint } = data;

    const payload = {
        type: WRITE_TYPE.APPROVAL,
        action: 'approve_node',
        target_node_id,
        target_fingerprint,
        node_id: nodeId, // Use node_id for consistent verification
        timestamp: Date.now(),
        nonce: crypto.randomBytes(16).toString('hex')
    };

    const signature = nodeKeys.signData(payload, privateKey);

    return {
        ...payload,
        signature,
        write_type: WRITE_TYPE.APPROVAL
    };
}

/**
 * Verify a signed write
 * @param {Object} signedWrite - The signed write package
 * @param {Object} db - Database connection
 * @returns {Object} Verification result { valid: boolean, error?: string }
 */
async function verifySignedWrite(signedWrite, db) {
    const { signature, write_type: _write_type, ...restOfPayload } = signedWrite;

    // Extract node_id and timestamp from the payload
    const { node_id, timestamp } = restOfPayload;

    // 1. Check timestamp is recent (prevent replay attacks)
    const age = Date.now() - timestamp;
    if (age > genesis.SECURITY_RULES.MAX_MESSAGE_AGE_MS) {
        return { valid: false, error: 'Signature expired' };
    }

    // 2. Get the node's public key
    let publicKey = null;

    // Check genesis first
    const genesisNode = genesis.getGenesisNode(node_id);
    if (genesisNode) {
        publicKey = genesisNode.publicKey;
    } else {
        // Check database
        const result = await db.query(
            'SELECT node_public_key FROM nodes WHERE node_id = $1 AND approval_status = $2',
            [node_id, 'approved']
        );

        if (result.rows.length === 0) {
            return { valid: false, error: 'Node not found or not approved' };
        }

        publicKey = result.rows[0].node_public_key;
    }

    if (!publicKey) {
        return { valid: false, error: 'No public key for node' };
    }

    // 3. The payload to verify is everything except 'signature' and 'write_type'
    // which were added AFTER signing
    const verifyPayload = { ...restOfPayload };

    const verifyResult = nodeKeys.verifySignature(verifyPayload, signature, publicKey);

    if (!verifyResult.valid) {
        return { valid: false, error: verifyResult.error || 'Invalid signature' };
    }

    return { valid: true, node_id, timestamp };
}

/**
 * Execute a signed write to the database
 * Only proceeds if signature is valid
 * @param {Object} signedWrite - The signed write package
 * @param {Object} db - Database connection
 * @returns {Object} Result { success: boolean, error?: string }
 */
async function executeSignedWrite(signedWrite, db) {
    // 1. Verify the signature first
    const verification = await verifySignedWrite(signedWrite, db);

    if (!verification.valid) {
        logger.warn(`[SignedWrites] Rejected invalid write: ${verification.error}`);
        return { success: false, error: verification.error };
    }

    const { write_type, mint, node_id, timestamp } = signedWrite;

    try {
        switch (write_type) {
            case WRITE_TYPE.KSCORE_UPDATE:
                await executeKScoreWrite(signedWrite, db);
                break;

            case WRITE_TYPE.VERIFICATION:
                await executeVerificationWrite(signedWrite, db);
                break;

            case WRITE_TYPE.APPROVAL:
                // Approval writes are handled by nodeApproval service
                logger.debug('[SignedWrites] Approval write delegated to nodeApproval');
                break;

            default:
                return { success: false, error: `Unknown write type: ${write_type}` };
        }

        logger.info(`[SignedWrites] ✅ Executed ${write_type} by ${node_id} for ${mint?.slice(0, 8) || 'N/A'}`);
        return { success: true, node_id, timestamp };

    } catch (error) {
        logger.error(`[SignedWrites] ❌ Write failed: ${error.message}`);
        return { success: false, error: error.message };
    }
}

/**
 * Execute a signed K-Score update
 */
async function executeKScoreWrite(signedWrite, db) {
    const { mint, k_score, d_score: _d_score, o_score: _o_score, l_score: _l_score, conviction_score: _conviction_score, node_id, signature, timestamp } = signedWrite;

    // Update token with signed K-Score
    await db.query(`
        UPDATE tokens SET
            k_score = $1,
            last_k_score_update = $2,
            sig_node_id = $3,
            sig_node_signature = $4,
            sig_node_timestamp = $5
        WHERE mint = $6
    `, [k_score, timestamp, node_id, signature, timestamp, mint]);

    // Record in token_verifications for consensus tracking
    // Wrapped in try-catch to handle foreign key violations if node doesn't exist yet
    try {
        await db.query(`
            INSERT INTO token_verifications (mint, node_id, k_score, node_signature, verified_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (mint, node_id) DO UPDATE SET
                k_score = EXCLUDED.k_score,
                node_signature = EXCLUDED.node_signature,
                verified_at = EXCLUDED.verified_at
        `, [mint, node_id, k_score, signature, timestamp]);
    } catch (verifyErr) {
        // Foreign key violation (node not in nodes table) - log but don't fail the K-Score update
        if (verifyErr.code === '23503') {
            logger.warn(`[SignedWrites] Skipping token_verification for ${mint}: node ${node_id} not registered`);
        } else {
            throw verifyErr;
        }
    }
}

/**
 * Execute a signed verification write
 */
async function executeVerificationWrite(signedWrite, db) {
    const { mint, k_score, verification_hash: _verification_hash, node_id, signature, timestamp } = signedWrite;

    try {
        await db.query(`
            INSERT INTO token_verifications (mint, node_id, k_score, node_signature, verified_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (mint, node_id) DO UPDATE SET
                k_score = EXCLUDED.k_score,
                node_signature = EXCLUDED.node_signature,
                verified_at = EXCLUDED.verified_at
        `, [mint, node_id, k_score, signature, timestamp]);
    } catch (err) {
        // Foreign key violation (node not in nodes table) - log but don't fail
        if (err.code === '23503') {
            logger.warn(`[SignedWrites] Skipping verification write for ${mint}: node ${node_id} not registered`);
        } else {
            throw err;
        }
    }
}

/**
 * Get all verifications for a token (for consensus checking)
 * @param {string} mint - Token mint address
 * @param {Object} db - Database connection
 * @returns {Array} Array of verified K-Scores from different nodes
 */
async function getTokenVerifications(mint, db) {
    const cutoff = Date.now() - genesis.CONSENSUS_RULES.CONSENSUS_WINDOW_MS;

    const result = await db.query(`
        SELECT
            v.node_id, v.k_score, v.node_signature, v.verified_at,
            n.is_genesis, n.approval_status
        FROM token_verifications v
        JOIN nodes n ON v.node_id = n.node_id
        WHERE v.mint = $1
          AND v.verified_at > $2
          AND (n.approval_status = 'approved' OR n.is_genesis = TRUE)
        ORDER BY v.verified_at DESC
    `, [mint, cutoff]);

    return result.rows;
}

/**
 * Check if a token has reached consensus
 * @param {string} mint - Token mint address
 * @param {Object} db - Database connection
 * @returns {Object} { hasConsensus: boolean, k_score?: number, agreeing?: number, total?: number }
 */
async function checkTokenConsensus(mint, db) {
    const verifications = await getTokenVerifications(mint, db);

    if (verifications.length < genesis.CONSENSUS_RULES.MIN_VERIFICATIONS) {
        return { hasConsensus: false, reason: 'Not enough verifications' };
    }

    // Count active nodes
    const activeResult = await db.query(`
        SELECT COUNT(*) as count FROM nodes
        WHERE (approval_status = 'approved' OR is_genesis = TRUE)
          AND status IN ('active', 'degraded')
    `);
    const totalNodes = parseInt(activeResult.rows[0].count, 10);

    // Find median K-Score
    const kScores = verifications.map(v => v.k_score).sort((a, b) => a - b);
    const median = kScores[Math.floor(kScores.length / 2)];

    // Count agreeing nodes (within tolerance)
    const agreeing = verifications.filter(v =>
        Math.abs(v.k_score - median) <= genesis.CONSENSUS_RULES.KSCORE_TOLERANCE
    );

    const hasConsensus = genesis.CONSENSUS_RULES.isConsensusReached(agreeing.length, totalNodes);

    return {
        hasConsensus,
        k_score: median,
        agreeing: agreeing.length,
        total: totalNodes,
        required: genesis.CONSENSUS_RULES.getRequiredApprovals(totalNodes),
        verifications: verifications.length
    };
}

/**
 * Record consensus once reached
 * @param {string} mint - Token mint address
 * @param {number} k_score - Consensus K-Score
 * @param {number} agreeing - Number of agreeing nodes
 * @param {number} total - Total active nodes
 * @param {Object} db - Database connection
 */
async function recordConsensus(mint, k_score, agreeing, total, db) {
    await db.query(`
        INSERT INTO consensus_snapshots (mint, k_score_consensus, agreeing_nodes, total_nodes, consensus_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (mint) DO UPDATE SET
            k_score_consensus = EXCLUDED.k_score_consensus,
            agreeing_nodes = EXCLUDED.agreeing_nodes,
            total_nodes = EXCLUDED.total_nodes,
            consensus_at = EXCLUDED.consensus_at
    `, [mint, k_score, agreeing, total, Date.now()]);

    // Also update the token's consensus fields
    await db.query(`
        UPDATE tokens SET
            k_score_consensus_nodes = $1,
            k_score_confidence = $2
        WHERE mint = $3
    `, [agreeing, agreeing / total, mint]);

    logger.info(`[SignedWrites] 🔒 Consensus recorded: ${mint.slice(0, 8)}... K=${k_score} (${agreeing}/${total} nodes)`);
}

module.exports = {
    WRITE_TYPE,
    signKScoreUpdate,
    signVerification,
    signApproval,
    verifySignedWrite,
    executeSignedWrite,
    getTokenVerifications,
    checkTokenConsensus,
    recordConsensus
};
