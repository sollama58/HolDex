/**
 * Node Approval Service
 *
 * Manages node registration and approval for the HolDex verification network.
 *
 * Phase 1: Human approval by existing nodes (φ⁻¹ threshold)
 * Phase 2: Economic stake (future)
 *
 * "Don't Trust, Verify" - New nodes must earn trust through approval.
 */

'use strict';

const _crypto = require('crypto');
const logger = require('./logger');
const nodeKeys = require('../utils/nodeKeys');
const genesis = require('../config/genesis');

// Approval status constants
const APPROVAL_STATUS = {
    PENDING: 'pending',
    APPROVED: 'approved',
    REJECTED: 'rejected',
    EXPIRED: 'expired'
};

/**
 * Initialize genesis nodes in database
 * Called once at startup to ensure genesis nodes are registered
 */
async function initializeGenesisNodes(db) {
    const genesisIds = genesis.getGenesisNodeIds();
    const now = Date.now();

    logger.info(`[Genesis] Initializing ${genesisIds.length} genesis nodes...`);

    for (const nodeId of genesisIds) {
        const node = genesis.getGenesisNode(nodeId);

        try {
            await db.query(`
                INSERT INTO nodes (
                    node_id, name, operator, status,
                    node_public_key, node_key_fingerprint,
                    is_genesis, approval_status, approved_at,
                    capabilities, joined_at, last_heartbeat
                )
                VALUES ($1, $2, $3, 'active', $4, $5, TRUE, 'approved', $6, $7, $6, $6)
                ON CONFLICT (node_id) DO UPDATE SET
                    node_public_key = COALESCE(EXCLUDED.node_public_key, nodes.node_public_key),
                    node_key_fingerprint = COALESCE(EXCLUDED.node_key_fingerprint, nodes.node_key_fingerprint),
                    is_genesis = TRUE,
                    approval_status = 'approved'
            `, [
                node.id,
                node.name,
                node.operator,
                node.publicKey,
                node.fingerprint,
                now,
                JSON.stringify(node.capabilities)
            ]);

            logger.info(`[Genesis] ✅ Node registered: ${node.id} (${node.fingerprint})`);
        } catch (error) {
            logger.error(`[Genesis] ❌ Failed to register ${node.id}: ${error.message}`);
        }
    }
}

/**
 * Request approval for a new node
 * Creates a pending approval request that needs signatures from existing nodes
 */
async function requestApproval(db, nodeData, signature) {
    const { node_id, name, operator, public_key, fingerprint, capabilities } = nodeData;

    // Validate required fields
    if (!node_id || !public_key || !fingerprint) {
        return { success: false, error: 'Missing required fields: node_id, public_key, fingerprint' };
    }

    // Check if already a genesis node (no approval needed)
    if (genesis.isGenesisNode(node_id)) {
        return { success: false, error: 'Genesis nodes are pre-approved' };
    }

    // Verify the signature proves ownership of the private key
    const message = { node_id, public_key, fingerprint, timestamp: Math.floor(Date.now() / 1000) };
    const sigValid = nodeKeys.verifySignature(message, signature, public_key);

    if (!sigValid.valid) {
        return { success: false, error: 'Invalid signature - cannot prove key ownership' };
    }

    const now = Date.now();
    const expiresAt = now + (genesis.APPROVAL_RULES.PHASE_1.approvalExpiryDays * 24 * 60 * 60 * 1000);

    try {
        // Check if node already exists
        const existing = await db.query('SELECT * FROM nodes WHERE node_id = $1', [node_id]);

        if (existing.rows.length > 0) {
            const existingNode = existing.rows[0];
            if (existingNode.approval_status === 'approved') {
                return { success: false, error: 'Node already approved' };
            }
            if (existingNode.approval_status === 'pending') {
                return { success: false, error: 'Approval already pending' };
            }
        }

        // Get current active node count for threshold calculation
        const activeNodes = await getActiveNodeCount(db);
        const requiredApprovals = genesis.APPROVAL_RULES.PHASE_1.getRequiredApprovals(activeNodes);

        // Insert pending node
        await db.query(`
            INSERT INTO nodes (
                node_id, name, operator, status,
                node_public_key, node_key_fingerprint,
                is_genesis, approval_status,
                required_approvals, current_approvals,
                approval_expires_at, joined_at, capabilities
            )
            VALUES ($1, $2, $3, 'pending', $4, $5, FALSE, 'pending', $6, 0, $7, $8, $9)
            ON CONFLICT (node_id) DO UPDATE SET
                approval_status = 'pending',
                required_approvals = $6,
                current_approvals = 0,
                approval_expires_at = $7,
                node_public_key = $4,
                node_key_fingerprint = $5
        `, [
            node_id, name || node_id, operator || 'unknown',
            public_key, fingerprint,
            requiredApprovals, expiresAt, now,
            JSON.stringify(capabilities || ['polling', 'verification'])
        ]);

        logger.info(`[Approval] New node pending: ${node_id} (needs ${requiredApprovals} approvals)`);

        return {
            success: true,
            node_id,
            status: APPROVAL_STATUS.PENDING,
            required_approvals: requiredApprovals,
            expires_at: expiresAt
        };

    } catch (error) {
        logger.error(`[Approval] Request failed: ${error.message}`);
        return { success: false, error: error.message };
    }
}

/**
 * Approve a pending node
 * An existing approved node signs their approval
 */
async function approveNode(db, targetNodeId, approverNodeId, signature) {
    // Verify approver is an approved node
    const approver = await db.query(
        'SELECT * FROM nodes WHERE node_id = $1 AND (approval_status = $2 OR is_genesis = TRUE)',
        [approverNodeId, APPROVAL_STATUS.APPROVED]
    );

    if (approver.rows.length === 0) {
        return { success: false, error: 'Approver is not an approved node' };
    }

    const approverNode = approver.rows[0];

    // Verify target node exists and is pending
    const target = await db.query(
        'SELECT * FROM nodes WHERE node_id = $1 AND approval_status = $2',
        [targetNodeId, APPROVAL_STATUS.PENDING]
    );

    if (target.rows.length === 0) {
        return { success: false, error: 'Target node not found or not pending approval' };
    }

    const targetNode = target.rows[0];

    // Check expiry
    if (targetNode.approval_expires_at && Date.now() > targetNode.approval_expires_at) {
        await db.query(
            'UPDATE nodes SET approval_status = $1 WHERE node_id = $2',
            [APPROVAL_STATUS.EXPIRED, targetNodeId]
        );
        return { success: false, error: 'Approval request has expired' };
    }

    // Verify signature
    const message = {
        action: 'approve_node',
        target_node_id: targetNodeId,
        target_fingerprint: targetNode.node_key_fingerprint,
        approver_node_id: approverNodeId,
        timestamp: Math.floor(Date.now() / 1000)
    };

    const sigValid = nodeKeys.verifySignature(message, signature, approverNode.node_public_key);
    if (!sigValid.valid) {
        return { success: false, error: 'Invalid approval signature' };
    }

    const now = Date.now();

    try {
        // Record the approval
        await db.query(`
            INSERT INTO node_approvals (
                node_id, approved_by, approval_signature, approved_at
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (node_id, approved_by) DO UPDATE SET
                approval_signature = $3,
                approved_at = $4
        `, [targetNodeId, approverNodeId, signature, now]);

        // Count current approvals
        const approvalCount = await db.query(
            'SELECT COUNT(*) as count FROM node_approvals WHERE node_id = $1',
            [targetNodeId]
        );
        const currentApprovals = parseInt(approvalCount.rows[0].count, 10);

        // Update node approval count
        await db.query(
            'UPDATE nodes SET current_approvals = $1 WHERE node_id = $2',
            [currentApprovals, targetNodeId]
        );

        // Check if threshold reached
        const requiredApprovals = targetNode.required_approvals;

        if (currentApprovals >= requiredApprovals) {
            // Fully approved!
            await db.query(`
                UPDATE nodes SET
                    approval_status = $1,
                    approved_at = $2,
                    status = 'active'
                WHERE node_id = $3
            `, [APPROVAL_STATUS.APPROVED, now, targetNodeId]);

            logger.info(`[Approval] ✅ Node APPROVED: ${targetNodeId} (${currentApprovals}/${requiredApprovals})`);

            return {
                success: true,
                node_id: targetNodeId,
                status: APPROVAL_STATUS.APPROVED,
                approvals: currentApprovals,
                required: requiredApprovals,
                approved: true
            };
        }

        logger.info(`[Approval] Node ${targetNodeId}: ${currentApprovals}/${requiredApprovals} approvals`);

        return {
            success: true,
            node_id: targetNodeId,
            status: APPROVAL_STATUS.PENDING,
            approvals: currentApprovals,
            required: requiredApprovals,
            approved: false
        };

    } catch (error) {
        logger.error(`[Approval] Failed: ${error.message}`);
        return { success: false, error: error.message };
    }
}

/**
 * Get the count of active (approved) nodes
 */
async function getActiveNodeCount(db) {
    const result = await db.query(`
        SELECT COUNT(*) as count FROM nodes
        WHERE (approval_status = 'approved' OR is_genesis = TRUE)
          AND status IN ('active', 'degraded')
    `);
    return parseInt(result.rows[0].count, 10) || 0;
}

/**
 * Check if a node is approved (can participate in consensus)
 */
async function isNodeApproved(db, nodeId) {
    // Genesis nodes are always approved
    if (genesis.isGenesisNode(nodeId)) {
        return true;
    }

    const result = await db.query(
        'SELECT approval_status FROM nodes WHERE node_id = $1',
        [nodeId]
    );

    if (result.rows.length === 0) {
        return false;
    }

    return result.rows[0].approval_status === APPROVAL_STATUS.APPROVED;
}

/**
 * Verify a node's identity (public key matches registered key)
 */
async function verifyNodeIdentity(db, nodeId, publicKey) {
    // Check genesis first
    if (genesis.isGenesisNode(nodeId)) {
        return genesis.verifyGenesisPublicKey(nodeId, publicKey);
    }

    // Check database
    const result = await db.query(
        'SELECT node_public_key FROM nodes WHERE node_id = $1',
        [nodeId]
    );

    if (result.rows.length === 0) {
        return false;
    }

    return result.rows[0].node_public_key === publicKey;
}

/**
 * Get pending approval requests
 */
async function getPendingApprovals(db) {
    const result = await db.query(`
        SELECT
            n.node_id, n.name, n.operator,
            n.node_key_fingerprint as fingerprint,
            n.required_approvals, n.current_approvals,
            n.approval_expires_at, n.joined_at,
            array_agg(na.approved_by) as approvers
        FROM nodes n
        LEFT JOIN node_approvals na ON n.node_id = na.node_id
        WHERE n.approval_status = 'pending'
        GROUP BY n.node_id, n.name, n.operator, n.node_key_fingerprint,
                 n.required_approvals, n.current_approvals,
                 n.approval_expires_at, n.joined_at
        ORDER BY n.joined_at ASC
    `);

    return result.rows;
}

/**
 * Get all approved nodes
 */
async function getApprovedNodes(db) {
    const result = await db.query(`
        SELECT
            node_id, name, operator,
            node_key_fingerprint as fingerprint,
            is_genesis, approved_at, status,
            capabilities, last_heartbeat
        FROM nodes
        WHERE approval_status = 'approved' OR is_genesis = TRUE
        ORDER BY is_genesis DESC, approved_at ASC
    `);

    return result.rows;
}

module.exports = {
    APPROVAL_STATUS,
    initializeGenesisNodes,
    requestApproval,
    approveNode,
    getActiveNodeCount,
    isNodeApproved,
    verifyNodeIdentity,
    getPendingApprovals,
    getApprovedNodes
};
