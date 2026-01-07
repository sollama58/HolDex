/**
 * Node Service - Decentralized Validation Infrastructure
 *
 * "Multiple nodes, shared truth, verified consensus."
 *
 * Manages the HolDex node network:
 * - Node registration and heartbeat
 * - Status tracking (active, degraded, offline)
 * - Token verification logging
 * - Consensus calculations
 */

const logger = require('./logger');
const crypto = require('crypto');
const config = require('../config/env');

// Node status constants
const NODE_STATUS = {
    PENDING: 'pending',
    ACTIVE: 'active',
    DEGRADED: 'degraded',
    OFFLINE: 'offline'
};

// Timeouts
const _HEARTBEAT_INTERVAL = 60 * 1000;          // 60 seconds (used in index.js)
const DEGRADED_THRESHOLD = 5 * 60 * 1000;       // 5 minutes without heartbeat
const OFFLINE_THRESHOLD = 15 * 60 * 1000;       // 15 minutes without heartbeat

// Current node identifier (set via env or generated)
let currentNodeId = null;

/**
 * Initialize the current node
 * @param {Object} db - Database wrapper
 * @returns {string} Node ID
 */
async function initializeNode(db) {
    // Use NODE_ID from env or generate from hostname + process
    currentNodeId = process.env.NODE_ID ||
        `node-${process.env.RENDER_SERVICE_ID || 'local'}-${process.pid}`;

    const nodeName = process.env.NODE_NAME || currentNodeId;
    const operator = process.env.NODE_OPERATOR || 'unknown';
    const apiUrl = process.env.NODE_API_URL || null;
    const region = process.env.NODE_REGION || 'unknown';

    try {
        // Upsert node registration
        await db.query(`
            INSERT INTO nodes (node_id, name, operator, api_url, region, status, last_heartbeat, joined_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
            ON CONFLICT (node_id) DO UPDATE SET
                name = EXCLUDED.name,
                operator = EXCLUDED.operator,
                api_url = EXCLUDED.api_url,
                region = EXCLUDED.region,
                status = 'active',
                last_heartbeat = EXCLUDED.last_heartbeat,
                updated_at = EXCLUDED.last_heartbeat
        `, [currentNodeId, nodeName, operator, apiUrl, region, NODE_STATUS.ACTIVE, Date.now()]);

        logger.info(`🌐 Node initialized: ${currentNodeId} (${nodeName})`);
        return currentNodeId;
    } catch (e) {
        logger.error(`❌ Node initialization failed: ${e.message}`);
        return currentNodeId;
    }
}

/**
 * Get current node ID
 * @returns {string} Node ID
 */
function getNodeId() {
    return currentNodeId;
}

/**
 * Send heartbeat for current node
 * @param {Object} db - Database wrapper
 * @param {Object} stats - Optional stats to include
 * @returns {Object} Heartbeat result
 */
async function sendHeartbeat(db, stats = {}) {
    if (!currentNodeId) {
        logger.warn('⚠️ Heartbeat skipped: Node not initialized');
        return { success: false, error: 'Node not initialized' };
    }

    const now = Date.now();

    try {
        await db.query(`
            UPDATE nodes SET
                last_heartbeat = $1,
                status = $2,
                version = $3,
                verifications_24h = COALESCE($4, verifications_24h),
                updated_at = $1
            WHERE node_id = $5
        `, [
            now,
            NODE_STATUS.ACTIVE,
            process.env.npm_package_version || '1.0.0',
            stats.verifications_24h || null,
            currentNodeId
        ]);

        // Get network status
        const networkStatus = await getNetworkStatus(db);

        logger.debug(`💓 Heartbeat sent: ${currentNodeId}`);
        return {
            success: true,
            node_id: currentNodeId,
            timestamp: now,
            network: networkStatus
        };
    } catch (e) {
        logger.error(`❌ Heartbeat failed: ${e.message}`);
        return { success: false, error: e.message };
    }
}

/**
 * Process incoming heartbeat from another node
 * @param {Object} db - Database wrapper
 * @param {Object} payload - Heartbeat payload
 * @param {string} signature - HMAC signature
 * @returns {Object} Processing result
 */
async function processHeartbeat(db, payload, signature) {
    // Verify signature
    if (!verifyHeartbeatSignature(payload, signature)) {
        logger.warn(`⚠️ Invalid heartbeat signature from ${payload.node_id}`);
        return { success: false, error: 'Invalid signature' };
    }

    const { node_id, timestamp, stats = {}, version } = payload;
    const now = Date.now();

    // Reject stale heartbeats (more than 5 minutes old)
    if (now - timestamp > DEGRADED_THRESHOLD) {
        return { success: false, error: 'Heartbeat too old' };
    }

    try {
        // Update node status
        const result = await db.query(`
            UPDATE nodes SET
                last_heartbeat = $1,
                status = $2,
                version = $3,
                verifications_24h = COALESCE($4, verifications_24h),
                tokens_verified = COALESCE($5, tokens_verified),
                updated_at = $1
            WHERE node_id = $6
            RETURNING *
        `, [
            timestamp,
            NODE_STATUS.ACTIVE,
            version || '1.0.0',
            stats.verifications_24h,
            stats.tokens_verified,
            node_id
        ]);

        if (result.rows.length === 0) {
            return { success: false, error: 'Node not registered' };
        }

        const networkStatus = await getNetworkStatus(db);

        return {
            success: true,
            acknowledged: true,
            nodes_active: networkStatus.nodes_active,
            consensus_status: networkStatus.status
        };
    } catch (e) {
        logger.error(`❌ Process heartbeat failed: ${e.message}`);
        return { success: false, error: e.message };
    }
}

/**
 * Verify heartbeat signature
 * @param {Object} payload - Heartbeat payload
 * @param {string} signature - HMAC signature
 * @returns {boolean} Valid or not
 */
function verifyHeartbeatSignature(payload, signature) {
    if (!signature || !config.DATA_SIGNING_SECRET) return false;

    const expected = crypto
        .createHmac('sha256', config.DATA_SIGNING_SECRET)
        .update(JSON.stringify(payload))
        .digest('hex');

    return signature === expected;
}

/**
 * Create heartbeat signature
 * @param {Object} payload - Heartbeat payload
 * @returns {string} HMAC signature
 */
function createHeartbeatSignature(payload) {
    return crypto
        .createHmac('sha256', config.DATA_SIGNING_SECRET)
        .update(JSON.stringify(payload))
        .digest('hex');
}

/**
 * Get network status (active nodes, health)
 * @param {Object} db - Database wrapper
 * @returns {Object} Network status
 */
async function getNetworkStatus(db) {
    try {
        const now = Date.now();

        // Get all nodes with their status
        const result = await db.query(`
            SELECT
                node_id,
                name,
                operator,
                status,
                last_heartbeat,
                verifications_24h,
                consensus_rate
            FROM nodes
            ORDER BY last_heartbeat DESC
        `);

        const nodes = result.rows.map(node => {
            const timeSinceHeartbeat = now - (node.last_heartbeat || 0);
            let currentStatus = node.status;

            // Update status based on heartbeat age
            if (timeSinceHeartbeat > OFFLINE_THRESHOLD) {
                currentStatus = NODE_STATUS.OFFLINE;
            } else if (timeSinceHeartbeat > DEGRADED_THRESHOLD) {
                currentStatus = NODE_STATUS.DEGRADED;
            }

            return {
                ...node,
                current_status: currentStatus,
                time_since_heartbeat: timeSinceHeartbeat
            };
        });

        const activeNodes = nodes.filter(n => n.current_status === NODE_STATUS.ACTIVE);
        const degradedNodes = nodes.filter(n => n.current_status === NODE_STATUS.DEGRADED);

        return {
            nodes_total: nodes.length,
            nodes_active: activeNodes.length,
            nodes_degraded: degradedNodes.length,
            nodes_offline: nodes.length - activeNodes.length - degradedNodes.length,
            status: activeNodes.length >= 1 ? 'healthy' : 'degraded',
            nodes: nodes.map(n => ({
                node_id: n.node_id,
                name: n.name,
                status: n.current_status,
                last_heartbeat: n.last_heartbeat
            }))
        };
    } catch (e) {
        logger.error(`❌ Get network status failed: ${e.message}`);
        return {
            nodes_total: 0,
            nodes_active: 0,
            status: 'unknown',
            error: e.message
        };
    }
}

/**
 * Record token verification by current node
 * @param {Object} db - Database wrapper
 * @param {string} mint - Token mint address
 * @param {number} kScore - Calculated K-Score
 * @param {boolean} signaturesValid - Whether signatures are valid
 */
async function recordVerification(db, mint, kScore, signaturesValid = true) {
    if (!currentNodeId) return;

    try {
        await db.query(`
            INSERT INTO token_verifications (mint, node_id, verified_at, k_score, signatures_valid)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (mint, node_id, (verified_at / 3600000)) DO UPDATE SET
                k_score = EXCLUDED.k_score,
                signatures_valid = EXCLUDED.signatures_valid
        `, [mint, currentNodeId, Date.now(), kScore, signaturesValid]);

        // Update node stats
        await db.query(`
            UPDATE nodes SET
                tokens_verified = tokens_verified + 1,
                verifications_24h = verifications_24h + 1,
                updated_at = $1
            WHERE node_id = $2
        `, [Date.now(), currentNodeId]);
    } catch (e) {
        // Non-critical, just log
        logger.debug(`Verification record failed: ${e.message}`);
    }
}

/**
 * Get validation info for a token (nodes that verified it)
 * @param {Object} db - Database wrapper
 * @param {string} mint - Token mint address
 * @returns {Object} Validation info
 */
async function getTokenValidation(db, mint) {
    try {
        const now = Date.now();
        const dayAgo = now - 24 * 60 * 60 * 1000;

        // Get recent verifications for this token
        const verifications = await db.query(`
            SELECT
                tv.node_id,
                tv.verified_at,
                tv.k_score,
                tv.signatures_valid,
                n.name as node_name,
                n.status as node_status,
                n.last_heartbeat
            FROM token_verifications tv
            JOIN nodes n ON tv.node_id = n.node_id
            WHERE tv.mint = $1 AND tv.verified_at > $2
            ORDER BY tv.verified_at DESC
        `, [mint, dayAgo]);

        // Get active nodes
        const networkStatus = await getNetworkStatus(db);

        // Calculate consensus
        const kScores = verifications.rows
            .filter(v => v.k_score != null)
            .map(v => v.k_score);

        let consensus = 'unknown';
        let nodesAgreed = 0;

        if (kScores.length >= 2) {
            const min = Math.min(...kScores);
            const max = Math.max(...kScores);
            const tolerance = 5; // ±5 points

            if (max - min <= tolerance) {
                consensus = 'unanimous';
                nodesAgreed = kScores.length;
            } else {
                consensus = 'divergent';
                // Count nodes within tolerance of median
                const median = kScores.sort((a, b) => a - b)[Math.floor(kScores.length / 2)];
                nodesAgreed = kScores.filter(k => Math.abs(k - median) <= tolerance).length;
            }
        } else if (kScores.length === 1) {
            consensus = 'single';
            nodesAgreed = 1;
        }

        // Most recent verification
        const lastVerified = verifications.rows[0]?.verified_at || null;

        return {
            nodes_active: networkStatus.nodes_active,
            nodes_verified: verifications.rows.length,
            nodes_agreed: nodesAgreed,
            consensus,
            last_verified: lastVerified,
            verifiers: verifications.rows.map(v => ({
                node_id: v.node_id,
                name: v.node_name,
                verified_at: v.verified_at,
                k_score: v.k_score,
                signatures_valid: v.signatures_valid
            }))
        };
    } catch (e) {
        logger.error(`❌ Get token validation failed: ${e.message}`);
        return {
            nodes_active: 0,
            nodes_verified: 0,
            consensus: 'unknown',
            error: e.message
        };
    }
}

/**
 * Update node statuses based on heartbeat age
 * Should be called periodically (e.g., every minute)
 * @param {Object} db - Database wrapper
 */
async function updateNodeStatuses(db) {
    const now = Date.now();

    try {
        // Mark nodes as degraded if no heartbeat for 5 minutes
        await db.query(`
            UPDATE nodes SET
                status = $1,
                updated_at = $2
            WHERE last_heartbeat < $3
            AND last_heartbeat >= $4
            AND status = $5
        `, [
            NODE_STATUS.DEGRADED,
            now,
            now - DEGRADED_THRESHOLD,
            now - OFFLINE_THRESHOLD,
            NODE_STATUS.ACTIVE
        ]);

        // Mark nodes as offline if no heartbeat for 15 minutes
        await db.query(`
            UPDATE nodes SET
                status = $1,
                updated_at = $2
            WHERE last_heartbeat < $3
            AND status != $1
        `, [
            NODE_STATUS.OFFLINE,
            now,
            now - OFFLINE_THRESHOLD
        ]);

        // Reset 24h verification counts at midnight UTC
        const hour = new Date().getUTCHours();
        if (hour === 0) {
            await db.query(`
                UPDATE nodes SET verifications_24h = 0
                WHERE verifications_24h > 0
            `);
        }
    } catch (e) {
        logger.error(`❌ Update node statuses failed: ${e.message}`);
    }
}

/**
 * Register a new node (admin only)
 * @param {Object} db - Database wrapper
 * @param {Object} nodeData - Node registration data
 * @returns {Object} Registration result
 */
async function registerNode(db, nodeData) {
    const { node_id, name, operator, api_url, region } = nodeData;

    if (!node_id || !name || !operator) {
        return { success: false, error: 'Missing required fields: node_id, name, operator' };
    }

    try {
        const result = await db.query(`
            INSERT INTO nodes (node_id, name, operator, api_url, region, status, joined_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (node_id) DO UPDATE SET
                name = EXCLUDED.name,
                operator = EXCLUDED.operator,
                api_url = EXCLUDED.api_url,
                region = EXCLUDED.region,
                updated_at = $7
            RETURNING *
        `, [node_id, name, operator, api_url, region, NODE_STATUS.PENDING, Date.now()]);

        logger.info(`✅ Node registered: ${node_id} (${name})`);
        return { success: true, node: result.rows[0] };
    } catch (e) {
        logger.error(`❌ Node registration failed: ${e.message}`);
        return { success: false, error: e.message };
    }
}

/**
 * Get list of all nodes
 * @param {Object} db - Database wrapper
 * @returns {Array} List of nodes
 */
async function listNodes(db) {
    try {
        const result = await db.query(`
            SELECT
                node_id,
                name,
                operator,
                api_url,
                region,
                status,
                last_heartbeat,
                tokens_verified,
                verifications_24h,
                consensus_rate,
                version,
                joined_at
            FROM nodes
            ORDER BY
                CASE status
                    WHEN 'active' THEN 1
                    WHEN 'degraded' THEN 2
                    WHEN 'pending' THEN 3
                    ELSE 4
                END,
                last_heartbeat DESC
        `);

        return result.rows;
    } catch (e) {
        logger.error(`❌ List nodes failed: ${e.message}`);
        return [];
    }
}

module.exports = {
    NODE_STATUS,
    initializeNode,
    getNodeId,
    sendHeartbeat,
    processHeartbeat,
    createHeartbeatSignature,
    verifyHeartbeatSignature,
    getNetworkStatus,
    recordVerification,
    getTokenValidation,
    updateNodeStatuses,
    registerNode,
    listNodes
};
