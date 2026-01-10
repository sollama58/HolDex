/**
 * Node Service - Decentralized Validation Infrastructure
 *
 * "Multiple nodes, shared truth, verified consensus."
 * Philosophy: "Don't trust. Verify." - Each node has unique cryptographic identity
 *
 * Manages the HolDex node network:
 * - Node registration and heartbeat
 * - Per-node Ed25519 key management
 * - Status tracking (active, degraded, offline)
 * - Token verification logging with cryptographic signatures
 * - Consensus calculations
 */

const logger = require('./logger');
const crypto = require('crypto');
const config = require('../config/env');
const nodeKeys = require('../utils/nodeKeys');
const { signNodeIdentity, signNodeStatus, verifyNodeSignatures } = require('../utils/dataSignature');

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

// Current node state
let currentNodeId = null;
let currentNodePublicKey = null;
let currentNodeFingerprint = null;

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
    const now = Date.now();

    // Derive public key from private key if available
    const privateKey = nodeKeys.getNodePrivateKey();
    let publicKey = null;
    let fingerprint = null;

    if (privateKey) {
        const derived = nodeKeys.derivePublicKey(privateKey);
        if (derived) {
            publicKey = derived.publicKey;
            fingerprint = derived.fingerprint;
            currentNodePublicKey = publicKey;
            currentNodeFingerprint = fingerprint;
            logger.info(`🔑 Node key loaded: ${fingerprint}`);
        } else {
            logger.warn('⚠️ NODE_PRIVATE_KEY set but failed to derive public key');
        }
    } else {
        logger.warn('⚠️ No NODE_PRIVATE_KEY - verifications will be unsigned');
    }

    try {
        // Build node object for signing
        const nodeData = {
            node_id: currentNodeId,
            name: nodeName,
            operator,
            node_key_fingerprint: fingerprint,
            region,
            status: NODE_STATUS.ACTIVE,
            last_heartbeat: now,
            version: process.env.npm_package_version || '1.0.0',
            tokens_verified: 0,
            verifications_24h: 0
        };

        // Sign node data (HMAC integrity protection)
        const sigIdentity = signNodeIdentity(nodeData);
        const sigStatus = signNodeStatus(nodeData);
        const chaosNonce = require('crypto').randomBytes(16).toString('hex');

        // Upsert node registration with key info AND signatures
        await db.query(`
            INSERT INTO nodes (
                node_id, name, operator, api_url, region,
                status, last_heartbeat, joined_at,
                node_public_key, node_key_fingerprint, key_registered_at,
                sig_node_identity, sig_node_status, node_chaos_nonce, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8, $9, $10, $11, $12, $13, $7)
            ON CONFLICT (node_id) DO UPDATE SET
                name = EXCLUDED.name,
                operator = EXCLUDED.operator,
                api_url = EXCLUDED.api_url,
                region = EXCLUDED.region,
                status = 'active',
                last_heartbeat = EXCLUDED.last_heartbeat,
                updated_at = EXCLUDED.last_heartbeat,
                node_public_key = COALESCE(EXCLUDED.node_public_key, nodes.node_public_key),
                node_key_fingerprint = COALESCE(EXCLUDED.node_key_fingerprint, nodes.node_key_fingerprint),
                key_registered_at = CASE
                    WHEN nodes.node_public_key IS NULL AND EXCLUDED.node_public_key IS NOT NULL
                    THEN EXCLUDED.key_registered_at
                    ELSE nodes.key_registered_at
                END,
                sig_node_identity = EXCLUDED.sig_node_identity,
                sig_node_status = EXCLUDED.sig_node_status,
                node_chaos_nonce = EXCLUDED.node_chaos_nonce
        `, [
            currentNodeId, nodeName, operator, apiUrl, region,
            NODE_STATUS.ACTIVE, now,
            publicKey, fingerprint, publicKey ? now : null,
            sigIdentity, sigStatus, chaosNonce
        ]);

        const keyStatus = fingerprint ? `🔑 ${fingerprint}` : '⚠️ unsigned';
        logger.info(`🌐 Node initialized: ${currentNodeId} (${nodeName}) [${keyStatus}] [sig:✅]`);
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
    const version = process.env.npm_package_version || '1.0.0';
    const newStatus = NODE_STATUS.ACTIVE;

    try {
        // First update the heartbeat
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
            newStatus,
            version,
            stats.verifications_24h || null,
            currentNodeId
        ]);

        // CRITICAL: Re-sign the status signature after heartbeat update
        // The status signature includes last_heartbeat, so it must be re-signed
        // Fetch current node state to ensure signature matches exactly what's in DB
        const nodeResult = await db.query(`
            SELECT tokens_verified, verifications_24h FROM nodes WHERE node_id = $1
        `, [currentNodeId]);

        const nodeForSigning = {
            node_id: currentNodeId,
            status: newStatus,
            last_heartbeat: now,
            version: version,
            tokens_verified: nodeResult.rows[0]?.tokens_verified || 0,
            verifications_24h: nodeResult.rows[0]?.verifications_24h || 0
        };
        const newStatusSig = signNodeStatus(nodeForSigning);

        await db.query(`
            UPDATE nodes SET sig_node_status = $1 WHERE node_id = $2
        `, [newStatusSig, currentNodeId]);

        // Get network status
        const networkStatus = await getNetworkStatus(db);

        logger.debug(`💓 Heartbeat sent: ${currentNodeId} [sig:✅]`);
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
    const { node_id, timestamp, stats = {}, version } = payload;

    if (!node_id) {
        return { success: false, error: 'Missing node_id in payload' };
    }

    try {
        // SECURITY: Fetch node's public key for Ed25519 verification
        const nodeResult = await db.query(
            'SELECT node_public_key FROM nodes WHERE node_id = $1',
            [node_id]
        );

        if (nodeResult.rows.length === 0) {
            logger.warn(`⚠️ Heartbeat from unknown node: ${node_id}`);
            return { success: false, error: 'Node not registered' };
        }

        const publicKey = nodeResult.rows[0].node_public_key;

        // Verify signature with node's Ed25519 key (or legacy HMAC fallback)
        if (!verifyHeartbeatSignature(payload, signature, publicKey)) {
            logger.warn(`⚠️ Invalid heartbeat signature from ${node_id}`);
            return { success: false, error: 'Invalid signature' };
        }

        const now = Date.now();

        // Reject stale heartbeats (more than 5 minutes old)
        if (now - timestamp > DEGRADED_THRESHOLD) {
            return { success: false, error: 'Heartbeat too old' };
        }

        // Update node status and re-sign status (HMAC integrity)
        const nodeData = {
            node_id,
            status: NODE_STATUS.ACTIVE,
            last_heartbeat: timestamp,
            version: version || '1.0.0',
            tokens_verified: stats.tokens_verified || 0,
            verifications_24h: stats.verifications_24h || 0
        };
        const sigStatus = signNodeStatus(nodeData);

        const result = await db.query(`
            UPDATE nodes SET
                last_heartbeat = $1,
                status = $2,
                version = $3,
                verifications_24h = COALESCE($4, verifications_24h),
                tokens_verified = COALESCE($5, tokens_verified),
                updated_at = $1,
                sig_node_status = $6
            WHERE node_id = $7
            RETURNING *
        `, [
            timestamp,
            NODE_STATUS.ACTIVE,
            version || '1.0.0',
            stats.verifications_24h,
            stats.tokens_verified,
            sigStatus,
            node_id
        ]);

        if (result.rows.length === 0) {
            return { success: false, error: 'Node update failed' };
        }

        const networkStatus = await getNetworkStatus(db);

        return {
            success: true,
            acknowledged: true,
            nodes_active: networkStatus.nodes_active,
            consensus_status: networkStatus.status,
            signature_type: publicKey ? 'ed25519' : 'legacy_hmac'
        };
    } catch (e) {
        logger.error(`❌ Process heartbeat failed: ${e.message}`);
        return { success: false, error: e.message };
    }
}

/**
 * Verify heartbeat signature using Ed25519 per-node key
 * SECURITY: Uses per-node asymmetric key, not compromised shared secret
 * @param {Object} payload - Heartbeat payload (must include node_id)
 * @param {string} signature - Ed25519 signature
 * @param {string} publicKey - Node's public key (fetched from DB)
 * @returns {boolean} Valid or not
 */
function verifyHeartbeatSignature(payload, signature, publicKey) {
    if (!signature || !publicKey) {
        // Fallback to HMAC for backward compatibility during migration
        if (config.DATA_SIGNING_SECRET && signature) {
            const expected = crypto
                .createHmac('sha256', config.DATA_SIGNING_SECRET)
                .update(JSON.stringify(payload))
                .digest('hex');
            if (signature === expected) {
                logger.debug('[Heartbeat] Verified with legacy HMAC (migration mode)');
                return true;
            }
        }
        return false;
    }

    // Use Ed25519 verification (per-node key) - preferred
    const result = nodeKeys.verifySignature(payload, signature, publicKey);
    return result.valid;
}

/**
 * Create heartbeat signature with node's Ed25519 private key
 * SECURITY: Uses per-node asymmetric key, not compromised shared secret
 * @param {Object} payload - Heartbeat payload
 * @returns {string} Ed25519 signature or legacy HMAC if no key
 */
function createHeartbeatSignature(payload) {
    const privateKey = nodeKeys.getNodePrivateKey();
    if (privateKey) {
        // Use Ed25519 per-node signature (preferred)
        return nodeKeys.signData(payload, privateKey);
    }

    // Fallback to legacy HMAC (during migration)
    if (config.DATA_SIGNING_SECRET) {
        logger.warn('[Heartbeat] Using legacy HMAC (set NODE_PRIVATE_KEY for Ed25519)');
        return crypto
            .createHmac('sha256', config.DATA_SIGNING_SECRET)
            .update(JSON.stringify(payload))
            .digest('hex');
    }

    logger.warn('[Heartbeat] No signing key available - heartbeat unsigned');
    return null;
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
 * Signs the verification with node's private key for cryptographic proof
 * @param {Object} db - Database wrapper
 * @param {string} mint - Token mint address
 * @param {number} kScore - Calculated K-Score
 * @param {boolean} signaturesValid - Whether signatures are valid
 */
async function recordVerification(db, mint, kScore, signaturesValid = true) {
    if (!currentNodeId) return;

    const now = Date.now();

    // Create verification object for signing
    const verification = {
        mint,
        node_id: currentNodeId,
        verified_at: now,
        k_score: kScore,
        signatures_valid: signaturesValid
    };

    // Sign with node's private key (if available)
    const privateKey = nodeKeys.getNodePrivateKey();
    const nodeSignature = privateKey
        ? nodeKeys.signVerification(verification, privateKey)
        : null;

    try {
        await db.query(`
            INSERT INTO token_verifications (
                mint, node_id, verified_at, k_score, signatures_valid,
                node_signature, signature_version
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (mint, node_id) DO UPDATE SET
                verified_at = EXCLUDED.verified_at,
                k_score = EXCLUDED.k_score,
                signatures_valid = EXCLUDED.signatures_valid,
                node_signature = EXCLUDED.node_signature,
                signature_version = EXCLUDED.signature_version
        `, [
            mint, currentNodeId, now, kScore, signaturesValid,
            nodeSignature, nodeSignature ? 'v1' : null
        ]);

        // Update node stats
        await db.query(`
            UPDATE nodes SET
                tokens_verified = tokens_verified + 1,
                verifications_24h = verifications_24h + 1,
                updated_at = $1
            WHERE node_id = $2
        `, [now, currentNodeId]);
    } catch (e) {
        // Non-critical, just log
        logger.debug(`Verification record failed: ${e.message}`);
    }
}

/**
 * Get validation info for a token (nodes that verified it)
 * Includes cryptographic verification of node signatures
 * @param {Object} db - Database wrapper
 * @param {string} mint - Token mint address
 * @returns {Object} Validation info with signature verification
 */
async function getTokenValidation(db, mint) {
    try {
        const now = Date.now();
        const dayAgo = now - 24 * 60 * 60 * 1000;

        // Get recent verifications with node public keys for signature verification
        const verifications = await db.query(`
            SELECT
                tv.node_id,
                tv.verified_at,
                tv.k_score,
                tv.signatures_valid,
                tv.node_signature,
                tv.signature_version,
                n.name as node_name,
                n.status as node_status,
                n.last_heartbeat,
                n.node_public_key,
                n.node_key_fingerprint
            FROM token_verifications tv
            JOIN nodes n ON tv.node_id = n.node_id
            WHERE tv.mint = $1 AND tv.verified_at > $2
            ORDER BY tv.verified_at DESC
        `, [mint, dayAgo]);

        // Get active nodes
        const networkStatus = await getNetworkStatus(db);

        // Verify signatures and calculate consensus
        const verifiedResults = verifications.rows.map(v => {
            let nodeSigValid = false;
            let nodeSigError = null;

            // Verify node signature if present
            if (v.node_signature && v.node_public_key) {
                const verification = {
                    mint,
                    node_id: v.node_id,
                    verified_at: parseInt(v.verified_at, 10),
                    k_score: v.k_score,
                    signatures_valid: v.signatures_valid
                };
                const result = nodeKeys.verifyVerificationSignature(
                    verification,
                    v.node_signature,
                    v.node_public_key
                );
                nodeSigValid = result.valid;
                nodeSigError = result.error;
            } else if (v.node_signature && !v.node_public_key) {
                nodeSigError = 'No public key registered';
            } else {
                nodeSigError = 'Unsigned verification';
            }

            return {
                ...v,
                node_sig_valid: nodeSigValid,
                node_sig_error: nodeSigError
            };
        });

        // Calculate consensus (only from cryptographically verified nodes)
        const trustedVerifications = verifiedResults.filter(v => v.node_sig_valid);
        const allKScores = verifiedResults
            .filter(v => v.k_score != null)
            .map(v => v.k_score);
        const trustedKScores = trustedVerifications
            .filter(v => v.k_score != null)
            .map(v => v.k_score);

        let consensus = 'unknown';
        let nodesAgreed = 0;
        let trustedConsensus = 'unknown';
        let trustedNodesAgreed = 0;

        // Calculate overall consensus
        if (allKScores.length >= 2) {
            const min = Math.min(...allKScores);
            const max = Math.max(...allKScores);
            const tolerance = 5; // ±5 points

            if (max - min <= tolerance) {
                consensus = 'unanimous';
                nodesAgreed = allKScores.length;
            } else {
                consensus = 'divergent';
                const sorted = [...allKScores].sort((a, b) => a - b);
                const median = sorted[Math.floor(sorted.length / 2)];
                nodesAgreed = allKScores.filter(k => Math.abs(k - median) <= tolerance).length;
            }
        } else if (allKScores.length === 1) {
            consensus = 'single';
            nodesAgreed = 1;
        }

        // Calculate trusted (signed) consensus separately
        if (trustedKScores.length >= 2) {
            const min = Math.min(...trustedKScores);
            const max = Math.max(...trustedKScores);
            const tolerance = 5;

            if (max - min <= tolerance) {
                trustedConsensus = 'unanimous';
                trustedNodesAgreed = trustedKScores.length;
            } else {
                trustedConsensus = 'divergent';
                const sorted = [...trustedKScores].sort((a, b) => a - b);
                const median = sorted[Math.floor(sorted.length / 2)];
                trustedNodesAgreed = trustedKScores.filter(k => Math.abs(k - median) <= tolerance).length;
            }
        } else if (trustedKScores.length === 1) {
            trustedConsensus = 'single';
            trustedNodesAgreed = 1;
        }

        // Most recent verification
        const lastVerified = verifiedResults[0]?.verified_at || null;

        return {
            nodes_active: networkStatus.nodes_active,
            nodes_verified: verifiedResults.length,
            nodes_signed: trustedVerifications.length,
            nodes_agreed: nodesAgreed,
            consensus,
            trusted: {
                nodes_verified: trustedVerifications.length,
                nodes_agreed: trustedNodesAgreed,
                consensus: trustedConsensus
            },
            last_verified: lastVerified,
            verifiers: verifiedResults.map(v => ({
                node_id: v.node_id,
                name: v.node_name,
                fingerprint: v.node_key_fingerprint,
                verified_at: v.verified_at,
                k_score: v.k_score,
                signatures_valid: v.signatures_valid,
                node_sig_valid: v.node_sig_valid,
                node_sig_error: v.node_sig_error
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
 * @returns {Array} List of nodes with key info
 */
async function listNodes(db, { verifiedOnly = true, checkSignatures = true } = {}) {
    try {
        // SECURITY: Only show nodes with valid signing keys by default
        // Attackers can INSERT fake nodes but they won't appear without a key
        const whereClause = verifiedOnly
            ? 'WHERE node_public_key IS NOT NULL'
            : '';

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
                joined_at,
                node_key_fingerprint,
                key_registered_at,
                sig_node_identity,
                sig_node_status,
                node_chaos_nonce
            FROM nodes
            ${whereClause}
            ORDER BY
                CASE status
                    WHEN 'active' THEN 1
                    WHEN 'degraded' THEN 2
                    WHEN 'pending' THEN 3
                    ELSE 4
                END,
                last_heartbeat DESC
        `);

        // SECURITY: Verify signatures and filter out tampered nodes
        const nodes = result.rows.map(node => {
            const sigResult = checkSignatures ? verifyNodeSignatures(node) : { valid: true };
            return {
                ...node,
                has_signing_key: !!node.node_key_fingerprint,
                signatures_valid: sigResult.valid,
                tampered_fields: sigResult.tampered || []
            };
        });

        // Filter out tampered nodes if verifying signatures
        if (checkSignatures) {
            const validNodes = nodes.filter(n => n.signatures_valid);
            const tamperedCount = nodes.length - validNodes.length;
            if (tamperedCount > 0) {
                logger.warn(`[Nodes] ${tamperedCount} tampered node(s) filtered out (HMAC mismatch)`);
            }
            return validNodes;
        }

        return nodes;
    } catch (e) {
        logger.error(`❌ List nodes failed: ${e.message}`);
        return [];
    }
}

/**
 * Get current node's key info
 * @returns {Object} Key info
 */
function getNodeKeyInfo() {
    return {
        node_id: currentNodeId,
        public_key: currentNodePublicKey,
        fingerprint: currentNodeFingerprint,
        has_key: !!currentNodePublicKey
    };
}

/**
 * Get a node's public key from database
 * @param {Object} db - Database wrapper
 * @param {string} nodeId - Node ID
 * @returns {Object|null} Node key info
 */
async function getNodePublicKey(db, nodeId) {
    try {
        const result = await db.query(`
            SELECT node_id, node_public_key, node_key_fingerprint, key_registered_at
            FROM nodes
            WHERE node_id = $1
        `, [nodeId]);

        if (result.rows.length === 0) {
            return null;
        }

        const node = result.rows[0];
        return {
            node_id: node.node_id,
            public_key: node.node_public_key,
            fingerprint: node.node_key_fingerprint,
            registered_at: node.key_registered_at
        };
    } catch (e) {
        logger.error(`❌ Get node public key failed: ${e.message}`);
        return null;
    }
}

/**
 * Verify a raw verification signature
 * @param {Object} verification - Verification data
 * @param {string} signature - Node signature
 * @param {string} publicKey - Node public key
 * @returns {{ valid: boolean, error?: string }}
 */
function verifyNodeSignature(verification, signature, publicKey) {
    return nodeKeys.verifyVerificationSignature(verification, signature, publicKey);
}

/**
 * Generate a new keypair (for CLI tools)
 * @returns {{ publicKey: string, privateKey: string, fingerprint: string }}
 */
function generateNodeKeyPair() {
    return nodeKeys.generateKeyPair();
}

module.exports = {
    NODE_STATUS,
    initializeNode,
    getNodeId,
    getNodeKeyInfo,
    getNodePublicKey,
    sendHeartbeat,
    processHeartbeat,
    createHeartbeatSignature,
    verifyHeartbeatSignature,
    getNetworkStatus,
    recordVerification,
    getTokenValidation,
    updateNodeStatuses,
    registerNode,
    listNodes,
    verifyNodeSignature,
    generateNodeKeyPair
};
