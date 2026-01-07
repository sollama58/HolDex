/**
 * Node Routes - Decentralized Validation Network
 *
 * Public endpoints:
 *   GET /api/nodes           - List all nodes
 *   GET /api/nodes/status    - Network status
 *   GET /api/nodes/:id       - Node details
 *   GET /api/nodes/:id/key   - Node public key (for signature verification)
 *
 * Internal endpoints (authenticated):
 *   POST /internal/heartbeat - Node heartbeat
 *   POST /internal/nodes     - Register node (admin)
 */

const express = require('express');
const router = express.Router();
const nodeService = require('../services/nodeService');
const config = require('../config/env');

/**
 * GET /api/nodes - List verified nodes only
 * SECURITY: Only nodes with valid signing keys are shown
 * Attackers can INSERT fake nodes but they won't appear without cryptographic identity
 */
router.get('/', async (req, res) => {
    try {
        const db = req.app.get('db');

        // Only show verified nodes (with signing keys)
        const nodes = await nodeService.listNodes(db, { verifiedOnly: true });

        // Count unverified for monitoring (not exposed in response)
        const allNodes = await nodeService.listNodes(db, { verifiedOnly: false });
        const unverifiedCount = allNodes.length - nodes.length;

        if (unverifiedCount > 0) {
            // Log potential attack attempts
            const logger = require('../services/logger');
            logger.warn(`[Nodes] ${unverifiedCount} unverified node(s) hidden from API (potential attack)`);
        }

        res.json({
            success: true,
            count: nodes.length,
            verified_nodes: nodes.length,
            active_nodes: nodes.filter(n => n.status === 'active').length,
            nodes: nodes.map(n => ({
                node_id: n.node_id,
                name: n.name,
                operator: n.operator,
                region: n.region,
                status: n.status,
                last_heartbeat: n.last_heartbeat,
                tokens_verified: n.tokens_verified,
                verifications_24h: n.verifications_24h,
                version: n.version,
                joined_at: n.joined_at,
                // Key info - proves cryptographic identity
                fingerprint: n.node_key_fingerprint,
                key_registered_at: n.key_registered_at
            }))
        });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

/**
 * GET /api/nodes/status - Network status summary
 */
router.get('/status', async (req, res) => {
    try {
        const db = req.app.get('db');
        const status = await nodeService.getNetworkStatus(db);

        res.json({
            success: true,
            ...status
        });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

/**
 * GET /api/nodes/:id/key - Get node's public key for signature verification
 * Philosophy: "Don't trust. Verify." - Anyone can verify node signatures
 */
router.get('/:id/key', async (req, res) => {
    try {
        const db = req.app.get('db');
        const { id } = req.params;

        const keyInfo = await nodeService.getNodePublicKey(db, id);

        if (!keyInfo) {
            return res.status(404).json({ success: false, error: 'Node not found' });
        }

        if (!keyInfo.public_key) {
            return res.status(404).json({
                success: false,
                error: 'Node has no registered public key',
                node_id: id
            });
        }

        res.json({
            success: true,
            node_id: keyInfo.node_id,
            public_key: keyInfo.public_key,
            fingerprint: keyInfo.fingerprint,
            registered_at: keyInfo.registered_at,
            algorithm: 'Ed25519',
            encoding: 'base64-der-spki',
            usage: 'Use this key to verify node_signature on token verifications'
        });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

/**
 * GET /api/nodes/:id - Get specific node details
 */
router.get('/:id', async (req, res) => {
    try {
        const db = req.app.get('db');
        const { id } = req.params;

        const result = await db.query(`
            SELECT * FROM nodes WHERE node_id = $1
        `, [id]);

        if (result.rows.length === 0) {
            return res.status(404).json({ success: false, error: 'Node not found' });
        }

        const node = result.rows[0];

        // Get recent verifications by this node (with signatures)
        const verifications = await db.query(`
            SELECT mint, k_score, verified_at, signatures_valid, node_signature, signature_version
            FROM token_verifications
            WHERE node_id = $1
            ORDER BY verified_at DESC
            LIMIT 20
        `, [id]);

        res.json({
            success: true,
            node: {
                ...node,
                // Expose key info (but not the full public key here, use /key endpoint)
                has_signing_key: !!node.node_key_fingerprint,
                key_fingerprint: node.node_key_fingerprint,
                key_registered_at: node.key_registered_at,
                recent_verifications: verifications.rows.map(v => ({
                    ...v,
                    is_signed: !!v.node_signature
                }))
            }
        });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

module.exports = router;

// ============================================================================
// Internal Routes (separate router for /internal prefix)
// ============================================================================

const internalRouter = express.Router();

/**
 * POST /internal/heartbeat - Receive heartbeat from node
 */
internalRouter.post('/heartbeat', async (req, res) => {
    try {
        const db = req.app.get('db');
        const { payload, signature } = req.body;

        if (!payload || !signature) {
            return res.status(400).json({
                success: false,
                error: 'Missing payload or signature'
            });
        }

        const result = await nodeService.processHeartbeat(db, payload, signature);

        if (!result.success) {
            return res.status(401).json(result);
        }

        res.json(result);
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

/**
 * POST /internal/nodes - Register new node (admin only)
 */
internalRouter.post('/nodes', async (req, res) => {
    try {
        // Check admin auth
        const authHeader = req.headers.authorization;
        if (!authHeader || authHeader !== `Bearer ${config.ADMIN_PASSWORD}`) {
            return res.status(401).json({ success: false, error: 'Unauthorized' });
        }

        const db = req.app.get('db');
        const result = await nodeService.registerNode(db, req.body);

        if (!result.success) {
            return res.status(400).json(result);
        }

        res.json(result);
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

/**
 * GET /internal/heartbeat/sign - Get signed heartbeat for current node
 * Used by node operators to get a valid heartbeat payload
 */
internalRouter.get('/heartbeat/sign', async (req, res) => {
    try {
        const nodeId = nodeService.getNodeId();
        if (!nodeId) {
            return res.status(400).json({
                success: false,
                error: 'Node not initialized'
            });
        }

        const payload = {
            node_id: nodeId,
            timestamp: Date.now(),
            version: process.env.npm_package_version || '1.0.0',
            stats: {}
        };

        const signature = nodeService.createHeartbeatSignature(payload);

        res.json({
            success: true,
            payload,
            signature
        });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

module.exports.internalRouter = internalRouter;
