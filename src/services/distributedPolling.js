/**
 * Distributed Polling Service
 *
 * Coordinates K-Score polling across N nodes using φ-based work distribution.
 * "N nodes, shared truth, verified consensus."
 *
 * ARCHITECTURE:
 * - PostgreSQL = shared coordination layer (source of truth)
 * - Local Redis = per-node cache only (NOT shared)
 * - All coordination happens via PostgreSQL
 *
 * Key concepts:
 * - Task queue in PostgreSQL (polling_tasks table)
 * - Atomic task claiming via SELECT FOR UPDATE SKIP LOCKED
 * - φ-weighted node selection via credits
 * - Consensus verification before K-Score finalization
 */

'use strict';

const logger = require('./logger');
const nodeService = require('./nodeService');
const genesis = require('../config/genesis');

// φ constants (same as genesis.js)
const PHI = genesis.PHI;
const PHI_INV = genesis.PHI_INV;                // 0.618
const PHI_INV_SQ = genesis.PHI_INV_SQ;          // 0.382

// Configuration
const CONFIG = {
    HEARTBEAT_INTERVAL_MS: PHI * 5000,          // ~8 seconds
    POLL_CHECK_INTERVAL_MS: PHI * 3000,         // ~4.8 seconds
    DEAD_NODE_THRESHOLD_MS: PHI * PHI * 30000,  // ~78 seconds
    MAX_CREDITS: PHI * PHI,                     // 2.618
    CREDIT_COST_POLL: PHI_INV,                  // 0.618 per poll
    CREDIT_REGEN_RATE: PHI_INV_SQ,              // 0.382 per heartbeat
    CONSENSUS_TOLERANCE: 5,                     // K-Score points
    TASK_LOCK_TIMEOUT_MS: 60000,                // 1 minute task timeout
    TASK_RETRY_LIMIT: 3
};

// Task reasons
const TASK_REASON = {
    SCHEDULED: 'scheduled',
    MANUAL: 'manual',
    ANOMALY: 'anomaly',
    VERIFICATION: 'verification',
    NEW_TOKEN: 'new_token'
};

// Task status
const TASK_STATUS = {
    PENDING: 'pending',
    CLAIMED: 'claimed',
    COMPLETED: 'completed',
    FAILED: 'failed'
};

// Current node state
let db = null;
let currentNodeId = null;
let isRunning = false;
let heartbeatInterval = null;
let pollCheckInterval = null;
let capabilities = [];

/**
 * Initialize distributed polling for this node
 * @param {Object} options - Configuration options
 * @returns {Promise<boolean>} Success status
 */
async function initialize(options = {}) {
    try {
        // Get database from services
        const { getDB } = require('./database');
        db = getDB();

        if (!db) {
            logger.error('[DistributedPolling] Database not available');
            return false;
        }

        currentNodeId = nodeService.getNodeId();
        if (!currentNodeId) {
            logger.error('[DistributedPolling] Node not initialized');
            return false;
        }

        capabilities = options.capabilities || ['polling', 'webhooks', 'verification'];

        // Register/update this node in the nodes table
        const now = Date.now();
        await db.query(`
            UPDATE nodes SET
                status = 'active',
                last_heartbeat = $1,
                capabilities = $2,
                credits = COALESCE(credits, $3)
            WHERE node_id = $4
        `, [now, JSON.stringify(capabilities), PHI, currentNodeId]);

        // Check if node is approved
        const nodeApproval = require('./nodeApproval');
        const isApproved = await nodeApproval.isNodeApproved(db, currentNodeId);

        if (!isApproved) {
            logger.warn(`[DistributedPolling] Node ${currentNodeId} is NOT APPROVED - will run in observation mode`);
        }

        isRunning = true;
        logger.info(`[DistributedPolling] Node ${currentNodeId} initialized with φ credits (approved: ${isApproved})`);

        return true;
    } catch (error) {
        logger.error(`[DistributedPolling] Initialization failed: ${error.message}`);
        return false;
    }
}

/**
 * Start the distributed polling loops
 * @param {Object} deps - Dependencies (db, broadcast)
 */
function start(deps) {
    if (!isRunning) {
        logger.warn('[DistributedPolling] Not initialized, cannot start');
        return;
    }

    // Update db reference if passed
    if (deps.db) db = deps.db;

    // Start heartbeat loop
    heartbeatInterval = setInterval(() => {
        sendHeartbeat().catch(e => logger.error(`Heartbeat error: ${e.message}`));
    }, CONFIG.HEARTBEAT_INTERVAL_MS);

    // Start poll check loop
    pollCheckInterval = setInterval(() => {
        checkAndClaimTask(deps).catch(e => logger.error(`Poll check error: ${e.message}`));
    }, CONFIG.POLL_CHECK_INTERVAL_MS);

    // Run dead node pruning periodically
    setInterval(() => {
        pruneDeadNodes().catch(e => logger.error(`Prune error: ${e.message}`));
    }, CONFIG.DEAD_NODE_THRESHOLD_MS);

    // Run stale task cleanup periodically
    setInterval(() => {
        cleanupStaleTasks().catch(e => logger.error(`Cleanup error: ${e.message}`));
    }, CONFIG.TASK_LOCK_TIMEOUT_MS);

    logger.info(`[DistributedPolling] Started (heartbeat: ${CONFIG.HEARTBEAT_INTERVAL_MS}ms, poll: ${CONFIG.POLL_CHECK_INTERVAL_MS}ms)`);
}

/**
 * Stop distributed polling
 */
async function stop() {
    isRunning = false;

    if (heartbeatInterval) {
        clearInterval(heartbeatInterval);
        heartbeatInterval = null;
    }

    if (pollCheckInterval) {
        clearInterval(pollCheckInterval);
        pollCheckInterval = null;
    }

    // Mark node as inactive
    if (db && currentNodeId) {
        await db.query(
            "UPDATE nodes SET status = 'inactive', last_heartbeat = $1 WHERE node_id = $2",
            [Date.now(), currentNodeId]
        );
    }

    logger.info('[DistributedPolling] Stopped');
}

/**
 * Send heartbeat and regenerate credits
 */
async function sendHeartbeat() {
    if (!db || !currentNodeId) return;

    const now = Date.now();

    // Atomic update: heartbeat + credit regeneration (capped at MAX_CREDITS)
    await db.query(`
        UPDATE nodes SET
            last_heartbeat = $1,
            status = 'active',
            credits = LEAST($2, COALESCE(credits, 0) + $3)
        WHERE node_id = $4
    `, [now, CONFIG.MAX_CREDITS, CONFIG.CREDIT_REGEN_RATE, currentNodeId]);

    logger.debug(`[Heartbeat] ${currentNodeId}: +${CONFIG.CREDIT_REGEN_RATE.toFixed(3)} credits`);
}

/**
 * Prune dead nodes from registry
 */
async function pruneDeadNodes() {
    if (!db) return;

    const cutoff = Math.floor(Date.now() - CONFIG.DEAD_NODE_THRESHOLD_MS);

    // Mark nodes as degraded if no heartbeat
    const result = await db.query(`
        UPDATE nodes SET status = 'degraded'
        WHERE status = 'active'
          AND last_heartbeat < $1
          AND is_genesis = FALSE
        RETURNING node_id
    `, [cutoff]);

    if (result.rows.length > 0) {
        const deadNodes = result.rows.map(r => r.node_id).join(', ');
        logger.warn(`[DistributedPolling] Nodes marked as degraded: ${deadNodes}`);
    }
}

/**
 * Cleanup stale tasks (claimed but not completed)
 */
async function cleanupStaleTasks() {
    if (!db) return;

    const cutoff = Date.now() - CONFIG.TASK_LOCK_TIMEOUT_MS;

    // Release tasks that have been claimed too long
    const result = await db.query(`
        UPDATE polling_tasks SET
            status = 'pending',
            claimed_by = NULL,
            claimed_at = NULL,
            attempts = attempts + 1
        WHERE status = 'claimed'
          AND claimed_at < $1
          AND attempts < $2
        RETURNING id as task_id, mint
    `, [cutoff, CONFIG.TASK_RETRY_LIMIT]);

    if (result.rows.length > 0) {
        logger.info(`[DistributedPolling] Released ${result.rows.length} stale tasks`);
    }

    // Mark tasks that exceeded retry limit as failed
    await db.query(`
        UPDATE polling_tasks SET status = 'failed'
        WHERE status = 'pending' AND attempts >= $1
    `, [CONFIG.TASK_RETRY_LIMIT]);
}

/**
 * Add a polling task to the queue
 * @param {string} mint - Token mint address
 * @param {string} reason - Why this task was created
 * @param {number} priority - Lower = more urgent (default 100)
 */
async function addTask(mint, reason = TASK_REASON.SCHEDULED, priority = 100) {
    if (!db) {
        logger.warn('[DistributedPolling] Cannot add task - DB not available');
        return false;
    }

    try {
        // Insert or update (if same mint already pending)
        await db.query(`
            INSERT INTO polling_tasks (mint, reason, priority, status, created_by, created_at)
            VALUES ($1, $2, $3, 'pending', $4, $5)
            ON CONFLICT (mint) WHERE status = 'pending' DO UPDATE SET
                priority = LEAST(polling_tasks.priority, EXCLUDED.priority),
                reason = EXCLUDED.reason
        `, [mint, reason, priority, currentNodeId, Date.now()]);

        logger.debug(`[Task] Added: ${mint.slice(0, 8)}... (reason: ${reason}, priority: ${priority})`);
        return true;
    } catch (error) {
        logger.error(`[Task] Failed to add: ${error.message}`);
        return false;
    }
}

/**
 * Check for available tasks and attempt to claim one
 * Uses SELECT FOR UPDATE SKIP LOCKED for atomic claiming
 * @param {Object} deps - Dependencies (db, broadcast)
 */
async function checkAndClaimTask(deps) {
    if (!db || !currentNodeId) return;

    // 1. Check if this node is approved (can participate)
    const nodeApproval = require('./nodeApproval');
    const isApproved = await nodeApproval.isNodeApproved(db, currentNodeId);

    if (!isApproved) {
        // Observation mode - don't claim tasks
        return;
    }

    // 2. Get this node's credits
    const nodeResult = await db.query(
        'SELECT credits FROM nodes WHERE node_id = $1',
        [currentNodeId]
    );

    if (nodeResult.rows.length === 0) return;
    const myCredits = parseFloat(nodeResult.rows[0].credits || 0);

    if (myCredits < CONFIG.CREDIT_COST_POLL) {
        // Not enough credits, wait for regeneration
        return;
    }

    // 3. Get total active credits for probability calculation
    const creditsResult = await db.query(`
        SELECT SUM(credits) as total_credits
        FROM nodes
        WHERE status = 'active' AND (approval_status = 'approved' OR is_genesis = TRUE)
    `);

    const totalCredits = parseFloat(creditsResult.rows[0]?.total_credits || 0);
    if (totalCredits <= 0) return;

    // 4. Calculate claim probability (φ-weighted)
    const myProbability = myCredits / totalCredits;

    // Random decision - should I try to claim?
    if (Math.random() > myProbability) {
        return; // Let another node with higher credits take it
    }

    // 5. Attempt to claim a task atomically
    // SELECT FOR UPDATE SKIP LOCKED ensures only one node gets each task
    const claimResult = await db.query(`
        UPDATE polling_tasks SET
            status = 'claimed',
            claimed_by = $1,
            claimed_at = $2
        WHERE id = (
            SELECT id FROM polling_tasks
            WHERE status = 'pending'
            ORDER BY priority ASC, created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id as task_id, mint, reason, attempts
    `, [currentNodeId, Date.now()]);

    if (claimResult.rows.length === 0) {
        // No tasks available or lost race
        return;
    }

    const task = claimResult.rows[0];

    // 6. Deduct credits
    await db.query(
        'UPDATE nodes SET credits = credits - $1 WHERE node_id = $2',
        [CONFIG.CREDIT_COST_POLL, currentNodeId]
    );

    logger.info(`[Task] Claimed: ${task.mint.slice(0, 8)}... (reason: ${task.reason})`);

    // 7. Execute the task
    try {
        await executePollingTask(task, deps);

        // Success - mark task as completed
        await db.query(`
            UPDATE polling_tasks SET
                status = 'completed',
                completed_at = $1
            WHERE id = $2
        `, [Date.now(), task.task_id]);

        // Record work history
        await db.query(`
            INSERT INTO node_work_history (node_id, task_type, mint, completed_at, k_score_calculated)
            VALUES ($1, 'polling', $2, $3, TRUE)
        `, [currentNodeId, task.mint, Date.now()]);

        logger.info(`[Task] Completed: ${task.mint.slice(0, 8)}...`);

    } catch (error) {
        logger.error(`[Task] Failed: ${task.mint.slice(0, 8)}... - ${error.message}`);

        // Mark as failed or pending for retry
        const newStatus = task.attempts >= CONFIG.TASK_RETRY_LIMIT - 1 ? 'failed' : 'pending';
        await db.query(`
            UPDATE polling_tasks SET
                status = $1,
                claimed_by = NULL,
                claimed_at = NULL,
                last_error = $2,
                attempts = attempts + 1
            WHERE id = $3
        `, [newStatus, error.message, task.task_id]);
    }
}

/**
 * Execute a polling task
 * @param {Object} task - Task data
 * @param {Object} deps - Dependencies (db, broadcast)
 */
async function executePollingTask(task, deps) {
    const { mint, reason: _reason } = task;

    // Import kScoreUpdater dynamically to avoid circular deps
    const kScoreUpdater = require('../tasks/kScoreUpdater');

    // Execute single token update
    await kScoreUpdater.updateSingleToken(deps, mint);

    // Record verification (for consensus tracking)
    await recordVerification(mint, deps);
}

/**
 * Record this node's verification result
 * @param {string} mint - Token mint
 * @param {Object} deps - Dependencies
 */
async function recordVerification(mint, _deps) {
    if (!db) return;

    // Get the K-Score we just calculated
    const token = await db.query(
        'SELECT k_score, sig_kscore FROM tokens WHERE mint = $1',
        [mint]
    );

    if (token.rows.length === 0) return;

    const { k_score, sig_kscore } = token.rows[0];

    // Record our verification
    // Wrapped in try-catch to handle foreign key violations if node doesn't exist yet
    try {
        await db.query(`
            INSERT INTO token_verifications (mint, node_id, k_score, node_signature, verified_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (mint, node_id) DO UPDATE SET
                k_score = EXCLUDED.k_score,
                node_signature = EXCLUDED.node_signature,
                verified_at = EXCLUDED.verified_at
        `, [mint, currentNodeId, k_score, sig_kscore, Date.now()]);
    } catch (err) {
        // Foreign key violation (node not in nodes table) - log but don't fail
        if (err.code === '23503') {
            logger.warn(`[DistributedPolling] Skipping verification record for ${mint}: node ${currentNodeId} not registered`);
        } else {
            throw err;
        }
    }

    // Check for consensus
    await checkConsensus(mint);
}

/**
 * Check if consensus has been reached for a token's K-Score
 * @param {string} mint - Token mint
 */
async function checkConsensus(mint) {
    if (!db) return;

    // Get all recent verifications
    const cutoff = Date.now() - genesis.CONSENSUS_RULES.CONSENSUS_WINDOW_MS;

    const verifications = await db.query(`
        SELECT v.node_id, v.k_score, v.verified_at
        FROM token_verifications v
        JOIN nodes n ON v.node_id = n.node_id
        WHERE v.mint = $1
          AND v.verified_at > $2
          AND (n.approval_status = 'approved' OR n.is_genesis = TRUE)
          AND n.status IN ('active', 'degraded')
        ORDER BY v.verified_at DESC
    `, [mint, cutoff]);

    if (verifications.rows.length < genesis.CONSENSUS_RULES.MIN_VERIFICATIONS) {
        // Not enough verifications yet
        return;
    }

    // Get active node count
    const nodeApproval = require('./nodeApproval');
    const activeNodes = await nodeApproval.getActiveNodeCount(db);
    const _requiredApprovals = genesis.CONSENSUS_RULES.getRequiredApprovals(activeNodes);

    // Group by K-Score (within tolerance)
    const kScores = verifications.rows.map(v => v.k_score);
    const median = kScores.sort((a, b) => a - b)[Math.floor(kScores.length / 2)];

    const agreeing = verifications.rows.filter(v =>
        Math.abs(v.k_score - median) <= CONFIG.CONSENSUS_TOLERANCE
    );

    if (genesis.CONSENSUS_RULES.isConsensusReached(agreeing.length, activeNodes)) {
        // Consensus reached! Record it
        await db.query(`
            INSERT INTO consensus_snapshots (mint, k_score_consensus, agreeing_nodes, total_nodes, consensus_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (mint) DO UPDATE SET
                k_score_consensus = EXCLUDED.k_score_consensus,
                agreeing_nodes = EXCLUDED.agreeing_nodes,
                total_nodes = EXCLUDED.total_nodes,
                consensus_at = EXCLUDED.consensus_at
        `, [mint, median, agreeing.length, activeNodes, Date.now()]);

        logger.info(`[Consensus] ${mint.slice(0, 8)}... K=${median} (${agreeing.length}/${activeNodes} nodes agree)`);
    }
}

/**
 * Get network status
 * @returns {Object} Network statistics
 */
async function getNetworkStatus() {
    if (!db) {
        return { nodes_active: 0, tasks_pending: 0, mode: 'standalone' };
    }

    try {
        // Count active nodes
        const nodesResult = await db.query(`
            SELECT COUNT(*) as count
            FROM nodes
            WHERE status = 'active'
              AND (approval_status = 'approved' OR is_genesis = TRUE)
        `);

        // Count pending tasks
        const tasksResult = await db.query(`
            SELECT COUNT(*) as count FROM polling_tasks WHERE status = 'pending'
        `);

        // Get total credits
        const creditsResult = await db.query(`
            SELECT SUM(credits) as total, MAX(credits) as max
            FROM nodes
            WHERE status = 'active' AND (approval_status = 'approved' OR is_genesis = TRUE)
        `);

        // Get this node's credits
        const myResult = await db.query(
            'SELECT credits FROM nodes WHERE node_id = $1',
            [currentNodeId]
        );

        const nodesActive = parseInt(nodesResult.rows[0]?.count || 0);
        const totalCredits = parseFloat(creditsResult.rows[0]?.total || 0);
        const myCredits = parseFloat(myResult.rows[0]?.credits || 0);

        return {
            mode: 'distributed',
            nodes_active: nodesActive,
            tasks_pending: parseInt(tasksResult.rows[0]?.count || 0),
            total_credits: totalCredits,
            my_credits: myCredits,
            my_claim_probability: totalCredits > 0 ? myCredits / totalCredits : 0,
            consensus_threshold: genesis.CONSENSUS_RULES.getRequiredApprovals(nodesActive)
        };
    } catch (error) {
        logger.error(`[DistributedPolling] getNetworkStatus error: ${error.message}`);
        return { nodes_active: 0, tasks_pending: 0, mode: 'error' };
    }
}

/**
 * Schedule polling tasks for all tokens needing refresh
 * @param {Object} database - Database wrapper
 * @param {Object} options - Scheduling options
 */
async function scheduleRefreshTasks(database, options = {}) {
    const dbRef = database || db;
    if (!dbRef) return 0;

    const {
        maxAge = PHI * PHI * PHI * 60 * 60 * 1000, // ~4.2 hours
        limit = 100,
        onlyVerified = false
    } = options;

    const cutoff = Date.now() - maxAge;

    // Find tokens needing refresh
    const query = onlyVerified
        ? `SELECT mint FROM tokens WHERE hascommunityupdate = TRUE AND (last_k_score_update IS NULL OR last_k_score_update < $1) ORDER BY liquidity DESC NULLS LAST LIMIT $2`
        : `SELECT mint FROM tokens WHERE last_k_score_update IS NULL OR last_k_score_update < $1 ORDER BY liquidity DESC NULLS LAST LIMIT $2`;

    const result = await dbRef.query(query, [cutoff, limit]);

    let scheduled = 0;
    for (const { mint } of result.rows) {
        // Priority based on age (older = lower score = higher priority)
        const token = await dbRef.query('SELECT last_k_score_update, liquidity FROM tokens WHERE mint = $1', [mint]);
        const tokenData = token.rows[0];
        const age = Date.now() - (tokenData?.last_k_score_update || 0);
        const priority = Math.max(1, 100 - Math.floor(age / (60 * 60 * 1000))); // 1 per hour of age

        await addTask(mint, TASK_REASON.SCHEDULED, priority);
        scheduled++;
    }

    logger.info(`[Scheduler] Added ${scheduled} refresh tasks`);
    return scheduled;
}

module.exports = {
    initialize,
    start,
    stop,
    addTask,
    getNetworkStatus,
    scheduleRefreshTasks,
    checkConsensus,
    TASK_REASON,
    TASK_STATUS,
    CONFIG
};
