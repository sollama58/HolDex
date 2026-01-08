/**
 * Integrity Watchdog - Self-Healing Database
 *
 * "Don't Trust, Verify" - $asdfasdfa philosophy
 *
 * Monitors the database for tampering and auto-restores from signed snapshots.
 * 0 RPC calls - just cryptographic verification and restoration.
 *
 * Flow:
 * 1. Scan tokens periodically
 * 2. Verify signatures against stored data
 * 3. If tampered: restore from Redis snapshot + re-sign
 * 4. Attacker can modify DB 1000x, we restore for free
 */

const logger = require('../services/logger');
const { getClient: getRedisClient } = require('../services/redis');
const { verifyAllSignatures, signAllCategories } = require('../utils/dataSignature');
const config = require('../config/env');
const alerting = require('../services/alerting');
const { QualityBuilder, QUALITY_PROFILES } = require('../shared/geometric-quality');

// Watchdog config
const SCAN_INTERVAL = 5 * 60 * 1000; // 5 minutes
const SNAPSHOT_TTL = 7 * 24 * 60 * 60; // 7 days
const SNAPSHOT_PREFIX = 'kscore:snapshot:';

let watchdogInterval = null;
let isRunning = false;

/**
 * Save a token snapshot to Redis (called after successful K-Score calculation)
 * @param {string} mint - Token mint address
 * @param {Object} tokenData - Complete token data with all fields
 * @param {Array} holderSnapshots - Optional: top 20 holder snapshots for integrity
 */
async function saveSnapshot(mint, tokenData, holderSnapshots = null) {
    const redis = getRedisClient();
    if (!redis) return false;

    try {
        const snapshot = {
            ...tokenData,
            _snapshotTime: Date.now(),
            _snapshotVersion: 3, // v3: includes holder snapshots
            _holderSnapshots: holderSnapshots || [] // Top 20 for integrity verification
        };

        await redis.set(
            `${SNAPSHOT_PREFIX}${mint}`,
            JSON.stringify(snapshot),
            'EX',
            SNAPSHOT_TTL
        );

        return true;
    } catch (e) {
        logger.error(`[Watchdog] Snapshot save failed for ${mint}: ${e.message}`);
        return false;
    }
}

/**
 * Get a token snapshot from Redis
 * @param {string} mint - Token mint address
 * @returns {Object|null} Token snapshot or null
 */
async function getSnapshot(mint) {
    const redis = getRedisClient();
    if (!redis) return null;

    try {
        const data = await redis.get(`${SNAPSHOT_PREFIX}${mint}`);
        return data ? JSON.parse(data) : null;
    } catch (e) {
        logger.error(`[Watchdog] Snapshot read failed for ${mint}: ${e.message}`);
        return null;
    }
}

// =============================================================================
// INTEGRITY SCORE (D × O × L)
// =============================================================================

// All 8 signature categories for coverage calculation
const ALL_CATEGORIES = ['identity', 'security', 'lp', 'supply', 'kscore', 'market', 'origin', 'full'];
const STABLE_CATEGORIES = ['identity', 'security', 'lp', 'supply', 'kscore', 'origin']; // Excludes volatile

/**
 * Calculate integrity score for a token using D×O×L formula
 * D (Coverage): Signature completeness (valid/total categories)
 * O (Consistency): Cross-signature agreement (stable categories match)
 * L (Recency): Time since last verification/snapshot
 *
 * @param {Object} verificationResult - Result from verifyAllSignatures
 * @param {Object} token - Token data with timestamps
 * @param {Object} snapshot - Optional snapshot data
 * @returns {Object} Integrity score with components
 */
function calculateIntegrityScore(verificationResult, token, snapshot = null) {
  // Defensive: ensure arrays exist
  const valid = verificationResult?.valid || [];
  const invalid = verificationResult?.invalid || [];
  const tampered = verificationResult?.tampered || [];

  // D (Coverage): % of valid signatures
  const totalCategories = ALL_CATEGORIES.length;
  const validCount = valid.length;
  const D = validCount / totalCategories;

  // O (Consistency): % of stable categories that are NOT tampered
  const stableTampered = tampered.filter(cat => STABLE_CATEGORIES.includes(cat));
  const O = 1 - (stableTampered.length / STABLE_CATEGORIES.length);

  // L (Recency): Based on snapshot age or last_k_score_update
  const now = Date.now();
  let lastVerified = token.last_k_score_update || token.timestamp || 0;
  if (snapshot && snapshot._snapshotTime) {
    lastVerified = Math.max(lastVerified, snapshot._snapshotTime);
  }

  // Decay: half-life of 24 hours (in ms)
  const HALF_LIFE_MS = 24 * 60 * 60 * 1000;
  const age = now - lastVerified;
  const L = Math.max(0.1, Math.pow(0.5, age / HALF_LIFE_MS));

  // Calculate using QualityBuilder
  const result = new QualityBuilder('INTEGRITY')
    .setDimensions({ D, O, L })
    .calculate();

  return {
    score: result.score,
    level: result.level,
    emoji: result.emoji,
    action: result.action,
    components: {
      coverage: Math.round(D * 100),
      consistency: Math.round(O * 100),
      recency: Math.round(L * 100),
    },
    details: {
      validCategories: valid,
      invalidCategories: invalid,
      tamperedCategories: tampered,
      stableTampered,
      lastVerified: new Date(lastVerified).toISOString(),
    },
  };
}

/**
 * Get integrity action based on score
 * @param {Object} integrityResult - From calculateIntegrityScore
 * @returns {string} Action to take
 */
function getIntegrityAction(integrityResult) {
  const { level, score } = integrityResult;

  switch (level) {
    case 'excellent':
      return 'none'; // Fully verified, no action
    case 'good':
      return 'monitor'; // Minor gaps, watch
    case 'warning':
      return 'refresh_snapshot'; // Needs fresh snapshot
    case 'critical':
      return 'heal'; // Integrity breach, restore from snapshot
    case 'failed':
      return 'recalculate'; // Full recalculation needed
    default:
      return score < 40 ? 'heal' : 'monitor';
  }
}

// =============================================================================
// RESTORATION
// =============================================================================

/**
 * Restore a token from snapshot (0 RPC calls)
 * @param {Object} db - Database instance
 * @param {string} mint - Token mint address
 * @param {Array} tamperedCategories - Which categories were tampered
 * @returns {boolean} Success
 */
async function restoreFromSnapshot(db, mint, tamperedCategories) {
    const snapshot = await getSnapshot(mint);

    if (!snapshot) {
        logger.warn(`[Watchdog] No snapshot for ${mint} - cannot restore, queuing for recalc`);
        // No snapshot = need real recalculation (rare case)
        return false;
    }

    try {
        // CRITICAL: Ensure snapshot.mint is set (legacy snapshots may not have it)
        // All signature functions use token.mint as the first field
        snapshot.mint = mint;

        // DEBUG: Log identity data string that will be signed
        const identityData = [snapshot.mint, snapshot.name || '', snapshot.symbol || '', snapshot.image || '', snapshot.decimals || 9].join('|');
        logger.debug(`[Watchdog] SIGN identity for ${mint.slice(0, 8)}: "${identityData.slice(0, 80)}..."`);

        // Re-sign the snapshot data (new chaos_nonce for unpredictability)
        const signatures = signAllCategories(snapshot);
        logger.debug(`[Watchdog] SIGNED ${mint.slice(0, 8)}: sig_identity=${signatures.sig_identity?.slice(0, 20)}...`);

        // Restore ALL signed fields from snapshot (data + signatures)
        // CRITICAL: Must restore data fields too, not just signatures!
        // Otherwise sig_identity is based on snapshot.name but DB keeps old name → mismatch
        await db.run(`
            UPDATE tokens
            SET
                -- Identity fields (sig_identity)
                name = $1,
                symbol = $2,
                image = $3,
                decimals = $4,
                -- Security fields (sig_security)
                mint_authority_revoked = $5,
                freeze_authority_revoked = $6,
                is_mutable_supply = $7,
                hasCommunityUpdate = $8,
                -- LP fields (sig_lp)
                lp_burn_pct = $9,
                lp_locked_pct = $10,
                lp_status = $11,
                -- Supply fields (sig_supply)
                supply = $12,
                initial_supply = $13,
                burned_amount = $14,
                burned_percent = $15,
                -- K-Score fields (sig_kscore)
                k_score = $16,
                conviction_score = $17,
                conviction_accumulators = $18,
                conviction_holders = $19,
                conviction_reducers = $20,
                conviction_extractors = $21,
                conviction_analyzed = $22,
                holders = $23,
                last_k_score_update = $24,
                -- Market fields (sig_market) - volatile but needed for consistency
                priceusd = $25,
                marketcap = $26,
                liquidity = $27,
                -- Origin fields (sig_origin)
                is_pump_fun = $28,
                bonding_curve_complete = $29,
                timestamp = $30,
                metadata = $31,
                -- All signatures
                sig_identity = $32,
                sig_security = $33,
                sig_lp = $34,
                sig_supply = $35,
                sig_kscore = $36,
                sig_market = $37,
                sig_origin = $38,
                sig_full = $39,
                chaos_nonce = $40
            WHERE mint = $41
        `, [
            // Identity (1-4)
            snapshot.name || '',
            snapshot.symbol || '',
            snapshot.image || '',
            snapshot.decimals || 9,
            // Security (5-8)
            snapshot.mint_authority_revoked || false,
            snapshot.freeze_authority_revoked || false,
            snapshot.is_mutable_supply || false,
            snapshot.hasCommunityUpdate || snapshot.hascommunityupdate || false,
            // LP (9-11)
            snapshot.lp_burn_pct || 0,
            snapshot.lp_locked_pct || 0,
            snapshot.lp_status || 'unknown',
            // Supply (12-15)
            snapshot.supply || '0',
            snapshot.initial_supply || snapshot.supply || '0',
            snapshot.burned_amount || 0,
            snapshot.burned_percent || 0,
            // K-Score (16-24)
            snapshot.k_score,
            snapshot.conviction_score,
            snapshot.conviction_accumulators,
            snapshot.conviction_holders,
            snapshot.conviction_reducers,
            snapshot.conviction_extractors,
            snapshot.conviction_analyzed,
            snapshot.holders,
            snapshot.last_k_score_update || 0,  // Must match signKScore fallback
            // Market (25-27)
            snapshot.priceusd,
            snapshot.marketcap,
            snapshot.liquidity,
            // Origin (28-31)
            snapshot.is_pump_fun || false,
            snapshot.bonding_curve_complete || false,
            snapshot.timestamp || 0,
            snapshot.metadata || '',
            // Signatures (32-40)
            signatures.sig_identity,
            signatures.sig_security,
            signatures.sig_lp,
            signatures.sig_supply,
            signatures.sig_kscore,
            signatures.sig_market,
            signatures.sig_origin,
            signatures.sig_full,
            signatures.chaos_nonce,
            // WHERE (41)
            mint
        ]);

        // Restore holder_snapshots if 'holders' was tampered AND snapshot has them (v3+)
        if (tamperedCategories.includes('holders') && snapshot._holderSnapshots && snapshot._holderSnapshots.length > 0) {
            logger.info(`[Watchdog] Restoring ${snapshot._holderSnapshots.length} holder snapshots for ${mint.slice(0, 8)}...`);

            // Delete current snapshots and restore from backup
            await db.run('DELETE FROM holder_snapshots WHERE mint = $1', [mint]);

            for (const hs of snapshot._holderSnapshots) {
                await db.run(`
                    INSERT INTO holder_snapshots (mint, holder, balance, updated_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (mint, holder) DO UPDATE SET balance = $3, updated_at = $4
                `, [mint, hs.holder, hs.balance, Date.now()]);
            }

            // Re-sign sig_holders with restored data
            const { signHolders } = require('../utils/dataSignature');
            const sig_holders = signHolders(mint, snapshot._holderSnapshots);
            await db.run('UPDATE tokens SET sig_holders = $1, holders_snapshot_check = $2 WHERE mint = $3',
                [sig_holders, Date.now(), mint]);
        }

        // DEBUG: Immediately verify the token to confirm healing worked
        const verifiedToken = await db.get(`
            SELECT mint, name, symbol, image, decimals, sig_identity
            FROM tokens WHERE mint = $1
        `, [mint]);
        if (verifiedToken) {
            const verifyIdentityData = [verifiedToken.mint, verifiedToken.name || '', verifiedToken.symbol || '', verifiedToken.image || '', verifiedToken.decimals || 9].join('|');
            logger.debug(`[Watchdog] VERIFY identity for ${mint.slice(0, 8)}: "${verifyIdentityData.slice(0, 80)}..."`);
            logger.debug(`[Watchdog] DB sig_identity=${verifiedToken.sig_identity?.slice(0, 20)}...`);

            // Compare the signed data strings
            if (identityData !== verifyIdentityData) {
                logger.error(`[Watchdog] DATA MISMATCH! Signed: "${identityData.slice(0, 50)}" vs DB: "${verifyIdentityData.slice(0, 50)}"`);
            }
        }

        logger.info(`[Watchdog] HEALED: ${mint.slice(0, 8)}... (tampered: ${tamperedCategories.join(',')})`);
        return true;

    } catch (e) {
        logger.error(`[Watchdog] Restore failed for ${mint}: ${e.message}`);
        return false;
    }
}

/**
 * Scan all verified tokens for tampering
 * @param {Object} db - Database instance
 */
async function scanForTampering(db) {
    if (isRunning) {
        logger.debug('[Watchdog] Scan already in progress, skipping');
        return;
    }

    isRunning = true;
    const startTime = Date.now();

    try {
        // Get all verified tokens with signatures
        // CRITICAL: Must select ALL fields used by signMarket/signLP/signKScore etc.
        const tokens = await db.all(`
            SELECT mint, symbol, name, image, decimals,
                   k_score, conviction_score, conviction_accumulators,
                   conviction_holders, conviction_reducers, conviction_extractors,
                   conviction_analyzed, holders, priceusd, marketcap, liquidity,
                   supply, initial_supply, burned_amount, burned_percent,
                   mint_authority_revoked, freeze_authority_revoked, is_mutable_supply,
                   hasCommunityUpdate, lp_burn_pct, lp_locked_pct, lp_status,
                   is_pump_fun, bonding_curve_complete, timestamp, metadata,
                   sig_identity, sig_security, sig_lp, sig_supply,
                   sig_kscore, sig_market, sig_origin, sig_holders, sig_full, chaos_nonce,
                   last_k_score_update,
                   -- Fields required for sig_market verification:
                   price_source, price_timestamp, price_pool,
                   mcap_calculated, liquidity_source, liquidity_timestamp,
                   holders_source, holders_timestamp, age_days
            FROM tokens
            WHERE hasCommunityUpdate = TRUE
              AND sig_full IS NOT NULL
        `);

        if (!tokens || tokens.length === 0) {
            logger.debug('[Watchdog] No signed tokens to scan');
            return;
        }

        let scanned = 0;
        let tampered = 0;
        let healed = 0;
        let failed = 0;

        // Categories to IGNORE for tampering (volatile data that changes frequently)
        // "market" = price, mcap, liquidity - these change every 30s via PriceWorker
        // "holders" = holder_snapshots evolve with every buy/sell between K-Score updates
        // "full" = composite HMAC of ALL category signatures - inherently volatile when market changes
        // Healing these causes infinite loops and wastes resources
        // Note: actual tampering is still detected via the 6 stable categories (identity, security, lp, supply, kscore, origin)
        const IGNORED_CATEGORIES = new Set(['market', 'holders', 'full']);

        // Integrity score distribution tracking
        const scoreDistribution = { excellent: 0, good: 0, warning: 0, critical: 0, failed: 0 };

        for (const token of tokens) {
            scanned++;

            // Load holder snapshots for sig_holders verification
            let holderSnapshots = [];
            try {
                holderSnapshots = await db.all(
                    'SELECT holder, balance FROM holder_snapshots WHERE mint = $1 ORDER BY balance DESC LIMIT 20',
                    [token.mint]
                );
            } catch (_e) {
                // Ignore - snapshots may not exist
            }

            // Verify all 8 signatures (+ holders if snapshots exist)
            const result = verifyAllSignatures(token, { holderSnapshots });

            // DEBUG: Log identity data being verified (only if tampered)
            if (result.tampered.includes('identity')) {
                const identData = [token.mint, token.name || '', token.symbol || '', token.image || '', token.decimals || 9].join('|');
                logger.debug(`[Watchdog] VERIFY FAILED identity for ${token.mint.slice(0, 8)}: data="${identData.slice(0, 60)}..." sig=${token.sig_identity?.slice(0, 20)}...`);
            }

            // Get snapshot for recency calculation
            const snapshot = await getSnapshot(token.mint);

            // Calculate integrity score using D×O×L formula
            const integrity = calculateIntegrityScore(result, token, snapshot);
            const action = getIntegrityAction(integrity);

            // Track score distribution
            scoreDistribution[integrity.level] = (scoreDistribution[integrity.level] || 0) + 1;

            // Filter out ignored categories (volatile data)
            const criticalTampered = result.tampered.filter(cat => !IGNORED_CATEGORIES.has(cat));

            // FIX: Only heal when there are ACTUALLY tampered signatures
            // Previously this also healed on action='heal'/'recalculate' even with no tampering,
            // causing an infinite loop when integrity score calculation returned NaN/failed
            if (criticalTampered.length > 0) {
                tampered++;
                logger.warn(`[Watchdog] ${integrity.emoji} I:${integrity.score} ${token.symbol} (${token.mint.slice(0, 8)}...) - ${criticalTampered.join(',')} [${action}]`);

                // ALERT: Tampering detected (fire-and-forget)
                alerting.alertTamperingDetected(token.mint, token.symbol, criticalTampered).catch(() => {});

                // Attempt restoration from snapshot
                const restored = await restoreFromSnapshot(db, token.mint, criticalTampered);
                if (restored) {
                    healed++;
                    // ALERT: Tampering healed
                    alerting.alertTamperingHealed(token.mint, token.symbol, criticalTampered).catch(() => {});
                } else {
                    failed++;
                }
            } else if (action === 'heal' || action === 'recalculate') {
                // Integrity score indicates problem but no signatures are tampered
                // This can happen with stale snapshots - log but don't heal
                logger.debug(`[Watchdog] ${integrity.emoji} I:${integrity.score} ${token.symbol} needs K-Score recalc (no tampering detected)`);
            } else if (action === 'refresh_snapshot') {
                // Warning level - snapshot is getting stale, but don't heal yet
                logger.debug(`[Watchdog] ${integrity.emoji} I:${integrity.score} ${token.symbol} needs fresh snapshot (L:${integrity.components.recency}%)`);
            } else if (result.tampered.length > 0) {
                // Only volatile categories tampered - log but don't heal
                logger.debug(`[Watchdog] Volatile change: ${token.symbol} (${token.mint.slice(0, 8)}...) - ${result.tampered.join(',')}`);
            }
        }

        const duration = Date.now() - startTime;

        // Build integrity distribution string
        const distStr = Object.entries(scoreDistribution)
            .filter(([_, count]) => count > 0)
            .map(([level, count]) => {
                const emoji = { excellent: '🟢', good: '🟡', warning: '🟠', critical: '🔴', failed: '⛔' }[level] || '?';
                return `${emoji}${count}`;
            })
            .join(' ');

        if (tampered > 0) {
            logger.info(`[Watchdog] Scan: ${scanned} tokens [${distStr}] | ${tampered} tampered, ${healed} healed, ${failed} failed (${duration}ms)`);
        } else {
            logger.debug(`[Watchdog] Scan: ${scanned} tokens [${distStr}] (${duration}ms)`);
        }

    } catch (e) {
        logger.error(`[Watchdog] Scan error: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

/**
 * Start the integrity watchdog
 * @param {Object} deps - Dependencies { db }
 */
function start(deps) {
    const { db } = deps;

    if (!config.DATA_SIGNING_SECRET) {
        logger.warn('[Watchdog] DATA_SIGNING_SECRET not set - watchdog disabled');
        return;
    }

    logger.info(`[Watchdog] Starting integrity monitor (interval: ${SCAN_INTERVAL / 1000}s)`);

    // Initial scan after 30 seconds
    setTimeout(() => scanForTampering(db), 30 * 1000);

    // Periodic scans
    watchdogInterval = setInterval(() => scanForTampering(db), SCAN_INTERVAL);
}

/**
 * Stop the watchdog
 */
function stop() {
    if (watchdogInterval) {
        clearInterval(watchdogInterval);
        watchdogInterval = null;
        logger.info('[Watchdog] Stopped');
    }
}

/**
 * Force an immediate scan (for admin/testing)
 * @param {Object} db - Database instance
 */
async function forceScan(db) {
    logger.info('[Watchdog] Force scan triggered');
    await scanForTampering(db);
}

// =============================================================================
// NODE WATCHDOG - Defense against fake node injection
// =============================================================================
// Attackers can INSERT fake nodes in the public DB, but:
// 1. Nodes without signing keys are hidden from API
// 2. Nodes without valid HMAC signatures are deleted
// 3. Nodes with stale heartbeats are marked offline/deleted

const { verifyNodeSignatures, signNodeAllCategories } = require('../utils/dataSignature');

// Node watchdog config
const NODE_SCAN_INTERVAL = 60 * 1000; // 1 minute (more aggressive than token watchdog)
const NODE_STALE_THRESHOLD = 24 * 60 * 60 * 1000; // 24 hours without heartbeat = delete
const NODE_DEGRADED_THRESHOLD = 5 * 60 * 1000; // 5 minutes = degraded
const NODE_SNAPSHOT_PREFIX = 'holdex:node:snapshot:';
const NODE_SNAPSHOT_TTL = 7 * 24 * 60 * 60; // 7 days

let nodeWatchdogInterval = null;

/**
 * Save node snapshot to Redis (for audit + network recovery)
 * @param {Object} node - Node data with all fields
 */
async function saveNodeSnapshot(node) {
    const redis = getRedisClient();
    if (!redis || !node.node_id) return false;

    try {
        const snapshot = {
            ...node,
            _snapshotTime: Date.now(),
            _snapshotVersion: 1
        };

        await redis.set(
            `${NODE_SNAPSHOT_PREFIX}${node.node_id}`,
            JSON.stringify(snapshot),
            'EX',
            NODE_SNAPSHOT_TTL
        );
        return true;
    } catch (e) {
        logger.error(`[NodeWatchdog] Snapshot save failed for ${node.node_id}: ${e.message}`);
        return false;
    }
}

/**
 * Get node snapshot from Redis
 * @param {string} nodeId - Node ID
 * @returns {Object|null}
 */
async function getNodeSnapshot(nodeId) {
    const redis = getRedisClient();
    if (!redis) return null;

    try {
        const data = await redis.get(`${NODE_SNAPSHOT_PREFIX}${nodeId}`);
        return data ? JSON.parse(data) : null;
    } catch (e) {
        logger.error(`[NodeWatchdog] Snapshot read failed for ${nodeId}: ${e.message}`);
        return null;
    }
}

/**
 * Save snapshots of all valid nodes
 * @param {Object} db - Database instance
 */
async function snapshotAllValidNodes(db) {
    try {
        const validNodes = await db.all(`
            SELECT * FROM nodes
            WHERE node_public_key IS NOT NULL
              AND sig_node_identity IS NOT NULL
        `);

        let saved = 0;
        for (const node of validNodes) {
            if (await saveNodeSnapshot(node)) saved++;
        }

        if (saved > 0) {
            logger.debug(`[NodeWatchdog] Saved ${saved} node snapshot(s) to Redis`);
        }
        return saved;
    } catch (e) {
        logger.error(`[NodeWatchdog] Snapshot all failed: ${e.message}`);
        return 0;
    }
}

/**
 * Scan nodes for tampering and clean up fake/stale nodes
 * @param {Object} db - Database instance
 */
async function scanNodesForTampering(db) {
    if (!config.DATA_SIGNING_SECRET) return;

    try {
        const startTime = Date.now();

        // Get ALL nodes (including those without keys, to clean them up)
        const allNodes = await db.all(`
            SELECT node_id, name, operator, region, status, last_heartbeat,
                   tokens_verified, verifications_24h, version,
                   node_public_key, node_key_fingerprint,
                   sig_node_identity, sig_node_status, node_chaos_nonce
            FROM nodes
        `);

        if (!allNodes || allNodes.length === 0) {
            logger.debug('[NodeWatchdog] No nodes to scan');
            return;
        }

        const now = Date.now();
        let scanned = 0;
        let deleted = 0;
        let degraded = 0;
        let tamperedCount = 0;

        for (const node of allNodes) {
            scanned++;

            // 1. DELETE nodes without signing key (no cryptographic identity)
            if (!node.node_public_key) {
                await db.query('DELETE FROM nodes WHERE node_id = $1', [node.node_id]);
                deleted++;
                logger.warn(`[NodeWatchdog] 🗑️ Deleted unsigned node: ${node.node_id} (operator: ${node.operator})`);
                continue;
            }

            // 2. VERIFY HMAC signatures
            const sigResult = verifyNodeSignatures(node);
            if (!sigResult.valid && sigResult.tampered && sigResult.tampered.length > 0) {
                // Tampered node - DELETE it
                await db.query('DELETE FROM nodes WHERE node_id = $1', [node.node_id]);
                deleted++;
                tamperedCount++;
                logger.error(`[NodeWatchdog] 🚨 Deleted TAMPERED node: ${node.node_id} (fields: ${sigResult.tampered.join(',')})`);
                continue;
            }

            // 3. Check heartbeat staleness
            const heartbeatAge = now - (node.last_heartbeat || 0);

            if (heartbeatAge > NODE_STALE_THRESHOLD) {
                // Very stale - DELETE (abandoned or attacker-injected)
                await db.query('DELETE FROM nodes WHERE node_id = $1', [node.node_id]);
                deleted++;
                logger.warn(`[NodeWatchdog] 🗑️ Deleted stale node: ${node.node_id} (${Math.round(heartbeatAge / 3600000)}h old)`);
                continue;
            }

            if (heartbeatAge > NODE_DEGRADED_THRESHOLD && node.status !== 'degraded') {
                // Mark as degraded
                await db.query(`
                    UPDATE nodes SET status = 'degraded', updated_at = $1
                    WHERE node_id = $2 AND status != 'degraded'
                `, [now, node.node_id]);
                degraded++;
            }
        }

        const duration = Date.now() - startTime;

        if (deleted > 0 || degraded > 0 || tamperedCount > 0) {
            logger.info(`[NodeWatchdog] Scan: ${scanned} nodes | 🗑️${deleted} deleted, ⚠️${degraded} degraded, 🚨${tamperedCount} tampered (${duration}ms)`);

            // Alert if tampering detected
            if (tamperedCount > 0) {
                alerting.alertHealthIssue(
                    'NodeNetwork',
                    'Tampering Detected',
                    `${tamperedCount} tampered node(s) were deleted from the network.`
                ).catch(() => {});
            }
        } else {
            logger.debug(`[NodeWatchdog] Scan: ${scanned} nodes, all valid (${duration}ms)`);
        }

        // Save snapshots of valid nodes to Redis (every 5 minutes)
        if (scanned > 0 && Date.now() % (5 * 60 * 1000) < NODE_SCAN_INTERVAL) {
            await snapshotAllValidNodes(db);
        }

    } catch (e) {
        logger.error(`[NodeWatchdog] Scan error: ${e.message}`);
    }
}

/**
 * Try to acquire a distributed lock for NodeWatchdog
 * Only one instance should run the watchdog at a time to prevent conflicts during rolling deploys
 */
async function tryAcquireNodeWatchdogLock() {
    const redis = require('../services/redis');
    const lockKey = 'holdex:node_watchdog:lock';
    const lockTTL = NODE_SCAN_INTERVAL + 30000; // Lock expires slightly after scan interval
    const instanceId = process.env.RENDER_INSTANCE_ID || process.pid;

    try {
        // SET NX with expiry - only succeeds if key doesn't exist
        const result = await redis.set(lockKey, instanceId, 'PX', lockTTL, 'NX');
        if (result === 'OK') {
            return true;
        }
        // Check if we already hold the lock
        const holder = await redis.get(lockKey);
        return holder === String(instanceId);
    } catch (e) {
        // If Redis fails, don't run watchdog to avoid conflicts
        logger.debug(`[NodeWatchdog] Redis lock check failed: ${e.message}`);
        return false;
    }
}

/**
 * Start the node watchdog
 * @param {Object} deps - Dependencies { db }
 */
function startNodeWatchdog(deps) {
    const { db } = deps;

    if (!config.DATA_SIGNING_SECRET) {
        logger.warn('[NodeWatchdog] DATA_SIGNING_SECRET not set - node watchdog disabled');
        return;
    }

    logger.info(`[NodeWatchdog] Starting node integrity monitor (interval: ${NODE_SCAN_INTERVAL / 1000}s)`);

    // Wrapper that checks for distributed lock before scanning
    const lockedScan = async () => {
        const hasLock = await tryAcquireNodeWatchdogLock();
        if (hasLock) {
            await scanNodesForTampering(db);
        } else {
            logger.debug('[NodeWatchdog] Another instance holds the lock, skipping scan');
        }
    };

    // Initial scan after 30 seconds (give nodes time to initialize and avoid race conditions)
    setTimeout(lockedScan, 30 * 1000);

    // Periodic scans
    nodeWatchdogInterval = setInterval(lockedScan, NODE_SCAN_INTERVAL);
}

/**
 * Stop the node watchdog
 */
function stopNodeWatchdog() {
    if (nodeWatchdogInterval) {
        clearInterval(nodeWatchdogInterval);
        nodeWatchdogInterval = null;
        logger.info('[NodeWatchdog] Stopped');
    }
}

module.exports = {
    // Token watchdog
    start,
    stop,
    forceScan,
    saveSnapshot,
    getSnapshot,
    restoreFromSnapshot,
    calculateIntegrityScore,
    getIntegrityAction,
    SNAPSHOT_PREFIX,
    ALL_CATEGORIES,
    STABLE_CATEGORIES,

    // Node watchdog
    startNodeWatchdog,
    stopNodeWatchdog,
    scanNodesForTampering,
    saveNodeSnapshot,
    getNodeSnapshot,
    snapshotAllValidNodes,
    NODE_SCAN_INTERVAL,
    NODE_STALE_THRESHOLD,
    NODE_SNAPSHOT_PREFIX
};
