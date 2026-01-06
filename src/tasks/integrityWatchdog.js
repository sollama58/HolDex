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
        // Re-sign the snapshot data (new chaos_nonce for unpredictability)
        const signatures = signAllCategories(snapshot);

        // Restore all signed fields from snapshot
        await db.run(`
            UPDATE tokens
            SET k_score = $1,
                conviction_score = $2,
                conviction_accumulators = $3,
                conviction_holders = $4,
                conviction_reducers = $5,
                conviction_extractors = $6,
                conviction_analyzed = $7,
                holders = $8,
                priceusd = $9,
                marketcap = $10,
                liquidity = $11,
                burned_amount = $12,
                burned_percent = $13,
                sig_identity = $14,
                sig_security = $15,
                sig_lp = $16,
                sig_supply = $17,
                sig_kscore = $18,
                sig_market = $19,
                sig_origin = $20,
                sig_full = $21,
                chaos_nonce = $22
            WHERE mint = $23
        `, [
            snapshot.k_score,
            snapshot.conviction_score,
            snapshot.conviction_accumulators,
            snapshot.conviction_holders,
            snapshot.conviction_reducers,
            snapshot.conviction_extractors,
            snapshot.conviction_analyzed,
            snapshot.holders,
            snapshot.priceusd,
            snapshot.marketcap,
            snapshot.liquidity,
            snapshot.burned_amount,
            snapshot.burned_percent,
            signatures.sig_identity,
            signatures.sig_security,
            signatures.sig_lp,
            signatures.sig_supply,
            signatures.sig_kscore,
            signatures.sig_market,
            signatures.sig_origin,
            signatures.sig_full,
            signatures.chaos_nonce,
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

            // Filter out ignored categories (volatile data)
            const criticalTampered = result.tampered.filter(cat => !IGNORED_CATEGORIES.has(cat));

            if (criticalTampered.length > 0) {
                tampered++;
                logger.warn(`[Watchdog] TAMPERED: ${token.symbol} (${token.mint.slice(0, 8)}...) - categories: ${criticalTampered.join(',')}`);

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
            } else if (result.tampered.length > 0) {
                // Only volatile categories tampered - log but don't heal
                logger.debug(`[Watchdog] Volatile change: ${token.symbol} (${token.mint.slice(0, 8)}...) - ${result.tampered.join(',')}`);
            }
        }

        const duration = Date.now() - startTime;

        if (tampered > 0) {
            logger.info(`[Watchdog] Scan complete: ${scanned} tokens, ${tampered} tampered, ${healed} healed, ${failed} failed (${duration}ms)`);
        } else {
            logger.debug(`[Watchdog] Scan complete: ${scanned} tokens verified (${duration}ms)`);
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

module.exports = {
    start,
    stop,
    forceScan,
    saveSnapshot,
    getSnapshot,
    restoreFromSnapshot,
    SNAPSHOT_PREFIX
};
