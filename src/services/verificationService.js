/**
 * Verification Service - "Don't Trust, Verify"
 *
 * Core principle: All external data must be verified against on-chain state.
 *
 * Features:
 * 1. On-chain transaction verification (verify webhook events exist)
 * 2. Multi-RPC fallback with cross-validation
 * 3. Conviction staleness detection
 * 4. Audit logging for all data changes
 */

const { Connection: _Connection, PublicKey: _PublicKey } = require('@solana/web3.js');
const crypto = require('crypto');
const config = require('../config/env');
const logger = require('./logger');

// ============================================
// RPC PROVIDERS (Priority Order)
// ============================================
const RPC_PROVIDERS = [
    {
        name: 'helius',
        url: 'https://mainnet.helius-rpc.com/',
        getHeaders: () => config.HELIUS_API_KEY
            ? { 'Content-Type': 'application/json', 'Authorization': `Bearer ${config.HELIUS_API_KEY}` }
            : { 'Content-Type': 'application/json' },
        priority: 1,
        healthy: true,
        lastCheck: 0,
        failures: 0
    },
    {
        name: 'public',
        url: 'https://api.mainnet-beta.solana.com',
        getHeaders: () => ({ 'Content-Type': 'application/json' }),
        priority: 2,
        healthy: true,
        lastCheck: 0,
        failures: 0
    }
];

// Add additional providers from env if configured
if (config.QUICKNODE_RPC_URL) {
    RPC_PROVIDERS.push({
        name: 'quicknode',
        url: config.QUICKNODE_RPC_URL,
        getHeaders: () => ({ 'Content-Type': 'application/json' }),
        priority: 1.5,
        healthy: true,
        lastCheck: 0,
        failures: 0
    });
}

// ============================================
// CONSTANTS
// ============================================
const VERIFICATION_TIMEOUT_MS = 5000;
const STALENESS_THRESHOLD_MS = 6 * 60 * 60 * 1000; // 6 hours
const RPC_HEALTH_CHECK_INTERVAL = 60000; // 1 minute
const MAX_RPC_FAILURES = 3;
const AUDIT_LOG_RETENTION_DAYS = 30;

// Token programs (reserved for future verification)
const _TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const _TOKEN_2022_PROGRAM_ID = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';

// ============================================
// MULTI-RPC MANAGEMENT
// ============================================

/**
 * Get the best available RPC provider
 * @returns {Object} RPC provider config
 */
function getBestProvider() {
    const now = Date.now();

    // Sort by: healthy first, then by priority
    const available = RPC_PROVIDERS
        .filter(p => p.healthy || (now - p.lastCheck) > RPC_HEALTH_CHECK_INTERVAL)
        .sort((a, b) => {
            if (a.healthy !== b.healthy) return b.healthy - a.healthy;
            return a.priority - b.priority;
        });

    if (available.length === 0) {
        // All providers failed, reset and try the first one
        logger.warn('[Verification] All RPC providers failed, resetting health status');
        RPC_PROVIDERS.forEach(p => {
            p.healthy = true;
            p.failures = 0;
        });
        return RPC_PROVIDERS[0];
    }

    return available[0];
}

/**
 * Record RPC call success
 * @param {string} providerName - Name of the provider
 */
function recordProviderSuccess(providerName) {
    const provider = RPC_PROVIDERS.find(p => p.name === providerName);
    if (provider) {
        provider.healthy = true;
        provider.failures = 0;
        provider.lastCheck = Date.now();
    }
}

/**
 * Record RPC call failure
 * @param {string} providerName - Name of the provider
 */
function recordProviderFailure(providerName) {
    const provider = RPC_PROVIDERS.find(p => p.name === providerName);
    if (provider) {
        provider.failures++;
        provider.lastCheck = Date.now();
        if (provider.failures >= MAX_RPC_FAILURES) {
            provider.healthy = false;
            logger.warn(`[Verification] RPC provider ${providerName} marked unhealthy after ${provider.failures} failures`);
        }
    }
}

/**
 * Make RPC call with automatic failover
 * @param {string} method - RPC method name
 * @param {Array} params - RPC params
 * @returns {Object} RPC result
 */
async function rpcCall(method, params) {
    const maxRetries = RPC_PROVIDERS.length;
    let lastError = null;

    for (let i = 0; i < maxRetries; i++) {
        const provider = getBestProvider();

        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), VERIFICATION_TIMEOUT_MS);

            const response = await fetch(provider.url, {
                method: 'POST',
                headers: provider.getHeaders(),
                body: JSON.stringify({
                    jsonrpc: '2.0',
                    id: Date.now(),
                    method,
                    params
                }),
                signal: controller.signal
            });

            clearTimeout(timeout);

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const data = await response.json();

            if (data.error) {
                throw new Error(data.error.message || 'RPC Error');
            }

            recordProviderSuccess(provider.name);
            return { result: data.result, provider: provider.name };

        } catch (error) {
            lastError = error;
            recordProviderFailure(provider.name);
            logger.debug(`[Verification] RPC call failed on ${provider.name}: ${error.message}`);
        }
    }

    throw new Error(`All RPC providers failed: ${lastError?.message}`);
}

// ============================================
// TRANSACTION VERIFICATION
// ============================================

/**
 * Verify a transaction exists on-chain and matches expected data
 * @param {string} signature - Transaction signature
 * @param {Object} expectedData - Expected transaction data (optional)
 * @returns {Object} { verified: boolean, transaction: Object, mismatch: string[] }
 */
async function verifyTransaction(signature, expectedData = null) {
    if (!signature || typeof signature !== 'string' || signature.length < 40) {
        return { verified: false, error: 'Invalid signature format' };
    }

    try {
        const { result, provider } = await rpcCall('getTransaction', [
            signature,
            { encoding: 'jsonParsed', maxSupportedTransactionVersion: 0 }
        ]);

        if (!result) {
            return { verified: false, error: 'Transaction not found on-chain' };
        }

        const verification = {
            verified: true,
            provider,
            blockTime: result.blockTime,
            slot: result.slot,
            signature,
            mismatch: []
        };

        // If expected data provided, verify it matches
        if (expectedData) {
            // Verify timestamp (allow 60 second variance)
            if (expectedData.timestamp) {
                const diff = Math.abs(result.blockTime - expectedData.timestamp);
                if (diff > 60) {
                    verification.mismatch.push(`timestamp: expected ${expectedData.timestamp}, got ${result.blockTime}`);
                }
            }

            // Verify token transfers if present
            if (expectedData.transfers && result.meta?.postTokenBalances) {
                const onChainMints = new Set(
                    result.meta.postTokenBalances.map(b => b.mint)
                );

                for (const transfer of expectedData.transfers) {
                    if (!onChainMints.has(transfer.mint)) {
                        verification.mismatch.push(`mint ${transfer.mint.slice(0,8)}... not in transaction`);
                    }
                }
            }

            if (verification.mismatch.length > 0) {
                verification.verified = false;
            }
        }

        return verification;

    } catch (error) {
        return { verified: false, error: error.message };
    }
}

/**
 * Verify holder balance on-chain
 * @param {string} holderAddress - Holder wallet address
 * @param {string} mint - Token mint address
 * @returns {Object} { verified: boolean, balance: number, source: string }
 */
async function verifyHolderBalance(holderAddress, mint) {
    if (!holderAddress || !mint) {
        return { verified: false, error: 'Missing address or mint' };
    }

    try {
        // Get token accounts for this holder and mint using Helius API
        const { result, provider } = await rpcCall('getTokenAccountsByOwner', [
            holderAddress,
            { mint },
            { encoding: 'jsonParsed' }
        ]);

        if (!result?.value || result.value.length === 0) {
            return { verified: true, balance: 0, provider };
        }

        // Sum all token accounts (usually just one)
        let totalBalance = 0;
        for (const account of result.value) {
            const parsed = account.account?.data?.parsed?.info;
            if (parsed?.tokenAmount?.uiAmount) {
                totalBalance += parsed.tokenAmount.uiAmount;
            }
        }

        return {
            verified: true,
            balance: totalBalance,
            provider,
            accounts: result.value.length
        };

    } catch (error) {
        return { verified: false, error: error.message };
    }
}

/**
 * Cross-validate data between multiple RPC providers
 * @param {string} method - RPC method
 * @param {Array} params - RPC params
 * @returns {Object} { consensus: boolean, results: Array, value: any }
 */
async function crossValidate(method, params) {
    const results = [];

    // Query all healthy providers
    for (const provider of RPC_PROVIDERS.filter(p => p.healthy)) {
        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), VERIFICATION_TIMEOUT_MS);

            const response = await fetch(provider.url, {
                method: 'POST',
                headers: provider.getHeaders(),
                body: JSON.stringify({
                    jsonrpc: '2.0',
                    id: Date.now(),
                    method,
                    params
                }),
                signal: controller.signal
            });

            clearTimeout(timeout);
            const data = await response.json();

            if (data.result !== undefined) {
                results.push({
                    provider: provider.name,
                    value: data.result,
                    hash: crypto.createHash('sha256')
                        .update(JSON.stringify(data.result))
                        .digest('hex')
                        .slice(0, 16)
                });
            }
        } catch (_e) {
            // Skip failed providers
        }
    }

    if (results.length === 0) {
        return { consensus: false, error: 'No providers responded' };
    }

    if (results.length === 1) {
        return {
            consensus: true,
            single: true,
            value: results[0].value,
            provider: results[0].provider
        };
    }

    // Check consensus (all hashes match)
    const hashes = results.map(r => r.hash);
    const consensus = hashes.every(h => h === hashes[0]);

    return {
        consensus,
        results,
        value: consensus ? results[0].value : null,
        divergence: consensus ? null : hashes
    };
}

// ============================================
// STALENESS DETECTION
// ============================================

/**
 * Check if conviction data for a token is stale
 * @param {Object} db - Database instance
 * @param {string} mint - Token mint address
 * @returns {Object} { stale: boolean, lastUpdate: Date, reason: string }
 */
async function checkConvictionStaleness(db, mint) {
    try {
        // Get last holder snapshot update
        const lastSnapshot = await db.get(`
            SELECT MAX(updated_at) as last_update
            FROM holder_snapshots
            WHERE mint = $1
        `, [mint]);

        // Get token verification status
        const token = await db.get(`
            SELECT last_k_score_update, hasCommunityUpdate
            FROM tokens
            WHERE mint = $1
        `, [mint]);

        if (!token?.hascommunityupdate) {
            return { stale: false, reason: 'Token not verified - staleness check skipped' };
        }

        const now = Date.now();
        const lastUpdate = lastSnapshot?.last_update || token?.last_k_score_update || 0;
        const age = now - lastUpdate;

        if (age > STALENESS_THRESHOLD_MS) {
            return {
                stale: true,
                lastUpdate: new Date(lastUpdate),
                ageHours: Math.round(age / (60 * 60 * 1000)),
                reason: 'No holder activity detected in staleness window'
            };
        }

        return {
            stale: false,
            lastUpdate: new Date(lastUpdate),
            ageHours: Math.round(age / (60 * 60 * 1000))
        };

    } catch (error) {
        return { stale: false, error: error.message };
    }
}

/**
 * Get all tokens with stale conviction data
 * @param {Object} db - Database instance
 * @returns {Array} List of stale tokens
 */
async function getStaleTokens(db) {
    try {
        const thresholdMs = STALENESS_THRESHOLD_MS;
        const cutoff = Date.now() - thresholdMs;

        const staleTokens = await db.all(`
            SELECT t.mint, t.symbol, t.name, t.k_score,
                   t.last_k_score_update,
                   MAX(hs.updated_at) as last_holder_update
            FROM tokens t
            LEFT JOIN holder_snapshots hs ON t.mint = hs.mint
            WHERE t.hasCommunityUpdate = TRUE
            GROUP BY t.mint, t.symbol, t.name, t.k_score, t.last_k_score_update
            HAVING COALESCE(MAX(hs.updated_at), t.last_k_score_update, 0) < $1
            ORDER BY COALESCE(MAX(hs.updated_at), t.last_k_score_update, 0) ASC
        `, [cutoff]);

        return staleTokens.map(t => ({
            ...t,
            ageHours: Math.round((Date.now() - (t.last_holder_update || t.last_k_score_update || 0)) / (60 * 60 * 1000))
        }));

    } catch (error) {
        logger.error(`[Verification] getStaleTokens error: ${error.message}`);
        return [];
    }
}

// ============================================
// AUDIT LOGGING
// ============================================

/**
 * Log a data change to the audit trail
 * @param {Object} db - Database instance
 * @param {Object} entry - Audit entry
 */
async function logAudit(db, { action, entity, entityId, oldValue, newValue, source, metadata = {} }) {
    try {
        await db.run(`
            INSERT INTO audit_log (
                action, entity, entity_id, old_value, new_value,
                source, metadata, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `, [
            action,
            entity,
            entityId,
            oldValue ? JSON.stringify(oldValue) : null,
            newValue ? JSON.stringify(newValue) : null,
            source,
            JSON.stringify(metadata),
            Date.now()
        ]);
    } catch (error) {
        // Log to console if DB insert fails - never lose audit data
        logger.error(`[Audit] Failed to log: ${JSON.stringify({ action, entity, entityId, error: error.message })}`);
    }
}

/**
 * Get audit history for an entity
 * @param {Object} db - Database instance
 * @param {string} entity - Entity type (token, holder, etc.)
 * @param {string} entityId - Entity ID
 * @param {number} limit - Max entries to return
 * @returns {Array} Audit entries
 */
async function getAuditHistory(db, entity, entityId, limit = 50) {
    try {
        const entries = await db.all(`
            SELECT * FROM audit_log
            WHERE entity = $1 AND entity_id = $2
            ORDER BY created_at DESC
            LIMIT $3
        `, [entity, entityId, limit]);

        return entries.map(e => ({
            ...e,
            old_value: e.old_value ? JSON.parse(e.old_value) : null,
            new_value: e.new_value ? JSON.parse(e.new_value) : null,
            metadata: e.metadata ? JSON.parse(e.metadata) : {}
        }));

    } catch (error) {
        logger.error(`[Audit] getAuditHistory error: ${error.message}`);
        return [];
    }
}

/**
 * Create audit_log table if not exists
 * @param {Object} db - Database instance
 */
async function ensureAuditTable(db) {
    try {
        await db.run(`
            CREATE TABLE IF NOT EXISTS audit_log (
                id SERIAL PRIMARY KEY,
                action VARCHAR(50) NOT NULL,
                entity VARCHAR(50) NOT NULL,
                entity_id VARCHAR(64) NOT NULL,
                old_value TEXT,
                new_value TEXT,
                source VARCHAR(50),
                metadata TEXT,
                created_at BIGINT NOT NULL
            )
        `);

        await db.run(`
            CREATE INDEX IF NOT EXISTS idx_audit_entity
            ON audit_log (entity, entity_id, created_at DESC)
        `);

        await db.run(`
            CREATE INDEX IF NOT EXISTS idx_audit_created
            ON audit_log (created_at)
        `);

        logger.info('[Audit] Table and indexes ensured');

    } catch (error) {
        logger.error(`[Audit] ensureAuditTable error: ${error.message}`);
    }
}

/**
 * Clean up old audit entries
 * @param {Object} db - Database instance
 */
async function cleanupAuditLog(db) {
    try {
        const cutoff = Date.now() - (AUDIT_LOG_RETENTION_DAYS * 24 * 60 * 60 * 1000);
        const result = await db.run(`
            DELETE FROM audit_log WHERE created_at < $1
        `, [cutoff]);

        if (result?.rowCount > 0) {
            logger.info(`[Audit] Cleaned up ${result.rowCount} old entries`);
        }

    } catch (error) {
        logger.error(`[Audit] cleanupAuditLog error: ${error.message}`);
    }
}

// ============================================
// VERIFICATION REPORT
// ============================================

/**
 * Generate verification report for a token
 * @param {Object} db - Database instance
 * @param {string} mint - Token mint address
 * @returns {Object} Verification report
 */
async function generateVerificationReport(db, mint) {
    const report = {
        mint,
        timestamp: new Date().toISOString(),
        checks: [],
        score: 0,
        maxScore: 0
    };

    try {
        // 1. Check staleness
        const staleness = await checkConvictionStaleness(db, mint);
        report.checks.push({
            name: 'conviction_freshness',
            passed: !staleness.stale,
            details: staleness
        });
        report.maxScore += 25;
        if (!staleness.stale) report.score += 25;

        // 2. Verify top holders exist on-chain
        const topHolders = await db.all(`
            SELECT holder, balance FROM holder_snapshots
            WHERE mint = $1 AND balance > 0
            ORDER BY balance DESC
            LIMIT 5
        `, [mint]);

        let holdersVerified = 0;
        for (const h of topHolders) {
            const verification = await verifyHolderBalance(h.holder, mint);
            if (verification.verified && verification.balance > 0) {
                holdersVerified++;
            }
        }

        report.checks.push({
            name: 'holder_verification',
            passed: holdersVerified >= Math.min(3, topHolders.length),
            details: {
                checked: topHolders.length,
                verified: holdersVerified
            }
        });
        report.maxScore += 25;
        if (holdersVerified >= Math.min(3, topHolders.length)) report.score += 25;

        // 3. Check RPC provider diversity
        const healthyProviders = RPC_PROVIDERS.filter(p => p.healthy).length;
        report.checks.push({
            name: 'rpc_diversity',
            passed: healthyProviders >= 2,
            details: {
                healthy: healthyProviders,
                total: RPC_PROVIDERS.length,
                providers: RPC_PROVIDERS.map(p => ({ name: p.name, healthy: p.healthy }))
            }
        });
        report.maxScore += 25;
        if (healthyProviders >= 2) report.score += 25;

        // 4. Check audit trail exists
        const auditCount = await db.get(`
            SELECT COUNT(*) as count FROM audit_log
            WHERE entity = 'token' AND entity_id = $1
        `, [mint]);

        report.checks.push({
            name: 'audit_trail',
            passed: (auditCount?.count || 0) > 0,
            details: { entries: auditCount?.count || 0 }
        });
        report.maxScore += 25;
        if ((auditCount?.count || 0) > 0) report.score += 25;

        report.trustScore = Math.round((report.score / report.maxScore) * 100);
        report.status = report.trustScore >= 75 ? 'VERIFIED' :
                        report.trustScore >= 50 ? 'PARTIAL' : 'UNVERIFIED';

    } catch (error) {
        report.error = error.message;
        report.status = 'ERROR';
    }

    return report;
}

// ============================================
// EXPORTS
// ============================================

module.exports = {
    // Multi-RPC
    rpcCall,
    crossValidate,
    getBestProvider,
    RPC_PROVIDERS,

    // Transaction Verification
    verifyTransaction,
    verifyHolderBalance,

    // Staleness Detection
    checkConvictionStaleness,
    getStaleTokens,
    STALENESS_THRESHOLD_MS,

    // Audit Logging
    logAudit,
    getAuditHistory,
    ensureAuditTable,
    cleanupAuditLog,

    // Reports
    generateVerificationReport
};
