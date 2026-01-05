/**
 * HARMONY ENGINE SERVICE
 *
 * Central service for the $ASDFASDFA E-Score system.
 * Integrates the harmony module with database operations.
 *
 * Responsibilities:
 * - Calculate and cache E-Scores
 * - Apply discounts with efficiency floor
 * - Distribute rewards
 * - Track contributions
 */

'use strict';

const harmony = require('../shared/harmony');
const { logger } = require('./index');
const { getClient: getRedis } = require('./redis');

// Cache TTLs (in seconds)
const ESCORE_CACHE_TTL = 300;      // 5 minutes
const _BENEFITS_CACHE_TTL = 300;  // 5 minutes (reserved)
const COSTS_CACHE_TTL = 3600;     // 1 hour

// In-memory fallback cache
const memoryCache = new Map();
const MEMORY_CACHE_MAX = 1000;

class HarmonyEngine {
    constructor(db) {
        this.db = db;
        this.operationCostsCache = null;
        this.operationCostsCacheTime = 0;
    }

    // ═══════════════════════════════════════════════════════════════
    // E-SCORE MANAGEMENT
    // ═══════════════════════════════════════════════════════════════

    /**
     * Get E-Score for a wallet (cached)
     */
    async getEScore(wallet) {
        const cacheKey = `escore:${wallet}`;

        // Try Redis cache
        const redis = getRedis();
        if (redis) {
            try {
                const cached = await redis.get(cacheKey);
                if (cached) {
                    return JSON.parse(cached);
                }
            } catch (_e) { /* continue */ }
        }

        // Try memory cache
        const memCached = memoryCache.get(cacheKey);
        if (memCached && Date.now() - memCached.ts < ESCORE_CACHE_TTL * 1000) {
            return memCached.data;
        }

        // Calculate from DB
        const result = await this.calculateEScore(wallet);

        // Cache result
        this._cacheResult(cacheKey, result, ESCORE_CACHE_TTL);

        return result;
    }

    /**
     * Calculate E-Score from database
     */
    async calculateEScore(wallet) {
        // Get participant data
        const participant = await this.db.get(`
            SELECT
                holdings,
                total_burned,
                api_calls_30d,
                apps_live,
                nodes_active,
                referrals_active,
                first_activity_at
            FROM participants
            WHERE wallet = $1
        `, [wallet]);

        if (!participant) {
            return {
                score: 0,
                tier: harmony.getTier(0),
                breakdown: {},
                isRegistered: false
            };
        }

        // Calculate days active
        const daysActive = participant.first_activity_at
            ? Math.floor((Date.now() - new Date(participant.first_activity_at).getTime()) / (24 * 60 * 60 * 1000))
            : 0;

        // Calculate E-Score using harmony module
        const eScoreResult = harmony.calculateEScore({
            holdings: Number(participant.holdings) || 0,
            burned: Number(participant.total_burned) || 0,
            apiCalls30d: Number(participant.api_calls_30d) || 0,
            appsLive: Number(participant.apps_live) || 0,
            nodesActive: Number(participant.nodes_active) || 0,
            referralsActive: Number(participant.referrals_active) || 0,
            daysActive
        });

        const tier = harmony.getTier(eScoreResult.score);

        // Update E-Score in DB (async, don't wait)
        this._updateEScoreInDb(wallet, eScoreResult.score, tier).catch(e => {
            logger.warn(`[HarmonyEngine] Failed to update E-Score in DB: ${e.message}`);
        });

        return {
            wallet,
            score: eScoreResult.score,
            tier,
            breakdown: eScoreResult.breakdown,
            activeDimensions: eScoreResult.activeDimensions,
            diversityBonus: eScoreResult.diversityBonus,
            isRegistered: true
        };
    }

    /**
     * Update E-Score in database
     */
    async _updateEScoreInDb(wallet, score, tier) {
        await this.db.run(`
            UPDATE participants
            SET e_score = $1, tier = $2, tier_icon = $3, e_score_updated_at = NOW()
            WHERE wallet = $4
        `, [score, tier.name, tier.icon, wallet]);
    }

    /**
     * Invalidate E-Score cache for a wallet
     */
    async invalidateEScore(wallet) {
        const cacheKey = `escore:${wallet}`;
        memoryCache.delete(cacheKey);

        const redis = getRedis();
        if (redis) {
            await redis.del(cacheKey).catch(() => {});
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // BENEFITS CALCULATION
    // ═══════════════════════════════════════════════════════════════

    /**
     * Get full benefits for a wallet
     */
    async getBenefits(wallet) {
        const eScoreResult = await this.getEScore(wallet);
        const benefits = harmony.calculateBenefits(eScoreResult.score);
        const progress = harmony.getProgressToNextTier(eScoreResult.score);

        return {
            wallet,
            ...eScoreResult,
            benefits: benefits.benefits,
            display: benefits.display,
            progress
        };
    }

    // ═══════════════════════════════════════════════════════════════
    // FEE CALCULATION WITH EFFICIENCY FLOOR
    // ═══════════════════════════════════════════════════════════════

    /**
     * Get operation costs (cached)
     */
    async getOperationCosts() {
        // Check cache
        if (this.operationCostsCache &&
            Date.now() - this.operationCostsCacheTime < COSTS_CACHE_TTL * 1000) {
            return this.operationCostsCache;
        }

        // Load from DB
        const rows = await this.db.all(`
            SELECT operation_type, base_fee, actual_cost, min_fee, max_discount
            FROM operation_costs
            WHERE is_active = TRUE
        `);

        const costs = {};
        for (const row of rows) {
            costs[row.operation_type] = {
                baseFee: Number(row.base_fee),
                cost: Number(row.actual_cost),
                minFee: Number(row.min_fee),
                maxDiscount: Number(row.max_discount)
            };
        }

        this.operationCostsCache = costs;
        this.operationCostsCacheTime = Date.now();

        return costs;
    }

    /**
     * Calculate fee for an operation
     */
    async calculateFee(wallet, operationType) {
        // Get E-Score
        const { score } = await this.getEScore(wallet);

        // Get operation costs
        const costs = await this.getOperationCosts();
        const opCost = costs[operationType];

        if (!opCost) {
            logger.warn(`[HarmonyEngine] Unknown operation type: ${operationType}`);
            return {
                error: 'Unknown operation type',
                operationType
            };
        }

        // Calculate using harmony module
        return harmony.calculateFinalFee(score, opCost.baseFee, opCost.cost);
    }

    /**
     * Quick discount check (for rate limiter)
     */
    async getDiscount(wallet, operationType) {
        const feeResult = await this.calculateFee(wallet, operationType);

        if (feeResult.error) {
            return { discount: 0, error: feeResult.error };
        }

        return {
            discount: feeResult.discounts.effective / 100,
            finalFee: feeResult.finalFee,
            limited: feeResult.discounts.limited
        };
    }

    // ═══════════════════════════════════════════════════════════════
    // CONTRIBUTION TRACKING
    // ═══════════════════════════════════════════════════════════════

    /**
     * Record a contribution
     */
    async recordContribution(wallet, type, amount, metadata = {}) {
        const validTypes = ['hold', 'burn', 'use', 'build', 'run', 'refer'];
        if (!validTypes.includes(type)) {
            throw new Error(`Invalid contribution type: ${type}`);
        }

        // Ensure participant exists
        await this.ensureParticipant(wallet);

        // Insert contribution record
        await this.db.run(`
            INSERT INTO contributions (wallet, type, amount, source, tx_signature, details)
            VALUES ($1, $2, $3, $4, $5, $6)
        `, [
            wallet,
            type,
            amount,
            metadata.source || 'holdex',
            metadata.txSignature || null,
            metadata.details ? JSON.stringify(metadata.details) : null
        ]);

        // Update participant counters based on type
        const updateField = this._getUpdateFieldForType(type);
        if (updateField) {
            await this.db.run(`
                UPDATE participants
                SET ${updateField} = ${updateField} + $1,
                    last_activity_at = NOW()
                WHERE wallet = $2
            `, [amount, wallet]);
        }

        // Invalidate cache
        await this.invalidateEScore(wallet);

        // Return updated E-Score
        return this.getEScore(wallet);
    }

    /**
     * Get field name for contribution type
     */
    _getUpdateFieldForType(type) {
        const mapping = {
            'hold': 'holdings',
            'burn': 'total_burned',
            'use': 'api_calls_30d',
            'build': 'apps_live',
            'run': 'nodes_active',
            'refer': 'referrals_active'
        };
        return mapping[type];
    }

    /**
     * Record API usage (optimized for high volume)
     */
    async recordApiUsage(wallet, count = 1) {
        // Batch updates in Redis, flush to DB periodically
        const redis = getRedis();
        if (redis) {
            const key = `api_usage:${wallet}`;
            await redis.incrby(key, count);
            await redis.expire(key, 86400); // 24h TTL
        }

        // Update participant last activity (debounced)
        await this.db.run(`
            UPDATE participants
            SET api_calls_30d = api_calls_30d + $1, last_activity_at = NOW()
            WHERE wallet = $2
        `, [count, wallet]);

        // Don't invalidate cache for every API call (too expensive)
        // E-Score will be recalculated on next explicit getEScore call
    }

    // ═══════════════════════════════════════════════════════════════
    // PARTICIPANT MANAGEMENT
    // ═══════════════════════════════════════════════════════════════

    /**
     * Ensure participant exists in database
     */
    async ensureParticipant(wallet, type = 'user') {
        await this.db.run(`
            INSERT INTO participants (wallet, type)
            VALUES ($1, $2)
            ON CONFLICT (wallet) DO UPDATE SET
                last_activity_at = NOW()
        `, [wallet, type]);
    }

    /**
     * Get participant profile
     */
    async getParticipant(wallet) {
        const participant = await this.db.get(`
            SELECT * FROM participants WHERE wallet = $1
        `, [wallet]);

        if (!participant) {
            return null;
        }

        // Enrich with calculated E-Score
        const eScoreResult = await this.getEScore(wallet);
        const benefits = harmony.calculateBenefits(eScoreResult.score);

        return {
            ...participant,
            eScore: eScoreResult.score,
            tier: eScoreResult.tier,
            benefits: benefits.benefits,
            progress: harmony.getProgressToNextTier(eScoreResult.score)
        };
    }

    /**
     * Update participant type
     */
    async updateParticipantType(wallet, type) {
        const validTypes = ['user', 'holder', 'burner', 'dev', 'infra'];
        if (!validTypes.includes(type)) {
            throw new Error(`Invalid participant type: ${type}`);
        }

        await this.db.run(`
            UPDATE participants SET type = $1 WHERE wallet = $2
        `, [type, wallet]);
    }

    // ═══════════════════════════════════════════════════════════════
    // REWARD DISTRIBUTION
    // ═══════════════════════════════════════════════════════════════

    /**
     * Distribute rewards from collected fees
     */
    async distributeRewards(totalFeesCollected, periodStart, periodEnd) {
        if (totalFeesCollected <= 0) {
            return { distributed: 0, recipients: 0 };
        }

        // Calculate pools using φ ratios
        const distribution = harmony.distributeFee(totalFeesCollected);

        // Get all participants with E-Score > 0
        const participants = await this.db.all(`
            SELECT wallet, e_score
            FROM participants
            WHERE e_score > 0
            ORDER BY e_score DESC
        `);

        if (participants.length === 0) {
            logger.info('[HarmonyEngine] No participants to reward');
            return { distributed: 0, recipients: 0 };
        }

        // Calculate total E-Score
        const totalEScore = participants.reduce((sum, p) => sum + Number(p.e_score), 0);

        // Distribute rewards
        let totalDistributed = 0;
        const recipientDetails = [];

        for (const participant of participants) {
            const share = harmony.calculateRewardShare(
                Number(participant.e_score),
                totalEScore,
                distribution.rewards
            );

            if (share < 0.00000001) continue; // Skip dust

            // Credit reward
            await this.db.run(`
                UPDATE participants
                SET rewards_pending = rewards_pending + $1,
                    rewards_lifetime = rewards_lifetime + $1
                WHERE wallet = $2
            `, [share, participant.wallet]);

            totalDistributed += share;
            recipientDetails.push({
                wallet: participant.wallet,
                eScore: participant.e_score,
                share
            });
        }

        // Record distribution
        await this.db.run(`
            INSERT INTO reward_distributions (
                period_start, period_end, total_fees_collected, total_pool,
                burn_amount, rewards_amount, treasury_amount,
                recipients_count, total_e_score
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [
            periodStart,
            periodEnd,
            totalFeesCollected,
            distribution.total,
            distribution.burn,
            distribution.rewards,
            distribution.treasury,
            recipientDetails.length,
            totalEScore
        ]);

        logger.info(`[HarmonyEngine] Distributed ${totalDistributed.toFixed(4)} $ASDF to ${recipientDetails.length} participants`);

        return {
            totalFees: totalFeesCollected,
            burn: distribution.burn,
            rewards: distribution.rewards,
            treasury: distribution.treasury,
            distributed: totalDistributed,
            recipients: recipientDetails.length,
            topRecipients: recipientDetails.slice(0, 10)
        };
    }

    // ═══════════════════════════════════════════════════════════════
    // UTILITY METHODS
    // ═══════════════════════════════════════════════════════════════

    /**
     * Cache helper
     */
    _cacheResult(key, data, ttlSeconds) {
        // Redis cache
        const redis = getRedis();
        if (redis) {
            redis.set(key, JSON.stringify(data), { EX: ttlSeconds }).catch(() => {});
        }

        // Memory cache (with LRU eviction)
        if (memoryCache.size >= MEMORY_CACHE_MAX) {
            const firstKey = memoryCache.keys().next().value;
            memoryCache.delete(firstKey);
        }
        memoryCache.set(key, { data, ts: Date.now() });
    }

    /**
     * Get leaderboard
     */
    async getLeaderboard(limit = 100) {
        const rows = await this.db.all(`
            SELECT wallet, e_score, tier, tier_icon, type,
                   holdings, total_burned, api_calls_30d
            FROM participants
            WHERE e_score > 0
            ORDER BY e_score DESC
            LIMIT $1
        `, [limit]);

        return rows.map((row, index) => ({
            rank: index + 1,
            ...row
        }));
    }

    /**
     * Get system stats
     */
    async getStats() {
        const stats = await this.db.get(`
            SELECT
                COUNT(*) as total_participants,
                COUNT(*) FILTER (WHERE e_score > 0) as active_participants,
                SUM(e_score) as total_e_score,
                AVG(e_score) FILTER (WHERE e_score > 0) as avg_e_score,
                SUM(total_burned) as total_burned,
                SUM(rewards_lifetime) as total_rewards_distributed
            FROM participants
        `);

        const tierBreakdown = await this.db.all(`
            SELECT tier, COUNT(*) as count
            FROM participants
            WHERE e_score > 0
            GROUP BY tier
            ORDER BY COUNT(*) DESC
        `);

        return {
            ...stats,
            tierBreakdown
        };
    }
}

// Singleton pattern
let instance = null;

function getHarmonyEngine(db) {
    if (!instance && db) {
        instance = new HarmonyEngine(db);
    }
    return instance;
}

module.exports = {
    HarmonyEngine,
    getHarmonyEngine,
    harmony // Re-export for convenience
};
