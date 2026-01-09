/**
 * RPC Credit Monitoring and Budgeting System
 *
 * Tracks Helius RPC usage to prevent runaway credit consumption
 * Features:
 * - Per-hour credit tracking
 * - Budget alerts at 80% threshold
 * - Daily/weekly aggregation
 * - Per-operation breakdown
 */

const { getRedis } = require('./redis');
const logger = require('./logger');
const config = require('../config/env');

// Credit budgets (customize based on your Helius plan)
const BUDGET = {
    DAILY: process.env.HELIUS_DAILY_BUDGET || 50000,    // Default: 50k credits/day
    HOURLY: process.env.HELIUS_HOURLY_BUDGET || 2500,   // Default: 2.5k credits/hour
    ALERT_THRESHOLD: 0.80,                               // Alert at 80% usage
    CRITICAL_THRESHOLD: 0.95                             // Critical at 95% usage
};

/**
 * Track an RPC call
 * @param {string} method - RPC method name (e.g., 'getTokenSupply', 'getAccountInfo')
 * @param {number} credits - Credits consumed (default: 1)
 * @param {Object} metadata - Optional metadata (mint, address, etc.)
 */
async function trackRpcCall(method, credits = 1, metadata = {}) {
    try {
        const redis = await getRedis();
        if (!redis) return;

        const now = new Date();
        const hourKey = `rpc:credits:hour:${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}-${String(now.getHours()).padStart(2, '0')}`;
        const dayKey = `rpc:credits:day:${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
        const methodKey = `rpc:method:${method}:${hourKey}`;

        // Increment counters
        const pipeline = redis.pipeline();
        pipeline.incrby(hourKey, credits);
        pipeline.incrby(dayKey, credits);
        pipeline.incrby(methodKey, credits);
        pipeline.expire(hourKey, 7200);  // 2 hour TTL
        pipeline.expire(dayKey, 172800); // 2 day TTL
        pipeline.expire(methodKey, 7200);

        await pipeline.exec();

        // Check if we should alert
        const hourlyUsage = parseInt(await redis.get(hourKey) || '0');
        const dailyUsage = parseInt(await redis.get(dayKey) || '0');

        const hourlyPercent = hourlyUsage / BUDGET.HOURLY;
        const dailyPercent = dailyUsage / BUDGET.DAILY;

        // Alert on threshold breach
        if (hourlyPercent >= BUDGET.CRITICAL_THRESHOLD) {
            logger.error(`🚨 CRITICAL: Helius credits at ${Math.round(hourlyPercent * 100)}% of hourly budget (${hourlyUsage}/${BUDGET.HOURLY})`);
        } else if (hourlyPercent >= BUDGET.ALERT_THRESHOLD) {
            logger.warn(`⚠️  WARNING: Helius credits at ${Math.round(hourlyPercent * 100)}% of hourly budget (${hourlyUsage}/${BUDGET.HOURLY})`);
        }

        if (dailyPercent >= BUDGET.CRITICAL_THRESHOLD) {
            logger.error(`🚨 CRITICAL: Helius credits at ${Math.round(dailyPercent * 100)}% of daily budget (${dailyUsage}/${BUDGET.DAILY})`);
        } else if (dailyPercent >= BUDGET.ALERT_THRESHOLD && dailyPercent % 0.05 < 0.01) { // Alert every 5%
            logger.warn(`⚠️  WARNING: Helius credits at ${Math.round(dailyPercent * 100)}% of daily budget (${dailyUsage}/${BUDGET.DAILY})`);
        }

        // Log debug info
        logger.debug(`[RPC] ${method}: +${credits} credits (Hour: ${hourlyUsage}, Day: ${dailyUsage})`);

    } catch (e) {
        // Don't fail the operation if monitoring fails
        logger.debug(`[RPC Monitor] Failed to track call: ${e.message}`);
    }
}

/**
 * Get current credit usage stats
 * @returns {Promise<Object>} Usage statistics
 */
async function getUsageStats() {
    try {
        const redis = await getRedis();
        if (!redis) return null;

        const now = new Date();
        const hourKey = `rpc:credits:hour:${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}-${String(now.getHours()).padStart(2, '0')}`;
        const dayKey = `rpc:credits:day:${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;

        const hourlyUsage = parseInt(await redis.get(hourKey) || '0');
        const dailyUsage = parseInt(await redis.get(dayKey) || '0');

        // Get top methods
        const methodPattern = `rpc:method:*:${hourKey}`;
        const methodKeys = await redis.keys(methodPattern);
        const methods = {};

        for (const key of methodKeys) {
            const count = parseInt(await redis.get(key) || '0');
            const methodName = key.split(':')[2];
            methods[methodName] = count;
        }

        return {
            hourly: {
                usage: hourlyUsage,
                budget: BUDGET.HOURLY,
                percent: Math.round((hourlyUsage / BUDGET.HOURLY) * 100)
            },
            daily: {
                usage: dailyUsage,
                budget: BUDGET.DAILY,
                percent: Math.round((dailyUsage / BUDGET.DAILY) * 100)
            },
            methods,
            timestamp: Date.now()
        };
    } catch (e) {
        logger.debug(`[RPC Monitor] Failed to get stats: ${e.message}`);
        return null;
    }
}

/**
 * Check if we should throttle RPC calls based on budget
 * @returns {Promise<boolean>} True if we should throttle
 */
async function shouldThrottle() {
    try {
        const stats = await getUsageStats();
        if (!stats) return false;

        // Throttle if we're above critical threshold
        return stats.hourly.percent >= (BUDGET.CRITICAL_THRESHOLD * 100);
    } catch (_e) {
        return false;
    }
}

/**
 * Reset counters (admin use only)
 */
async function resetCounters() {
    try {
        const redis = await getRedis();
        if (!redis) return;

        const keys = await redis.keys('rpc:*');
        if (keys.length > 0) {
            await redis.del(...keys);
            logger.info(`✅ Reset ${keys.length} RPC monitoring keys`);
        }
    } catch (e) {
        logger.error(`[RPC Monitor] Failed to reset: ${e.message}`);
    }
}

module.exports = {
    trackRpcCall,
    getUsageStats,
    shouldThrottle,
    resetCounters,
    BUDGET
};
