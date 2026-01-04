/**
 * Unified Alerting Service v2 - "Don't Trust, Verify"
 *
 * Centralized monitoring for HolDex infrastructure:
 * - Helius credits (thresholds + consumption rate)
 * - Data tampering detection
 * - Service health (DB, Redis, API)
 * - Memory usage
 * - Critical errors
 *
 * Compatible with GASdf alerting (same Discord webhook)
 */

const logger = require('./logger');

// Alert webhook URL (Discord/Slack)
const ALERT_WEBHOOK = process.env.ALERT_WEBHOOK || process.env.ALERTING_WEBHOOK || null;

// ============================================
// HELIUS CREDIT TRACKING
// ============================================

// Credit thresholds (alert when crossing DOWN)
const CREDIT_THRESHOLDS = [
    { level: 9_000_000, name: '9M', severity: 'INFO' },
    { level: 7_500_000, name: '7.5M', severity: 'INFO' },
    { level: 5_000_000, name: '5M', severity: 'WARNING' },
    { level: 2_500_000, name: '2.5M', severity: 'WARNING' },
    { level: 1_000_000, name: '1M', severity: 'WARNING' },
    { level: 500_000, name: '500K', severity: 'WARNING' },
    { level: 100_000, name: '100K', severity: 'CRITICAL' },
    { level: 50_000, name: '50K', severity: 'CRITICAL' },
    { level: 10_000, name: '10K', severity: 'CRITICAL' },
    { level: 1_000, name: '1K', severity: 'CRITICAL' }
];

// Track credit history for consumption rate
const creditHistory = [];
const MAX_HISTORY_POINTS = 60; // 1 hour of data at 1 sample/min
let lastKnownCredits = null;
let lastCreditCheck = 0;
let lastAlertedThreshold = Infinity;

/**
 * Record credit sample for consumption tracking
 */
function recordCreditSample(credits) {
    const now = Date.now();
    creditHistory.push({ credits, time: now });

    // Keep only last hour of data
    while (creditHistory.length > MAX_HISTORY_POINTS) {
        creditHistory.shift();
    }

    lastKnownCredits = credits;
    lastCreditCheck = now;
}

/**
 * Calculate consumption rate (credits/hour)
 * Uses weighted average favoring recent data
 */
function getConsumptionRate() {
    if (creditHistory.length < 2) return null;

    const oldest = creditHistory[0];
    const newest = creditHistory[creditHistory.length - 1];
    const timeDiffHours = (newest.time - oldest.time) / (1000 * 60 * 60);

    if (timeDiffHours < 0.01) return null; // Need at least ~36 seconds of data

    const creditDiff = oldest.credits - newest.credits;
    const ratePerHour = creditDiff / timeDiffHours;

    return Math.round(ratePerHour);
}

/**
 * Estimate time until depleted
 */
function getTimeUntilDepleted() {
    const rate = getConsumptionRate();
    if (!rate || rate <= 0 || !lastKnownCredits) return null;

    const hoursRemaining = lastKnownCredits / rate;

    if (hoursRemaining < 1) return `${Math.round(hoursRemaining * 60)} min`;
    if (hoursRemaining < 24) return `${hoursRemaining.toFixed(1)} hours`;
    return `${Math.round(hoursRemaining / 24)} days`;
}

// ============================================
// ALERT COOLDOWNS
// ============================================

const COOLDOWNS = {
    helius_threshold: 60 * 60 * 1000,  // 1 hour per threshold
    helius_depleted: 30 * 60 * 1000,   // 30 min
    tampering: 5 * 60 * 1000,          // 5 min
    health: 10 * 60 * 1000,            // 10 min
    memory: 30 * 60 * 1000,            // 30 min
    error: 60 * 1000,                  // 1 min
    status: 0,                         // No cooldown for status
    info: 0                            // No cooldown
};

const lastAlerts = new Map();

const SEVERITY = {
    CRITICAL: { color: 0xff0000, emoji: '🚨' },
    WARNING: { color: 0xffa500, emoji: '⚠️' },
    INFO: { color: 0x3498db, emoji: 'ℹ️' },
    HEALED: { color: 0x00ff00, emoji: '✅' },
    STATUS: { color: 0x9b59b6, emoji: '📊' }
};

function shouldAlert(type, key = 'default') {
    const cooldown = COOLDOWNS[type] || 0;
    if (cooldown === 0) return true;

    const cacheKey = `${type}:${key}`;
    const lastTime = lastAlerts.get(cacheKey) || 0;
    const now = Date.now();

    if (now - lastTime < cooldown) {
        return false;
    }

    lastAlerts.set(cacheKey, now);
    return true;
}

async function sendAlert({ title, message, severity = 'WARNING', type = 'info', key = 'default', fields = [] }) {
    const logFn = severity === 'CRITICAL' ? logger.error : logger.warn;
    logFn(`[Alert:${type}] ${title}: ${message}`);

    if (!shouldAlert(type, key)) {
        logger.debug(`[Alert] Cooldown active for ${type}:${key}`);
        return false;
    }

    if (!ALERT_WEBHOOK) {
        logger.warn('[Alert] No ALERT_WEBHOOK configured');
        return false;
    }

    const { color, emoji } = SEVERITY[severity] || SEVERITY.WARNING;

    try {
        const embed = {
            title: `${emoji} ${title}`,
            description: message,
            color,
            timestamp: new Date().toISOString(),
            footer: { text: 'HolDex Monitoring' },
            fields: fields.length > 0 ? fields : undefined
        };

        const response = await fetch(ALERT_WEBHOOK, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                username: 'HolDex Monitor',
                embeds: [embed]
            })
        });

        if (!response.ok) {
            logger.error(`[Alert] Webhook failed: ${response.status}`);
            return false;
        }

        logger.info(`[Alert] Sent: ${title}`);
        return true;

    } catch (e) {
        logger.error(`[Alert] Send failed: ${e.message}`);
        return false;
    }
}

// ============================================
// HELIUS CREDIT ALERTS
// ============================================

/**
 * Check credits and alert on threshold crossings
 * Call this periodically with current credit count
 */
async function checkHeliusCredits(credits) {
    recordCreditSample(credits);

    const rate = getConsumptionRate();
    const timeLeft = getTimeUntilDepleted();

    // Find crossed thresholds
    for (const threshold of CREDIT_THRESHOLDS) {
        if (credits <= threshold.level && lastAlertedThreshold > threshold.level) {
            lastAlertedThreshold = threshold.level;

            const fields = [
                { name: 'Remaining', value: credits.toLocaleString(), inline: true },
                { name: 'Threshold', value: threshold.name, inline: true }
            ];

            if (rate !== null) {
                fields.push({ name: 'Burn Rate', value: `${rate.toLocaleString()}/hour`, inline: true });
            }
            if (timeLeft) {
                fields.push({ name: 'Time Left', value: timeLeft, inline: true });
            }

            return sendAlert({
                title: `Helius Credits: ${threshold.name} Threshold`,
                message: `Credits dropped below **${threshold.name}**`,
                severity: threshold.severity,
                type: 'helius_threshold',
                key: threshold.name,
                fields
            });
        }
    }

    return false;
}

/**
 * Alert: Helius credits critically low (< 10)
 */
async function alertHeliusCreditsLow(remaining, threshold = 100) {
    recordCreditSample(remaining);
    const rate = getConsumptionRate();
    const timeLeft = getTimeUntilDepleted();

    const fields = [
        { name: 'Remaining', value: remaining.toLocaleString(), inline: true },
        { name: 'Threshold', value: threshold.toLocaleString(), inline: true }
    ];

    if (rate !== null) {
        fields.push({ name: 'Burn Rate', value: `${rate.toLocaleString()}/hour`, inline: true });
    }
    if (timeLeft) {
        fields.push({ name: 'Time Left', value: timeLeft, inline: true });
    }

    return sendAlert({
        title: 'Helius Credits Critical',
        message: `Only **${remaining.toLocaleString()}** credits remaining!`,
        severity: 'CRITICAL',
        type: 'helius_threshold',
        key: 'critical',
        fields
    });
}

/**
 * Alert: Helius credits depleted
 */
async function alertHeliusCreditsDepleted() {
    const rate = getConsumptionRate();

    return sendAlert({
        title: 'Helius Credits DEPLETED',
        message: 'No credits remaining! K-Score updates will fail.',
        severity: 'CRITICAL',
        type: 'helius_depleted',
        key: 'depleted',
        fields: [
            { name: 'Impact', value: 'K-Score updates disabled', inline: true },
            { name: 'Last Burn Rate', value: rate ? `${rate.toLocaleString()}/hour` : 'Unknown', inline: true },
            { name: 'Action', value: '**URGENT: Top up Helius**', inline: false }
        ]
    });
}

/**
 * Get current credit status (for status reports)
 */
function getCreditStatus() {
    return {
        credits: lastKnownCredits,
        lastCheck: lastCreditCheck,
        rate: getConsumptionRate(),
        timeLeft: getTimeUntilDepleted(),
        history: creditHistory.length
    };
}

// ============================================
// TAMPERING ALERTS
// ============================================

async function alertTamperingDetected(mint, symbol, categories) {
    return sendAlert({
        title: 'Data Tampering Detected',
        message: `Token **${symbol}** (${mint.slice(0, 8)}...) has tampered signatures`,
        severity: 'CRITICAL',
        type: 'tampering',
        key: mint,
        fields: [
            { name: 'Mint', value: `\`${mint}\``, inline: false },
            { name: 'Categories', value: categories.join(', '), inline: true },
            { name: 'Action', value: 'Auto-healing', inline: true }
        ]
    });
}

async function alertTamperingHealed(mint, symbol, categories) {
    return sendAlert({
        title: 'Tampering Auto-Healed',
        message: `Token **${symbol}** restored from snapshot`,
        severity: 'HEALED',
        type: 'tampering',
        key: `${mint}:healed`,
        fields: [
            { name: 'Healed', value: categories.join(', '), inline: true }
        ]
    });
}

// ============================================
// INFRASTRUCTURE HEALTH ALERTS
// ============================================

async function alertHealthIssue(service, issue, details = '') {
    return sendAlert({
        title: `Health Issue: ${service}`,
        message: issue,
        severity: 'WARNING',
        type: 'health',
        key: service,
        fields: details ? [{ name: 'Details', value: details }] : []
    });
}

async function alertHealthRecovered(service) {
    return sendAlert({
        title: `Recovered: ${service}`,
        message: `${service} is back online`,
        severity: 'HEALED',
        type: 'health',
        key: `${service}:recovered`
    });
}

async function alertDatabaseIssue(error) {
    return sendAlert({
        title: 'Database Connection Issue',
        message: 'PostgreSQL connection failed',
        severity: 'CRITICAL',
        type: 'health',
        key: 'database',
        fields: [
            { name: 'Error', value: error.message || String(error), inline: false }
        ]
    });
}

async function alertRedisIssue(error) {
    return sendAlert({
        title: 'Redis Connection Issue',
        message: 'Redis connection failed - using fallback',
        severity: 'WARNING',
        type: 'health',
        key: 'redis',
        fields: [
            { name: 'Error', value: error.message || String(error), inline: false },
            { name: 'Impact', value: 'Rate limiting uses in-memory fallback', inline: true }
        ]
    });
}

async function alertHighMemory(usedMB, limitMB) {
    const percent = Math.round((usedMB / limitMB) * 100);
    return sendAlert({
        title: 'High Memory Usage',
        message: `Memory at **${percent}%** of limit`,
        severity: percent > 90 ? 'CRITICAL' : 'WARNING',
        type: 'memory',
        key: 'high',
        fields: [
            { name: 'Used', value: `${usedMB} MB`, inline: true },
            { name: 'Limit', value: `${limitMB} MB`, inline: true },
            { name: 'Percent', value: `${percent}%`, inline: true }
        ]
    });
}

async function alertCriticalError(context, error) {
    return sendAlert({
        title: `Critical Error: ${context}`,
        message: error.message || String(error),
        severity: 'CRITICAL',
        type: 'error',
        key: context,
        fields: [
            { name: 'Stack', value: `\`\`\`${(error.stack || '').slice(0, 500)}\`\`\``, inline: false }
        ]
    });
}

async function alertRateLimitHit(remaining) {
    return sendAlert({
        title: 'RPC Rate Limit',
        message: `Rate limit hit. Remaining: ${remaining}`,
        severity: 'WARNING',
        type: 'helius_threshold',
        key: 'ratelimit',
        fields: [
            { name: 'Remaining', value: String(remaining), inline: true },
            { name: 'Action', value: 'Throttling', inline: true }
        ]
    });
}

// ============================================
// STATUS REPORTS
// ============================================

/**
 * Send periodic status report
 */
async function sendStatusReport(stats) {
    const creditStatus = getCreditStatus();

    const fields = [
        { name: 'Tokens Tracked', value: String(stats.tokens || 0), inline: true },
        { name: 'Verified', value: String(stats.verified || 0), inline: true },
        { name: 'Uptime', value: stats.uptime || 'Unknown', inline: true }
    ];

    if (creditStatus.credits !== null) {
        fields.push({ name: 'Helius Credits', value: creditStatus.credits.toLocaleString(), inline: true });
    }
    if (creditStatus.rate !== null) {
        fields.push({ name: 'Burn Rate', value: `${creditStatus.rate.toLocaleString()}/h`, inline: true });
    }
    if (creditStatus.timeLeft) {
        fields.push({ name: 'Time Left', value: creditStatus.timeLeft, inline: true });
    }

    if (stats.memory) {
        fields.push({ name: 'Memory', value: `${stats.memory} MB`, inline: true });
    }

    return sendAlert({
        title: 'HolDex Status Report',
        message: 'Periodic system health check',
        severity: 'STATUS',
        type: 'status',
        key: 'periodic',
        fields
    });
}

async function testAlert() {
    return sendAlert({
        title: 'Alert System Test',
        message: 'HolDex alerting is working!',
        severity: 'INFO',
        type: 'info',
        key: 'test',
        fields: [
            { name: 'Status', value: '✅ Connected', inline: true },
            { name: 'Time', value: new Date().toISOString(), inline: true }
        ]
    });
}

module.exports = {
    // Core
    sendAlert,
    shouldAlert,

    // Helius Credits
    checkHeliusCredits,
    alertHeliusCreditsLow,
    alertHeliusCreditsDepleted,
    alertRateLimitHit,
    recordCreditSample,
    getCreditStatus,
    getConsumptionRate,

    // Tampering
    alertTamperingDetected,
    alertTamperingHealed,

    // Infrastructure
    alertHealthIssue,
    alertHealthRecovered,
    alertDatabaseIssue,
    alertRedisIssue,
    alertHighMemory,
    alertCriticalError,

    // Status
    sendStatusReport,
    testAlert,

    // Config
    COOLDOWNS,
    CREDIT_THRESHOLDS
};
