/**
 * Unified Alerting Service - "Don't Trust, Verify"
 *
 * Centralized alerts for HolDex infrastructure:
 * - Data tampering detected
 * - Helius credit depletion
 * - Service health issues
 * - Critical errors
 *
 * Compatible with GASdf alerting (same Discord webhook)
 */

const logger = require('./logger');

// Alert webhook URL (Discord/Slack)
const ALERT_WEBHOOK = process.env.ALERT_WEBHOOK || process.env.ALERTING_WEBHOOK || null;

// Alert cooldowns to prevent spam (ms)
const COOLDOWNS = {
    helius_credits: 30 * 60 * 1000,    // 30 min
    tampering: 5 * 60 * 1000,          // 5 min
    health: 10 * 60 * 1000,            // 10 min
    error: 60 * 1000,                   // 1 min
    info: 0                             // No cooldown
};

// Track last alert times
const lastAlerts = new Map();

// Alert severity levels
const SEVERITY = {
    CRITICAL: { color: 0xff0000, emoji: '🚨' },
    WARNING: { color: 0xffa500, emoji: '⚠️' },
    INFO: { color: 0x00ff00, emoji: 'ℹ️' },
    HEALED: { color: 0x00ff00, emoji: '✅' }
};

/**
 * Check if we should send an alert (cooldown)
 * @param {string} type - Alert type
 * @param {string} key - Unique key for this alert
 * @returns {boolean}
 */
function shouldAlert(type, key = 'default') {
    const cooldown = COOLDOWNS[type] || 0;
    const cacheKey = `${type}:${key}`;
    const lastTime = lastAlerts.get(cacheKey) || 0;
    const now = Date.now();

    if (now - lastTime < cooldown) {
        return false;
    }

    lastAlerts.set(cacheKey, now);
    return true;
}

/**
 * Send alert to Discord webhook
 * @param {Object} options - Alert options
 */
async function sendAlert({ title, message, severity = 'WARNING', type = 'info', key = 'default', fields = [] }) {
    // Always log locally
    const logFn = severity === 'CRITICAL' ? logger.error : logger.warn;
    logFn(`[Alert:${type}] ${title}: ${message}`);

    // Check cooldown
    if (!shouldAlert(type, key)) {
        logger.debug(`[Alert] Cooldown active for ${type}:${key}`);
        return false;
    }

    // No webhook = log only
    if (!ALERT_WEBHOOK) {
        logger.warn('[Alert] No ALERT_WEBHOOK configured - alerts logged only');
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
// SPECIALIZED ALERT FUNCTIONS
// ============================================

/**
 * Alert: Helius credits running low
 */
async function alertHeliusCreditsLow(remaining, threshold = 100000) {
    return sendAlert({
        title: 'Helius Credits Low',
        message: `Only **${remaining.toLocaleString()}** credits remaining (threshold: ${threshold.toLocaleString()})`,
        severity: remaining < 10000 ? 'CRITICAL' : 'WARNING',
        type: 'helius_credits',
        key: 'low',
        fields: [
            { name: 'Remaining', value: remaining.toLocaleString(), inline: true },
            { name: 'Action', value: 'Top up Helius account', inline: true }
        ]
    });
}

/**
 * Alert: Helius credits depleted
 */
async function alertHeliusCreditsDepleted() {
    return sendAlert({
        title: 'Helius Credits DEPLETED',
        message: 'No Helius credits remaining! K-Score updates will fail.',
        severity: 'CRITICAL',
        type: 'helius_credits',
        key: 'depleted',
        fields: [
            { name: 'Impact', value: 'K-Score updates disabled', inline: true },
            { name: 'Action', value: 'URGENT: Top up Helius', inline: true }
        ]
    });
}

/**
 * Alert: Data tampering detected
 */
async function alertTamperingDetected(mint, symbol, categories) {
    return sendAlert({
        title: 'Data Tampering Detected',
        message: `Token **${symbol}** (${mint.slice(0, 8)}...) has tampered signatures`,
        severity: 'CRITICAL',
        type: 'tampering',
        key: mint,
        fields: [
            { name: 'Mint', value: `\`${mint}\``, inline: false },
            { name: 'Tampered Categories', value: categories.join(', '), inline: true },
            { name: 'Action', value: 'Auto-healing attempted', inline: true }
        ]
    });
}

/**
 * Alert: Tampering healed
 */
async function alertTamperingHealed(mint, symbol, categories) {
    return sendAlert({
        title: 'Tampering Auto-Healed',
        message: `Token **${symbol}** (${mint.slice(0, 8)}...) restored from snapshot`,
        severity: 'HEALED',
        type: 'tampering',
        key: `${mint}:healed`,
        fields: [
            { name: 'Healed Categories', value: categories.join(', '), inline: true }
        ]
    });
}

/**
 * Alert: Service health issue
 */
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

/**
 * Alert: Service recovered
 */
async function alertHealthRecovered(service) {
    return sendAlert({
        title: `Recovered: ${service}`,
        message: `${service} is back online`,
        severity: 'HEALED',
        type: 'health',
        key: `${service}:recovered`
    });
}

/**
 * Alert: Critical error
 */
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

/**
 * Alert: RPC rate limit
 */
async function alertRateLimitHit(remaining) {
    return sendAlert({
        title: 'RPC Rate Limit Warning',
        message: `Helius rate limit hit. Remaining: ${remaining}`,
        severity: 'WARNING',
        type: 'helius_credits',
        key: 'ratelimit',
        fields: [
            { name: 'Remaining', value: String(remaining), inline: true },
            { name: 'Action', value: 'Throttling requests', inline: true }
        ]
    });
}

/**
 * Test alert (for setup verification)
 */
async function testAlert() {
    return sendAlert({
        title: 'Alert System Test',
        message: 'If you see this, HolDex alerting is working!',
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
    sendAlert,
    alertHeliusCreditsLow,
    alertHeliusCreditsDepleted,
    alertTamperingDetected,
    alertTamperingHealed,
    alertHealthIssue,
    alertHealthRecovered,
    alertCriticalError,
    alertRateLimitHit,
    testAlert,
    // Export for testing
    shouldAlert,
    COOLDOWNS
};
