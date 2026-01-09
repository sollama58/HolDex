/**
 * Watchlist Alerts Worker
 *
 * Checks K-Score changes against user-defined thresholds and sends notifications.
 * Runs every 15 minutes as a background task.
 *
 * Features:
 * - Checks all watchlist items with alert thresholds
 * - Supports "below" and "above" direction alerts
 * - Tracks last_notified_at to prevent spam
 * - Minimum 4-hour cooldown between alerts for same token
 */

const { getDB, getWatchlistAlerts, markWatchlistAlertNotified } = require('../services/database');
const logger = require('../services/logger');

// Minimum time between alerts for the same token (4 hours)
const ALERT_COOLDOWN_MS = 4 * 60 * 60 * 1000;

/**
 * Check if an alert should be triggered based on K-Score and threshold
 * @param {number} currentScore - Current K-Score
 * @param {number} threshold - Alert threshold
 * @param {string} direction - 'below' or 'above'
 * @returns {boolean}
 */
function shouldTriggerAlert(currentScore, threshold, direction) {
    if (direction === 'below') {
        return currentScore < threshold;
    } else if (direction === 'above') {
        return currentScore > threshold;
    }
    return false;
}

/**
 * Send notification for triggered alert
 * Currently logs to console - can be extended to email/push/webhook
 * @param {Object} alert - Alert data with token info
 */
async function sendAlertNotification(alert) {
    const { wallet, mint, label, alert_threshold, alert_direction, token } = alert;
    const symbol = token?.symbol || 'Unknown';
    const kScore = token?.k_score || 0;

    // For now, just log - can be extended to:
    // - Email via SendGrid/AWS SES
    // - Push notifications via web push
    // - Webhook callbacks
    // - Discord/Telegram bots

    logger.info(`📧 [WATCHLIST ALERT] ${symbol} (${mint.slice(0, 8)}...)`);
    logger.info(`   Wallet: ${wallet.slice(0, 8)}...`);
    logger.info(`   K-Score: ${kScore} ${alert_direction === 'below' ? '<' : '>'} ${alert_threshold}`);
    if (label) logger.info(`   Label: ${label}`);

    // TODO: Implement actual notification delivery
    // Example email implementation:
    // await sendEmail({
    //     to: getUserEmail(wallet),
    //     subject: `HolDex Alert: ${symbol} K-Score ${alert_direction} ${alert_threshold}`,
    //     body: `Your watchlist token ${symbol} has a K-Score of ${kScore}...`
    // });

    return true;
}

/**
 * Main alert checking function
 * Called periodically by the worker
 */
async function checkWatchlistAlerts() {
    try {
        const db = getDB();
        if (!db) {
            logger.warn('[WatchlistAlerts] Database not initialized');
            return { checked: 0, triggered: 0 };
        }

        // Get all watchlist items with alert thresholds that haven't been notified recently
        const alerts = await getWatchlistAlerts();

        if (!alerts || alerts.length === 0) {
            logger.debug('[WatchlistAlerts] No alerts to check');
            return { checked: 0, triggered: 0 };
        }

        logger.info(`[WatchlistAlerts] Checking ${alerts.length} watchlist alerts...`);

        let triggered = 0;

        for (const alert of alerts) {
            try {
                const {
                    wallet,
                    mint,
                    alert_threshold,
                    alert_direction,
                    last_notified_at,
                    token
                } = alert;

                // Skip if no token data or no K-Score
                if (!token || token.k_score === null || token.k_score === undefined) {
                    continue;
                }

                // Check cooldown
                if (last_notified_at) {
                    const timeSinceLastAlert = Date.now() - last_notified_at;
                    if (timeSinceLastAlert < ALERT_COOLDOWN_MS) {
                        continue;
                    }
                }

                const currentScore = token.k_score;

                // Check if alert should trigger
                if (shouldTriggerAlert(currentScore, alert_threshold, alert_direction)) {
                    // Send notification
                    const sent = await sendAlertNotification(alert);

                    if (sent) {
                        // Mark as notified
                        await markWatchlistAlertNotified(wallet, mint);
                        triggered++;
                    }
                }
            } catch (err) {
                logger.error(`[WatchlistAlerts] Error processing alert for ${alert.mint}: ${err.message}`);
            }
        }

        logger.info(`[WatchlistAlerts] Completed: ${alerts.length} checked, ${triggered} triggered`);
        return { checked: alerts.length, triggered };

    } catch (e) {
        logger.error(`[WatchlistAlerts] Fatal error: ${e.message}`);
        return { checked: 0, triggered: 0, error: e.message };
    }
}

/**
 * Start the watchlist alerts worker
 * Runs every 15 minutes
 */
function startWatchlistAlertsWorker() {
    logger.info('🔔 Starting Watchlist Alerts Worker (15 min interval)');

    // Run immediately on start
    checkWatchlistAlerts().catch(e => {
        logger.error(`[WatchlistAlerts] Initial check failed: ${e.message}`);
    });

    // Then run every 15 minutes
    const interval = setInterval(async () => {
        try {
            await checkWatchlistAlerts();
        } catch (e) {
            logger.error(`[WatchlistAlerts] Scheduled check failed: ${e.message}`);
        }
    }, 15 * 60 * 1000);

    // Graceful shutdown
    process.on('SIGTERM', () => {
        logger.info('🔔 Stopping Watchlist Alerts Worker...');
        clearInterval(interval);
    });

    process.on('SIGINT', () => {
        logger.info('🔔 Stopping Watchlist Alerts Worker...');
        clearInterval(interval);
    });

    return interval;
}

module.exports = {
    checkWatchlistAlerts,
    startWatchlistAlertsWorker,
    shouldTriggerAlert,
    ALERT_COOLDOWN_MS
};
