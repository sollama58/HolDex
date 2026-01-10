/**
 * Holder Snapshots Module
 *
 * Manages holder snapshot storage and retrieval for conviction analysis.
 * Snapshots track top 20 holders for each token to detect accumulation/extraction.
 */

const logger = require('../../services/logger');

/**
 * Save holder snapshots to database
 * Uses UPSERT pattern to maintain historical data
 *
 * @param {Object} db - Database instance
 * @param {string} mint - Token mint address
 * @param {Array} holders - Array of { address, balance, ... }
 */
async function saveHolderSnapshots(db, mint, holders) {
    if (!holders || holders.length === 0) return;

    // Only snapshot top 20 by balance (conviction analysis focus)
    const top20 = holders
        .sort((a, b) => (b.balance || 0) - (a.balance || 0))
        .slice(0, 20);

    const now = Date.now();

    for (const holder of top20) {
        try {
            // Get previous balance for delta tracking
            const prev = await db.get(
                'SELECT balance, first_seen FROM holder_snapshots WHERE mint = $1 AND holder = $2',
                [mint, holder.address]
            );

            const previousBalance = prev ? parseFloat(prev.balance) : 0;
            const currentBalance = parseFloat(holder.balance) || 0;
            const balanceChange = currentBalance - previousBalance;

            // Classify behavior based on balance change
            let behavior = 'holder';
            if (balanceChange > previousBalance * 0.1) {
                behavior = 'accumulator';
            } else if (balanceChange < -previousBalance * 0.1) {
                behavior = 'extractor';
            } else if (Math.abs(balanceChange) < previousBalance * 0.05) {
                behavior = 'holder';
            }

            await db.run(`
                INSERT INTO holder_snapshots (mint, holder, balance, behavior, first_seen, last_updated)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (mint, holder) DO UPDATE SET
                    balance = $3,
                    behavior = $4,
                    last_updated = $6
            `, [
                mint,
                holder.address,
                currentBalance.toString(),
                behavior,
                prev?.first_seen || now.toString(),
                now.toString()
            ]);

        } catch (e) {
            logger.debug(`[HolderSnapshots] Error saving ${holder.address}: ${e.message}`);
        }
    }
}

/**
 * Load holder snapshots from database
 *
 * @param {Object} db - Database instance
 * @param {string} mint - Token mint address
 * @returns {Array} Array of holder snapshots
 */
async function loadHolderSnapshots(db, mint) {
    try {
        const snapshots = await db.all(
            'SELECT holder, balance, behavior, first_seen, last_updated FROM holder_snapshots WHERE mint = $1 ORDER BY balance DESC LIMIT 20',
            [mint]
        );
        return snapshots || [];
    } catch (e) {
        logger.debug(`[HolderSnapshots] Error loading for ${mint}: ${e.message}`);
        return [];
    }
}

/**
 * Prune holders that haven't been updated in 30 days
 * Helps prevent stale data from affecting conviction scores
 *
 * @param {Object} db - Database instance
 * @param {string} mint - Token mint address
 */
async function pruneStaleHolders(db, mint) {
    const thirtyDaysAgo = Date.now() - (30 * 24 * 60 * 60 * 1000);

    try {
        const result = await db.run(`
            DELETE FROM holder_snapshots
            WHERE mint = $1 AND last_updated < $2
        `, [mint, thirtyDaysAgo.toString()]);

        if (result?.rowCount > 0) {
            logger.debug(`[HolderSnapshots] Pruned ${result.rowCount} stale holders for ${mint.slice(0, 8)}...`);
        }
    } catch (e) {
        logger.debug(`[HolderSnapshots] Prune error: ${e.message}`);
    }
}

/**
 * Save holder history (daily aggregates)
 *
 * @param {Object} db - Database instance
 * @param {string} mint - Token mint address
 * @param {number} totalHolders - Total holder count
 * @param {number} realHolders - Real (non-dust) holder count
 */
async function saveHolderHistory(db, mint, totalHolders, realHolders) {
    const today = new Date().toISOString().split('T')[0];

    try {
        await db.run(`
            INSERT INTO holder_history (mint, date, total_holders, real_holders)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (mint, date) DO UPDATE SET
                total_holders = $3,
                real_holders = $4
        `, [mint, today, totalHolders, realHolders]);
    } catch (e) {
        logger.debug(`[HolderSnapshots] History save error: ${e.message}`);
    }
}

/**
 * Save K-Score history (daily snapshots for trend analysis)
 *
 * @param {Object} db - Database instance
 * @param {string} mint - Token mint address
 * @param {number} kScore - K-Score value
 * @param {number} convictionScore - Conviction score
 * @param {number} holders - Holder count
 */
async function saveKScoreHistory(db, mint, kScore, convictionScore, holders) {
    const today = new Date().toISOString().split('T')[0];

    try {
        await db.run(`
            INSERT INTO k_score_history (mint, date, k_score, conviction_score, holders)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (mint, date) DO UPDATE SET
                k_score = $3,
                conviction_score = $4,
                holders = $5
        `, [mint, today, kScore, convictionScore, holders]);
    } catch (e) {
        logger.debug(`[HolderSnapshots] K-Score history save error: ${e.message}`);
    }
}

module.exports = {
    saveHolderSnapshots,
    loadHolderSnapshots,
    pruneStaleHolders,
    saveHolderHistory,
    saveKScoreHistory
};
