/**
 * Cleanup Non-Community Tokens Script
 *
 * This script removes ALL tokens that do not have hasCommunityUpdate = TRUE.
 * Only community-verified tokens should remain in the database.
 *
 * Usage:
 *   node src/scripts/cleanup_placeholder_tokens.js          # Dry run (shows what would be deleted)
 *   node src/scripts/cleanup_placeholder_tokens.js --delete # Actually delete the tokens
 */

require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
    connectionString: config.DATABASE_URL,
    ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

const DRY_RUN = !process.argv.includes('--delete');

async function cleanup() {
    console.log('==========================================================');
    console.log('  HolDex Non-Community Token Cleanup Script');
    console.log('==========================================================\n');

    if (DRY_RUN) {
        console.log('MODE: DRY RUN (no changes will be made)');
        console.log('To actually delete tokens, run with --delete flag\n');
    } else {
        console.log('MODE: DELETE (tokens will be permanently removed)\n');
    }

    try {
        // Get count of community-verified tokens (will be kept)
        const verifiedResult = await pool.query(
            'SELECT COUNT(*) as total FROM tokens WHERE hascommunityupdate = TRUE'
        );
        const verifiedCount = parseInt(verifiedResult.rows[0].total);

        // Find ALL tokens without community update
        const nonCommunityQuery = `
            SELECT mint, name, symbol, image, priceusd, marketcap, volume24h, timestamp, hascommunityupdate
            FROM tokens
            WHERE hascommunityupdate IS NOT TRUE
            ORDER BY timestamp DESC
        `;

        const result = await pool.query(nonCommunityQuery);
        const tokensToDelete = result.rows;

        console.log(`Community-verified tokens (KEEPING): ${verifiedCount}`);
        console.log(`Non-community tokens (TO DELETE):    ${tokensToDelete.length}\n`);

        if (tokensToDelete.length === 0) {
            console.log('No non-community tokens found. Database is clean.');
            process.exit(0);
        }

        // Display tokens to be removed (first 50)
        console.log('Tokens to be removed:');
        console.log('-'.repeat(110));
        console.log(
            'Mint'.padEnd(48) +
            'Name'.padEnd(20) +
            'Symbol'.padEnd(12) +
            'Price'.padEnd(14) +
            'Volume24h'
        );
        console.log('-'.repeat(110));

        const displayLimit = 50;
        for (const token of tokensToDelete.slice(0, displayLimit)) {
            const mintDisplay = token.mint.slice(0, 8) + '...' + token.mint.slice(-4);
            const nameDisplay = (token.name || 'NULL').slice(0, 18).padEnd(20);
            const symbolDisplay = (token.symbol || 'NULL').slice(0, 10).padEnd(12);
            const priceDisplay = token.priceusd ? `$${parseFloat(token.priceusd).toFixed(8)}` : '$0';
            const volumeDisplay = token.volume24h ? `$${parseInt(token.volume24h).toLocaleString()}` : '$0';

            console.log(
                `${mintDisplay.padEnd(48)}${nameDisplay}${symbolDisplay}${priceDisplay.padEnd(14)}${volumeDisplay}`
            );
        }

        if (tokensToDelete.length > displayLimit) {
            console.log(`... and ${tokensToDelete.length - displayLimit} more tokens`);
        }
        console.log('-'.repeat(110));

        if (DRY_RUN) {
            console.log(`\nDRY RUN: Would delete ${tokensToDelete.length} token(s).`);
            console.log('Run with --delete flag to actually remove these tokens.');
        } else {
            console.log(`\nDeleting ${tokensToDelete.length} non-community token(s)...`);

            // Get list of mints to delete
            const mintsToDelete = tokensToDelete.map(t => t.mint);

            // Delete in batches to avoid memory issues with large arrays
            const BATCH_SIZE = 500;
            let totalDeleted = {
                holder_snapshots: 0,
                k_score_history: 0,
                pools: 0,
                community_updates: 0,
                active_trackers: 0,
                candles_1m: 0,
                tokens: 0
            };

            for (let i = 0; i < mintsToDelete.length; i += BATCH_SIZE) {
                const batch = mintsToDelete.slice(i, i + BATCH_SIZE);
                console.log(`  Processing batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(mintsToDelete.length / BATCH_SIZE)}...`);

                // Delete related data first (foreign key constraints)

                // 1. Delete holder snapshots
                const holderSnapshotsResult = await pool.query(
                    'DELETE FROM holder_snapshots WHERE mint = ANY($1)',
                    [batch]
                );
                totalDeleted.holder_snapshots += holderSnapshotsResult.rowCount;

                // 2. Delete K-Score history
                const kScoreHistoryResult = await pool.query(
                    'DELETE FROM k_score_history WHERE mint = ANY($1)',
                    [batch]
                );
                totalDeleted.k_score_history += kScoreHistoryResult.rowCount;

                // 3. Delete pools and their trackers
                const poolAddresses = await pool.query(
                    'SELECT address FROM pools WHERE mint = ANY($1)',
                    [batch]
                );
                if (poolAddresses.rows.length > 0) {
                    const addresses = poolAddresses.rows.map(r => r.address);
                    const trackersResult = await pool.query(
                        'DELETE FROM active_trackers WHERE pool_address = ANY($1)',
                        [addresses]
                    );
                    totalDeleted.active_trackers += trackersResult.rowCount;
                }

                const poolsResult = await pool.query(
                    'DELETE FROM pools WHERE mint = ANY($1)',
                    [batch]
                );
                totalDeleted.pools += poolsResult.rowCount;

                // 4. Delete community updates (shouldn't have any since hascommunityupdate is false, but check anyway)
                const communityUpdatesResult = await pool.query(
                    'DELETE FROM community_updates WHERE mint = ANY($1)',
                    [batch]
                );
                totalDeleted.community_updates += communityUpdatesResult.rowCount;

                // 5. Delete candles
                try {
                    const candlesResult = await pool.query(
                        'DELETE FROM candles_1m WHERE mint = ANY($1)',
                        [batch]
                    );
                    totalDeleted.candles_1m += candlesResult.rowCount;
                } catch (_e) {
                    // Table might not exist
                }

                // 6. Delete tokens
                const tokensResult = await pool.query(
                    'DELETE FROM tokens WHERE mint = ANY($1)',
                    [batch]
                );
                totalDeleted.tokens += tokensResult.rowCount;
            }

            console.log('\nDeletion summary:');
            console.log(`  - holder_snapshots: ${totalDeleted.holder_snapshots}`);
            console.log(`  - k_score_history:  ${totalDeleted.k_score_history}`);
            console.log(`  - pools:            ${totalDeleted.pools}`);
            console.log(`  - active_trackers:  ${totalDeleted.active_trackers}`);
            console.log(`  - candles_1m:       ${totalDeleted.candles_1m}`);
            console.log(`  - community_updates:${totalDeleted.community_updates}`);
            console.log(`  - tokens:           ${totalDeleted.tokens}`);

            console.log(`\nSuccessfully removed ${totalDeleted.tokens} non-community token(s).`);
        }

        // Show remaining stats
        const statsResult = await pool.query('SELECT COUNT(*) as total FROM tokens');
        const verifiedStatsResult = await pool.query('SELECT COUNT(*) as total FROM tokens WHERE hascommunityupdate = TRUE');
        console.log(`\nDatabase now contains ${statsResult.rows[0].total} token(s) (${verifiedStatsResult.rows[0].total} community-verified).`);

    } catch (err) {
        console.error('Error during cleanup:', err.message);
        console.error(err.stack);
        process.exit(1);
    } finally {
        await pool.end();
    }

    process.exit(0);
}

cleanup();
