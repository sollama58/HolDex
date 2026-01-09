/**
 * Cleanup Placeholder Tokens Script
 *
 * This script removes tokens with placeholder metadata from the database.
 * These are tokens that were added before the validation checks were implemented.
 *
 * Placeholder patterns detected:
 * - Names: 'Unknown', 'New Discovery', empty strings, names starting with 'Token '
 * - Symbols: 'UNK', 'UNKNOWN', 'NEW', empty strings
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
    console.log('============================================');
    console.log('  HolDex Placeholder Token Cleanup Script');
    console.log('============================================\n');

    if (DRY_RUN) {
        console.log('MODE: DRY RUN (no changes will be made)');
        console.log('To actually delete tokens, run with --delete flag\n');
    } else {
        console.log('MODE: DELETE (tokens will be permanently removed)\n');
    }

    try {
        // Find all placeholder tokens
        const placeholderQuery = `
            SELECT mint, name, symbol, image, "priceUsd", "marketCap", "volume24h", timestamp
            FROM tokens
            WHERE
                name IN ('Unknown', 'New Discovery', '')
                OR name IS NULL
                OR name LIKE 'Token %'
                OR symbol IN ('UNK', 'UNKNOWN', 'NEW', '')
                OR symbol IS NULL
            ORDER BY timestamp DESC
        `;

        const result = await pool.query(placeholderQuery);
        const placeholderTokens = result.rows;

        console.log(`Found ${placeholderTokens.length} placeholder token(s):\n`);

        if (placeholderTokens.length === 0) {
            console.log('No placeholder tokens found in database.');
            process.exit(0);
        }

        // Display tokens to be removed
        console.log('Tokens to be removed:');
        console.log('-'.repeat(100));
        console.log(
            'Mint'.padEnd(48) +
            'Name'.padEnd(20) +
            'Symbol'.padEnd(12) +
            'Price'.padEnd(12) +
            'Volume24h'
        );
        console.log('-'.repeat(100));

        for (const token of placeholderTokens) {
            const mintDisplay = token.mint.slice(0, 8) + '...' + token.mint.slice(-4);
            const nameDisplay = (token.name || 'NULL').slice(0, 18).padEnd(20);
            const symbolDisplay = (token.symbol || 'NULL').slice(0, 10).padEnd(12);
            const priceDisplay = token.priceUsd ? `$${parseFloat(token.priceUsd).toFixed(6)}` : '$0';
            const volumeDisplay = token.volume24h ? `$${parseInt(token.volume24h).toLocaleString()}` : '$0';

            console.log(
                `${mintDisplay.padEnd(48)}${nameDisplay}${symbolDisplay}${priceDisplay.padEnd(12)}${volumeDisplay}`
            );
        }
        console.log('-'.repeat(100));

        if (DRY_RUN) {
            console.log(`\nDRY RUN: Would delete ${placeholderTokens.length} token(s).`);
            console.log('Run with --delete flag to actually remove these tokens.');
        } else {
            console.log(`\nDeleting ${placeholderTokens.length} placeholder token(s)...`);

            // Get list of mints to delete
            const mintsToDelete = placeholderTokens.map(t => t.mint);

            // Delete related data first (foreign key constraints)
            // Delete from related tables that reference tokens.mint

            // 1. Delete holder snapshots
            const holderSnapshotsResult = await pool.query(
                'DELETE FROM holder_snapshots WHERE mint = ANY($1)',
                [mintsToDelete]
            );
            console.log(`  - Deleted ${holderSnapshotsResult.rowCount} holder_snapshots`);

            // 2. Delete K-Score history
            const kScoreHistoryResult = await pool.query(
                'DELETE FROM k_score_history WHERE mint = ANY($1)',
                [mintsToDelete]
            );
            console.log(`  - Deleted ${kScoreHistoryResult.rowCount} k_score_history records`);

            // 3. Delete pools
            const poolsResult = await pool.query(
                'DELETE FROM pools WHERE mint = ANY($1)',
                [mintsToDelete]
            );
            console.log(`  - Deleted ${poolsResult.rowCount} pools`);

            // 4. Delete community updates
            const communityUpdatesResult = await pool.query(
                'DELETE FROM community_updates WHERE mint = ANY($1)',
                [mintsToDelete]
            );
            console.log(`  - Deleted ${communityUpdatesResult.rowCount} community_updates`);

            // 5. Delete tokens
            const tokensResult = await pool.query(
                'DELETE FROM tokens WHERE mint = ANY($1)',
                [mintsToDelete]
            );
            console.log(`  - Deleted ${tokensResult.rowCount} tokens`);

            console.log(`\nSuccessfully removed ${tokensResult.rowCount} placeholder token(s).`);
        }

        // Show remaining stats
        const statsResult = await pool.query('SELECT COUNT(*) as total FROM tokens');
        console.log(`\nDatabase now contains ${statsResult.rows[0].total} token(s).`);

    } catch (err) {
        console.error('Error during cleanup:', err.message);
        process.exit(1);
    } finally {
        await pool.end();
    }

    process.exit(0);
}

cleanup();
