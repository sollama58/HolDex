/**
 * Wipe Unverified Tokens Script
 *
 * This script removes all tokens from the database that do NOT have community updates.
 * Only tokens with hasCommunityUpdate = TRUE will be preserved.
 *
 * WARNING: This is a destructive operation! Always backup your database first.
 *
 * Usage:
 *   node scripts/wipeUnverifiedTokens.js           # Dry run (shows what would be deleted)
 *   node scripts/wipeUnverifiedTokens.js --confirm # Actually delete the tokens
 */

require('dotenv').config();
const { Pool } = require('pg');

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
    console.error('❌ DATABASE_URL environment variable is required');
    process.exit(1);
}

const isConfirmed = process.argv.includes('--confirm');

async function main() {
    const pool = new Pool({
        connectionString: DATABASE_URL,
        ssl: DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false }
    });

    try {
        console.log('🔍 Analyzing tokens in database...\n');

        // Get counts
        const totalResult = await pool.query('SELECT COUNT(*) as count FROM tokens');
        const verifiedResult = await pool.query('SELECT COUNT(*) as count FROM tokens WHERE hasCommunityUpdate = TRUE');
        const unverifiedResult = await pool.query('SELECT COUNT(*) as count FROM tokens WHERE hasCommunityUpdate = FALSE OR hasCommunityUpdate IS NULL');

        const totalCount = parseInt(totalResult.rows[0].count);
        const verifiedCount = parseInt(verifiedResult.rows[0].count);
        const unverifiedCount = parseInt(unverifiedResult.rows[0].count);

        console.log('📊 Token Statistics:');
        console.log(`   Total tokens:      ${totalCount.toLocaleString()}`);
        console.log(`   ✅ Verified:       ${verifiedCount.toLocaleString()} (will be KEPT)`);
        console.log(`   ❌ Unverified:     ${unverifiedCount.toLocaleString()} (will be DELETED)`);
        console.log('');

        if (unverifiedCount === 0) {
            console.log('✅ No unverified tokens to delete. Database is clean!');
            await pool.end();
            return;
        }

        // Show sample of tokens to be deleted
        console.log('📋 Sample of tokens to be deleted (first 10):');
        const sampleResult = await pool.query(`
            SELECT mint, name, symbol, k_score, timestamp
            FROM tokens
            WHERE hasCommunityUpdate = FALSE OR hasCommunityUpdate IS NULL
            ORDER BY timestamp DESC NULLS LAST
            LIMIT 10
        `);

        for (const token of sampleResult.rows) {
            const age = token.timestamp ? Math.floor((Date.now() - token.timestamp) / 86400000) + 'd ago' : 'unknown';
            console.log(`   - ${token.symbol || 'UNKNOWN'} (${token.name || 'Unknown'}) | K: ${token.k_score || 0} | ${age}`);
            console.log(`     Mint: ${token.mint}`);
        }
        console.log('');

        if (!isConfirmed) {
            console.log('⚠️  DRY RUN MODE - No changes made');
            console.log('');
            console.log('To actually delete these tokens, run:');
            console.log('   node scripts/wipeUnverifiedTokens.js --confirm');
            console.log('');
            console.log('⚠️  WARNING: This will also delete related data from:');
            console.log('   - pools (liquidity pool data)');
            console.log('   - holders_history (holder count history)');
            console.log('   - holder_snapshots (holder conviction data)');
            console.log('   - k_score_history (K-Score evolution data)');
            await pool.end();
            return;
        }

        // CONFIRMED - Actually delete
        console.log('🗑️  DELETING unverified tokens and related data...\n');

        // Start transaction
        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            // 1. Delete related data first (foreign key constraints)
            console.log('   Deleting pools...');
            const poolsResult = await client.query(`
                DELETE FROM pools
                WHERE mint IN (
                    SELECT mint FROM tokens
                    WHERE hasCommunityUpdate = FALSE OR hasCommunityUpdate IS NULL
                )
            `);
            console.log(`   ✓ Deleted ${poolsResult.rowCount} pool records`);

            console.log('   Deleting holders_history...');
            const holdersHistResult = await client.query(`
                DELETE FROM holders_history
                WHERE mint IN (
                    SELECT mint FROM tokens
                    WHERE hasCommunityUpdate = FALSE OR hasCommunityUpdate IS NULL
                )
            `);
            console.log(`   ✓ Deleted ${holdersHistResult.rowCount} holder history records`);

            // Check if holder_snapshots table exists before deleting
            const snapshotsExist = await client.query(`
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'holder_snapshots'
                )
            `);
            if (snapshotsExist.rows[0].exists) {
                console.log('   Deleting holder_snapshots...');
                const snapshotsResult = await client.query(`
                    DELETE FROM holder_snapshots
                    WHERE mint IN (
                        SELECT mint FROM tokens
                        WHERE hasCommunityUpdate = FALSE OR hasCommunityUpdate IS NULL
                    )
                `);
                console.log(`   ✓ Deleted ${snapshotsResult.rowCount} holder snapshot records`);
            }

            // Check if k_score_history table exists before deleting
            const kScoreHistoryExist = await client.query(`
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'k_score_history'
                )
            `);
            if (kScoreHistoryExist.rows[0].exists) {
                console.log('   Deleting k_score_history...');
                const kScoreResult = await client.query(`
                    DELETE FROM k_score_history
                    WHERE mint IN (
                        SELECT mint FROM tokens
                        WHERE hasCommunityUpdate = FALSE OR hasCommunityUpdate IS NULL
                    )
                `);
                console.log(`   ✓ Deleted ${kScoreResult.rowCount} K-Score history records`);
            }

            // 2. Delete the tokens themselves
            console.log('   Deleting tokens...');
            const tokensResult = await client.query(`
                DELETE FROM tokens
                WHERE hasCommunityUpdate = FALSE OR hasCommunityUpdate IS NULL
            `);
            console.log(`   ✓ Deleted ${tokensResult.rowCount} token records`);

            await client.query('COMMIT');
            console.log('\n✅ Transaction committed successfully!');

        } catch (err) {
            await client.query('ROLLBACK');
            console.error('\n❌ Error during deletion, transaction rolled back:', err.message);
            throw err;
        } finally {
            client.release();
        }

        // Final count
        const finalResult = await pool.query('SELECT COUNT(*) as count FROM tokens');
        console.log(`\n📊 Final token count: ${parseInt(finalResult.rows[0].count).toLocaleString()}`);

    } catch (err) {
        console.error('❌ Script failed:', err.message);
        process.exit(1);
    } finally {
        await pool.end();
    }
}

main();
