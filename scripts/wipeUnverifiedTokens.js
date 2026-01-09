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
            console.log('   - holder_history (holder trend data)');
            console.log('   - holder_snapshots (holder conviction data)');
            console.log('   - k_score_history (K-Score evolution data)');
            console.log('   - supply_history (supply change data)');
            console.log('   - token_verifications (node verification data)');
            console.log('   - webhooks (Helius webhook subscriptions)');
            console.log('   - wallet_tx_cache (wallet transaction cache)');
            console.log('   - candles_1m (price candle data via pools)');
            await pool.end();
            return;
        }

        // CONFIRMED - Actually delete
        console.log('🗑️  DELETING unverified tokens and related data...\n');

        // Start transaction
        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            // Helper to check if table exists
            const tableExists = async (tableName) => {
                const result = await client.query(`
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = $1
                    )
                `, [tableName]);
                return result.rows[0].exists;
            };

            // Subquery for unverified token mints (reused throughout)
            const unverifiedMintsSubquery = `
                SELECT mint FROM tokens
                WHERE hasCommunityUpdate = FALSE OR hasCommunityUpdate IS NULL
            `;

            // ═══════════════════════════════════════════════════════════
            // DELETE ORDER: Must respect foreign key constraints
            // 1. candles_1m (references pools.address)
            // 2. pools (references tokens.mint via foreign key)
            // 3. All other tables that reference tokens.mint (no FK)
            // 4. tokens (last - parent table)
            // ═══════════════════════════════════════════════════════════

            // 1. Delete tables that reference pools.address FIRST
            if (await tableExists('candles_1m')) {
                console.log('   Deleting candles_1m (via pools)...');
                const candlesResult = await client.query(`
                    DELETE FROM candles_1m
                    WHERE pool_address IN (
                        SELECT address FROM pools
                        WHERE mint IN (${unverifiedMintsSubquery})
                    )
                `);
                console.log(`   ✓ Deleted ${candlesResult.rowCount} candle records`);
            }

            if (await tableExists('active_trackers')) {
                console.log('   Deleting active_trackers (via pools)...');
                const trackersResult = await client.query(`
                    DELETE FROM active_trackers
                    WHERE pool_address IN (
                        SELECT address FROM pools
                        WHERE mint IN (${unverifiedMintsSubquery})
                    )
                `);
                console.log(`   ✓ Deleted ${trackersResult.rowCount} active_trackers records`);
            }

            // 2. Delete pools (has foreign key to tokens.mint)
            console.log('   Deleting pools...');
            const poolsResult = await client.query(`
                DELETE FROM pools
                WHERE mint IN (${unverifiedMintsSubquery})
            `);
            console.log(`   ✓ Deleted ${poolsResult.rowCount} pool records`);

            // 3. Delete all other related tables (no FK constraints to tokens)

            console.log('   Deleting holders_history...');
            const holdersHistResult = await client.query(`
                DELETE FROM holders_history
                WHERE mint IN (${unverifiedMintsSubquery})
            `);
            console.log(`   ✓ Deleted ${holdersHistResult.rowCount} holders_history records`);

            if (await tableExists('holder_history')) {
                console.log('   Deleting holder_history...');
                const holderHistResult = await client.query(`
                    DELETE FROM holder_history
                    WHERE mint IN (${unverifiedMintsSubquery})
                `);
                console.log(`   ✓ Deleted ${holderHistResult.rowCount} holder_history records`);
            }

            if (await tableExists('holder_snapshots')) {
                console.log('   Deleting holder_snapshots...');
                const snapshotsResult = await client.query(`
                    DELETE FROM holder_snapshots
                    WHERE mint IN (${unverifiedMintsSubquery})
                `);
                console.log(`   ✓ Deleted ${snapshotsResult.rowCount} holder_snapshots records`);
            }

            if (await tableExists('k_score_history')) {
                console.log('   Deleting k_score_history...');
                const kScoreResult = await client.query(`
                    DELETE FROM k_score_history
                    WHERE mint IN (${unverifiedMintsSubquery})
                `);
                console.log(`   ✓ Deleted ${kScoreResult.rowCount} k_score_history records`);
            }

            if (await tableExists('supply_history')) {
                console.log('   Deleting supply_history...');
                const supplyHistResult = await client.query(`
                    DELETE FROM supply_history
                    WHERE mint IN (${unverifiedMintsSubquery})
                `);
                console.log(`   ✓ Deleted ${supplyHistResult.rowCount} supply_history records`);
            }

            if (await tableExists('token_verifications')) {
                console.log('   Deleting token_verifications...');
                const tokenVerifResult = await client.query(`
                    DELETE FROM token_verifications
                    WHERE mint IN (${unverifiedMintsSubquery})
                `);
                console.log(`   ✓ Deleted ${tokenVerifResult.rowCount} token_verifications records`);
            }

            if (await tableExists('webhooks')) {
                console.log('   Deleting webhooks...');
                const webhooksResult = await client.query(`
                    DELETE FROM webhooks
                    WHERE mint IN (${unverifiedMintsSubquery})
                `);
                console.log(`   ✓ Deleted ${webhooksResult.rowCount} webhook records`);
            }

            if (await tableExists('wallet_tx_cache')) {
                console.log('   Deleting wallet_tx_cache...');
                const walletTxResult = await client.query(`
                    DELETE FROM wallet_tx_cache
                    WHERE mint IN (${unverifiedMintsSubquery})
                `);
                console.log(`   ✓ Deleted ${walletTxResult.rowCount} wallet_tx_cache records`);
            }

            // 4. Delete tokens LAST (parent table)
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
