/**
 * Fix Integer Overflow Script
 *
 * This script alters columns that may have been created as INTEGER
 * but need to be DOUBLE PRECISION or BIGINT to handle large values.
 *
 * Common issues:
 * - marketCap exceeding INTEGER max (~2.1 billion)
 * - k_score being INTEGER instead of DOUBLE PRECISION
 * - liquidity/volume values exceeding limits
 *
 * Usage:
 *   node scripts/fixIntegerOverflow.js
 */

require('dotenv').config();
const { Pool } = require('pg');

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
    console.error('DATABASE_URL environment variable is required');
    process.exit(1);
}

async function main() {
    const pool = new Pool({
        connectionString: DATABASE_URL,
        ssl: DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false }
    });

    try {
        console.log('Checking and fixing column types to prevent integer overflow...\n');

        const client = await pool.connect();

        try {
            // ═══════════════════════════════════════════════════════════
            // TOKENS TABLE - Main token data
            // ═══════════════════════════════════════════════════════════
            console.log('═══════════════════════════════════════════════════════════');
            console.log('TOKENS TABLE');
            console.log('═══════════════════════════════════════════════════════════\n');

            const tokensNumericResult = await client.query(`
                SELECT column_name, data_type, numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_name = 'tokens'
                AND data_type IN ('integer', 'bigint', 'numeric', 'real', 'double precision', 'smallint')
                ORDER BY column_name
            `);

            console.log('All numeric columns in tokens table:');
            for (const col of tokensNumericResult.rows) {
                const marker = col.data_type === 'integer' ? ' <-- INTEGER!' : '';
                console.log(`  ${col.column_name}: ${col.data_type}${marker}`);
            }
            console.log('');

            // Fix columns that MUST be DOUBLE PRECISION (can have decimal values or exceed INTEGER max)
            const tokensColumnsToFix = [
                { name: 'marketcap', targetType: 'DOUBLE PRECISION' },
                { name: 'k_score', targetType: 'DOUBLE PRECISION' },
                { name: 'liquidity', targetType: 'DOUBLE PRECISION' },
                { name: 'volume24h', targetType: 'DOUBLE PRECISION' },
                { name: 'priceusd', targetType: 'DOUBLE PRECISION' },
                { name: 'change24h', targetType: 'DOUBLE PRECISION' },
                { name: 'change1h', targetType: 'DOUBLE PRECISION' },
                { name: 'change5m', targetType: 'DOUBLE PRECISION' },
                { name: 'conviction_score', targetType: 'DOUBLE PRECISION' },
                { name: 'age_days', targetType: 'DOUBLE PRECISION' },
                { name: 'lp_burn_pct', targetType: 'DOUBLE PRECISION' },
                { name: 'lp_locked_pct', targetType: 'DOUBLE PRECISION' },
                { name: 'burned_amount', targetType: 'DOUBLE PRECISION' },
                { name: 'burned_percent', targetType: 'DOUBLE PRECISION' },
                { name: 'supply_change_24h', targetType: 'DOUBLE PRECISION' }
            ];

            for (const col of tokensColumnsToFix) {
                const currentType = tokensNumericResult.rows.find(r => r.column_name === col.name);

                if (currentType && currentType.data_type !== 'double precision') {
                    console.log(`Altering tokens.${col.name} from ${currentType.data_type} to ${col.targetType}...`);
                    await client.query(`
                        ALTER TABLE tokens
                        ALTER COLUMN ${col.name} TYPE ${col.targetType}
                        USING ${col.name}::${col.targetType}
                    `);
                    console.log(`  ✓ tokens.${col.name} fixed`);
                } else if (currentType) {
                    console.log(`✓ tokens.${col.name} is already ${currentType.data_type}`);
                } else {
                    console.log(`- tokens.${col.name} column not found (will be created on next start)`);
                }
            }

            // ═══════════════════════════════════════════════════════════
            // POOLS TABLE
            // ═══════════════════════════════════════════════════════════
            console.log('\n═══════════════════════════════════════════════════════════');
            console.log('POOLS TABLE');
            console.log('═══════════════════════════════════════════════════════════\n');

            const poolsResult = await client.query(`
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'pools'
                AND column_name IN ('price_usd', 'liquidity_usd', 'volume_24h')
            `);

            if (poolsResult.rows.length > 0) {
                const poolColumnsToFix = [
                    { name: 'price_usd', targetType: 'DOUBLE PRECISION' },
                    { name: 'liquidity_usd', targetType: 'DOUBLE PRECISION' },
                    { name: 'volume_24h', targetType: 'DOUBLE PRECISION' }
                ];

                for (const col of poolColumnsToFix) {
                    const currentType = poolsResult.rows.find(r => r.column_name === col.name);
                    if (currentType && currentType.data_type !== 'double precision') {
                        console.log(`Altering pools.${col.name} from ${currentType.data_type} to ${col.targetType}...`);
                        await client.query(`
                            ALTER TABLE pools
                            ALTER COLUMN ${col.name} TYPE ${col.targetType}
                            USING ${col.name}::${col.targetType}
                        `);
                        console.log(`  ✓ pools.${col.name} fixed`);
                    } else if (currentType) {
                        console.log(`✓ pools.${col.name} is already ${currentType.data_type}`);
                    }
                }
            } else {
                console.log('Pools table not found or has no target columns');
            }

            // ═══════════════════════════════════════════════════════════
            // K_SCORE_HISTORY TABLE
            // ═══════════════════════════════════════════════════════════
            console.log('\n═══════════════════════════════════════════════════════════');
            console.log('K_SCORE_HISTORY TABLE');
            console.log('═══════════════════════════════════════════════════════════\n');

            const kScoreHistResult = await client.query(`
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'k_score_history'
            `);

            if (kScoreHistResult.rows.length > 0) {
                console.log('k_score_history columns:');
                for (const col of kScoreHistResult.rows) {
                    const marker = col.data_type === 'integer' ? ' <-- INTEGER!' : '';
                    console.log(`  ${col.column_name}: ${col.data_type}${marker}`);
                }

                const historyColumnsToFix = [
                    { name: 'k_score', targetType: 'DOUBLE PRECISION' },
                    { name: 'conviction_score', targetType: 'DOUBLE PRECISION' }
                ];

                for (const col of historyColumnsToFix) {
                    const currentType = kScoreHistResult.rows.find(r => r.column_name === col.name);
                    if (currentType && currentType.data_type !== 'double precision') {
                        console.log(`Altering k_score_history.${col.name} from ${currentType.data_type} to ${col.targetType}...`);
                        await client.query(`
                            ALTER TABLE k_score_history
                            ALTER COLUMN ${col.name} TYPE ${col.targetType}
                            USING ${col.name}::${col.targetType}
                        `);
                        console.log(`  ✓ k_score_history.${col.name} fixed`);
                    } else if (currentType) {
                        console.log(`✓ k_score_history.${col.name} is already ${currentType.data_type}`);
                    }
                }
            } else {
                console.log('k_score_history table not found');
            }

            // ═══════════════════════════════════════════════════════════
            // HOLDER_HISTORY TABLE (different from holders_history!)
            // ═══════════════════════════════════════════════════════════
            console.log('\n═══════════════════════════════════════════════════════════');
            console.log('HOLDER_HISTORY TABLE');
            console.log('═══════════════════════════════════════════════════════════\n');

            const holderHistResult = await client.query(`
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'holder_history'
            `);

            if (holderHistResult.rows.length > 0) {
                console.log('holder_history columns:');
                for (const col of holderHistResult.rows) {
                    const marker = col.data_type === 'integer' ? ' <-- INTEGER (OK for counts)' : '';
                    console.log(`  ${col.column_name}: ${col.data_type}${marker}`);
                }
                console.log('Note: INTEGER is acceptable for holder counts (they are always whole numbers)');
            } else {
                console.log('holder_history table not found');
            }

            // ═══════════════════════════════════════════════════════════
            // HOLDERS_HISTORY TABLE (legacy, with timestamp)
            // ═══════════════════════════════════════════════════════════
            console.log('\n═══════════════════════════════════════════════════════════');
            console.log('HOLDERS_HISTORY TABLE');
            console.log('═══════════════════════════════════════════════════════════\n');

            const holdersHistResult = await client.query(`
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'holders_history'
            `);

            if (holdersHistResult.rows.length > 0) {
                console.log('holders_history columns:');
                for (const col of holdersHistResult.rows) {
                    const marker = col.data_type === 'integer' ? ' <-- INTEGER (OK for counts)' : '';
                    console.log(`  ${col.column_name}: ${col.data_type}${marker}`);
                }
                console.log('Note: INTEGER is acceptable for holder counts (code now uses Math.floor())');
            } else {
                console.log('holders_history table not found');
            }

            // ═══════════════════════════════════════════════════════════
            // CANDLES_1M TABLE
            // ═══════════════════════════════════════════════════════════
            console.log('\n═══════════════════════════════════════════════════════════');
            console.log('CANDLES_1M TABLE');
            console.log('═══════════════════════════════════════════════════════════\n');

            const candlesResult = await client.query(`
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'candles_1m'
                AND column_name IN ('open', 'high', 'low', 'close', 'volume')
            `);

            if (candlesResult.rows.length > 0) {
                const candleColumnsToFix = [
                    { name: 'open', targetType: 'DOUBLE PRECISION' },
                    { name: 'high', targetType: 'DOUBLE PRECISION' },
                    { name: 'low', targetType: 'DOUBLE PRECISION' },
                    { name: 'close', targetType: 'DOUBLE PRECISION' },
                    { name: 'volume', targetType: 'DOUBLE PRECISION' }
                ];

                for (const col of candleColumnsToFix) {
                    const currentType = candlesResult.rows.find(r => r.column_name === col.name);
                    if (currentType && currentType.data_type !== 'double precision') {
                        console.log(`Altering candles_1m.${col.name} from ${currentType.data_type} to ${col.targetType}...`);
                        await client.query(`
                            ALTER TABLE candles_1m
                            ALTER COLUMN ${col.name} TYPE ${col.targetType}
                            USING ${col.name}::${col.targetType}
                        `);
                        console.log(`  ✓ candles_1m.${col.name} fixed`);
                    } else if (currentType) {
                        console.log(`✓ candles_1m.${col.name} is already ${currentType.data_type}`);
                    }
                }
            } else {
                console.log('candles_1m table not found');
            }

            // ═══════════════════════════════════════════════════════════
            // TOKEN_VERIFICATIONS TABLE
            // ═══════════════════════════════════════════════════════════
            console.log('\n═══════════════════════════════════════════════════════════');
            console.log('TOKEN_VERIFICATIONS TABLE');
            console.log('═══════════════════════════════════════════════════════════\n');

            const tokenVerifResult = await client.query(`
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'token_verifications'
                AND column_name = 'k_score'
            `);

            if (tokenVerifResult.rows.length > 0) {
                const col = tokenVerifResult.rows[0];
                if (col.data_type !== 'double precision') {
                    console.log(`Altering token_verifications.k_score from ${col.data_type} to DOUBLE PRECISION...`);
                    await client.query(`
                        ALTER TABLE token_verifications
                        ALTER COLUMN k_score TYPE DOUBLE PRECISION
                        USING k_score::DOUBLE PRECISION
                    `);
                    console.log(`  ✓ token_verifications.k_score fixed`);
                } else {
                    console.log(`✓ token_verifications.k_score is already ${col.data_type}`);
                }
            } else {
                console.log('token_verifications table not found or k_score column missing');
            }

            // ═══════════════════════════════════════════════════════════
            // PARTICIPANTS TABLE (Harmony E-Score)
            // ═══════════════════════════════════════════════════════════
            console.log('\n═══════════════════════════════════════════════════════════');
            console.log('PARTICIPANTS TABLE');
            console.log('═══════════════════════════════════════════════════════════\n');

            const participantsResult = await client.query(`
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'participants'
                AND column_name IN ('holdings', 'total_burned', 'cached_escore', 'e_score_delta')
            `);

            if (participantsResult.rows.length > 0) {
                const participantColumnsToFix = [
                    { name: 'holdings', targetType: 'DOUBLE PRECISION' },
                    { name: 'total_burned', targetType: 'DOUBLE PRECISION' },
                    { name: 'cached_escore', targetType: 'DOUBLE PRECISION' },
                    { name: 'e_score_delta', targetType: 'DOUBLE PRECISION' }
                ];

                for (const col of participantColumnsToFix) {
                    const currentType = participantsResult.rows.find(r => r.column_name === col.name);
                    if (currentType && currentType.data_type !== 'double precision') {
                        console.log(`Altering participants.${col.name} from ${currentType.data_type} to ${col.targetType}...`);
                        await client.query(`
                            ALTER TABLE participants
                            ALTER COLUMN ${col.name} TYPE ${col.targetType}
                            USING ${col.name}::${col.targetType}
                        `);
                        console.log(`  ✓ participants.${col.name} fixed`);
                    } else if (currentType) {
                        console.log(`✓ participants.${col.name} is already ${currentType.data_type}`);
                    }
                }
            } else {
                console.log('participants table not found');
            }

            // ═══════════════════════════════════════════════════════════
            // CONTRIBUTIONS TABLE
            // ═══════════════════════════════════════════════════════════
            console.log('\n═══════════════════════════════════════════════════════════');
            console.log('CONTRIBUTIONS TABLE');
            console.log('═══════════════════════════════════════════════════════════\n');

            const contributionsResult = await client.query(`
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'contributions'
                AND column_name IN ('amount', 'e_score_delta')
            `);

            if (contributionsResult.rows.length > 0) {
                const contribColumnsToFix = [
                    { name: 'amount', targetType: 'DOUBLE PRECISION' },
                    { name: 'e_score_delta', targetType: 'DOUBLE PRECISION' }
                ];

                for (const col of contribColumnsToFix) {
                    const currentType = contributionsResult.rows.find(r => r.column_name === col.name);
                    if (currentType && currentType.data_type !== 'double precision') {
                        console.log(`Altering contributions.${col.name} from ${currentType.data_type} to ${col.targetType}...`);
                        await client.query(`
                            ALTER TABLE contributions
                            ALTER COLUMN ${col.name} TYPE ${col.targetType}
                            USING ${col.name}::${col.targetType}
                        `);
                        console.log(`  ✓ contributions.${col.name} fixed`);
                    } else if (currentType) {
                        console.log(`✓ contributions.${col.name} is already ${currentType.data_type}`);
                    }
                }
            } else {
                console.log('contributions table not found');
            }

            // ═══════════════════════════════════════════════════════════
            // SUMMARY
            // ═══════════════════════════════════════════════════════════
            console.log('\n═══════════════════════════════════════════════════════════');
            console.log('COMPLETE');
            console.log('═══════════════════════════════════════════════════════════\n');
            console.log('All column type fixes completed!');
            console.log('\nNote: INTEGER columns for holder counts are OK - the code');
            console.log('now uses Math.floor() before inserting into these columns.');

        } finally {
            client.release();
        }

    } catch (err) {
        console.error('Error:', err.message);
        process.exit(1);
    } finally {
        await pool.end();
    }
}

main();
