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
            // First, show ALL numeric columns in tokens table
            const allNumericResult = await client.query(`
                SELECT column_name, data_type, numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_name = 'tokens'
                AND data_type IN ('integer', 'bigint', 'numeric', 'real', 'double precision', 'smallint')
                ORDER BY column_name
            `);

            console.log('ALL numeric columns in tokens table:');
            for (const col of allNumericResult.rows) {
                const marker = col.data_type === 'integer' ? ' <-- INTEGER!' : '';
                console.log(`  ${col.column_name}: ${col.data_type}${marker}`);
            }
            console.log('');

            // Get current column types
            const columnsResult = await client.query(`
                SELECT column_name, data_type, numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_name = 'tokens'
                AND column_name IN ('marketcap', 'k_score', 'liquidity', 'volume24h', 'priceusd', 'change24h', 'change1h', 'change5m', 'conviction_score', 'mcap_calculated')
                ORDER BY column_name
            `);

            console.log('Target columns to check:');
            for (const col of columnsResult.rows) {
                console.log(`  ${col.column_name}: ${col.data_type}`);
            }
            console.log('');

            // Fix columns that need to be DOUBLE PRECISION
            const columnsToFix = [
                { name: 'marketcap', targetType: 'DOUBLE PRECISION' },
                { name: 'k_score', targetType: 'DOUBLE PRECISION' },
                { name: 'liquidity', targetType: 'DOUBLE PRECISION' },
                { name: 'volume24h', targetType: 'DOUBLE PRECISION' },
                { name: 'priceusd', targetType: 'DOUBLE PRECISION' },
                { name: 'change24h', targetType: 'DOUBLE PRECISION' },
                { name: 'change1h', targetType: 'DOUBLE PRECISION' },
                { name: 'change5m', targetType: 'DOUBLE PRECISION' },
                { name: 'conviction_score', targetType: 'DOUBLE PRECISION' },
                { name: 'mcap_calculated', targetType: 'DOUBLE PRECISION' }
            ];

            for (const col of columnsToFix) {
                const currentType = columnsResult.rows.find(r => r.column_name === col.name);

                if (currentType && currentType.data_type !== 'double precision') {
                    console.log(`Altering ${col.name} from ${currentType.data_type} to ${col.targetType}...`);

                    await client.query(`
                        ALTER TABLE tokens
                        ALTER COLUMN ${col.name} TYPE ${col.targetType}
                        USING ${col.name}::${col.targetType}
                    `);

                    console.log(`  ${col.name} fixed`);
                } else if (currentType) {
                    console.log(`${col.name} is already ${currentType.data_type} (OK)`);
                } else {
                    console.log(`${col.name} column not found (will be created on next start)`);
                }
            }

            // Also check holders_history.count column
            const holdersHistResult = await client.query(`
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'holders_history'
                AND column_name = 'count'
            `);

            if (holdersHistResult.rows.length > 0 && holdersHistResult.rows[0].data_type === 'integer') {
                console.log('\nholders_history.count is INTEGER, which should be fine for holder counts.');
            }

            // Check and fix k_score_history table
            const kScoreHistResult = await client.query(`
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'k_score_history'
            `);

            if (kScoreHistResult.rows.length > 0) {
                console.log('\nk_score_history columns:');
                for (const col of kScoreHistResult.rows) {
                    console.log(`  ${col.column_name}: ${col.data_type}`);
                }

                // Fix k_score_history columns if needed
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
                        console.log(`  k_score_history.${col.name} fixed`);
                    }
                }
            }

            // Check and fix pools table
            const poolsResult = await client.query(`
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'pools'
                AND column_name IN ('price_usd', 'liquidity_usd', 'volume_24h')
            `);

            if (poolsResult.rows.length > 0) {
                console.log('\nPools table columns:');
                for (const col of poolsResult.rows) {
                    console.log(`  ${col.column_name}: ${col.data_type}`);
                }

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
                        console.log(`  pools.${col.name} fixed`);
                    }
                }
            }

            // Check and fix polling_tasks table (k_score_result)
            const pollingTasksResult = await client.query(`
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'polling_tasks'
                AND column_name = 'k_score_result'
            `);

            if (pollingTasksResult.rows.length > 0) {
                const col = pollingTasksResult.rows[0];
                console.log(`\npolling_tasks.k_score_result: ${col.data_type}`);
                if (col.data_type !== 'double precision') {
                    console.log(`Altering polling_tasks.k_score_result from ${col.data_type} to DOUBLE PRECISION...`);
                    await client.query(`
                        ALTER TABLE polling_tasks
                        ALTER COLUMN k_score_result TYPE DOUBLE PRECISION
                        USING k_score_result::DOUBLE PRECISION
                    `);
                    console.log(`  polling_tasks.k_score_result fixed`);
                }
            }

            // Check and fix token_verifications table
            const tokenVerifResult = await client.query(`
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'token_verifications'
                AND column_name = 'k_score'
            `);

            if (tokenVerifResult.rows.length > 0) {
                const col = tokenVerifResult.rows[0];
                console.log(`\ntoken_verifications.k_score: ${col.data_type}`);
                if (col.data_type !== 'double precision') {
                    console.log(`Altering token_verifications.k_score from ${col.data_type} to DOUBLE PRECISION...`);
                    await client.query(`
                        ALTER TABLE token_verifications
                        ALTER COLUMN k_score TYPE DOUBLE PRECISION
                        USING k_score::DOUBLE PRECISION
                    `);
                    console.log(`  token_verifications.k_score fixed`);
                }
            }

            // Check and fix consensus_snapshots table
            const consensusResult = await client.query(`
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'consensus_snapshots'
                AND column_name = 'k_score_consensus'
            `);

            if (consensusResult.rows.length > 0) {
                const col = consensusResult.rows[0];
                console.log(`\nconsensus_snapshots.k_score_consensus: ${col.data_type}`);
                if (col.data_type !== 'double precision') {
                    console.log(`Altering consensus_snapshots.k_score_consensus from ${col.data_type} to DOUBLE PRECISION...`);
                    await client.query(`
                        ALTER TABLE consensus_snapshots
                        ALTER COLUMN k_score_consensus TYPE DOUBLE PRECISION
                        USING k_score_consensus::DOUBLE PRECISION
                    `);
                    console.log(`  consensus_snapshots.k_score_consensus fixed`);
                }
            }

            console.log('\nColumn type fixes completed!');

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
