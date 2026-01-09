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
            // Get current column types
            const columnsResult = await client.query(`
                SELECT column_name, data_type, numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_name = 'tokens'
                AND column_name IN ('marketcap', 'k_score', 'liquidity', 'volume24h', 'priceusd', 'change24h', 'change1h', 'change5m')
                ORDER BY column_name
            `);

            console.log('Current column types:');
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
                { name: 'change5m', targetType: 'DOUBLE PRECISION' }
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

            // Check k_score_history if it exists
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
