/**
 * HolDex Database Reset Script
 *
 * WARNING: This script will COMPLETELY WIPE all data from the database!
 *
 * Usage:
 *   node scripts/resetDatabase.js              # Interactive confirmation required
 *   node scripts/resetDatabase.js --force      # Skip confirmation (use with caution!)
 *   node scripts/resetDatabase.js --dry-run    # Show what would be deleted without doing it
 *
 * Environment:
 *   DATABASE_URL must be set in .env or environment
 */

require('dotenv').config();
const { Pool } = require('pg');
const readline = require('readline');

// ═══════════════════════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
    console.error('ERROR: DATABASE_URL environment variable is not set');
    process.exit(1);
}

// All tables in the HolDex database (order matters for foreign keys!)
const TABLES_TO_DROP = [
    // Views first (depend on tables)
    'node_network_status',
    'polling_queue_status',

    // Tables with foreign key dependencies (drop dependents first)
    'token_verifications',
    'node_work_history',
    'consensus_snapshots',
    'polling_tasks',

    // Harmony system tables
    'space_actions',
    'wallet_sessions',
    'access_grants',
    'reward_distributions',
    'contributions',
    'operation_costs',
    'participants',

    // Core token data tables
    'holder_snapshots',
    'wallet_tx_cache',
    'k_score_history',
    'holder_history',
    'supply_history',
    'holders_history',
    'candles_1m',
    'active_trackers',
    'pools',
    'webhooks',

    // User data tables
    'token_updates',
    'api_keys',
    'wallet_credits',

    // Node network
    'nodes',

    // Main table (last due to potential references)
    'tokens'
];

// Sequences to reset
const SEQUENCES_TO_RESET = [
    'token_updates_id_seq',
    'contributions_id_seq',
    'reward_distributions_id_seq',
    'space_actions_id_seq',
    'token_verifications_id_seq',
    'node_work_history_id_seq',
    'polling_tasks_task_id_seq'
];

// Functions to drop
const FUNCTIONS_TO_DROP = [
    'cleanup_old_polling_tasks()'
];

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════

function isLocalDatabase(url) {
    return url.includes('localhost') || url.includes('127.0.0.1');
}

async function askConfirmation(question) {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    return new Promise((resolve) => {
        rl.question(question, (answer) => {
            rl.close();
            resolve(answer.toLowerCase() === 'yes' || answer.toLowerCase() === 'y');
        });
    });
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN RESET LOGIC
// ═══════════════════════════════════════════════════════════════════════════

async function resetDatabase(options = {}) {
    const { force = false, dryRun = false } = options;
    const isLocal = isLocalDatabase(DATABASE_URL);

    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('           HolDex Database Reset Script');
    console.log('═══════════════════════════════════════════════════════════════\n');

    // Safety checks
    if (!isLocal && !force) {
        console.log('WARNING: You are about to reset a PRODUCTION database!');
        console.log(`Database: ${DATABASE_URL.replace(/:[^:@]+@/, ':****@')}\n`);
    }

    if (dryRun) {
        console.log('[DRY RUN MODE] No changes will be made.\n');
    }

    // Show what will be deleted
    console.log('Tables to be dropped:');
    TABLES_TO_DROP.forEach(t => console.log(`  - ${t}`));
    console.log(`\nTotal: ${TABLES_TO_DROP.length} tables\n`);

    // Confirm unless forced
    if (!force && !dryRun) {
        const confirmed = await askConfirmation(
            'Are you ABSOLUTELY SURE you want to delete ALL data? Type "yes" to confirm: '
        );
        if (!confirmed) {
            console.log('\nOperation cancelled.');
            process.exit(0);
        }
        console.log('');
    }

    // Connect to database
    const sslConfig = isLocal ? false : { rejectUnauthorized: false };
    const pool = new Pool({
        connectionString: DATABASE_URL,
        ssl: sslConfig,
        max: 1
    });

    try {
        const client = await pool.connect();
        console.log('Connected to database.\n');

        if (dryRun) {
            // Just show table counts
            console.log('Current table row counts:');
            for (const table of TABLES_TO_DROP) {
                try {
                    const result = await client.query(`SELECT COUNT(*) FROM ${table}`);
                    console.log(`  ${table}: ${result.rows[0].count} rows`);
                } catch (e) {
                    if (e.code === '42P01') {
                        console.log(`  ${table}: (does not exist)`);
                    } else if (e.code === '42809') {
                        console.log(`  ${table}: (view)`);
                    } else {
                        console.log(`  ${table}: (error: ${e.message})`);
                    }
                }
            }
            client.release();
            await pool.end();
            console.log('\n[DRY RUN] No changes were made.');
            return;
        }

        // Start the reset
        console.log('Starting database reset...\n');
        let dropped = 0;
        let skipped = 0;

        // Drop views first
        console.log('Dropping views...');
        for (const view of ['node_network_status', 'polling_queue_status']) {
            try {
                await client.query(`DROP VIEW IF EXISTS ${view} CASCADE`);
                console.log(`  Dropped view: ${view}`);
                dropped++;
            } catch (e) {
                console.log(`  Skipped view ${view}: ${e.message}`);
                skipped++;
            }
        }

        // Drop functions
        console.log('\nDropping functions...');
        for (const func of FUNCTIONS_TO_DROP) {
            try {
                await client.query(`DROP FUNCTION IF EXISTS ${func} CASCADE`);
                console.log(`  Dropped function: ${func}`);
            } catch (e) {
                console.log(`  Skipped function ${func}: ${e.message}`);
            }
        }

        // Drop all tables
        console.log('\nDropping tables...');
        for (const table of TABLES_TO_DROP) {
            if (table === 'node_network_status' || table === 'polling_queue_status') continue; // Already dropped as views
            try {
                await client.query(`DROP TABLE IF EXISTS ${table} CASCADE`);
                console.log(`  Dropped table: ${table}`);
                dropped++;
            } catch (e) {
                console.log(`  Skipped table ${table}: ${e.message}`);
                skipped++;
            }
        }

        // Reset sequences
        console.log('\nResetting sequences...');
        for (const seq of SEQUENCES_TO_RESET) {
            try {
                await client.query(`DROP SEQUENCE IF EXISTS ${seq} CASCADE`);
                console.log(`  Dropped sequence: ${seq}`);
            } catch (e) {
                // Sequences may not exist, that's fine
            }
        }

        client.release();
        await pool.end();

        console.log('\n═══════════════════════════════════════════════════════════════');
        console.log('                    RESET COMPLETE');
        console.log('═══════════════════════════════════════════════════════════════');
        console.log(`\n  Dropped: ${dropped} objects`);
        console.log(`  Skipped: ${skipped} objects\n`);
        console.log('The database is now empty. Schema will be recreated on next server start.');
        console.log('Run "npm start" or "npm run dev" to initialize the fresh schema.\n');

    } catch (e) {
        console.error('\nFATAL ERROR:', e.message);
        await pool.end();
        process.exit(1);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// CLI
// ═══════════════════════════════════════════════════════════════════════════

const args = process.argv.slice(2);
const options = {
    force: args.includes('--force') || args.includes('-f'),
    dryRun: args.includes('--dry-run') || args.includes('-d')
};

if (args.includes('--help') || args.includes('-h')) {
    console.log(`
HolDex Database Reset Script

Usage:
  node scripts/resetDatabase.js [options]

Options:
  --force, -f     Skip confirmation prompt (dangerous!)
  --dry-run, -d   Show what would be deleted without making changes
  --help, -h      Show this help message

Environment:
  DATABASE_URL    PostgreSQL connection string (required)

Examples:
  node scripts/resetDatabase.js              # Interactive reset
  node scripts/resetDatabase.js --dry-run    # Preview changes
  node scripts/resetDatabase.js --force      # Force reset (no confirmation)
`);
    process.exit(0);
}

resetDatabase(options);
