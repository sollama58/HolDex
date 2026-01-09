/**
 * HolDex Database Reset Script
 *
 * WARNING: This script will COMPLETELY WIPE all data from the database!
 *
 * Usage:
 *   node scripts/resetDatabase.js              # Interactive confirmation required
 *   node scripts/resetDatabase.js --force      # Skip confirmation (use with caution!)
 *   node scripts/resetDatabase.js --dry-run    # Show what would be deleted without doing it
 *   node scripts/resetDatabase.js --reinit     # Reinitialize schema after reset
 *   node scripts/resetDatabase.js --clear-redis # Also clear Redis cache
 *
 * Environment:
 *   DATABASE_URL must be set in .env or environment
 *   REDIS_URL (optional) for clearing cache
 */

require('dotenv').config();
const { Pool } = require('pg');
const readline = require('readline');

// ═══════════════════════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════

const DATABASE_URL = process.env.DATABASE_URL;
const REDIS_URL = process.env.REDIS_URL;

if (!DATABASE_URL) {
    console.error('❌ ERROR: DATABASE_URL environment variable is not set');
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
    'node_approvals',
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
    'polling_tasks_task_id_seq',
    'node_approvals_id_seq'
];

// Functions to drop
const FUNCTIONS_TO_DROP = [
    'cleanup_old_polling_tasks()'
];

// TimescaleDB hypertables (need special handling)
const HYPERTABLES = [
    'candles_1m',
    'k_score_history'
];

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════

function isLocalDatabase(url) {
    return url.includes('localhost') || url.includes('127.0.0.1');
}

function maskConnectionString(url) {
    try {
        return url.replace(/:[^:@]+@/, ':****@');
    } catch {
        return '[masked]';
    }
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

async function clearRedisCache() {
    if (!REDIS_URL) {
        console.log('⚠️  REDIS_URL not set, skipping Redis cache clear');
        return false;
    }

    try {
        const Redis = require('ioredis');
        const redis = new Redis(REDIS_URL);

        console.log('\n📦 Clearing Redis cache...');
        await redis.flushdb();
        console.log('✅ Redis cache cleared');

        await redis.quit();
        return true;
    } catch (e) {
        console.log(`⚠️  Failed to clear Redis: ${e.message}`);
        return false;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN RESET LOGIC
// ═══════════════════════════════════════════════════════════════════════════

async function resetDatabase(options = {}) {
    const { force = false, dryRun = false, reinit = false, clearRedis = false } = options;
    const isLocal = isLocalDatabase(DATABASE_URL);

    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('           🗑️  HolDex Database Reset Script');
    console.log('═══════════════════════════════════════════════════════════════\n');

    // Safety checks
    if (!isLocal && !force) {
        console.log('⚠️  WARNING: You are about to reset a PRODUCTION database!');
        console.log(`📍 Database: ${maskConnectionString(DATABASE_URL)}\n`);
    } else if (isLocal) {
        console.log('📍 Target: Local development database\n');
    }

    if (dryRun) {
        console.log('🔍 [DRY RUN MODE] No changes will be made.\n');
    }

    // Show what will be deleted
    console.log('📋 Tables to be dropped:');
    TABLES_TO_DROP.forEach(t => console.log(`   - ${t}`));
    console.log(`\n   Total: ${TABLES_TO_DROP.length} tables\n`);

    if (clearRedis) {
        console.log('📦 Redis cache will also be cleared\n');
    }

    if (reinit) {
        console.log('🔄 Schema will be reinitialized after reset\n');
    }

    // Confirm unless forced
    if (!force && !dryRun) {
        console.log('⚠️  THIS ACTION CANNOT BE UNDONE!\n');
        const confirmed = await askConfirmation(
            '❓ Are you ABSOLUTELY SURE you want to delete ALL data? Type "yes" to confirm: '
        );
        if (!confirmed) {
            console.log('\n❌ Operation cancelled.');
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
        console.log('✅ Connected to database.\n');

        if (dryRun) {
            // Just show table counts
            console.log('📊 Current table row counts:');
            for (const table of TABLES_TO_DROP) {
                try {
                    const result = await client.query(`SELECT COUNT(*) FROM ${table}`);
                    console.log(`   ${table}: ${result.rows[0].count} rows`);
                } catch (e) {
                    if (e.code === '42P01') {
                        console.log(`   ${table}: (does not exist)`);
                    } else if (e.code === '42809') {
                        console.log(`   ${table}: (view)`);
                    } else {
                        console.log(`   ${table}: (error: ${e.message})`);
                    }
                }
            }

            // Check for any tables not in our list
            console.log('\n📋 Checking for unknown tables...');
            const allTables = await client.query(`
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public'
            `);
            const knownTables = new Set(TABLES_TO_DROP);
            const unknownTables = allTables.rows
                .map(r => r.tablename)
                .filter(t => !knownTables.has(t));

            if (unknownTables.length > 0) {
                console.log('⚠️  Found tables not in drop list:');
                unknownTables.forEach(t => console.log(`   - ${t}`));
            } else {
                console.log('✅ No unknown tables found');
            }

            client.release();
            await pool.end();
            console.log('\n🔍 [DRY RUN] No changes were made.');
            return;
        }

        // Start the reset
        console.log('🚀 Starting database reset...\n');
        let dropped = 0;
        let skipped = 0;

        // Step 1: Drop all foreign key constraints first
        console.log('🔗 Dropping foreign key constraints...');
        try {
            const fkResult = await client.query(`
                SELECT conname, conrelid::regclass AS table_name
                FROM pg_constraint
                WHERE contype = 'f' AND connamespace = 'public'::regnamespace
            `);
            for (const row of fkResult.rows) {
                try {
                    await client.query(`ALTER TABLE ${row.table_name} DROP CONSTRAINT IF EXISTS ${row.conname} CASCADE`);
                    console.log(`   Dropped FK: ${row.conname}`);
                } catch (e) {
                    // Ignore errors - constraint may already be gone
                }
            }
        } catch (e) {
            console.log(`   ⚠️  Could not query constraints: ${e.message}`);
        }

        // Step 2: Drop views
        console.log('\n👁️  Dropping views...');
        const viewsResult = await client.query(`
            SELECT viewname FROM pg_views WHERE schemaname = 'public'
        `);
        for (const row of viewsResult.rows) {
            try {
                await client.query(`DROP VIEW IF EXISTS ${row.viewname} CASCADE`);
                console.log(`   Dropped view: ${row.viewname}`);
                dropped++;
            } catch (e) {
                console.log(`   Skipped view ${row.viewname}: ${e.message}`);
                skipped++;
            }
        }

        // Step 3: Drop functions
        console.log('\n⚙️  Dropping functions...');
        for (const func of FUNCTIONS_TO_DROP) {
            try {
                await client.query(`DROP FUNCTION IF EXISTS ${func} CASCADE`);
                console.log(`   Dropped function: ${func}`);
            } catch (e) {
                console.log(`   Skipped function ${func}: ${e.message}`);
            }
        }

        // Step 4: Drop all tables (including any not in our list)
        console.log('\n📦 Dropping tables...');

        // First, drop tables from our list in order
        for (const table of TABLES_TO_DROP) {
            try {
                await client.query(`DROP TABLE IF EXISTS ${table} CASCADE`);
                console.log(`   Dropped table: ${table}`);
                dropped++;
            } catch (e) {
                console.log(`   Skipped table ${table}: ${e.message}`);
                skipped++;
            }
        }

        // Then, find and drop any remaining tables
        const remainingTables = await client.query(`
            SELECT tablename FROM pg_tables WHERE schemaname = 'public'
        `);
        for (const row of remainingTables.rows) {
            try {
                await client.query(`DROP TABLE IF EXISTS ${row.tablename} CASCADE`);
                console.log(`   Dropped remaining table: ${row.tablename}`);
                dropped++;
            } catch (e) {
                console.log(`   Skipped remaining table ${row.tablename}: ${e.message}`);
                skipped++;
            }
        }

        // Step 5: Drop sequences
        console.log('\n🔢 Dropping sequences...');
        const seqResult = await client.query(`
            SELECT sequencename FROM pg_sequences WHERE schemaname = 'public'
        `);
        for (const row of seqResult.rows) {
            try {
                await client.query(`DROP SEQUENCE IF EXISTS ${row.sequencename} CASCADE`);
                console.log(`   Dropped sequence: ${row.sequencename}`);
            } catch (e) {
                // Sequences may not exist, that's fine
            }
        }

        // Step 6: Drop types (enums etc)
        console.log('\n📝 Dropping custom types...');
        const typesResult = await client.query(`
            SELECT typname FROM pg_type
            WHERE typnamespace = 'public'::regnamespace
            AND typtype = 'e'
        `);
        for (const row of typesResult.rows) {
            try {
                await client.query(`DROP TYPE IF EXISTS ${row.typname} CASCADE`);
                console.log(`   Dropped type: ${row.typname}`);
            } catch (e) {
                // Types may be in use, ignore
            }
        }

        // Step 7: Drop indexes (should be gone with tables, but just in case)
        console.log('\n📑 Dropping remaining indexes...');
        const indexResult = await client.query(`
            SELECT indexname FROM pg_indexes WHERE schemaname = 'public'
        `);
        for (const row of indexResult.rows) {
            try {
                await client.query(`DROP INDEX IF EXISTS ${row.indexname} CASCADE`);
                console.log(`   Dropped index: ${row.indexname}`);
            } catch (e) {
                // Ignore - likely already dropped with table
            }
        }

        client.release();

        // Step 8: Clear Redis if requested
        if (clearRedis) {
            await clearRedisCache();
        }

        // Step 9: Reinitialize schema if requested
        if (reinit) {
            console.log('\n🔄 Reinitializing database schema...');
            try {
                // Import and run the database initialization
                const { initDB } = require('../src/services/database');
                await initDB();
                console.log('✅ Schema reinitialized successfully');
            } catch (e) {
                console.log(`⚠️  Schema reinit failed: ${e.message}`);
                console.log('   Run "npm start" to initialize schema on next startup');
            }
        }

        await pool.end();

        console.log('\n═══════════════════════════════════════════════════════════════');
        console.log('                    ✅ RESET COMPLETE');
        console.log('═══════════════════════════════════════════════════════════════');
        console.log(`\n   📊 Dropped: ${dropped} objects`);
        console.log(`   ⏭️  Skipped: ${skipped} objects\n`);

        if (!reinit) {
            console.log('💡 The database is now empty.');
            console.log('   Schema will be recreated on next server start.');
            console.log('   Run "npm start" or "npm run dev" to initialize.\n');
        }

    } catch (e) {
        console.error('\n❌ FATAL ERROR:', e.message);
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
    dryRun: args.includes('--dry-run') || args.includes('-d'),
    reinit: args.includes('--reinit') || args.includes('-r'),
    clearRedis: args.includes('--clear-redis') || args.includes('-c')
};

if (args.includes('--help') || args.includes('-h')) {
    console.log(`
🗑️  HolDex Database Reset Script

Usage:
  node scripts/resetDatabase.js [options]
  npm run db:reset [-- options]

Options:
  --force, -f       Skip confirmation prompt (dangerous!)
  --dry-run, -d     Show what would be deleted without making changes
  --reinit, -r      Reinitialize schema after reset
  --clear-redis, -c Also clear Redis cache
  --help, -h        Show this help message

Environment:
  DATABASE_URL      PostgreSQL connection string (required)
  REDIS_URL         Redis connection string (optional, for --clear-redis)

Examples:
  npm run db:reset                    # Interactive reset
  npm run db:reset -- --dry-run       # Preview changes
  npm run db:reset -- --force         # Force reset (no confirmation)
  npm run db:reset -- --force -r -c   # Force reset + reinit + clear Redis

npm scripts:
  npm run db:reset        # Interactive reset
  npm run db:reset:dry    # Preview changes (dry run)
  npm run db:reset:force  # Force reset without confirmation
`);
    process.exit(0);
}

resetDatabase(options);
