/**
 * Migration Script: Hash existing API keys
 *
 * This script migrates plaintext API keys to hashed format.
 * Run ONCE after deploying the new schema.
 *
 * Usage:
 *   DATABASE_URL=... node src/scripts/migrate_api_keys.js
 */

const { Pool } = require('pg');
const crypto = require('crypto');

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
    console.error('❌ DATABASE_URL required');
    process.exit(1);
}

const pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

function hashApiKey(key) {
    return crypto.createHash('sha256').update(key).digest('hex');
}

async function migrate() {
    const client = await pool.connect();

    try {
        console.log('🔄 Starting API key migration...\n');

        // Check if old schema exists (has 'key' column)
        const schemaCheck = await client.query(`
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'api_keys' AND column_name = 'key'
        `);

        if (schemaCheck.rows.length === 0) {
            console.log('✅ Already migrated (no "key" column found)');
            return;
        }

        // Check if new columns exist
        const newColCheck = await client.query(`
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'api_keys' AND column_name = 'key_hash'
        `);

        // Add new columns if they don't exist
        if (newColCheck.rows.length === 0) {
            console.log('📝 Adding new columns (key_hash, key_prefix)...');
            await client.query(`
                ALTER TABLE api_keys
                ADD COLUMN IF NOT EXISTS key_hash TEXT,
                ADD COLUMN IF NOT EXISTS key_prefix TEXT
            `);
        }

        // Get all existing keys
        const { rows: keys } = await client.query('SELECT key, owner FROM api_keys WHERE key IS NOT NULL');
        console.log(`📊 Found ${keys.length} keys to migrate\n`);

        if (keys.length === 0) {
            console.log('✅ No keys to migrate');
            return;
        }

        // Migrate each key
        let migrated = 0;
        let errors = 0;

        for (const row of keys) {
            try {
                const keyHash = hashApiKey(row.key);
                const keyPrefix = row.key.substring(0, 7);

                await client.query(`
                    UPDATE api_keys
                    SET key_hash = $1, key_prefix = $2
                    WHERE key = $3
                `, [keyHash, keyPrefix, row.key]);

                console.log(`  ✓ ${row.owner}: ${keyPrefix}... → ${keyHash.substring(0, 8)}...`);
                migrated++;
            } catch (e) {
                console.error(`  ✗ ${row.owner}: ${e.message}`);
                errors++;
            }
        }

        console.log(`\n📊 Migration complete: ${migrated} migrated, ${errors} errors`);

        // After successful migration, we can drop the old 'key' column
        // and set key_hash as primary key
        if (errors === 0 && migrated > 0) {
            console.log('\n🔧 Finalizing schema...');

            // Drop old primary key constraint if exists
            await client.query(`
                ALTER TABLE api_keys DROP CONSTRAINT IF EXISTS api_keys_pkey
            `).catch(() => {});

            // Drop old key column
            await client.query(`
                ALTER TABLE api_keys DROP COLUMN IF EXISTS key
            `);

            // Add new primary key
            await client.query(`
                ALTER TABLE api_keys ADD PRIMARY KEY (key_hash)
            `).catch(() => {
                console.log('  ⚠️  Primary key already exists or cannot be added');
            });

            console.log('✅ Schema finalized');
        }

        console.log('\n🎉 Migration complete!');
        console.log('\n⚠️  IMPORTANT: Users will need NEW API keys.');
        console.log('   Old plaintext keys no longer exist in the database.');

    } catch (e) {
        console.error('❌ Migration failed:', e.message);
        throw e;
    } finally {
        client.release();
        await pool.end();
    }
}

migrate().catch(e => {
    console.error(e);
    process.exit(1);
});
