/**
 * Create API Key Script
 *
 * Usage:
 *   DATABASE_URL=... node src/scripts/create_api_key.js <owner> [tier]
 *
 * Example:
 *   DATABASE_URL=postgres://... node src/scripts/create_api_key.js GASdf pro
 */

const { Pool } = require('pg');
const crypto = require('crypto');

const DATABASE_URL = process.env.DATABASE_URL;
const owner = process.argv[2];
const tier = process.argv[3] || 'free';

if (!DATABASE_URL) {
    console.error('❌ DATABASE_URL required');
    console.error('Usage: DATABASE_URL=... node src/scripts/create_api_key.js <owner> [tier]');
    process.exit(1);
}

if (!owner) {
    console.error('❌ Owner name required');
    console.error('Usage: DATABASE_URL=... node src/scripts/create_api_key.js <owner> [tier]');
    process.exit(1);
}

const pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

function hashApiKey(key) {
    return crypto.createHash('sha256').update(key).digest('hex');
}

async function createKey() {
    const client = await pool.connect();

    try {
        // Generate API key
        const key = 'hx_' + crypto.randomBytes(16).toString('hex');
        const keyHash = hashApiKey(key);
        const keyPrefix = key.substring(0, 7);

        // Determine request limit based on tier
        const limits = {
            'free': 1000,
            'pro': 100000,
            'enterprise': 1000000
        };
        const requestsLimit = limits[tier] || 1000;

        // Insert into database (supports both old and new schema)
        await client.query(`
            INSERT INTO api_keys (key, key_hash, key_prefix, owner, tier, requests_limit, is_active, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, TRUE, $7)
        `, [key, keyHash, keyPrefix, owner, tier, requestsLimit, Date.now()]);

        console.log('\n✅ API Key created successfully!\n');
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        console.log(`  Owner:    ${owner}`);
        console.log(`  Tier:     ${tier}`);
        console.log(`  Limit:    ${requestsLimit.toLocaleString()} requests/day`);
        console.log(`  Prefix:   ${keyPrefix}...`);
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        console.log(`\n  🔑 API KEY: ${key}\n`);
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        console.log('\n⚠️  SAVE THIS KEY! It will NOT be shown again.\n');

    } catch (e) {
        console.error('❌ Failed to create key:', e.message);
        throw e;
    } finally {
        client.release();
        await pool.end();
    }
}

createKey().catch(e => {
    console.error(e);
    process.exit(1);
});
