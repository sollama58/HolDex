#!/usr/bin/env node
/**
 * Test Integrity Script
 *
 * Tests the 9-category signature verification on a verified token.
 * Usage: DATABASE_URL=... node src/scripts/testIntegrity.js [mint]
 */

require('dotenv').config();
const pg = require('pg');
const { verifyAllSignatures, verifyHolders } = require('../utils/dataSignature');

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
    console.error('DATABASE_URL required');
    process.exit(1);
}

const pool = new pg.Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

const db = {
    get: (q, p) => pool.query(q, p).then(r => r.rows[0]),
    all: (q, p) => pool.query(q, p).then(r => r.rows),
};

async function main() {
    const mintArg = process.argv[2];

    console.log('═══════════════════════════════════════════════════════════════════');
    console.log('                    INTEGRITY TEST - 9 Categories                   ');
    console.log('═══════════════════════════════════════════════════════════════════\n');

    // Get token (specific or first verified)
    let token;
    if (mintArg) {
        token = await db.get(`
            SELECT * FROM tokens WHERE mint = $1
        `, [mintArg]);
        if (!token) {
            console.error(`Token ${mintArg} not found`);
            process.exit(1);
        }
    } else {
        token = await db.get(`
            SELECT * FROM tokens
            WHERE hasCommunityUpdate = TRUE
              AND sig_full IS NOT NULL
            ORDER BY k_score DESC
            LIMIT 1
        `);
        if (!token) {
            console.error('No verified tokens found');
            process.exit(1);
        }
    }

    console.log(`Token: ${token.symbol} (${token.mint.slice(0, 8)}...)`);
    console.log(`K-Score: ${token.k_score}`);
    console.log(`Holders: ${token.holders}`);
    console.log('');

    // Get holder snapshots for sig_holders verification
    const holderSnapshots = await db.all(
        'SELECT holder, balance FROM holder_snapshots WHERE mint = $1 ORDER BY balance DESC LIMIT 20',
        [token.mint]
    );
    console.log(`Holder Snapshots: ${holderSnapshots.length} loaded`);
    console.log('');

    // Verify all signatures
    console.log('─────────────────────────────────────────────────────────────────────');
    console.log('SIGNATURE VERIFICATION');
    console.log('─────────────────────────────────────────────────────────────────────\n');

    const result = verifyAllSignatures(token, { holderSnapshots });

    // Display results
    const categories = ['identity', 'security', 'lp', 'supply', 'kscore', 'market', 'origin', 'holders', 'full'];

    for (const cat of categories) {
        const status = result.details[cat] || 'not_checked';
        const sigField = `sig_${cat}`;
        const hasSig = token[sigField] ? 'present' : 'missing';

        let icon;
        if (status === 'verified') icon = '✅';
        else if (status === 'signature_mismatch') icon = '❌';
        else if (status === 'missing_signature' || status === 'unsigned') icon = '⚠️';
        else if (status === 'no_snapshots') icon = '⚠️';
        else icon = '❓';

        console.log(`  ${cat.padEnd(12)} ${icon} ${status.padEnd(20)} (sig: ${hasSig})`);
    }

    console.log('');
    console.log('─────────────────────────────────────────────────────────────────────');
    console.log('SUMMARY');
    console.log('─────────────────────────────────────────────────────────────────────\n');

    console.log(`  Valid: ${result.valid ? '✅ YES' : '❌ NO'}`);
    console.log(`  Tampered: ${result.tampered.length > 0 ? '❌ ' + result.tampered.join(', ') : '✅ None'}`);
    console.log(`  Unsigned: ${result.unsigned.length > 0 ? '⚠️ ' + result.unsigned.join(', ') : '✅ None'}`);
    console.log(`  Chaos Verified: ${result.chaosVerified ? '✅ YES' : '❌ NO'}`);

    // Test sig_holders specifically
    if (holderSnapshots.length > 0 && token.sig_holders) {
        console.log('');
        console.log('─────────────────────────────────────────────────────────────────────');
        console.log('SIG_HOLDERS DETAIL');
        console.log('─────────────────────────────────────────────────────────────────────\n');

        const holdersResult = verifyHolders(token.mint, token.sig_holders, holderSnapshots);
        console.log(`  Result: ${holdersResult.valid ? '✅ Valid' : '❌ Invalid'}`);
        console.log(`  Reason: ${holdersResult.reason}`);
        console.log(`  Top 3 holders:`);
        for (let i = 0; i < Math.min(3, holderSnapshots.length); i++) {
            const h = holderSnapshots[i];
            console.log(`    ${i+1}. ${h.holder.slice(0,8)}... = ${BigInt(h.balance).toLocaleString()}`);
        }
    }

    console.log('\n═══════════════════════════════════════════════════════════════════\n');

    await pool.end();
}

main().catch(e => {
    console.error('Error:', e.message);
    pool.end();
    process.exit(1);
});
