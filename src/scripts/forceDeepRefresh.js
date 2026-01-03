#!/usr/bin/env node
/**
 * Force Deep Refresh Script
 *
 * Fixes corrupted holder counts by forcing full RPC analysis
 * bypassing cached data.
 *
 * Usage: DATABASE_URL=... HELIUS_API_KEY=... node src/scripts/forceDeepRefresh.js
 */

require('dotenv').config();
const pg = require('pg');

const HELIUS_API_KEY = process.env.HELIUS_API_KEY;
const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
    console.error('DATABASE_URL required');
    process.exit(1);
}

if (!HELIUS_API_KEY) {
    console.error('HELIUS_API_KEY required');
    process.exit(1);
}

const pool = new pg.Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

const db = {
    get: (q, p) => pool.query(q, p).then(r => r.rows[0]),
    all: (q, p) => pool.query(q, p).then(r => r.rows),
    run: (q, p) => pool.query(q, p)
};

// Rate limiting
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function heliusRpc(method, params) {
    const response = await fetch(`https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            jsonrpc: '2.0',
            id: 1,
            method,
            params
        })
    });
    const data = await response.json();
    return data.result;
}

/**
 * Fetch real holder count from on-chain
 */
async function fetchRealHolderCount(mint) {
    let holders = [];
    let cursor = null;
    let pageCount = 0;
    const MAX_PAGES = 10;

    while (pageCount < MAX_PAGES) {
        const params = { mint, limit: 1000 };
        if (cursor) params.cursor = cursor;

        const result = await heliusRpc('getTokenAccounts', params);
        if (!result?.token_accounts) break;

        for (const acc of result.token_accounts) {
            if (acc.amount > 0) {
                holders.push({ address: acc.owner, balance: acc.amount });
            }
        }

        cursor = result.cursor;
        if (!cursor || result.token_accounts.length < 1000) break;
        pageCount++;

        // Rate limit
        await sleep(200);
    }

    return holders.length;
}

async function main() {
    console.log('═══════════════════════════════════════════════════════════════════');
    console.log('             FORCE DEEP REFRESH - Fix Corrupted Data               ');
    console.log('═══════════════════════════════════════════════════════════════════\n');

    // Get all verified tokens
    const tokens = await db.all(`
        SELECT mint, symbol, holders, k_score
        FROM tokens
        WHERE hascommunityupdate = TRUE
        ORDER BY symbol
    `);

    console.log(`Found ${tokens.length} verified tokens to refresh\n`);

    for (const token of tokens) {
        console.log(`\n[${token.symbol}] Current holders: ${token.holders}`);

        try {
            // Fetch real holder count from on-chain
            const realHolderCount = await fetchRealHolderCount(token.mint);

            console.log(`[${token.symbol}] On-chain holders: ${realHolderCount}`);

            if (realHolderCount !== token.holders) {
                // Update the database
                await db.run(`
                    UPDATE tokens
                    SET holders = $1
                    WHERE mint = $2
                `, [realHolderCount, token.mint]);

                console.log(`[${token.symbol}] ✅ FIXED: ${token.holders} → ${realHolderCount}`);
            } else {
                console.log(`[${token.symbol}] ✓ Already correct`);
            }

            // Rate limit between tokens
            await sleep(500);

        } catch (error) {
            console.error(`[${token.symbol}] ❌ Error: ${error.message}`);
        }
    }

    console.log('\n═══════════════════════════════════════════════════════════════════');
    console.log('                         REFRESH COMPLETE                           ');
    console.log('═══════════════════════════════════════════════════════════════════\n');

    // Show updated data
    const updated = await db.all(`
        SELECT symbol, holders, k_score, conviction_score
        FROM tokens
        WHERE hascommunityupdate = TRUE
        ORDER BY k_score DESC
    `);

    console.log('Updated Token Data:');
    console.log('Symbol'.padEnd(15) + 'Holders'.padEnd(10) + 'K-Score'.padEnd(10) + 'Conviction');
    console.log('-'.repeat(45));
    for (const t of updated) {
        console.log(
            t.symbol.padEnd(15) +
            String(t.holders).padEnd(10) +
            String(t.k_score).padEnd(10) +
            String(t.conviction_score)
        );
    }

    pool.end();
}

main().catch(e => {
    console.error('Fatal error:', e);
    pool.end();
    process.exit(1);
});
