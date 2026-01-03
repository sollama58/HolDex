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
 * Fetch holder data from on-chain
 * Returns { totalHolders, realHolders } where realHolders = holders with $1+ value
 */
async function fetchHolderData(mint, priceUsd = 0, decimals = 6) {
    let allHolders = [];
    let cursor = null;
    let pageCount = 0;
    const MAX_PAGES = 10;
    const MIN_USD_VALUE = 1; // $1 minimum

    while (pageCount < MAX_PAGES) {
        const params = { mint, limit: 1000 };
        if (cursor) params.cursor = cursor;

        const result = await heliusRpc('getTokenAccounts', params);
        if (!result?.token_accounts) break;

        for (const acc of result.token_accounts) {
            if (acc.amount > 0) {
                allHolders.push({ address: acc.owner, balance: acc.amount });
            }
        }

        cursor = result.cursor;
        if (!cursor || result.token_accounts.length < 1000) break;
        pageCount++;

        // Rate limit
        await sleep(200);
    }

    const totalHolders = allHolders.length;

    // Calculate real holders ($1+ value)
    let realHolders = 0;
    if (priceUsd > 0) {
        const divisor = Math.pow(10, decimals);
        for (const h of allHolders) {
            const usdValue = (h.balance / divisor) * priceUsd;
            if (usdValue >= MIN_USD_VALUE) {
                realHolders++;
            }
        }
    } else {
        // Fallback: all holders are "real"
        realHolders = totalHolders;
    }

    return { totalHolders, realHolders };
}

async function main() {
    console.log('═══════════════════════════════════════════════════════════════════');
    console.log('             FORCE DEEP REFRESH - On-Chain Truth                   ');
    console.log('═══════════════════════════════════════════════════════════════════\n');

    // Get all verified tokens with price for $1+ calculation
    const tokens = await db.all(`
        SELECT mint, symbol, holders, real_holders, total_holders, priceusd, k_score
        FROM tokens
        WHERE hascommunityupdate = TRUE
        ORDER BY symbol
    `);

    console.log(`Found ${tokens.length} verified tokens to refresh\n`);

    for (const token of tokens) {
        const currentReal = token.real_holders || token.holders || 0;
        const currentTotal = token.total_holders || token.holders || 0;
        console.log(`\n[${token.symbol}] Current: real=${currentReal}, total=${currentTotal}`);

        try {
            // Fetch holder data from on-chain with price for $1+ calculation
            const priceUsd = parseFloat(token.priceusd) || 0;
            const { totalHolders, realHolders } = await fetchHolderData(token.mint, priceUsd, 6);

            console.log(`[${token.symbol}] On-chain: real=${realHolders} ($1+), total=${totalHolders}`);

            // Update the database with both values
            await db.run(`
                UPDATE tokens
                SET holders = $1,
                    real_holders = $2,
                    total_holders = $3
                WHERE mint = $4
            `, [realHolders, realHolders, totalHolders, token.mint]);

            console.log(`[${token.symbol}] ✅ UPDATED: real=${realHolders}, total=${totalHolders}`);

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
        SELECT symbol, real_holders, total_holders, k_score, conviction_score
        FROM tokens
        WHERE hascommunityupdate = TRUE
        ORDER BY k_score DESC
    `);

    console.log('Updated Token Data:');
    console.log('Symbol'.padEnd(12) + 'Real($1+)'.padEnd(12) + 'Total'.padEnd(10) + 'K-Score'.padEnd(10) + 'Conv%');
    console.log('-'.repeat(55));
    for (const t of updated) {
        console.log(
            t.symbol.padEnd(12) +
            String(t.real_holders || 0).padEnd(12) +
            String(t.total_holders || 0).padEnd(10) +
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
