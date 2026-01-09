/**
 * Fix Placeholder Tokens Script
 *
 * This script finds all tokens with placeholder names ('Unknown', 'New Discovery', etc.)
 * and attempts to fetch real metadata for them.
 *
 * Usage:
 *   node scripts/fixPlaceholderTokens.js
 */

require('dotenv').config();
const { Pool } = require('pg');
const axios = require('axios');

const DATABASE_URL = process.env.DATABASE_URL;
const SOLSCAN_API_KEY = process.env.SOLSCAN_API_KEY || null;

if (!DATABASE_URL) {
    console.error('DATABASE_URL environment variable is required');
    process.exit(1);
}

async function fetchMetadata(mintAddress) {
    // 1. GeckoTerminal
    try {
        const geckoRes = await axios.get(
            `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mintAddress}`,
            { timeout: 5000 }
        );
        if (geckoRes.data?.data?.attributes) {
            const attrs = geckoRes.data.data.attributes;
            if (attrs.name && attrs.name !== 'Unknown' && attrs.symbol && attrs.symbol !== 'UNK') {
                return {
                    name: attrs.name,
                    symbol: attrs.symbol,
                    image: attrs.image_url || null,
                    source: 'GeckoTerminal'
                };
            }
        }
    } catch (_e) { }

    // 2. Solscan Pro API
    if (SOLSCAN_API_KEY) {
        try {
            const solscanRes = await axios.get(
                `https://pro-api.solscan.io/v2.0/token/meta?address=${mintAddress}`,
                { headers: { 'token': SOLSCAN_API_KEY }, timeout: 5000 }
            );
            if (solscanRes.data?.success && solscanRes.data?.data) {
                const d = solscanRes.data.data;
                const name = d.name || d.tokenName;
                const symbol = d.symbol || d.tokenSymbol;
                if (name && name !== 'Unknown' && symbol && symbol !== 'UNK') {
                    return {
                        name,
                        symbol,
                        image: d.icon || d.image || null,
                        source: 'Solscan Pro'
                    };
                }
            }
        } catch (_e) { }
    }

    // 3. Solscan Public API
    try {
        const solscanPublicRes = await axios.get(
            `https://api.solscan.io/token/meta?token=${mintAddress}`,
            { timeout: 5000 }
        );
        if (solscanPublicRes.data && solscanPublicRes.data.success !== false) {
            const d = solscanPublicRes.data;
            const name = d.name || d.tokenName;
            const symbol = d.symbol || d.tokenSymbol;
            if (name && name !== 'Unknown' && symbol && symbol !== 'UNK') {
                return {
                    name,
                    symbol,
                    image: d.icon || d.image || null,
                    source: 'Solscan Public'
                };
            }
        }
    } catch (_e) { }

    return null;
}

async function main() {
    const pool = new Pool({
        connectionString: DATABASE_URL,
        ssl: DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false }
    });

    try {
        const client = await pool.connect();

        try {
            // Find all placeholder tokens
            const placeholderTokens = await client.query(`
                SELECT mint, name, symbol
                FROM tokens
                WHERE name IN ('Unknown', 'New Discovery', '')
                   OR name IS NULL
                   OR symbol IN ('UNKNOWN', 'UNK', 'NEW', '')
                   OR symbol IS NULL
                ORDER BY updated_at DESC
                LIMIT 100
            `);

            console.log(`Found ${placeholderTokens.rows.length} tokens with placeholder data\n`);

            if (placeholderTokens.rows.length === 0) {
                console.log('No placeholder tokens found! All tokens have valid metadata.');
                return;
            }

            let fixed = 0;
            let failed = 0;

            for (const token of placeholderTokens.rows) {
                console.log(`Processing ${token.mint.slice(0, 8)}... (current: ${token.name || 'null'} / ${token.symbol || 'null'})`);

                // Rate limit
                await new Promise(r => setTimeout(r, 500));

                const meta = await fetchMetadata(token.mint);

                if (meta) {
                    console.log(`  ✅ Found: ${meta.name} (${meta.symbol}) via ${meta.source}`);

                    await client.query(`
                        UPDATE tokens
                        SET name = $1, symbol = $2, image = COALESCE($3, image), updated_at = NOW()
                        WHERE mint = $4
                    `, [meta.name, meta.symbol, meta.image, token.mint]);

                    fixed++;
                } else {
                    console.log(`  ❌ No metadata found - token may not exist on-chain`);
                    failed++;
                }
            }

            console.log(`\n========================================`);
            console.log(`Fixed: ${fixed} tokens`);
            console.log(`Failed: ${failed} tokens`);
            console.log(`========================================`);

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
