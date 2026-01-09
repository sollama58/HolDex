// Usage: node src/indexer/scripts/backfill.js <mint_address>
require('dotenv').config();
const { initDB, getDB } = require('../../services/database');

const MINT = process.argv[2];

if (!MINT) {
    console.log("Please provide a mint address.");
    process.exit(1);
}

/**
 * Fetch pool info from Raydium API (no DexScreener dependency)
 */
async function getRaydiumPoolInfo(mint) {
    try {
        const response = await fetch(
            `https://api-v3.raydium.io/pools/info/mint?mint1=${mint}&poolType=all&poolSortField=liquidity&sortType=desc&pageSize=5&page=1`,
            { timeout: 10000 }
        );

        if (!response.ok) return null;

        const data = await response.json();
        if (!data.success || !data.data?.data?.length) return null;

        return data.data.data[0]; // Return best pool (highest liquidity)

    } catch (e) {
        console.log(`Raydium API error: ${e.message}`);
        return null;
    }
}

/**
 * Fetch token metadata from Helius DAS API
 */
async function getTokenMetadata(mint) {
    const config = require('../../config/env');
    if (!config.HELIUS_API_KEY) return null;

    try {
        const response = await fetch('https://mainnet.helius-rpc.com', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${config.HELIUS_API_KEY}`
            },
            body: JSON.stringify({
                jsonrpc: '2.0',
                id: 'metadata',
                method: 'getAsset',
                params: { id: mint }
            })
        });

        const data = await response.json();
        if (data.error || !data.result) return null;

        return data.result;

    } catch (e) {
        console.log(`Helius API error: ${e.message}`);
        return null;
    }
}

async function backfill() {
    await initDB();
    const db = getDB();

    console.log(`Backfilling data for ${MINT}...`);

    try {
        // 1. Fetch pool info from Raydium
        console.log('Fetching pool info from Raydium...');
        const pool = await getRaydiumPoolInfo(MINT);

        if (!pool) {
            console.log("No pool found on Raydium. Trying Helius for token metadata only...");
        }

        // 2. Get token metadata from Helius
        console.log('Fetching token metadata from Helius...');
        const asset = await getTokenMetadata(MINT);

        if (!pool && !asset) {
            console.log("Token not found in any data source.");
            process.exit(1);
        }

        // Extract info
        const poolAddress = pool?.id || null;
        const dex = pool?.type || 'unknown';

        console.log(`Found Pool: ${poolAddress || 'None'} on ${dex}`);

        // 3. Insert Pool if found
        if (pool && poolAddress) {
            const baseToken = pool.mintA?.address || MINT;
            const quoteToken = pool.mintB?.address || 'So11111111111111111111111111111111111111112';

            await db.run(`
                INSERT INTO pools (address, mint, dex, token_a, token_b, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT(mint, dex) DO NOTHING
            `, [poolAddress, MINT, dex, baseToken, quoteToken, Date.now()]);

            // 4. Enable Tracking
            await db.run(`
                INSERT INTO active_trackers (pool_address, last_check)
                VALUES ($1, $2) ON CONFLICT DO NOTHING
            `, [poolAddress, Date.now()]);

            console.log("Pool indexed and tracking enabled.");
        }

        // 5. Insert/Update Token metadata if we have Helius data
        if (asset) {
            const content = asset.content || {};
            const metadata = content.metadata || {};
            const links = content.links || {};

            const name = metadata.name || asset.name;
            const symbol = metadata.symbol || asset.symbol;
            const image = content.files?.[0]?.uri || links.image || null;

            // Validate metadata - reject placeholder values
            const invalidNames = ['Unknown', 'New Discovery', '', null, undefined];
            const invalidSymbols = ['UNK', 'UNKNOWN', 'NEW', '', null, undefined];
            const hasRealName = name && !invalidNames.includes(name) && !name.startsWith('Token ');
            const hasRealSymbol = symbol && !invalidSymbols.includes(symbol);

            if (!hasRealName || !hasRealSymbol) {
                console.log(`❌ Cannot backfill ${MINT.slice(0, 8)} - no valid metadata (name: ${name}, symbol: ${symbol})`);
                console.log("   Token metadata must be available on-chain before backfilling.");
                process.exit(1);
            }

            await db.run(`
                INSERT INTO tokens (mint, name, symbol, image, timestamp)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT(mint) DO UPDATE SET
                    name = CASE WHEN tokens.name IN ('Unknown', 'New Discovery', '') OR tokens.name IS NULL THEN EXCLUDED.name ELSE tokens.name END,
                    symbol = CASE WHEN tokens.symbol IN ('UNKNOWN', 'UNK', 'NEW', '') OR tokens.symbol IS NULL THEN EXCLUDED.symbol ELSE tokens.symbol END,
                    image = CASE WHEN tokens.image IS NULL OR tokens.image = '' THEN EXCLUDED.image ELSE tokens.image END
            `, [MINT, name, symbol, image, Date.now()]);

            console.log(`✅ Token metadata updated: ${symbol} (${name})`);
        }

        console.log("Backfill Complete.");
        console.log("   (Note: Historical candles are not backfilled, but tracking starts NOW.)");

    } catch (err) {
        console.error("Backfill failed:", err.message);
    }
    process.exit();
}

backfill();
