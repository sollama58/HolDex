require('dotenv').config();
const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

// A list of popular tokens to "bootstrap" the database
const SEED_MINTS = [
    'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263', // BONK
    'JUPyiwrYJFskUPiHa7hkeR8VUtkqj82hWEzckhIZK3p', // JUP
    '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R', // RAY
    'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm', // WIF
];

/**
 * Fetch token metadata from Helius DAS API
 */
async function getTokenMetadata(mint) {
    if (!config.HELIUS_API_KEY) {
        console.log('Warning: HELIUS_API_KEY not set, skipping metadata fetch');
        return null;
    }

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

/**
 * Fetch pool info from Raydium API
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

        return data.data.data[0];

    } catch (e) {
        console.log(`Raydium API error: ${e.message}`);
        return null;
    }
}

/**
 * Fetch price from Jupiter API
 */
async function getJupiterPrice(mint) {
    try {
        const response = await fetch(
            `https://lite-api.jup.ag/price/v2?ids=${mint}`,
            { timeout: 10000 }
        );

        if (!response.ok) return null;

        const data = await response.json();
        const priceData = data?.data?.[mint] || data?.[mint];

        return priceData ? parseFloat(priceData.price) : null;

    } catch (e) {
        console.log(`Jupiter API error: ${e.message}`);
        return null;
    }
}

async function seed() {
    console.log("Seeding Database with Top Tokens (Jupiter + Raydium + Helius)...");

    for (const mint of SEED_MINTS) {
        try {
            console.log(`\nProcessing ${mint}...`);

            // 1. Get token metadata from Helius
            const asset = await getTokenMetadata(mint);
            const content = asset?.content || {};
            const metadata = content.metadata || {};
            const links = content.links || {};

            // 2. Get pool info from Raydium
            const raydiumPool = await getRaydiumPoolInfo(mint);

            // 3. Get price from Jupiter
            const jupiterPrice = await getJupiterPrice(mint);

            // Extract data
            const name = metadata.name || asset?.name || 'Unknown';
            const symbol = metadata.symbol || asset?.symbol || 'UNKNOWN';
            const image = content.files?.[0]?.uri || links.image || null;
            const priceUsd = jupiterPrice || raydiumPool?.price || 0;
            const liquidity = raydiumPool?.tvl || 0;
            const volume24h = raydiumPool?.day?.volume || 0;

            // Calculate rough market cap (FDV)
            // This is approximate - real mcap requires supply data
            const fdv = raydiumPool?.tvl ? raydiumPool.tvl * 2 : 0;

            // 4. Insert Token
            await pool.query(`
                INSERT INTO tokens (mint, name, symbol, image, marketCap, priceUsd, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT(mint) DO UPDATE SET
                    name = COALESCE(EXCLUDED.name, tokens.name),
                    symbol = COALESCE(EXCLUDED.symbol, tokens.symbol),
                    image = COALESCE(EXCLUDED.image, tokens.image),
                    marketCap = EXCLUDED.marketCap,
                    priceUsd = EXCLUDED.priceUsd
            `, [
                mint, name, symbol, image,
                fdv, priceUsd, Date.now()
            ]);

            // 5. Insert Pool if found
            if (raydiumPool) {
                const poolAddress = raydiumPool.id;
                const dex = raydiumPool.type || 'raydium';
                const baseToken = raydiumPool.mintA?.address || mint;
                const quoteToken = raydiumPool.mintB?.address || 'So11111111111111111111111111111111111111112';

                await pool.query(`
                    INSERT INTO pools (address, mint, dex, token_a, token_b, liquidity_usd, volume_24h, price_usd, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT(address) DO NOTHING
                `, [
                    poolAddress, mint, dex,
                    baseToken, quoteToken,
                    liquidity, volume24h, priceUsd, Date.now()
                ]);

                // 6. Track
                await pool.query(`
                    INSERT INTO active_trackers (pool_address, last_check)
                    VALUES ($1, $2) ON CONFLICT(pool_address) DO NOTHING
                `, [poolAddress, Date.now()]);
            }

            console.log(`Seeded ${symbol} - $${priceUsd?.toFixed(6) || '0'}`);

        } catch (e) {
            console.log(`Skipping ${mint}: ${e.message}`);
        }
    }

    console.log("\nSeeding Complete. Restart Server.");
    process.exit();
}

seed();
