require('dotenv').config();
const axios = require('axios');
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
    '7gc99ve50tN9f2382948243984928394829384923849', // Placeholder
];

async function seed() {
    console.log("🌱 Seeding Database with Top Tokens...");
    
    // We can use the existing API logic by just calling the public DexScreener API
    // and manually inserting into the DB, mimicking what the API does.
    
    for (const mint of SEED_MINTS) {
        try {
            console.log(`Fetching ${mint}...`);
            const res = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${mint}`);
            const pairs = res.data.pairs;
            
            if (!pairs || pairs.length === 0) continue;

            const best = pairs[0];
            
            // 1. Insert Token
            await pool.query(`
                INSERT INTO tokens (mint, name, symbol, image, marketCap, priceUsd, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT(mint) DO NOTHING
            `, [
                mint, best.baseToken.name, best.baseToken.symbol, best.info?.imageUrl,
                Number(best.fdv), Number(best.priceUsd), Date.now()
            ]);

            // 2. Insert Pool
            await pool.query(`
                INSERT INTO pools (address, mint, dex, token_a, token_b, liquidity_usd, volume_24h, price_usd, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT(address) DO NOTHING
            `, [
                best.pairAddress, mint, best.dexId, 
                best.baseToken.address, best.quoteToken.address,
                Number(best.liquidity?.usd), Number(best.volume?.h24), Number(best.priceUsd), Date.now()
            ]);

            // 3. Track
            await pool.query(`
                INSERT INTO active_trackers (pool_address, last_check) 
                VALUES ($1, $2) ON CONFLICT(pool_address) DO NOTHING
            `, [best.pairAddress, Date.now()]);

            console.log(`✅ Seeded ${best.baseToken.symbol}`);

        } catch (e) {
            console.log(`Skipping ${mint}: ${e.message}`);
        }
    }
    
    console.log("🎉 Seeding Complete. Restart Server.");
    process.exit();
}

seed();
