// Usage: node src/indexer/scripts/backfill.js <mint_address>
require('dotenv').config();
const axios = require('axios');
const { initDB, getDB } = require('../../services/database');

const MINT = process.argv[2];

if (!MINT) {
    console.log("Please provide a mint address.");
    process.exit(1);
}

async function backfill() {
    await initDB();
    const db = getDB();

    console.log(`⏳ Backfilling data for ${MINT}...`);

    try {
        // 1. Fetch from DexScreener (Temporary Dependency for Backfill)
        const response = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${MINT}`);
        const pairs = response.data.pairs;

        if (!pairs || pairs.length === 0) {
            console.log("No pairs found.");
            return;
        }

        const bestPair = pairs[0];
        const poolAddress = bestPair.pairAddress;
        
        console.log(`Found Pool: ${poolAddress} on ${bestPair.dexId}`);

        // 2. Insert Pool
        await db.run(`
            INSERT INTO pools (address, mint, dex, token_a, token_b, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT(mint, dex) DO NOTHING
        `, [poolAddress, MINT, bestPair.dexId, bestPair.baseToken.address, bestPair.quoteToken.address, Date.now()]);

        // 3. Enable Tracking
        await db.run(`INSERT INTO active_trackers (pool_address, last_check) VALUES ($1, $2) ON CONFLICT DO NOTHING`, [poolAddress, Date.now()]);

        console.log("✅ Backfill Complete: Pool indexed and tracking enabled.");
        console.log("   (Note: Historical candles are not backfilled, but tracking starts NOW.)");

    } catch (err) {
        console.error("Backfill failed:", err.message);
    }
    process.exit();
}

backfill();
