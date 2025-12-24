/**
 * K-Score Updater (Refactored for On-Demand)
 * Exports 'calculateTokenScore' for API usage.
 */
const axios = require('axios');
const config = require('../config/env');
const { logger } = require('../services');
const { getDB } = require('../services/database'); // Use getter to avoid circular dep issues if any

const SIX_HOURS_MS = 6 * 60 * 60 * 1000;
const TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';

// Helper: Parse u64
function readBigUInt64LE(buffer, offset = 0) {
    const first = buffer[offset];
    const last = buffer[offset + 7];
    if (first === undefined || last === undefined) return BigInt(0);
    return buffer.readBigUInt64LE(offset);
}

// --- CORE CALCULATION LOGIC (Single Token) ---
async function calculateTokenScore(mint) {
    const db = getDB();
    if (!config.HELIUS_API_KEY) throw new Error("HELIUS_API_KEY missing");

    const HELIUS_URL = `https://mainnet.helius-rpc.com/?api-key=${config.HELIUS_API_KEY}`;
    
    // 1. Get Token Metadata (DB)
    const token = await db.get('SELECT mint, marketCap, volume24h FROM tokens WHERE mint = $1', [mint]);
    if (!token) throw new Error("Token not found in DB");

    // 2. Fetch Holders (Helius)
    const response = await axios.post(HELIUS_URL, {
        jsonrpc: '2.0',
        id: 'k-score-single',
        method: 'getProgramAccounts',
        params: [
            TOKEN_PROGRAM_ID,
            {
                filters: [
                    { dataSize: 165 }, 
                    { memcmp: { offset: 0, bytes: mint } }
                ],
                encoding: 'base64',
                dataSlice: { offset: 64, length: 8 } 
            }
        ]
    }, { maxBodyLength: Infinity, maxContentLength: Infinity });

    const accounts = response.data?.result || [];
    
    // 3. Sort Top 50
    const holders = accounts
        .map(a => {
            // FIX: Safer data access using optional chaining
            const rawData = a.account.data?.[0];
            if (!rawData) return { amount: BigInt(0) };

            const buf = Buffer.from(rawData, 'base64');
            return { amount: readBigUInt64LE(buf) }; 
        })
        .sort((a, b) => (b.amount > a.amount ? 1 : -1))
        .slice(0, 50);

    if (holders.length === 0) return 0;

    // 4. Calculate
    const velocity = (token.volume24h || 1) / (token.marketCap || 1);
    
    const holdTimeScore = Math.min(1 / (velocity + 0.05), 40);
    const holderCountScore = Math.min(holders.length, 50); 
    const mcapScore = Math.min(Math.log10(token.marketCap || 1000) * 5, 30);

    let score = Math.floor(holdTimeScore + holderCountScore + mcapScore);
    score = Math.min(score, 99);
    score = Math.max(score, 1);

    // 5. Update DB
    await db.run(`UPDATE tokens SET k_score = $1, last_k_calc = $2 WHERE mint = $3`, [score, Date.now(), mint]);
    
    return score;
}

// --- BATCH CYCLE ---
async function updateKScores(deps) {
    const { db } = deps;
    if (!config.HELIUS_API_KEY) return;

    logger.info("🧠 K-Score: Starting Batch Cycle...");

    try {
        const now = Date.now();
        const cutoff = now - SIX_HOURS_MS;

        const tokens = await db.all(`
            SELECT mint FROM tokens 
            WHERE hasCommunityUpdate = TRUE 
            AND (last_k_calc < $1 OR last_k_calc IS NULL)
            ORDER BY marketCap DESC
            LIMIT 20
        `, [cutoff]);

        for (const t of tokens) {
            try {
                await calculateTokenScore(t.mint);
                await new Promise(r => setTimeout(r, 200)); 
            } catch (err) {
                logger.warn(`K-Score batch failed for ${t.mint}: ${err.message}`);
            }
        }
    } catch (e) {
        logger.error("K-Score Cycle Error", { error: e.message });
    }
}

function start(deps) {
    setTimeout(() => updateKScores(deps), 60000); 
    setInterval(() => updateKScores(deps), 5 * 60 * 1000); 
}

module.exports = { start, calculateTokenScore };
