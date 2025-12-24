/**
 * K-Score Updater (Community Tokens Only)
 * Uses Helius getProgramAccounts with BASE64 encoding (lightweight).
 * Parses raw data buffers locally to extract balances.
 * * Only processes tokens with 'hasCommunityUpdate = TRUE'.
 */
const axios = require('axios');
const config = require('../config/env');
const { logger } = require('../services');

const SIX_HOURS_MS = 6 * 60 * 60 * 1000;
const TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';

// Helper: Parse u64 from Buffer (Little Endian)
function readBigUInt64LE(buffer, offset = 0) {
    const first = buffer[offset];
    const last = buffer[offset + 7];
    if (first === undefined || last === undefined) return BigInt(0);
    
    // For SPL Token Amounts, they are stored as u64 Little Endian
    // We can use DataView or manual shift.
    // However, JS numbers lose precision above 2^53. 
    // Token amounts are raw (including decimals).
    // We will convert to BigInt for sorting.
    
    // Node.js Buffer has readBigUInt64LE
    return buffer.readBigUInt64LE(offset);
}

async function updateKScores(deps) {
    const { db } = deps;
    
    if (!config.HELIUS_API_KEY) {
        logger.warn("🧠 K-Score: HELIUS_API_KEY missing. Skipping.");
        return;
    }

    const HELIUS_URL = `https://mainnet.helius-rpc.com/?api-key=${config.HELIUS_API_KEY}`;

    logger.info("🧠 K-Score: Starting Analysis cycle (Community Tokens)...");

    try {
        const now = Date.now();
        const cutoff = now - SIX_HOURS_MS;

        // 1. Fetch STALE Community Tokens (Rolling Basis)
        // We only care about tokens that have verified updates
        const tokens = await db.all(`
            SELECT mint, marketCap, volume24h 
            FROM tokens 
            WHERE hasCommunityUpdate = TRUE 
            AND (last_k_calc < $1 OR last_k_calc IS NULL)
            ORDER BY marketCap DESC
            LIMIT 20
        `, [cutoff]);

        if (tokens.length === 0) return;

        for (const t of tokens) {
            try {
                // 2. Fetch ALL Accounts (Helius Optimized - Base64)
                // We request dataSlice to ONLY get the Amount (offset 64, length 8)
                // This drastically reduces payload size (from 165 bytes to 8 bytes per account)
                const response = await axios.post(HELIUS_URL, {
                    jsonrpc: '2.0',
                    id: 'k-score-calc',
                    method: 'getProgramAccounts',
                    params: [
                        TOKEN_PROGRAM_ID,
                        {
                            filters: [
                                { dataSize: 165 }, 
                                { memcmp: { offset: 0, bytes: t.mint } }
                            ],
                            encoding: 'base64',
                            // Optimize: Only fetch the 8 bytes for "Amount"
                            // Layout: Mint(32) + Owner(32) + Amount(8) ...
                            dataSlice: { offset: 64, length: 8 } 
                        }
                    ]
                }, {
                    maxBodyLength: Infinity,
                    maxContentLength: Infinity
                });

                const accounts = response.data?.result || [];
                
                // 3. Parse & Sort Top 50 (In-Memory)
                const holders = accounts
                    .map(a => {
                        // Data is base64 string of 8 bytes
                        const buf = Buffer.from(a.account.data[0], 'base64');
                        return { amount: readBigUInt64LE(buf) }; // BigInt
                    })
                    .sort((a, b) => (b.amount > a.amount ? 1 : -1)) // Sort BigInt descending
                    .slice(0, 50);

                if (holders.length === 0) {
                    await updateDbScore(db, t.mint, 0, now);
                    continue;
                }

                // 4. Analyze Conviction
                // Convert BigInt to Number for approx math (safe for score calc)
                const topHoldersCount = holders.length;
                
                const velocity = (t.volume24h || 1) / (t.marketCap || 1);
                
                // Scoring Logic
                const holdTimeScore = Math.min(1 / (velocity + 0.05), 40);
                const holderCountScore = Math.min(topHoldersCount, 50); 
                const mcapScore = Math.min(Math.log10(t.marketCap || 1000) * 5, 30);

                let score = Math.floor(holdTimeScore + holderCountScore + mcapScore);
                
                score = Math.min(score, 99);
                score = Math.max(score, 1);

                // 5. Update DB
                await updateDbScore(db, t.mint, score, now);
                
                // Rate limit
                await new Promise(r => setTimeout(r, 200)); 

            } catch (err) {
                logger.warn(`K-Score calc failed for ${t.mint}: ${err.message}`);
                await updateDbScore(db, t.mint, 0, now);
            }
        }
        
        logger.info(`🧠 K-Score: Processed ${tokens.length} community tokens.`);

    } catch (e) {
        logger.error("K-Score Cycle Error", { error: e.message });
    }
}

async function updateDbScore(db, mint, score, time) {
    await db.run(`
        UPDATE tokens 
        SET k_score = $1, last_k_calc = $2 
        WHERE mint = $3
    `, [score, time, mint]);
}

function start(deps) {
    setTimeout(() => updateKScores(deps), 60000); 
    // Check every 5 mins since the list is smaller (community only)
    setInterval(() => updateKScores(deps), 5 * 60 * 1000); 
}

module.exports = { start };
