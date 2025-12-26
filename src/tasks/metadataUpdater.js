const axios = require('axios');
const logger = require('../services/logger');
const { broadcastTokenUpdate } = require('../services/socket'); 
const { fetchSolscanData } = require('../services/solscan'); // Import Solscan for fallback

let isRunning = false;
const BATCH_SIZE = 5; 
const BATCH_DELAY_MS = 2000; 

async function fetchGeckoTerminalData(mintAddress) {
    try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mintAddress}/pools?page=1`;
        const response = await axios.get(url, { timeout: 5000 });
        if (!response.data || !response.data.data) return null;
        return response.data.data;
    } catch (e) {
        if (e.response && e.response.status === 429) {
            logger.warn("⚠️ GeckoTerminal Rate Limit! Slowing down...");
            await new Promise(r => setTimeout(r, 30000)); 
        }
        return null;
    }
}

async function fetchTokenDetails(mintAddress) {
    try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mintAddress}`;
        const response = await axios.get(url, { timeout: 5000 });
        if (!response.data || !response.data.data) return null;
        return response.data.data;
    } catch (e) { return null; }
}

async function processSingleToken(db, t, now) {
    try {
        const poolsData = await fetchGeckoTerminalData(t.mint);
        const tokenDetails = await fetchTokenDetails(t.mint);
        
        // --- HOLDER COUNT LOGIC (WITH FALLBACK) ---
        let holderCount = 0;
        
        // 1. Try GeckoTerminal
        if (tokenDetails && tokenDetails.attributes && tokenDetails.attributes.holder_count) {
            holderCount = parseInt(tokenDetails.attributes.holder_count);
        }

        // 2. Fallback to Solscan if Gecko fails or returns 0
        if (!holderCount || holderCount === 0) {
            const solscanData = await fetchSolscanData(t.mint);
            if (solscanData && solscanData.holders > 0) {
                holderCount = solscanData.holders;
                // logger.info(`Using Solscan Holder Count for ${t.mint}: ${holderCount}`);
            }
        }

        if (!poolsData || poolsData.length === 0) {
            await db.run(`UPDATE tokens SET updated_at = CURRENT_TIMESTAMP WHERE mint = $1`, [t.mint]);
            return;
        }

        let totalVolume24h = 0;
        let totalLiquidity = 0;
        let bestPrice = 0;
        let bestChange24h = null;
        let bestChange1h = null;
        let bestChange5m = null;
        let maxLiquidity = -1;
        let earliestPoolTime = null; // Track earliest pool creation time

        for (const poolData of poolsData) {
            const attr = poolData.attributes;
            const rel = poolData.relationships;

            // --- TRACK AGE ---
            if (attr.pool_created_at) {
                const createdAt = new Date(attr.pool_created_at).getTime();
                if (!earliestPoolTime || createdAt < earliestPoolTime) {
                    earliestPoolTime = createdAt;
                }
            }

            const address = attr.address;
            const dexId = rel?.dex?.data?.id || 'unknown';
            const price = parseFloat(attr.base_token_price_usd || 0);
            const liqUsd = parseFloat(attr.reserve_in_usd || 0);
            const vol24h = parseFloat(attr.volume_usd?.h24 || 0);
            
            let tokenA = rel?.base_token?.data?.id || null;
            let tokenB = rel?.quote_token?.data?.id || null;

            if (tokenA && tokenA.includes('solana_')) tokenA = tokenA.replace('solana_', '');
            if (tokenB && tokenB.includes('solana_')) tokenB = tokenB.replace('solana_', '');

            if (!tokenA) tokenA = t.mint;
            if (!tokenB) tokenB = 'So11111111111111111111111111111111111111112'; 

            totalVolume24h += vol24h;
            totalLiquidity += liqUsd;

            if (liqUsd > maxLiquidity) {
                maxLiquidity = liqUsd;
                bestPrice = price;
                
                const parseChange = (val) => (val !== undefined && val !== null) ? parseFloat(val) : null;
                bestChange24h = parseChange(attr.price_change_percentage?.h24);
                bestChange1h = parseChange(attr.price_change_percentage?.h1);
                bestChange5m = parseChange(attr.price_change_percentage?.m5);
            }

            await db.run(`
                INSERT INTO pools (
                    address, mint, dex, price_usd, liquidity_usd, volume_24h, created_at, token_a, token_b
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT(address) DO UPDATE SET
                    price_usd = EXCLUDED.price_usd,
                    liquidity_usd = EXCLUDED.liquidity_usd,
                    volume_24h = EXCLUDED.volume_24h
            `, [address, t.mint, dexId, price, liqUsd, vol24h, now, tokenA, tokenB]);
        }
        
        let marketCap = 0;
        if (bestPrice > 0) {
            const decimals = t.decimals || 9; 
            let rawSupply = parseFloat(t.supply || '0');
            if (rawSupply === 0) rawSupply = 1000000000 * Math.pow(10, decimals); 

            const divisor = Math.pow(10, decimals);
            const supply = rawSupply / divisor;
            marketCap = supply * bestPrice;
        }

        // Params for SQL Update
        const finalParams = [totalVolume24h, marketCap, bestPrice, totalLiquidity];
        let updateParts = [
            "volume24h = $1", 
            "marketCap = $2", 
            "priceUsd = $3", 
            "liquidity = $4", 
            "updated_at = CURRENT_TIMESTAMP"
        ];
        
        let idx = 5;
        
        // --- AGE FIX ---
        // Only update 'timestamp' if we found a valid pool creation date.
        // We do NOT update timestamp to 'now', preserving the "Age".
        if (earliestPoolTime && earliestPoolTime > 0) {
            updateParts.push(`timestamp = $${idx++}`);
            finalParams.push(earliestPoolTime);
        }

        if (bestChange24h !== null) { updateParts.push(`change24h = $${idx++}`); finalParams.push(bestChange24h); }
        if (bestChange1h !== null) { updateParts.push(`change1h = $${idx++}`); finalParams.push(bestChange1h); }
        if (bestChange5m !== null) { updateParts.push(`change5m = $${idx++}`); finalParams.push(bestChange5m); }
        
        if (holderCount > 0) {
            updateParts.push(`holders = $${idx++}`);
            finalParams.push(holderCount);

            const today = Math.floor(now / (24 * 60 * 60 * 1000)) * (24 * 60 * 60 * 1000);
            await db.run(`
                INSERT INTO holders_history (mint, count, timestamp)
                VALUES ($1, $2, $3)
                ON CONFLICT(mint, timestamp) DO NOTHING
            `, [t.mint, holderCount, today]);
        }

        const finalQuery = `UPDATE tokens SET ${updateParts.join(', ')} WHERE mint = $${idx}`;
        finalParams.push(t.mint);

        await db.run(finalQuery, finalParams);

        // Broadcast Real-Time Update
        broadcastTokenUpdate(t.mint, {
            priceUsd: bestPrice,
            marketCap: marketCap,
            volume24h: totalVolume24h,
            change1h: bestChange1h,
            change24h: bestChange24h,
            holders: holderCount,
            updatedAt: now
        });

    } catch (err) {
        logger.error(`Token Update Failed [${t.mint}]: ${err.message}`);
    }
}

async function updateMetadata(deps) {
    if (isRunning) return;
    isRunning = true;
    const { db } = deps;
    const now = Date.now();

    try {
        const tokens = await db.all(`
            SELECT mint, supply, decimals 
            FROM tokens 
            ORDER BY liquidity DESC, updated_at ASC 
            LIMIT 75
        `);
        
        if (tokens.length > 0) {
            logger.info(`🔄 Metadata: Syncing ${tokens.length} tokens (Gecko/Solscan)...`);
        }
        
        for (let i = 0; i < tokens.length; i += BATCH_SIZE) {
            const batch = tokens.slice(i, i + BATCH_SIZE);
            await Promise.all(batch.map(t => processSingleToken(db, t, now)));
            
            if (i + BATCH_SIZE < tokens.length) {
                await new Promise(r => setTimeout(r, BATCH_DELAY_MS));
            }
        }

    } catch (e) {
        logger.error(`Metadata Cycle Error: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    setInterval(() => updateMetadata(deps), 60 * 1000);
    setTimeout(() => updateMetadata(deps), 5000);
}

module.exports = { start };
