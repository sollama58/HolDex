const axios = require('axios');
const logger = require('../services/logger');

let isRunning = false;
const DELAY_BETWEEN_TOKENS_MS = 2000; 

async function fetchGeckoTerminalData(mintAddress) {
    try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mintAddress}/pools?page=1`;
        const response = await axios.get(url, { timeout: 5000 });
        if (!response.data || !response.data.data) return null;
        return response.data.data;
    } catch (e) {
        if (e.response && e.response.status === 429) {
            logger.warn("⚠️ GeckoTerminal Rate Limit! Slowing down...");
            await new Promise(r => setTimeout(r, 15000)); // Increased backoff to 15s
        }
        return null;
    }
}

async function updateMetadata(deps) {
    if (isRunning) return;
    isRunning = true;
    const { db } = deps;
    const now = Date.now();

    try {
        // Prioritize tokens with no price history or no liquidity
        const tokens = await db.all(`
            SELECT mint, supply, decimals 
            FROM tokens 
            ORDER BY liquidity DESC, updated_at ASC 
            LIMIT 50
        `);
        
        if (tokens.length > 0) {
            logger.info(`🔄 Metadata: Syncing ${tokens.length} tokens via GeckoTerminal...`);
        }
        
        for (const t of tokens) {
            try {
                const poolsData = await fetchGeckoTerminalData(t.mint);

                if (!poolsData || poolsData.length === 0) {
                    await db.run(`UPDATE tokens SET updated_at = CURRENT_TIMESTAMP WHERE mint = $1`, [t.mint]);
                    continue; 
                }

                let totalVolume24h = 0;
                let totalLiquidity = 0;
                let bestPrice = 0;
                let bestChange24h = null; // Changed to null initial to detect existence
                let maxLiquidity = -1;

                for (const poolData of poolsData) {
                    const attr = poolData.attributes;
                    const rel = poolData.relationships;

                    const address = attr.address;
                    const dexId = rel?.dex?.data?.id || 'unknown';
                    const price = parseFloat(attr.base_token_price_usd || 0);
                    const liqUsd = parseFloat(attr.reserve_in_usd || 0);
                    const vol24h = parseFloat(attr.volume_usd?.h24 || 0);
                    
                    totalVolume24h += vol24h;
                    totalLiquidity += liqUsd;

                    if (liqUsd > maxLiquidity) {
                        maxLiquidity = liqUsd;
                        bestPrice = price;
                        // Capture the change from the most liquid pool
                        bestChange24h = parseFloat(attr.price_change_percentage?.h24 || 0);
                    }

                    await db.run(`
                        INSERT INTO pools (
                            address, mint, dex, price_usd, liquidity_usd, volume_24h, created_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT(address) DO UPDATE SET
                            price_usd = EXCLUDED.price_usd,
                            liquidity_usd = EXCLUDED.liquidity_usd,
                            volume_24h = EXCLUDED.volume_24h
                    `, [address, t.mint, dexId, price, liqUsd, vol24h, now]);
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

                // --- CRITICAL FIX FOR 0% CHANGE ---
                // We only update change24h if we found a valid pool with data.
                // We do NOT overwrite it if bestChange24h is technically 0 but maybe valid? 
                // Gecko usually returns valid float or null.
                
                const params = [totalVolume24h, marketCap, bestPrice, totalLiquidity, now, t.mint];
                let query = `
                    UPDATE tokens 
                    SET volume24h = $1, marketCap = $2, priceUsd = $3, 
                        liquidity = $4, timestamp = $5,
                        updated_at = CURRENT_TIMESTAMP
                `;

                if (bestChange24h !== null) {
                    query += `, change24h = $${params.length + 1}`;
                    params.push(bestChange24h);
                }

                query += ` WHERE mint = $${params.length === 6 ? 6 : 7}`; // Adjust index based on whether we pushed change24h

                await db.run(query, params);

            } catch (err) {
                logger.error(`Token Update Failed [${t.mint}]: ${err.message}`);
            }

            await new Promise(r => setTimeout(r, DELAY_BETWEEN_TOKENS_MS));
        }
    } catch (e) {
        logger.error(`Metadata Cycle Error: ${e.message}`);
    } finally {
        isRunning = false;
    }
}

function start(deps) {
    setInterval(() => updateMetadata(deps), 120 * 1000);
    setTimeout(() => updateMetadata(deps), 5000);
}

module.exports = { start };
