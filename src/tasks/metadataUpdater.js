const axios = require('axios');
const logger = require('../services/logger');

let isRunning = false;

// GeckoTerminal Free Tier is ~30 requests per minute.
const DELAY_BETWEEN_TOKENS_MS = 2000; 

async function fetchGeckoTerminalData(mintAddress) {
    try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mintAddress}/pools?page=1`;
        const response = await axios.get(url, { timeout: 5000 });
        
        if (!response.data || !response.data.data || response.data.data.length === 0) {
            return null;
        }

        return response.data.data; // Return the RAW array of pools

    } catch (e) {
        if (e.response && e.response.status === 429) {
            logger.warn("⚠️ GeckoTerminal Rate Limit! Slowing down...");
            await new Promise(r => setTimeout(r, 5000)); 
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
        const tokens = await db.all(`SELECT mint, supply, decimals FROM tokens`);
        if (tokens.length > 0) {
            logger.info(`🔄 Syncing Pools & Metadata for ${tokens.length} tokens...`);
        }
        
        for (const t of tokens) {
            try {
                // 1. Fetch ALL pools for this token
                const poolsData = await fetchGeckoTerminalData(t.mint);

                if (!poolsData) {
                    continue; 
                }

                let totalVolume24h = 0;
                let totalLiquidity = 0; // Sum of ALL pools
                
                // Track Primary Pool (Largest Liquidity)
                let maxLiquidity = -1;
                let bestPrice = 0;
                let bestChange24h = 0;

                // 2. Iterate and Update INDIVIDUAL Pools
                for (const poolData of poolsData) {
                    const attr = poolData.attributes;
                    const rel = poolData.relationships;

                    const address = attr.address;
                    const dexId = rel?.dex?.data?.id || 'unknown';
                    const price = parseFloat(attr.base_token_price_usd || 0);
                    const liqUsd = parseFloat(attr.reserve_in_usd || 0);
                    const vol24h = parseFloat(attr.volume_usd?.h24 || 0);
                    
                    // Accumulate Totals
                    totalVolume24h += vol24h;
                    totalLiquidity += liqUsd;

                    // Determine Primary Pool (For Price & Mcap)
                    if (liqUsd > maxLiquidity) {
                        maxLiquidity = liqUsd;
                        bestPrice = price;
                        bestChange24h = parseFloat(attr.price_change_percentage?.h24 || 0);
                    }

                    // UPDATE POOL RECORD
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

                // Fallback: If no pool had liquidity > -1 (weird edge case), try to use any valid price
                if (bestPrice === 0 && poolsData.length > 0) {
                     const fallback = poolsData.find(p => parseFloat(p.attributes.base_token_price_usd) > 0);
                     if (fallback) {
                         bestPrice = parseFloat(fallback.attributes.base_token_price_usd);
                         bestChange24h = parseFloat(fallback.attributes.price_change_percentage?.h24 || 0);
                     }
                }

                // 3. Update PARENT TOKEN Stats
                let marketCap = 0;
                if (bestPrice > 0) {
                    const decimals = t.decimals || 9;
                    let rawSupply = parseFloat(t.supply || '0');
                    // Fallback for missing supply (common in some memes)
                    if (rawSupply === 0) rawSupply = 1000000000 * Math.pow(10, decimals); 

                    const divisor = Math.pow(10, decimals);
                    const supply = rawSupply / divisor;
                    
                    // Mcap is ALWAYS Supply * Primary Pool Price
                    marketCap = supply * bestPrice;
                }

                await db.run(`
                    UPDATE tokens 
                    SET volume24h = $1, marketCap = $2, priceUsd = $3, 
                        liquidity = $4, change24h = $5, timestamp = $6 
                    WHERE mint = $7
                `, [totalVolume24h, marketCap, bestPrice, totalLiquidity, bestChange24h, now, t.mint]);

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
