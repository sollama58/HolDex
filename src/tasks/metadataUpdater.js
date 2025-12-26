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
                
                // Track all timeframes. Initialize to null to avoid overwriting with 0.
                let bestChange24h = null;
                let bestChange1h = null;
                let bestChange5m = null;

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
                        
                        // --- CRITICAL FIX: Safe Parsing ---
                        // Only set if the value is defined and not null.
                        // We do NOT use "|| 0" here, because 0 is a specific value we don't want to fake.
                        const parseChange = (val) => (val !== undefined && val !== null) ? parseFloat(val) : null;
                        
                        bestChange24h = parseChange(attr.price_change_percentage?.h24);
                        bestChange1h = parseChange(attr.price_change_percentage?.h1);
                        bestChange5m = parseChange(attr.price_change_percentage?.m5);
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

                const params = [totalVolume24h, marketCap, bestPrice, totalLiquidity, now, t.mint];
                let query = `
                    UPDATE tokens 
                    SET volume24h = $1, marketCap = $2, priceUsd = $3, 
                        liquidity = $4, timestamp = $5,
                        updated_at = CURRENT_TIMESTAMP
                `;

                // Only append update clauses if we actually found valid data
                if (bestChange24h !== null) {
                    query += `, change24h = $${params.length + 1}`;
                    params.push(bestChange24h);
                }
                if (bestChange1h !== null) {
                    query += `, change1h = $${params.length + 1}`;
                    params.push(bestChange1h);
                }
                if (bestChange5m !== null) {
                    query += `, change5m = $${params.length + 1}`;
                    params.push(bestChange5m);
                }

                // Dynamically find the index for the WHERE clause
                query += ` WHERE mint = $${params.length}`; 

                // FIX: params.length includes t.mint which is the last one pushed before the optionals
                // wait, the logic above for params.push order is tricky.
                // Let's rewrite the query builder to be cleaner and safer.
                
                const finalParams = [totalVolume24h, marketCap, bestPrice, totalLiquidity, now];
                let updateParts = [
                    "volume24h = $1", 
                    "marketCap = $2", 
                    "priceUsd = $3", 
                    "liquidity = $4", 
                    "timestamp = $5", 
                    "updated_at = CURRENT_TIMESTAMP"
                ];
                
                let idx = 6;
                if (bestChange24h !== null) {
                    updateParts.push(`change24h = $${idx++}`);
                    finalParams.push(bestChange24h);
                }
                if (bestChange1h !== null) {
                    updateParts.push(`change1h = $${idx++}`);
                    finalParams.push(bestChange1h);
                }
                if (bestChange5m !== null) {
                    updateParts.push(`change5m = $${idx++}`);
                    finalParams.push(bestChange5m);
                }
                
                const finalQuery = `UPDATE tokens SET ${updateParts.join(', ')} WHERE mint = $${idx}`;
                finalParams.push(t.mint);

                await db.run(finalQuery, finalParams);

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
