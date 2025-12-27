const axios = require('axios');
const logger = require('../services/logger');
const { broadcastTokenUpdate } = require('../services/socket'); 
const { getHolderCountFromRPC } = require('../services/solana');
const config = require('../config/env');

let isRunning = false;

// OOM FIX: Reduced concurrency to 1 to prevent multiple massive holder arrays (100k+ items) 
// from existing in memory simultaneously.
const BATCH_SIZE = 1; 
const BATCH_DELAY_MS = 1000; 

async function fetchGeckoTerminalData(mintAddress) {
    try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mintAddress}/pools?page=1`;
        const response = await axios.get(url, { timeout: 5000 });
        if (!response.data || !response.data.data) return null;
        return response.data.data;
    } catch (e) {
        if (e.response && e.response.status === 429) {
            // logger.warn("⚠️ GeckoTerminal Rate Limit");
            await new Promise(r => setTimeout(r, 10000)); 
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
        let poolsData = await fetchGeckoTerminalData(t.mint);
        let tokenDetails = await fetchTokenDetails(t.mint);
        
        // --- 1. HOLDER COUNT LOGIC (OPTIMIZED) ---
        let holderCount = t.holders || 0;
        let foundNewData = false;
        let didCheckRpc = false;

        // Strategy A: GeckoTerminal (Free/Cheap)
        // If Gecko gives us data, we use it and consider it "checked"
        if (tokenDetails && tokenDetails.attributes) {
            if (tokenDetails.attributes.holder_count || tokenDetails.attributes.holders_count) {
                const geckoHolders = parseInt(tokenDetails.attributes.holder_count || tokenDetails.attributes.holders_count);
                if (geckoHolders > 0) {
                    holderCount = geckoHolders;
                    foundNewData = true;
                }
            }
        }

        // Strategy B: RPC Direct Check (Expensive - Limited to once per 24h)
        // MEMORY FIX: Only run this if explicitly enabled in ENV
        const lastCheck = parseInt(t.last_holder_check || 0);
        const msSinceCheck = now - lastCheck;
        const ONE_DAY_MS = 24 * 60 * 60 * 1000;

        if (config.ENABLE_RPC_HOLDER_CHECK && !foundNewData && msSinceCheck > ONE_DAY_MS) {
            // logger.info(`🔍 RPC Holder Scan for ${t.mint} (Last check: ${Math.floor(msSinceCheck / 3600000)}h ago)`);
            try {
                const rpcHolders = await getHolderCountFromRPC(t.mint);
                if (rpcHolders > 0) {
                    holderCount = rpcHolders;
                }
                didCheckRpc = true; // Mark as checked so we update timestamp
            } catch (e) {
                // Ignore RPC errors, try again next cycle or wait
            }
        }

        // --- 2. PREPARE DATA ---
        let totalVolume24h = 0;
        let totalLiquidity = 0;
        let bestPrice = 0;
        let bestChange24h = null;
        let bestChange1h = null;
        let bestChange5m = null;
        let maxLiquidity = -1;
        let earliestPoolTime = null; 

        // If pools exist, calculate stats
        if (poolsData && poolsData.length > 0) {
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
        }
        
        // --- 3. MARKET CAP LOGIC ---
        let marketCap = 0;
        
        // A. Try direct FDV from Gecko
        if (tokenDetails && tokenDetails.attributes) {
            marketCap = parseFloat(tokenDetails.attributes.fdv_usd || tokenDetails.attributes.market_cap_usd || 0);
        }

        // B. Fallback: Manual Calculation
        if (marketCap === 0 && bestPrice > 0) {
            const decimals = t.decimals || 9; 
            let rawSupply = parseFloat(t.supply || '0');
            
            if (rawSupply === 0 && tokenDetails?.attributes?.total_supply) {
                rawSupply = parseFloat(tokenDetails.attributes.total_supply);
            }
            if (rawSupply === 0) rawSupply = 1000000000 * Math.pow(10, decimals); 

            const divisor = Math.pow(10, decimals);
            const supply = rawSupply / divisor;
            marketCap = supply * bestPrice;
        }

        // Explicitly clear large objects
        poolsData = null;
        tokenDetails = null;

        // --- 4. CONSTRUCT QUERY ---
        const finalParams = [];
        const updateParts = [];
        let idx = 1;

        if (totalVolume24h > 0 || totalLiquidity > 0) {
            updateParts.push(`volume24h = $${idx++}`); finalParams.push(totalVolume24h);
            updateParts.push(`marketCap = $${idx++}`); finalParams.push(marketCap);
            updateParts.push(`priceUsd = $${idx++}`); finalParams.push(bestPrice);
            updateParts.push(`liquidity = $${idx++}`); finalParams.push(totalLiquidity);
            
            if (bestChange24h !== null) { updateParts.push(`change24h = $${idx++}`); finalParams.push(bestChange24h); }
            if (bestChange1h !== null) { updateParts.push(`change1h = $${idx++}`); finalParams.push(bestChange1h); }
            if (bestChange5m !== null) { updateParts.push(`change5m = $${idx++}`); finalParams.push(bestChange5m); }
            
            // FIX: Only update timestamp if it's missing (0) or if we found an OLDER timestamp (earlier creation).
            // Never overwrite an old timestamp with a newer one.
            if (earliestPoolTime && earliestPoolTime > 0) {
                const currentTs = parseInt(t.timestamp) || 0;
                if (currentTs === 0 || earliestPoolTime < currentTs) {
                    updateParts.push(`timestamp = $${idx++}`); finalParams.push(earliestPoolTime);
                }
            }
        }
        
        // Update Holders if we found new data OR if we performed a valid RPC check (even if result was same)
        if (foundNewData || didCheckRpc) {
            updateParts.push(`holders = $${idx++}`);
            finalParams.push(holderCount);
            
            // Update the check timestamp so we don't spam RPC
            updateParts.push(`last_holder_check = $${idx++}`);
            finalParams.push(now);

            const today = Math.floor(now / (24 * 60 * 60 * 1000)) * (24 * 60 * 60 * 1000);
            await db.run(`
                INSERT INTO holders_history (mint, count, timestamp)
                VALUES ($1, $2, $3)
                ON CONFLICT(mint, timestamp) DO UPDATE SET count = EXCLUDED.count
            `, [t.mint, holderCount, today]);
        }

        updateParts.push(`updated_at = CURRENT_TIMESTAMP`);

        if (updateParts.length > 0) {
            const finalQuery = `UPDATE tokens SET ${updateParts.join(', ')} WHERE mint = $${idx}`;
            finalParams.push(t.mint);
            await db.run(finalQuery, finalParams);
        }

        // Broadcast (Always broadcast current state)
        if (totalLiquidity > 0) {
            broadcastTokenUpdate(t.mint, {
                priceUsd: bestPrice,
                marketCap: marketCap,
                volume24h: totalVolume24h,
                change1h: bestChange1h,
                change24h: bestChange24h,
                holders: holderCount,
                updatedAt: now
            });
        } else if (holderCount > 0) {
             broadcastTokenUpdate(t.mint, {
                holders: holderCount,
                updatedAt: now
            });
        }

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
        // Fetch tokens that haven't been updated recently or need holder checks
        // OOM FIX: Reduced LIMIT from 75 to 25 to reduce working set size
        // AGE FIX: Added 'timestamp' to selection to perform comparisons
        let tokens = await db.all(`
            SELECT mint, supply, decimals, holders, last_holder_check, timestamp 
            FROM tokens 
            ORDER BY liquidity DESC, updated_at ASC 
            LIMIT 25
        `);
        
        if (tokens && tokens.length > 0) {
            // Process serially or in small batches
            for (let i = 0; i < tokens.length; i += BATCH_SIZE) {
                const batch = tokens.slice(i, i + BATCH_SIZE);
                await Promise.all(batch.map(t => processSingleToken(db, t, now)));
                if (i + BATCH_SIZE < tokens.length) {
                    await new Promise(r => setTimeout(r, BATCH_DELAY_MS));
                }
            }
        }
        
        // OOM FIX: Explicitly nullify the large array to help GC
        tokens = null;

        // OOM FIX: Manual GC Trigger (if available)
        if (global.gc) {
            try { global.gc(); } catch (e) {}
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
