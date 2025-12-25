/**
 * Price Indexer (The Engine)
 * Replaces DexScreener dependency for price tracking.
 * Polls Helius RPC directly for Vault Balances to calculate Price.
 */
const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const { logger } = require('../services');

const HELIUS_RPC = config.HELIUS_API_KEY 
    ? `https://mainnet.helius-rpc.com/?api-key=${config.HELIUS_API_KEY}` 
    : config.SOLANA_RPC_URL;

const connection = new Connection(HELIUS_RPC);

// RECURSIVE LOOP: Better than setInterval
async function startLoop(deps) {
    try {
        const start = Date.now();
        logger.info(`[Indexer] Starting price cycle...`);
        
        await updatePrices(deps);
        
        const duration = Date.now() - start;
        logger.info(`[Indexer] Cycle took ${duration}ms`);

        // Wait remaining time to hit 60s cadence
        const delay = Math.max(0, 60000 - duration);
        setTimeout(() => startLoop(deps), delay);

    } catch (e) {
        logger.error("[Indexer] Critical Error (Restarting in 10s):", e);
        setTimeout(() => startLoop(deps), 10000);
    }
}

async function updatePrices(deps) {
    const { db } = deps;

    // 1. Fetch Pools Config from DB
    // (You populate this table when discovering new tokens)
    const pools = await db.all('SELECT * FROM pools');
    
    if (!pools || pools.length === 0) {
        logger.warn("[Indexer] No pools to track. Waiting for discovery...");
        return;
    }

    // 2. Map Keys
    const keysToFetch = [];
    
    pools.forEach(p => {
        try {
            // Validate keys to prevent crash
            if (p.base_vault && p.quote_vault) {
                keysToFetch.push(new PublicKey(p.base_vault));
                keysToFetch.push(new PublicKey(p.quote_vault));
            }
        } catch (e) {
            logger.warn(`[Indexer] Invalid keys for ${p.symbol}`);
        }
    });

    if (keysToFetch.length === 0) return;

    // 3. Batch Fetch (Chunk size 100 for Helius)
    const balances = new Map();
    const CHUNK_SIZE = 100;
    
    for (let i = 0; i < keysToFetch.length; i += CHUNK_SIZE) {
        const chunk = keysToFetch.slice(i, i + CHUNK_SIZE);
        try {
            const infos = await connection.getMultipleAccountsInfo(chunk);
            infos.forEach((info, idx) => {
                if (info) {
                    // Parse Little Endian u64 from byte 64 (SPL Token Layout)
                    const amount = info.data.readBigUInt64LE(64);
                    balances.set(chunk[idx].toBase58(), Number(amount));
                }
            });
        } catch (e) {
            logger.error(`[Indexer] Batch RPC failed: ${e.message}`);
        }
    }

    // 4. Calculate Prices & Prepare Secure Query
    const now = Math.floor(Date.now() / 1000);
    const timeBucket = now - (now % 60); // Align to minute

    const queryValues = [];
    const placeholders = [];
    let paramIndex = 1;

    pools.forEach(p => {
        const baseRaw = balances.get(p.base_vault);
        const quoteRaw = balances.get(p.quote_vault);

        if (baseRaw !== undefined && quoteRaw !== undefined) {
            const baseVal = baseRaw / (10 ** p.base_decimals);
            const quoteVal = quoteRaw / (10 ** p.quote_decimals);
            
            if (baseVal > 0) {
                const price = quoteVal / baseVal;

                // FIX: Parameterized Query Construction
                // We add the values to the flat array
                queryValues.push(p.symbol, timeBucket, price);
                
                // Format: ($1, $2, $3, $3, $3, $3) -> (symbol, time, open, high, low, close)
                // We assume OHLC are same for this snapshot
                placeholders.push(`($${paramIndex}, $${paramIndex+1}, $${paramIndex+2}, $${paramIndex+2}, $${paramIndex+2}, $${paramIndex+2})`);
                
                paramIndex += 3; // We used 3 unique values per row
            }
        }
    });

    // 5. Bulk Upsert
    if (placeholders.length > 0) {
        // We construct one massive query
        const query = `
            INSERT INTO candles (symbol, time, open, high, low, close)
            VALUES ${placeholders.join(', ')}
            ON CONFLICT (symbol, time) DO UPDATE SET
                high = GREATEST(candles.high, EXCLUDED.high),
                low = LEAST(candles.low, EXCLUDED.low),
                close = EXCLUDED.close;
        `;

        try {
            await db.run(query, queryValues);
            logger.info(`[Indexer] Saved ${placeholders.length} prices to DB.`);
        } catch (err) {
            logger.error(`[Indexer] DB Write Failed: ${err.message}`);
        }
    }
}

function start(deps) {
    // Initial delay to let DB connect
    setTimeout(() => startLoop(deps), 5000);
}

module.exports = { start };
