/**
 * Price Indexer (Refactored for Stability)
 * - Uses 'symbol' as key for candles (matches production schema)
 * - Handles empty pool tables gracefully
 * - Batches RPC calls efficiently
 */
const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const { logger } = require('../services');

// Security: Use public RPC for Connection (no API key exposure in URL)
// For authenticated calls, use axios with header-based auth instead
const connection = new Connection(
    config.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com'
);

async function startLoop(deps) {
    try {
        const start = Date.now();
        await updatePrices(deps);
        
        // Ensure we don't spam if the cycle is too fast
        const duration = Date.now() - start;
        const delay = Math.max(10000, 60000 - duration); // Minimum 10s delay, aim for 60s cycle
        
        setTimeout(() => startLoop(deps), delay);

    } catch (e) {
        logger.error("[Indexer] Critical Error (Restarting in 10s):", e);
        setTimeout(() => startLoop(deps), 10000);
    }
}

async function updatePrices(deps) {
    const { db } = deps;

    // 1. Fetch Pools Config
    // NOTE: This table MUST be populated by a separate task that resolves Vault addresses.
    // If this list is empty, the indexer does nothing.
    const pools = await db.all('SELECT * FROM pools');
    
    if (!pools || pools.length === 0) {
        // Be verbose about this because it's a common setup error
        logger.warn("[Indexer] ⚠️  Pools table is empty! No prices will be indexed. Please run a pool discovery task.");
        return;
    }

    // 2. Map Keys for RPC
    const keysToFetch = [];
    // We map keys back to the pool object for easy lookup later
    const poolMap = new Map(); 

    pools.forEach(p => {
        try {
            if (p.base_vault && p.quote_vault) {
                keysToFetch.push(new PublicKey(p.base_vault));
                keysToFetch.push(new PublicKey(p.quote_vault));
                poolMap.set(p.mint, p);
            }
        } catch (e) {
            logger.warn(`[Indexer] Invalid keys for ${p.mint}`);
        }
    });

    if (keysToFetch.length === 0) return;

    // 3. Batch Fetch (Chunk size 100)
    const balances = new Map();
    const CHUNK_SIZE = 100;
    
    for (let i = 0; i < keysToFetch.length; i += CHUNK_SIZE) {
        const chunk = keysToFetch.slice(i, i + CHUNK_SIZE);
        try {
            const infos = await connection.getMultipleAccountsInfo(chunk);
            infos.forEach((info, idx) => {
                if (info) {
                    const amount = info.data.readBigUInt64LE(64);
                    balances.set(chunk[idx].toBase58(), Number(amount));
                }
            });
        } catch (e) {
            logger.error(`[Indexer] Batch RPC failed: ${e.message}`);
        }
    }

    // 4. Calculate Prices
    const now = Math.floor(Date.now() / 1000);
    const timeBucket = now - (now % 60); // 1-minute buckets

    const queryValues = [];
    const placeholders = [];
    let paramIndex = 1;

    pools.forEach(p => {
        const baseRaw = balances.get(p.base_vault);
        const quoteRaw = balances.get(p.quote_vault);

        if (baseRaw !== undefined && quoteRaw !== undefined) {
            const baseVal = baseRaw / (10 ** p.base_decimals);
            const quoteVal = quoteRaw / (10 ** p.quote_decimals);
            
            // Basic CPMM Price = Quote / Base
            if (baseVal > 0) {
                const price = quoteVal / baseVal;

                // Push flatten values (use symbol for candles table)
                queryValues.push(p.symbol, timeBucket, price);

                // ($1, $2, $3, $3, $3, $3) = (symbol, time, open, high, low, close)
                placeholders.push(`($${paramIndex}, $${paramIndex+1}, $${paramIndex+2}, $${paramIndex+2}, $${paramIndex+2}, $${paramIndex+2})`);
                
                paramIndex += 3; 
            }
        }
    });

    // 5. Bulk Upsert
    if (placeholders.length > 0) {
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
            logger.info(`[Indexer] Updated candles for ${placeholders.length} pools.`);
        } catch (err) {
            logger.error(`[Indexer] DB Write Failed: ${err.message}`);
        }
    }
}

function start(deps) {
    setTimeout(() => startLoop(deps), 5000);
}

module.exports = { start };
