const { PublicKey } = require('@solana/web3.js');
const { getDB, enableIndexing, aggregateAndSaveToken } = require('./database');
const { findPoolsOnChain } = require('./pool_finder');
const { fetchTokenMetadata } = require('../utils/metaplex');
const { getSolanaConnection } = require('./solana');
const { enqueueTokenUpdate } = require('./queue');
const { snapshotPools } = require('../indexer/tasks/snapshotter');
const logger = require('./logger');
const axios = require('axios');

const solanaConnection = getSolanaConnection();

async function fetchInitialMarketData(mint) {
    try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mint}`;
        const res = await axios.get(url, { timeout: 3000 });
        const attrs = res.data.data.attributes;
        return {
            priceUsd: parseFloat(attrs.price_usd || 0),
            volume24h: parseFloat(attrs.volume_usd?.h24 || 0),
            change24h: parseFloat(attrs.price_change_percentage?.h24 || 0),
            change1h: parseFloat(attrs.price_change_percentage?.h1 || 0),
            change5m: parseFloat(attrs.price_change_percentage?.m5 || 0),
            marketCap: parseFloat(attrs.fdv_usd || attrs.market_cap_usd || 0)
        };
    } catch (_e) { return null; }
}

async function indexTokenOnChain(mint, retryCount = 0) {
    try {
        const db = getDB();
        logger.info(`🔍 [Indexer] Starting indexing for ${mint.slice(0, 8)}...${retryCount > 0 ? ` (retry ${retryCount})` : ''}`);

        let meta = await fetchTokenMetadata(mint);

        // For brand new tokens, metadata might not be indexed yet - retry with delay
        const MAX_RETRIES = 3;
        const RETRY_DELAY_MS = 2000;

        // Check if we got real metadata (not placeholder)
        const hasRealName = meta && meta.name && meta.name !== 'Unknown' && meta.name !== 'New Discovery' && meta.name.length > 0;
        const hasRealSymbol = meta && meta.symbol && meta.symbol !== 'UNK' && meta.symbol !== 'UNKNOWN';

        if ((!hasRealName || !hasRealSymbol) && retryCount < MAX_RETRIES) {
            logger.info(`⏳ [Indexer] Metadata not ready for ${mint.slice(0, 8)} (name: ${meta?.name}, symbol: ${meta?.symbol}), will retry in ${RETRY_DELAY_MS}ms...`);
            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
            return indexTokenOnChain(mint, retryCount + 1);
        }

        // If still no valid metadata after retries, skip this token entirely
        if (!hasRealName || !hasRealSymbol) {
            logger.warn(`⚠️ [Indexer] Skipping ${mint.slice(0, 8)} - no valid metadata after ${MAX_RETRIES} retries`);
            return { name: 'Unknown', ticker: 'UNK', pairs: [] };
        }

        logger.info(`📝 [Indexer] Metadata: ${meta.name} (${meta.symbol})`);

        let supply = '1000000000';
        let decimals = 9;
        try {
            const supplyInfo = await solanaConnection.getTokenSupply(new PublicKey(mint));
            supply = supplyInfo.value.amount;
            decimals = supplyInfo.value.decimals;
        } catch (e) {
            logger.warn(`⚠️ [Indexer] Failed to fetch supply for ${mint.slice(0, 8)}: ${e.message}`);
        }

        const marketData = await fetchInitialMarketData(mint);
        if (marketData) {
            logger.info(`💹 [Indexer] Market data: $${marketData.priceUsd} | Vol: $${marketData.volume24h}`);
        } else {
            logger.warn(`⚠️ [Indexer] No market data from GeckoTerminal for ${mint.slice(0, 8)}`);
        }

        const baseData = { name: meta.name, ticker: meta.symbol, image: meta?.image || null };
        const initialPrice = marketData?.priceUsd || 0;
        const initialVol = marketData?.volume24h || 0;
        const initialChange = marketData?.change24h || 0;
        const initialChange1h = marketData?.change1h || 0;
        const initialChange5m = marketData?.change5m || 0;
        const initialMcap = marketData?.marketCap || 0;

        // 1. CREATE TOKEN RECORD
        // FIX: Update market data on conflict, but only update identity if it's still "Unknown"
        // Identity fields (name, symbol, image) are signed with sig_identity
        // If already indexed with real data, preserve it. If still "Unknown", update it.
        // FIX: Only update market data if we have valid values (> 0), otherwise preserve existing
        try {
            await db.run(`
                INSERT INTO tokens (mint, name, symbol, image, supply, decimals, priceUsd, liquidity, marketCap, volume24h, change24h, change1h, change5m, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT(mint) DO UPDATE SET
                name = CASE
                    WHEN tokens.name IN ('Unknown', 'New Discovery', '') OR tokens.name IS NULL
                    THEN EXCLUDED.name
                    ELSE tokens.name
                END,
                symbol = CASE
                    WHEN tokens.symbol IN ('UNKNOWN', 'UNK', 'NEW', '') OR tokens.symbol IS NULL
                    THEN EXCLUDED.symbol
                    ELSE tokens.symbol
                END,
                image = CASE
                    WHEN tokens.image IS NULL OR tokens.image = ''
                    THEN EXCLUDED.image
                    ELSE tokens.image
                END,
                priceUsd = CASE WHEN EXCLUDED.priceUsd > 0 THEN EXCLUDED.priceUsd ELSE tokens.priceUsd END,
                marketCap = CASE WHEN EXCLUDED.marketCap > 0 THEN EXCLUDED.marketCap ELSE tokens.marketCap END,
                volume24h = CASE WHEN EXCLUDED.volume24h > 0 THEN EXCLUDED.volume24h ELSE tokens.volume24h END,
                change24h = CASE WHEN EXCLUDED.priceUsd > 0 THEN EXCLUDED.change24h ELSE tokens.change24h END,
                change1h = CASE WHEN EXCLUDED.priceUsd > 0 THEN EXCLUDED.change1h ELSE tokens.change1h END,
                change5m = CASE WHEN EXCLUDED.priceUsd > 0 THEN EXCLUDED.change5m ELSE tokens.change5m END,
                updated_at = NOW()
            `, [
                mint, baseData.name, baseData.ticker, baseData.image, supply, decimals,
                initialPrice, 0, initialMcap, initialVol, initialChange,
                initialChange1h, initialChange5m, Date.now()
            ]);
            logger.info(`💾 [Indexer] Token record created/updated for ${mint.slice(0, 8)}`);
        } catch (tokenErr) {
            logger.error(`❌ [Indexer] Failed to create token record for ${mint}: ${tokenErr.message}`);
            throw tokenErr; // Can't continue without token record
        }

        // Verify token exists before adding pools (foreign key constraint)
        const tokenExists = await db.get('SELECT mint FROM tokens WHERE mint = $1', [mint]);
        if (!tokenExists) {
            logger.error(`❌ [Indexer] Token record not found after insert for ${mint.slice(0, 8)}`);
            throw new Error('Token insert verification failed');
        }

        // 2. FIND POOLS
        const pools = await findPoolsOnChain(mint);
        const poolAddresses = [];

        logger.info(`🏊 [Indexer] Found ${pools.length} pool(s) for ${mint.slice(0, 8)}`);

        for (const pool of pools) {
            poolAddresses.push(pool.pairAddress);
            await enableIndexing(db, mint, {
                pairAddress: pool.pairAddress,
                dexId: pool.dexId,
                liquidity: pool.liquidity || { usd: 0 },
                volume: pool.volume || { h24: 0 },
                priceUsd: pool.priceUsd || 0,
                baseToken: pool.baseToken,
                quoteToken: pool.quoteToken,
                reserve_a: pool.reserve_a,
                reserve_b: pool.reserve_b
            });
        }

        await enqueueTokenUpdate(mint);
        if (poolAddresses.length > 0) {
            await snapshotPools(poolAddresses).catch(e => logger.error(`❌ [Indexer] Snapshot error: ${e.message}`));
            await aggregateAndSaveToken(db, mint);
            logger.info(`✅ [Indexer] Successfully indexed ${baseData.name} (${mint.slice(0, 8)})`);
        } else {
            logger.warn(`⚠️ [Indexer] No pools found for ${mint.slice(0, 8)} - token added but no price data`);
        }

        return { ...baseData, pairs: pools };
    } catch (error) {
        logger.error(`❌ [Indexer] CRITICAL ERROR indexing ${mint}: ${error.message}`);
        logger.error(error.stack);
        throw error; // Re-throw to let caller handle
    }
}

module.exports = { indexTokenOnChain };
