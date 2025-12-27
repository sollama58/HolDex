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
    } catch (e) { return null; }
}

async function indexTokenOnChain(mint) {
    const db = getDB();
    const meta = await fetchTokenMetadata(mint);
    let supply = '1000000000'; 
    let decimals = 9; 
    try {
        const supplyInfo = await solanaConnection.getTokenSupply(new PublicKey(mint));
        supply = supplyInfo.value.amount;
        decimals = supplyInfo.value.decimals;
    } catch (e) {}

    const marketData = await fetchInitialMarketData(mint);
    const baseData = { name: meta?.name || 'Unknown', ticker: meta?.symbol || 'UNKNOWN', image: meta?.image || null };
    const initialPrice = marketData?.priceUsd || 0;
    const initialVol = marketData?.volume24h || 0;
    const initialChange = marketData?.change24h || 0;
    const initialChange1h = marketData?.change1h || 0;
    const initialChange5m = marketData?.change5m || 0;
    const initialMcap = marketData?.marketCap || 0;

    // 1. CREATE TOKEN RECORD
    await db.run(`
        INSERT INTO tokens (mint, name, symbol, image, supply, decimals, priceUsd, liquidity, marketCap, volume24h, change24h, change1h, change5m, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT(mint) DO UPDATE SET
        name = EXCLUDED.name,
        symbol = EXCLUDED.symbol,
        image = EXCLUDED.image,
        decimals = EXCLUDED.decimals
    `, [
        mint, baseData.name, baseData.ticker, baseData.image, supply, decimals, 
        initialPrice, 0, initialMcap, initialVol, initialChange,
        initialChange1h, initialChange5m, Date.now()
    ]);

    // 2. FIND POOLS
    const pools = await findPoolsOnChain(mint);
    const poolAddresses = [];

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
        await snapshotPools(poolAddresses).catch(e => console.error("Snapshot Err:", e.message));
        await aggregateAndSaveToken(db, mint);
    }
    return { ...baseData, pairs: pools };
}

module.exports = { indexTokenOnChain };
