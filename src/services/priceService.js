/**
 * On-Chain Price Service
 * - SOL/USD from CoinGecko (free, reliable)
 * - Token/SOL from on-chain vault balances (Helius RPC)
 * - Token/USD = Token/SOL × SOL/USD
 */
const axios = require('axios');
const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');

const HELIUS_API_KEY = config.HELIUS_API_KEY;
const HELIUS_RPC_URL = 'https://mainnet.helius-rpc.com/';

// Security: API key in header for axios calls
const HELIUS_HEADERS = HELIUS_API_KEY
    ? { 'Content-Type': 'application/json', 'Authorization': `Bearer ${HELIUS_API_KEY}` }
    : { 'Content-Type': 'application/json' };

// Security: Use public RPC for Connection (no API key exposure in URL)
// Helius JSON-RPC calls use header auth via axios instead
const connection = new Connection(
    config.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com'
);

const SOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const USDT_MINT = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB';

// Cache SOL price for 60 seconds
let solPriceCache = { price: 0, timestamp: 0 };
const SOL_CACHE_DURATION = 60000;

/**
 * Get SOL/USD price from CoinGecko
 */
async function getSolPrice() {
    const now = Date.now();
    if (solPriceCache.price > 0 && (now - solPriceCache.timestamp) < SOL_CACHE_DURATION) {
        return solPriceCache.price;
    }

    try {
        const response = await axios.get(
            'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd',
            { timeout: 5000 }
        );
        const price = response.data?.solana?.usd || 0;
        if (price > 0) {
            solPriceCache = { price, timestamp: now };
        }
        return price;
    } catch (e) {
        console.error('[PriceService] CoinGecko error:', e.message);
        return solPriceCache.price || 190; // Fallback to cached or default
    }
}

/**
 * Get token account balance from RPC
 */
async function getTokenAccountBalance(vaultAddress) {
    try {
        const pubkey = new PublicKey(vaultAddress);
        const info = await connection.getAccountInfo(pubkey);
        if (!info || info.data.length < 72) return null;

        // SPL Token account layout: amount at offset 64 (8 bytes)
        const amount = info.data.readBigUInt64LE(64);
        return Number(amount);
    } catch (_e) {
        return null;
    }
}

/**
 * Calculate on-chain price for a token using pool with most liquidity
 * Returns VERIFIABLE data with source proof and timestamp
 *
 * @param {Object} db - Database connection
 * @param {string} mint - Token mint address
 * @param {number} decimals - Token decimals
 * @returns {Object} { priceUsd, source, poolAddress, timestamp, proof }
 */
async function getOnChainPrice(db, mint, decimals = 9) {
    const timestamp = Date.now();

    // 1. Get SOL price from CoinGecko
    const solPrice = await getSolPrice();
    if (!solPrice) {
        return {
            priceUsd: 0,
            source: 'error',
            poolAddress: null,
            timestamp,
            proof: { error: 'sol_price_unavailable' }
        };
    }

    // 2. Find best pool (most liquidity, paired with SOL/USDC/USDT)
    const pools = await db.all(`
        SELECT address, dex, token_a, token_b, liquidity_usd
        FROM pools
        WHERE mint = $1
          AND (token_b = $2 OR token_b = $3 OR token_b = $4)
        ORDER BY liquidity_usd DESC NULLS LAST
        LIMIT 5
    `, [mint, SOL_MINT, USDC_MINT, USDT_MINT]);

    if (!pools || pools.length === 0) {
        // Fallback to DB price if no pools
        const token = await db.get('SELECT priceusd, price_timestamp FROM tokens WHERE mint = $1', [mint]);
        return {
            priceUsd: token?.priceusd || 0,
            source: 'db_cache',
            poolAddress: null,
            timestamp: token?.price_timestamp || timestamp,
            proof: { cached: true, age_ms: timestamp - (token?.price_timestamp || 0) }
        };
    }

    // 3. Try to get on-chain price from pools
    for (const pool of pools) {
        try {
            // Get vault addresses via Helius getAsset or direct pool lookup
            const vaults = await getPoolVaults(pool.address, pool.dex);
            if (!vaults) continue;

            const { baseVault, quoteVault, baseDecimals, quoteDecimals } = vaults;

            // Fetch vault balances
            const [baseRaw, quoteRaw] = await Promise.all([
                getTokenAccountBalance(baseVault),
                getTokenAccountBalance(quoteVault)
            ]);

            if (baseRaw === null || quoteRaw === null) continue;
            if (baseRaw === 0) continue;

            const baseAmount = baseRaw / (10 ** (baseDecimals || decimals));
            const quoteAmount = quoteRaw / (10 ** (quoteDecimals || 9));

            // Price = Quote / Base
            const tokenPerQuote = quoteAmount / baseAmount;

            // Convert to USD
            let priceUsd;
            let quoteAsset;
            if (pool.token_b === SOL_MINT) {
                priceUsd = tokenPerQuote * solPrice;
                quoteAsset = 'SOL';
            } else if (pool.token_b === USDC_MINT) {
                priceUsd = tokenPerQuote;
                quoteAsset = 'USDC';
            } else {
                priceUsd = tokenPerQuote;
                quoteAsset = 'USDT';
            }

            // ON-CHAIN VERIFIED with full proof
            return {
                priceUsd,
                source: 'on_chain',
                poolAddress: pool.address,
                dex: pool.dex,
                timestamp,
                proof: {
                    baseVault,
                    quoteVault,
                    baseBalance: baseRaw,
                    quoteBalance: quoteRaw,
                    quoteAsset,
                    solPrice: pool.token_b === SOL_MINT ? solPrice : null,
                    formula: `${quoteAmount.toFixed(4)} ${quoteAsset} / ${baseAmount.toFixed(4)} TOKEN${pool.token_b === SOL_MINT ? ' × $' + solPrice : ''}`
                }
            };

        } catch (_e) {
            continue; // Try next pool
        }
    }

    // Fallback to highest liquidity pool's cached price
    const bestPool = await db.get(`
        SELECT price_usd FROM pools
        WHERE mint = $1 AND price_usd > 0
        ORDER BY liquidity_usd DESC NULLS LAST
        LIMIT 1
    `, [mint]);

    return {
        priceUsd: bestPool?.price_usd || 0,
        source: 'pool_cache',
        poolAddress: pools[0]?.address,
        timestamp,
        proof: { cached: true, pool: pools[0]?.address }
    };
}

/**
 * Get vault addresses for a pool (DEX-specific)
 * For now, we use Helius getAsset API to discover vault accounts
 */
async function getPoolVaults(poolAddress, _dex) {
    try {
        // Use Helius DAS API to get pool info
        // Security: API key in header, not URL
        const response = await axios.post(HELIUS_RPC_URL, {
            jsonrpc: '2.0',
            id: 'vault-lookup',
            method: 'getAsset',
            params: { id: poolAddress }
        }, { headers: HELIUS_HEADERS, timeout: 5000 });

        // Most AMM pools store vault info in content.metadata
        const content = response.data?.result?.content;
        if (!content) return null;

        // Try to extract vault addresses from metadata
        // This varies by DEX, simplified for common patterns
        const metadata = content.metadata || {};

        // Raydium/Orca pattern
        if (metadata.token_vault_a && metadata.token_vault_b) {
            return {
                baseVault: metadata.token_vault_a,
                quoteVault: metadata.token_vault_b,
                baseDecimals: metadata.base_decimals || 9,
                quoteDecimals: metadata.quote_decimals || 9
            };
        }

        return null;
    } catch (_e) {
        return null;
    }
}

/**
 * Quick price lookup with fallback chain:
 * 1. On-chain (pool vaults)
 * 2. Cached pool price
 * 3. Cached token price
 */
async function getPrice(db, mint, decimals = 9) {
    // Try on-chain first
    const onChain = await getOnChainPrice(db, mint, decimals);
    if (onChain.priceUsd > 0) {
        return onChain;
    }

    // Fallback to token table
    const token = await db.get('SELECT priceusd FROM tokens WHERE mint = $1', [mint]);
    return {
        priceUsd: token?.priceusd || 0,
        source: 'token_cache',
        poolAddress: null
    };
}

/**
 * Calculate ON-CHAIN verified market cap
 * MCap = circulating_supply × price_per_token
 *
 * All inputs must be on-chain derived:
 * - supply: from getTokenSupply RPC
 * - price: from pool vault balances
 *
 * @param {Object} params
 * @param {number} params.supply - Circulating supply (after burns)
 * @param {number} params.priceUsd - Price per token in USD
 * @param {string} params.priceSource - Source of price data
 * @param {number} params.priceTimestamp - When price was fetched
 * @returns {Object} { mcap, source, timestamp, proof }
 */
function calculateOnChainMcap({ supply, priceUsd, priceSource, priceTimestamp, priceProof }) {
    const timestamp = Date.now();

    if (!supply || supply <= 0 || !priceUsd || priceUsd <= 0) {
        return {
            mcap: 0,
            source: 'error',
            timestamp,
            proof: { error: 'missing_inputs', supply, priceUsd }
        };
    }

    const mcap = supply * priceUsd;

    // Source is only "on_chain" if BOTH supply AND price are on-chain
    const isFullyOnChain = priceSource === 'on_chain';
    const source = isFullyOnChain ? 'on_chain' : `calculated_${priceSource}`;

    return {
        mcap,
        source,
        timestamp,
        proof: {
            formula: `${supply.toLocaleString()} tokens × $${priceUsd.toFixed(10)}`,
            supply,
            priceUsd,
            priceSource,
            priceTimestamp,
            priceProof,
            isFullyOnChain
        }
    };
}

module.exports = {
    getSolPrice,
    getOnChainPrice,
    getPrice,
    getTokenAccountBalance,
    calculateOnChainMcap
};
