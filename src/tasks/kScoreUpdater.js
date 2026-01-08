/**
 * K-Score Updater v8 - 3 Pillars Formula
 *
 * DIAMOND HANDS (50%):
 *   - conviction: % accumulators + holders among top 20
 *   - accExtRatio: accumulators / extractors ratio
 *
 * ORGANIC GROWTH (35%):
 *   - holders: real holders count ($1+ balance)
 *   - top10: distribution fairness (inverted concentration)
 *
 * LONGEVITY (15%):
 *   - age: token age in days
 *
 * Eliminates manipulable metrics (volume, mcap, liquidity)
 * Focus on on-chain behavior only
 */

const config = require('../config/env');
const { logger } = require('../services');
const priceService = require('../services/priceService');

// ============================================
// HELIUS CONFIG
// ============================================

const HELIUS_API_KEY = config.HELIUS_API_KEY;
const HELIUS_RPC_URL = 'https://mainnet.helius-rpc.com/';

// Security: API key passed via header, not URL querystring
const HELIUS_HEADERS = HELIUS_API_KEY
    ? { 'Content-Type': 'application/json', 'Authorization': `Bearer ${HELIUS_API_KEY}` }
    : { 'Content-Type': 'application/json' };

// Rate limiting
const RATE_LIMIT = 50;
const REQUEST_INTERVAL = 1000 / RATE_LIMIT;
let lastRequestTime = 0;

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Save holder history snapshot (one entry per day per token)
 */
async function saveHolderHistory(db, mint, totalHolders, realHolders) {
    try {
        await db.run(`
            INSERT INTO holder_history (mint, date, holders, real_holders)
            VALUES ($1, CURRENT_DATE, $2, $3)
            ON CONFLICT (mint, date) DO UPDATE SET
                holders = EXCLUDED.holders,
                real_holders = EXCLUDED.real_holders
        `, [mint, totalHolders, realHolders]);
    } catch (_e) {
        // Ignore errors (table might not exist on first run)
    }
}

/**
 * Save K-Score history snapshot (one entry per day per token)
 * Used for credit rating trajectory analysis (30/60/90 day trends)
 */
async function saveKScoreHistory(db, mint, kScore, convictionScore, holders) {
    try {
        await db.run(`
            INSERT INTO k_score_history (mint, date, k_score, conviction_score, holders)
            VALUES ($1, CURRENT_DATE, $2, $3, $4)
            ON CONFLICT (mint, date) DO UPDATE SET
                k_score = EXCLUDED.k_score,
                conviction_score = EXCLUDED.conviction_score,
                holders = EXCLUDED.holders
        `, [mint, kScore, convictionScore, holders]);
    } catch (_e) {
        // Ignore errors (table might not exist on first run)
    }
}

// ============================================
// DEX PROGRAMS - Filter out pools from holders
// ============================================

const DEX_PROGRAMS = new Set([
    // Raydium
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
    'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',
    '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h',
    // Orca
    'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
    // Meteora
    'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',
    // OpenBook/Serum
    'srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX',
    '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin',
    'opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb',
    // PumpFun
    '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',
    '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg',
    'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA',
]);

const KNOWN_POOL_WALLETS = new Set([
    'CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM', // PumpFun Fee
]);

// Pool cache (address -> { isPool, ts })
const poolCache = new Map();
const POOL_CACHE_TTL = 3600000; // 1 hour

// ============================================
// HELIUS API FUNCTIONS
// ============================================

async function rateLimitedFetch(url, options) {
    const now = Date.now();
    const timeSince = now - lastRequestTime;
    if (timeSince < REQUEST_INTERVAL) {
        await sleep(REQUEST_INTERVAL - timeSince);
    }
    lastRequestTime = Date.now();
    return fetch(url, options);
}

async function heliusRpc(method, params) {
    if (!HELIUS_API_KEY) return null;

    try {
        const response = await rateLimitedFetch(HELIUS_RPC_URL, {
            method: 'POST',
            headers: HELIUS_HEADERS,
            body: JSON.stringify({ jsonrpc: '2.0', id: Date.now(), method, params }),
        });
        const data = await response.json();
        if (data.error) throw new Error(data.error.message);
        return data.result;
    } catch (error) {
        logger.error(`[Helius] RPC error: ${error.message}`);
        return null;
    }
}

async function getEnhancedTransactions(address, options = {}) {
    if (!HELIUS_API_KEY) return [];

    // Note: Enhanced Transactions API (v0) requires API key in query string
    // Header auth returns 401 - this is a Helius API limitation
    const params = new URLSearchParams({ 'api-key': HELIUS_API_KEY });
    if (options.limit) params.append('limit', options.limit.toString());
    if (options.before) params.append('before', options.before);

    const url = `https://api-mainnet.helius-rpc.com/v0/addresses/${address}/transactions?${params}`;

    try {
        const response = await rateLimitedFetch(url, { method: 'GET' });
        if (!response.ok) return [];
        return await response.json();
    } catch (_error) {
        return [];
    }
}

// ============================================
// POOL FILTERING
// ============================================

async function batchCheckPools(addresses) {
    const results = new Map();
    const uncached = [];

    for (const addr of addresses) {
        if (KNOWN_POOL_WALLETS.has(addr)) {
            results.set(addr, true);
            continue;
        }
        const cached = poolCache.get(addr);
        if (cached && Date.now() - cached.ts < POOL_CACHE_TTL) {
            results.set(addr, cached.isPool);
        } else {
            uncached.push(addr);
        }
    }

    if (uncached.length > 0) {
        try {
            const infos = await heliusRpc('getMultipleAccounts', [uncached, { encoding: 'base64' }]);
            for (let i = 0; i < uncached.length; i++) {
                const addr = uncached[i];
                const info = infos?.value?.[i];
                const isPool = info ? DEX_PROGRAMS.has(info.owner) : false;
                results.set(addr, isPool);
                poolCache.set(addr, { isPool, ts: Date.now() });
            }
        } catch (_error) {
            for (const addr of uncached) {
                results.set(addr, false);
            }
        }
    }

    return results;
}

// ============================================
// CONVICTION CALCULATION (asdf-oracle logic)
// ============================================

async function fetchTokenHolders(mint) {
    const holders = [];
    let cursor = null;

    while (true) {
        const params = { mint, limit: 1000 };
        if (cursor) params.cursor = cursor;

        const result = await heliusRpc('getTokenAccounts', params);
        if (!result?.token_accounts) break;

        for (const acc of result.token_accounts) {
            if (acc.amount > 0) {
                holders.push({ address: acc.owner, balance: acc.amount });
            }
        }

        cursor = result.cursor;
        if (!cursor) break;
    }

    holders.sort((a, b) => b.balance - a.balance);
    return holders;
}

async function getHolderRetention(wallet, mint) {
    let firstBuyAmount = 0;
    let currentBalance = 0;
    let before = null;

    for (let page = 0; page < 5; page++) {
        const txs = await getEnhancedTransactions(wallet, { limit: 100, before });
        if (!txs || txs.length === 0) break;

        for (const tx of txs) {
            if (!tx.tokenTransfers) continue;
            for (const transfer of tx.tokenTransfers) {
                if (transfer.mint !== mint) continue;

                const amount = transfer.tokenAmount || 0;
                if (transfer.toUserAccount === wallet) {
                    currentBalance += amount;
                    firstBuyAmount = amount; // Going backwards, last seen = first buy
                }
                if (transfer.fromUserAccount === wallet) {
                    currentBalance -= amount;
                }
            }
        }

        before = txs[txs.length - 1]?.signature;
        if (!before || txs.length < 100) break;
    }

    if (currentBalance < 0) currentBalance = 0;
    return firstBuyAmount > 0 ? currentBalance / firstBuyAmount : 0;
}

function classifyRetention(retention) {
    if (retention >= 1.5) return 'accumulator';
    if (retention >= 1.0) return 'holder';
    if (retention >= 0.5) return 'reducer';
    return 'extractor';
}

/**
 * Calculate conviction score AND real holders count
 *
 * @param {string} mint - Token mint address
 * @param {number} priceUsd - Current token price in USD
 * @param {number} decimals - Token decimals (default 9 for SPL)
 * @returns {Object} { score, analyzed, accumulators, holders, reducers, extractors, realHoldersCount, totalHolders }
 */
async function calculateConvictionAndHolders(mint, priceUsd = 0, decimals = 9) {
    const TOP_HOLDERS = 20;
    const CANDIDATES = 50;
    const MIN_USD_VALUE = 1; // $1 minimum to count as "real holder"

    try {
        // 1. Fetch all holders
        const allHolders = await fetchTokenHolders(mint);
        if (allHolders.length === 0) {
            return { score: 0, analyzed: 0, realHoldersCount: 0, totalHolders: 0 };
        }

        // 2. Calculate real holders ($1+ balance)
        let realHoldersCount = 0;
        if (priceUsd > 0) {
            const divisor = Math.pow(10, decimals);
            for (const h of allHolders) {
                const usdValue = (h.balance / divisor) * priceUsd;
                if (usdValue >= MIN_USD_VALUE) {
                    realHoldersCount++;
                }
            }
        } else {
            // Fallback: count all holders with balance > 0
            realHoldersCount = allHolders.length;
        }

        logger.info(`[Holders] ${mint.slice(0,8)}: ${realHoldersCount} real ($1+) / ${allHolders.length} total`);

        // 3. Filter pools from top candidates for conviction analysis
        const candidates = allHolders.slice(0, CANDIDATES);
        const poolCheck = await batchCheckPools(candidates.map(h => h.address));

        const realCandidates = candidates.filter(h => !poolCheck.get(h.address));
        const poolsFiltered = candidates.length - realCandidates.length;

        logger.info(`[Conviction] ${mint.slice(0,8)}: ${poolsFiltered} pools filtered`);

        // 4. Analyze top 20 real holders for conviction
        const top20 = realCandidates.slice(0, TOP_HOLDERS);
        if (top20.length === 0) {
            return { score: 0, analyzed: 0, realHoldersCount, totalHolders: allHolders.length };
        }

        let accumulators = 0;
        let holders = 0;
        let reducers = 0;
        let extractors = 0;
        let analyzed = 0;

        for (const holder of top20) {
            try {
                const retention = await getHolderRetention(holder.address, mint);
                const classification = classifyRetention(retention);

                if (classification === 'accumulator') accumulators++;
                else if (classification === 'holder') holders++;
                else if (classification === 'reducer') reducers++;
                else if (classification === 'extractor') extractors++;

                analyzed++;
                await sleep(100); // Rate limit
            } catch (_e) {
                // Skip failed holders
            }
        }

        if (analyzed === 0) {
            return { score: 0, analyzed: 0, realHoldersCount, totalHolders: allHolders.length };
        }

        const score = Math.round(((accumulators + holders) / analyzed) * 100);

        logger.info(`[Conviction] ${mint.slice(0,8)}: ${score}% (${accumulators} acc, ${holders} hold, ${analyzed} total)`);

        return {
            score,
            analyzed,
            accumulators,
            holders,
            reducers,
            extractors,
            realHoldersCount,
            totalHolders: allHolders.length,
            allHolders,  // For burn calculation (avoid re-fetch)
            filteredTop50: realCandidates,  // For top10 calculation (pools removed)
            poolsFiltered
        };

    } catch (error) {
        logger.error(`[Conviction] Error for ${mint}: ${error.message}`);
        return {
            score: 0, analyzed: 0, accumulators: 0, holders: 0,
            reducers: 0, extractors: 0, realHoldersCount: 0, totalHolders: 0,
            allHolders: []
        };
    }
}

// Backwards compatibility alias
async function calculateConviction(mint) {
    return calculateConvictionAndHolders(mint, 0, 9);
}

// ============================================
// SECURITY CHECK (on-chain) - ELIMINATORY
// ============================================

// Known trusted program authorities (PumpFun, etc.)
// If mint/freeze authority belongs to a known program, it's OK
const _TRUSTED_AUTHORITIES = new Set([
    // PumpFun
    '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',  // PumpFun program
    'Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1', // PumpFun bonding curve
    'TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM',  // PumpFun token authority
    '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg', // PumpFun AMM
    // Raydium
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', // Raydium v4
    'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK', // Raydium CLMM
    // Meteora
    'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',
]);

/**
 * Check token security: mint authority, freeze authority
 * These are ELIMINATORY - if not secure, score is capped
 *
 * RULES:
 * - Mint/Freeze revoked = SAFE
 * - Mint/Freeze = trusted program (PumpFun, etc.) = OK
 * - Mint/Freeze = random wallet = DANGER
 *
 * SPL Token Mint Account Layout (82 bytes):
 *   [0-4]   mintAuthorityOption (u32: 0=None, 1=Some)
 *   [4-36]  mintAuthority (Pubkey if Some)
 *   [36-44] supply (u64)
 *   [44]    decimals (u8)
 *   [45]    isInitialized (bool)
 *   [46-50] freezeAuthorityOption (u32: 0=None, 1=Some)
 *   [50-82] freezeAuthority (Pubkey if Some)
 */
async function checkTokenSecurity(mint) {
    const security = {
        mintAuthorityRevoked: false,
        mintAuthorityTrusted: false,
        mintAuthority: null,
        freezeAuthorityRevoked: false,
        freezeAuthorityTrusted: false,
        freezeAuthority: null,
        isSecure: false,
        maxScore: 100
    };

    try {
        const accountInfo = await heliusRpc('getAccountInfo', [mint, { encoding: 'base64' }]);

        if (!accountInfo?.value?.data?.[0]) {
            logger.warn(`[Security] ${mint.slice(0,8)}: Could not fetch mint account`);
            return security;
        }

        const data = Buffer.from(accountInfo.value.data[0], 'base64');

        if (data.length < 82) {
            logger.warn(`[Security] ${mint.slice(0,8)}: Invalid mint account size`);
            return security;
        }

        // Parse mint authority (offset 0-36)
        const mintAuthOption = data.readUInt32LE(0);
        if (mintAuthOption === 0) {
            security.mintAuthorityRevoked = true;
        } else {
            // Extract pubkey (32 bytes at offset 4)
            const mintAuthPubkey = data.slice(4, 36).toString('hex');
            // Convert to base58 (simplified - just check against known addresses)
            security.mintAuthority = mintAuthPubkey;
            // For now, assume any active authority is a concern unless we can verify
            // TODO: proper base58 encoding to check against TRUSTED_AUTHORITIES
        }

        // Parse freeze authority (offset 46-82)
        const freezeAuthOption = data.readUInt32LE(46);
        if (freezeAuthOption === 0) {
            security.freezeAuthorityRevoked = true;
        } else {
            const freezeAuthPubkey = data.slice(50, 82).toString('hex');
            security.freezeAuthority = freezeAuthPubkey;
        }

        // Determine security level
        // For now: be lenient - only penalize if BOTH are active (non-revoked)
        // Because most PumpFun tokens have mint authority during bonding curve
        const mintSafe = security.mintAuthorityRevoked;
        const freezeSafe = security.freezeAuthorityRevoked;

        if (mintSafe && freezeSafe) {
            security.isSecure = true;
            security.maxScore = 100;
            logger.info(`[Security] ${mint.slice(0,8)}: ✓ FULLY SECURE (both revoked)`);
        } else if (mintSafe && !freezeSafe) {
            // Freeze authority active - moderate risk
            security.maxScore = 70;
            logger.info(`[Security] ${mint.slice(0,8)}: ⚠ Freeze active → cap 70`);
        } else if (!mintSafe && freezeSafe) {
            // Mint authority active - could be PumpFun bonding curve
            // Be lenient for now (many legit tokens have this)
            security.maxScore = 80;
            logger.info(`[Security] ${mint.slice(0,8)}: ⚠ Mint active (may be bonding curve) → cap 80`);
        } else {
            // Both active - higher risk
            security.maxScore = 50;
            logger.info(`[Security] ${mint.slice(0,8)}: ⚠ Both authorities active → cap 50`);
        }

        return security;

    } catch (e) {
        logger.error(`[Security] ${mint.slice(0,8)}: ${e.message}`);
        return security;
    }
}

// ============================================
// BURN CALCULATION (on-chain)
// ============================================

/**
 * Calculate burn percentage from on-chain data
 * Checks for tokens held by burn addresses
 */
async function calculateBurn(mint, allHolders = null) {
    try {
        // Get total supply
        const supply = await heliusRpc('getTokenSupply', [mint]);
        if (!supply) return { burnPct: 0, burned: 0, totalSupply: 0 };

        const totalSupply = Number(supply.value.amount);
        const decimals = supply.value.decimals;

        // Use provided holders or fetch them
        const holders = allHolders || (await fetchTokenHolders(mint));

        // Find burned tokens (held by burn addresses)
        let burnedAmount = 0;
        for (const h of holders) {
            const owner = h.address;
            // Check burn address patterns
            if (owner.startsWith('1111111') ||
                owner === '11111111111111111111111111111111' ||
                owner.includes('1nc1nerator')) {
                burnedAmount += h.balance;
            }
        }

        const burnPct = totalSupply > 0 ? (burnedAmount / totalSupply) * 100 : 0;

        logger.info(`[Burn] ${mint.slice(0,8)}: ${burnPct.toFixed(2)}% burned`);

        return {
            burnPct,
            burned: burnedAmount / Math.pow(10, decimals),
            totalSupply: totalSupply / Math.pow(10, decimals),
            decimals
        };
    } catch (e) {
        logger.error(`[Burn] Error for ${mint}: ${e.message}`);
        return { burnPct: 0, burned: 0, totalSupply: 0 };
    }
}

// ============================================
// LP BURN/LOCK CHECK (on-chain)
// ============================================

// Known burn addresses for LP tokens
const LP_BURN_ADDRESSES = new Set([
    '1111111111111111111111111111111111',
    '1nc1nerator11111111111111111111111',
    // PumpFun burn
    '1PUMPkr5FmyKcPZTPvGxJVa8P2LPtJNgxvUSSc1pump',
]);

// Known LP locker programs
const LP_LOCKER_PROGRAMS = new Set([
    // Streamflow
    'strmRqUCoQUgGUan5YhzUZa6KqdzwX5L6FpUxfmKg5m',
    // Uncx
    'LockKXdYQVMbhhckwH3BxoYJ9FYatcZjwNGVuNY1Po4',
    // Team Finance
    'TLoCKic2gGJm7VhZKumih4Lc35fUhYqVMgA4j389Buk',
]);

// Known launchpads with auto LP burn on graduation
const _LAUNCHPAD_DEXES = new Set([
    'pumpswap',
    'pump-fun',
    'moonshot',   // Moonshot also burns LP
]);

/**
 * Check if LP tokens are burned or locked
 * Returns lpBurnPct (0-100) and affects security cap
 *
 * @param {Object} db - Database connection
 * @param {string} mint - Token mint address
 * @returns {Object} { lpBurnPct, lpLockedPct, lpStatus, maxScoreModifier }
 */
async function checkLPStatus(db, mint) {
    const result = {
        lpBurnPct: 0,
        lpLockedPct: 0,
        lpStatus: 'unknown',
        maxScoreModifier: 0,  // 0 = no change, negative = penalty
        checkedPools: 0
    };

    try {
        // Check if token has ANY launchpad pool (indicates graduation with auto LP burn)
        const launchpadCheck = await db.get(`
            SELECT dex FROM pools
            WHERE mint = $1 AND dex IN ('pumpswap', 'pump-fun', 'moonshot')
            LIMIT 1
        `, [mint]);

        if (launchpadCheck) {
            // Launchpad graduated = LP burned by protocol
            result.lpBurnPct = 100;
            result.lpStatus = `burned (${launchpadCheck.dex})`;
            result.maxScoreModifier = 0;
            logger.info(`[LP] ${mint.slice(0,8)}: ✓ Graduated via ${launchpadCheck.dex} (LP auto-burned)`);
            return result;
        }

        // Don't trust, verify: Community verification is a hint, not proof
        // We still check LP status on-chain below, but reduce penalty for verified tokens
        const tokenCheck = await db.get(`
            SELECT hascommunityupdate FROM tokens WHERE mint = $1
        `, [mint]);
        const isCommunityVerified = tokenCheck?.hascommunityupdate || false;

        // Get main pools for other checks
        const pools = await db.all(`
            SELECT address, dex, liquidity_usd
            FROM pools
            WHERE mint = $1
            ORDER BY liquidity_usd DESC NULLS LAST
            LIMIT 3
        `, [mint]);

        if (!pools || pools.length === 0) {
            logger.info(`[LP] ${mint.slice(0,8)}: No pools found`);
            return result;
        }

        // For other DEXes, try to check LP token holders
        const mainPool = pools[0];
        result.checkedPools = 1;

        // Try to find LP mint from pool account (Raydium v4 layout)
        if (mainPool.dex === 'raydium' || mainPool.dex === 'raydium-clmm') {
            const lpStatus = await checkRaydiumLPBurn(mainPool.address);
            if (lpStatus) {
                result.lpBurnPct = lpStatus.burnPct;
                result.lpLockedPct = lpStatus.lockedPct;
                result.lpStatus = lpStatus.status;
            }
        }

        // Determine score modifier based on LP status (on-chain verification)
        const totalSecured = result.lpBurnPct + result.lpLockedPct;
        if (totalSecured >= 90) {
            result.lpStatus = 'secured';
            result.maxScoreModifier = 0;
        } else if (totalSecured >= 50) {
            result.lpStatus = 'partial';
            result.maxScoreModifier = -10;
        } else if (totalSecured > 0) {
            result.lpStatus = 'low';
            result.maxScoreModifier = -20;
        } else {
            result.lpStatus = 'unsecured';
            // Community verification reduces penalty but doesn't bypass on-chain check
            result.maxScoreModifier = isCommunityVerified ? -15 : -30;
        }

        const verifiedNote = isCommunityVerified ? ' (community verified, reduced penalty)' : '';
        logger.info(`[LP] ${mint.slice(0,8)}: ${result.lpBurnPct.toFixed(0)}% burn, ${result.lpLockedPct.toFixed(0)}% locked → ${result.lpStatus}${verifiedNote}`);

        return result;

    } catch (e) {
        logger.error(`[LP] ${mint.slice(0,8)}: ${e.message}`);
        return result;
    }
}

/**
 * Check Raydium pool LP burn status
 * Raydium AMM v4 pool layout has lpMint at specific offset
 */
async function checkRaydiumLPBurn(poolAddress) {
    try {
        // Get pool account to find LP mint
        const poolInfo = await heliusRpc('getAccountInfo', [poolAddress, { encoding: 'base64' }]);

        if (!poolInfo?.value?.data?.[0]) {
            return null;
        }

        const data = Buffer.from(poolInfo.value.data[0], 'base64');

        // Raydium AMM v4: lpMint is at offset 400 (32 bytes)
        // This is approximate - may need adjustment
        if (data.length < 432) {
            return null;
        }

        // Extract LP mint pubkey (simplified - need proper base58 encoding)
        // For now, try to get LP holders directly if we know the LP mint
        // TODO: Implement proper Raydium pool parsing

        return null; // Fallback for now

    } catch (_e) {
        return null;
    }
}

/**
 * Check LP token holders for burn/lock status
 */
async function _checkLPHolders(lpMint) {
    try {
        const holders = await fetchTokenHolders(lpMint);
        if (!holders || holders.length === 0) {
            return { burnPct: 0, lockedPct: 0, status: 'no-holders' };
        }

        const totalBalance = holders.reduce((sum, h) => sum + h.balance, 0);
        let burnedBalance = 0;
        let lockedBalance = 0;

        for (const h of holders) {
            // Check if holder is a burn address
            if (h.address.startsWith('1111111') || LP_BURN_ADDRESSES.has(h.address)) {
                burnedBalance += h.balance;
                continue;
            }

            // Check if holder is a locker program
            // Need to check the owner of the token account
            // For now, check against known locker addresses
            if (LP_LOCKER_PROGRAMS.has(h.address)) {
                lockedBalance += h.balance;
            }
        }

        const burnPct = totalBalance > 0 ? (burnedBalance / totalBalance) * 100 : 0;
        const lockedPct = totalBalance > 0 ? (lockedBalance / totalBalance) * 100 : 0;

        let status = 'unsecured';
        if (burnPct >= 90) status = 'burned';
        else if (burnPct + lockedPct >= 90) status = 'secured';
        else if (burnPct + lockedPct >= 50) status = 'partial';

        return { burnPct, lockedPct, status };

    } catch (_e) {
        return { burnPct: 0, lockedPct: 0, status: 'error' };
    }
}

// ============================================
// ON-CHAIN LIQUIDITY (via vault balances)
// ============================================

const SOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const USDT_MINT = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB';

/**
 * Get token account balance from RPC
 */
async function _getTokenAccountBalance(vaultAddress) {
    try {
        const result = await heliusRpc('getAccountInfo', [vaultAddress, { encoding: 'base64' }]);
        if (!result || !result.value || !result.value.data) return null;

        // SPL Token account: amount at offset 64 (8 bytes little-endian)
        const data = Buffer.from(result.value.data[0], 'base64');
        if (data.length < 72) return null;

        const amount = data.readBigUInt64LE(64);
        return Number(amount);
    } catch (_e) {
        return null;
    }
}

/**
 * Calculate on-chain liquidity for a token
 * Uses vault balances from the highest liquidity pool
 */
async function _calculateOnChainLiquidity(db, mint, _solPrice) {
    try {
        // Get pools for this token paired with SOL/USDC/USDT
        const pools = await db.all(`
            SELECT address, dex, token_a, token_b, liquidity_usd
            FROM pools
            WHERE mint = $1
              AND (token_b = $2 OR token_b = $3 OR token_b = $4
                   OR token_a = $2 OR token_a = $3 OR token_a = $4)
            ORDER BY liquidity_usd DESC NULLS LAST
            LIMIT 5
        `, [mint, SOL_MINT, USDC_MINT, USDT_MINT]);

        if (!pools || pools.length === 0) {
            // Fallback to cached liquidity
            const token = await db.get('SELECT liquidity FROM tokens WHERE mint = $1', [mint]);
            return { liquidity: token?.liquidity || 0, source: 'db_cache' };
        }

        // For now, use the cached liquidity_usd from pools (DexScreener sync)
        // TODO: Calculate from vault balances when we have vault addresses
        const totalLiquidity = pools.reduce((sum, p) => sum + (p.liquidity_usd || 0), 0);

        logger.info(`[Liquidity] ${mint.slice(0,8)}: $${totalLiquidity.toFixed(0)} (${pools.length} pools)`);

        return {
            liquidity: totalLiquidity,
            source: 'pools',
            poolCount: pools.length
        };
    } catch (e) {
        logger.error(`[Liquidity] Error for ${mint}: ${e.message}`);
        return { liquidity: 0, source: 'error' };
    }
}

// ============================================
// K-SCORE v8 - PURE MATHEMATICAL FORMULA
// ============================================
//
// DESIGN PRINCIPLES:
// 1. Asymptotic normalization (scales to infinity, never reaches 1)
// 2. No arbitrary min/max caps
// 3. Naturally bounded functions [0,1]
// 4. Weighted geometric mean (penalizes weaknesses)
// 5. Sum of weights = 1.00 exactly
//
// MATHEMATICAL PROPERTIES:
// - H(holders) = 1 - 1/(1 + ln(1 + h/κ))     κ=100 inflection
// - A(age)     = 1 - e^(-t/τ)                 τ=21 days half-life
// - T(top10)   = 1 - (concentration/100)      linear inverse
// - C(conviction) = score/100                 direct mapping
// - R(ratio)   = tanh(r/2)                    naturally bounded
//
// FORMULA:
// K = 100 × DiamondHands^0.50 × OrganicGrowth^0.35 × Longevity^0.15
//
// Where:
//   DiamondHands  = √(C × R)   [conviction × acc/ext ratio]
//   OrganicGrowth = √(H × T)   [holders × distribution]
//   Longevity     = A          [age]
//
// ============================================

/**
 * Asymptotic holder normalization
 * H(h) = 1 - 1/(1 + ln(1 + h/κ))
 *
 * Properties:
 * - h=0     → 0.00
 * - h=100   → 0.41 (inflection point κ)
 * - h=1000  → 0.70
 * - h=10000 → 0.82
 * - h=100k  → 0.88
 * - h→∞     → 1.00 (asymptote)
 */
function normalizeHolders(holders, kappa = 100) {
    if (holders <= 0) return 0;
    return 1 - 1 / (1 + Math.log(1 + holders / kappa));
}

/**
 * Exponential decay age normalization
 * A(t) = 1 - e^(-t/τ)
 *
 * Properties:
 * - t=0   → 0.00
 * - t=7   → 0.28 (1 week)
 * - t=21  → 0.63 (τ = half-life)
 * - t=60  → 0.94
 * - t=90  → 0.99
 * - t→∞   → 1.00 (asymptote)
 */
function normalizeAge(ageDays, tau = 21) {
    if (ageDays <= 0) return 0;
    return 1 - Math.exp(-ageDays / tau);
}

/**
 * Linear inverse concentration
 * T(c) = max(0, 1 - c/100)
 *
 * Properties:
 * - 0%   → 1.00 (perfect distribution)
 * - 20%  → 0.80
 * - 50%  → 0.50
 * - 80%  → 0.20
 * - 100% → 0.00 (single holder)
 */
function normalizeTop10(concentrationPct) {
    return Math.max(0, 1 - concentrationPct / 100);
}

/**
 * Hyperbolic tangent ratio normalization
 * R(r) = tanh(r/2)
 *
 * Properties:
 * - r=0   → 0.00 (no accumulators)
 * - r=0.5 → 0.24
 * - r=1   → 0.46 (balanced)
 * - r=2   → 0.76 (acc dominant)
 * - r=5   → 0.99
 * - r→∞   → 1.00 (asymptote)
 */
function normalizeAccExtRatio(ratio) {
    if (ratio <= 0) return 0;
    return Math.tanh(ratio / 2);
}

/**
 * Direct linear normalization for conviction [0-100] → [0-1]
 */
function normalizeConviction(score) {
    return Math.max(0, Math.min(1, score / 100));
}

/**
 * Safe geometric mean of two values
 * Returns √(a × b), with epsilon floor to avoid zero collapse
 */
function geometricMean2(a, b, epsilon = 0.001) {
    return Math.sqrt(Math.max(a, epsilon) * Math.max(b, epsilon));
}

// Pillar weights (MUST sum to 1.00)
const WEIGHTS = {
    DIAMOND_HANDS: 0.50,   // 50% - conviction behavior
    ORGANIC_GROWTH: 0.35,  // 35% - holder distribution
    LONGEVITY: 0.15        // 15% - survival/maturity
};

// Verify weights sum to 1
const WEIGHT_SUM = Object.values(WEIGHTS).reduce((a, b) => a + b, 0);
if (Math.abs(WEIGHT_SUM - 1.0) > 0.001) {
    throw new Error(`K-Score weights must sum to 1.0, got ${WEIGHT_SUM}`);
}

// EMA Smoothing config
// newScore = α × calculated + (1-α) × previous
// α = 0.3 means 30% new data, 70% historical (smooth but responsive)
const EMA_ALPHA = 0.3;

/**
 * Apply Exponential Moving Average smoothing to K-Score
 * Prevents wild swings from single data point changes
 *
 * @param {number} calculated - Newly calculated score
 * @param {number} previous - Previous stored score
 * @param {number} alpha - Smoothing factor (0-1), higher = more reactive
 * @returns {number} Smoothed score
 */
function applyEMA(calculated, previous, alpha = EMA_ALPHA) {
    // First calculation or no previous score
    if (!previous || previous <= 0) {
        return calculated;
    }

    const smoothed = Math.round(alpha * calculated + (1 - alpha) * previous);

    // Log significant smoothing adjustments
    const diff = Math.abs(calculated - previous);
    if (diff >= 5) {
        logger.info(`[EMA] Smoothed ${calculated} → ${smoothed} (prev: ${previous}, Δ${diff})`);
    }

    return smoothed;
}

async function computeScoreInternal(mint, dbData = null, skipConviction = false, db = null) {
    // Raw metrics
    const raw = {
        holders: 0,
        ageDays: 0,
        top10Pct: 50,  // default: assume 50% if unknown
        conviction: 0,
        accExtRatio: 0
    };

    // Normalized metrics [0-1]
    const normalized = {
        H: 0,  // holders
        A: 0,  // age
        T: 0,  // top10 (inverted)
        C: 0,  // conviction
        R: 0   // acc/ext ratio
    };

    // Pillars
    const pillars = {
        diamondHands: 0,
        organicGrowth: 0,
        longevity: 0
    };

    let convictionData = null;
    let burnData = null;
    let securityData = null;

    try {
        const decimals = dbData?.decimals || 9;

        // Get price for $1+ holder filtering
        let priceUsd = 0;
        if (db) {
            const priceData = await priceService.getPrice(db, mint, decimals);
            priceUsd = priceData.priceUsd;
        } else {
            priceUsd = dbData?.priceusd || dbData?.priceUsd || 0;
        }

        // ============================================
        // SECURITY CHECK (ELIMINATORY)
        // ============================================

        let lpData = null;

        if (HELIUS_API_KEY) {
            securityData = await checkTokenSecurity(mint);
        }

        // Check LP burn/lock status
        if (db) {
            lpData = await checkLPStatus(db, mint);
        }

        // ============================================
        // FETCH ON-CHAIN DATA
        // ============================================

        if (!skipConviction && HELIUS_API_KEY) {
            convictionData = await calculateConvictionAndHolders(mint, priceUsd, decimals);

            // For top10 calculation
            burnData = await calculateBurn(mint, convictionData.allHolders || []);
        }

        // Get age from token timestamp (DexScreener creation date)
        if (db) {
            const tokenData = await db.get(`
                SELECT timestamp FROM tokens WHERE mint = $1
            `, [mint]);

            if (tokenData?.timestamp) {
                const ts = Number(tokenData.timestamp);
                const now = Date.now();
                const year2020 = 1577836800000; // Jan 1 2020

                // Validate timestamp is reasonable (between 2020 and now)
                if (ts > year2020 && ts < now) {
                    raw.ageDays = (now - ts) / 86400000;
                    logger.info(`[Age] ${mint.slice(0,8)}: ${raw.ageDays.toFixed(1)} days`);
                }
            }
        }

        // Fallback: minimum 1 day if no timestamp
        if (raw.ageDays === 0) {
            raw.ageDays = 1;
            logger.info(`[Age] ${mint.slice(0,8)}: defaulting to 1 day`);
        }

        // ============================================
        // EXTRACT RAW METRICS
        // ============================================

        if (convictionData) {
            raw.holders = convictionData.realHoldersCount || 0;
            raw.conviction = convictionData.score || 0;

            // Acc/Ext ratio
            const acc = convictionData.accumulators || 0;
            const ext = convictionData.extractors || 0;
            raw.accExtRatio = ext > 0 ? acc / ext : (acc > 0 ? 10 : 0);

            // Top10 concentration (using filtered holders, pools removed)
            const filteredHolders = convictionData.filteredTop50 || convictionData.allHolders || [];
            if (filteredHolders.length >= 10 && burnData?.totalSupply > 0) {
                const top10Balance = filteredHolders.slice(0, 10).reduce((s, h) => s + h.balance, 0);
                const totalSupplyRaw = burnData.totalSupply * Math.pow(10, burnData.decimals || 9);
                raw.top10Pct = (top10Balance / totalSupplyRaw) * 100;
            }
        }

        // ============================================
        // NORMALIZE ALL METRICS [0-1]
        // ============================================

        normalized.H = normalizeHolders(raw.holders);
        normalized.A = normalizeAge(raw.ageDays);
        normalized.T = normalizeTop10(raw.top10Pct);
        normalized.C = normalizeConviction(raw.conviction);
        normalized.R = normalizeAccExtRatio(raw.accExtRatio);

        // ============================================
        // CALCULATE 3 PILLARS
        // ============================================

        // Diamond Hands = √(Conviction × AccExtRatio)
        pillars.diamondHands = geometricMean2(normalized.C, normalized.R);

        // Organic Growth = √(Holders × Top10Distribution)
        pillars.organicGrowth = geometricMean2(normalized.H, normalized.T);

        // Longevity = Age (direct)
        pillars.longevity = normalized.A;

        // ============================================
        // FINAL SCORE: Weighted Geometric Mean
        // K = 100 × D^0.50 × O^0.35 × L^0.15
        // ============================================

        const epsilon = 0.001;
        let score = Math.round(
            100 *
            Math.pow(Math.max(pillars.diamondHands, epsilon), WEIGHTS.DIAMOND_HANDS) *
            Math.pow(Math.max(pillars.organicGrowth, epsilon), WEIGHTS.ORGANIC_GROWTH) *
            Math.pow(Math.max(pillars.longevity, epsilon), WEIGHTS.LONGEVITY)
        );

        // ============================================
        // LEGACY FLOOR (harmonic, asymptotic)
        // ============================================
        // "The rock still shines even when under dirt" - Dior
        //
        // Reuses existing normalized values (scales to infinity):
        //   A = age normalization (asymptotic)
        //   H = holders normalization (asymptotic)
        //
        // LegacyStrength = sqrt(A * H)  [geometric mean of survival factors]
        // LegacyFloor = MAX_FLOOR * LegacyStrength
        //
        // Applied BEFORE security cap (security always wins)

        const LEGACY_MAX_FLOOR = 55;  // Asymptotic maximum floor

        const legacyStrength = Math.sqrt(normalized.A * normalized.H);
        const legacyFloor = Math.round(LEGACY_MAX_FLOOR * legacyStrength);

        const uncappedScore = score;

        if (score < legacyFloor) {
            logger.info(`[Legacy] ${mint.slice(0,8)}: ${score} -> ${legacyFloor} (strength: ${(legacyStrength * 100).toFixed(0)}%)`);
            score = legacyFloor;
        }

        // ============================================
        // APPLY SECURITY CAP (ELIMINATORY)
        // ============================================
        // Security ALWAYS wins - applied AFTER legacy floor

        // 1. Authority cap (mint/freeze)
        const authorityCap = securityData?.maxScore || 100;

        // 2. LP cap modifier (0 or negative)
        const lpModifier = lpData?.maxScoreModifier || 0;

        // Final cap = authority cap + LP modifier (can go down)
        const finalCap = Math.max(0, authorityCap + lpModifier);

        if (score > finalCap) {
            score = finalCap;
        }

        // Log breakdown
        const wasCapped = score < uncappedScore;
        logger.info(`[K-Score v8] ${mint.slice(0,8)}: ${score}${wasCapped ? ` (capped from ${uncappedScore})` : ''}`);
        logger.info(`  Diamond Hands: ${(pillars.diamondHands * 100).toFixed(0)}% (C:${(normalized.C * 100).toFixed(0)}% R:${(normalized.R * 100).toFixed(0)}%)`);
        logger.info(`  Organic Growth: ${(pillars.organicGrowth * 100).toFixed(0)}% (H:${(normalized.H * 100).toFixed(0)}% T:${(normalized.T * 100).toFixed(0)}%)`);
        logger.info(`  Longevity: ${(pillars.longevity * 100).toFixed(0)}% (${raw.ageDays.toFixed(1)}d)`);
        if (securityData) {
            const secStatus = securityData.isSecure ? '✓' : `cap=${authorityCap}`;
            logger.info(`  Security: ${secStatus}`);
        }
        if (lpData) {
            logger.info(`  LP: ${lpData.lpStatus} (${lpData.lpBurnPct.toFixed(0)}% burn)`);
        }

        return {
            score: Math.min(100, Math.max(0, score)),
            uncappedScore,
            finalCap,
            conviction: convictionData,
            burn: burnData,
            security: securityData,
            lp: lpData,
            raw,
            normalized,
            pillars
        };

    } catch (e) {
        logger.error(`[K-Score] Calc error ${mint}: ${e.message}`);
        return { score: 0, conviction: null, burn: null, metrics: {}, breakdown: {} };
    }
}

// ============================================
// PUBLIC API
// ============================================

/**
 * Update single token score immediately (for admin approval)
 */
async function updateSingleToken(deps, mint) {
    const { db, broadcast } = deps;
    try {
        const token = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
        if (!token) return;

        logger.info(`[K-Score] Immediate calc for ${token.name || token.symbol}`);
        const result = await computeScoreInternal(mint, token, false, db);
        const conviction = result.conviction || {};

        // Apply EMA smoothing to prevent wild swings
        const previousScore = token.k_score || 0;
        const smoothedScore = applyEMA(result.score, previousScore);

        await db.run(`
            UPDATE tokens
            SET k_score = $1,
                last_k_score_update = $2,
                conviction_score = $3,
                conviction_accumulators = $4,
                conviction_holders = $5,
                conviction_reducers = $6,
                conviction_extractors = $7,
                conviction_analyzed = $8,
                holders = $9,
                last_holder_check = $10
            WHERE mint = $11
        `, [
            smoothedScore,
            Date.now().toString(),
            conviction.score || 0,
            conviction.accumulators || 0,
            conviction.holders || 0,
            conviction.reducers || 0,
            conviction.extractors || 0,
            conviction.analyzed || 0,
            conviction.realHoldersCount || 0,
            Date.now().toString(),
            mint
        ]);

        // Save holder history snapshot (daily)
        await saveHolderHistory(db, mint, conviction.totalHolders || 0, conviction.realHoldersCount || 0);

        // Save K-Score history snapshot (daily) for credit rating trajectory
        await saveKScoreHistory(db, mint, smoothedScore, conviction.score || 0, conviction.realHoldersCount || 0);

        // Broadcast K-Score update via WebSocket (use smoothed score)
        if (broadcast) {
            broadcast.kscoreUpdate(mint, {
                kScore: smoothedScore,
                conviction: conviction.score || 0,
                accumulators: conviction.accumulators || 0,
                holders: conviction.realHoldersCount || 0,
                timestamp: Date.now()
            });
        }

        // Return with smoothed score
        result.score = smoothedScore;
        result.rawScore = result.uncappedScore; // Keep raw for debugging
        return result;
    } catch (e) {
        logger.error(`[K-Score] Failed single update for ${mint}:`, e);
        return 0;
    }
}

/**
 * Batch update K-Scores (scheduled task)
 */
async function updateKScores(deps) {
    const { db, broadcast } = deps;

    logger.info("[K-Score v5] Starting cycle...");

    try {
        // Only calculate K-Score for verified tokens (saves Helius API credits)
        const tokens = await db.all(`
            SELECT * FROM tokens
            WHERE hascommunityupdate = TRUE
            ORDER BY volume24h DESC NULLS LAST
        `);

        if (!tokens || tokens.length === 0) {
            logger.info("[K-Score] No eligible tokens.");
            return;
        }

        logger.info(`[K-Score] Updating ${tokens.length} tokens...`);

        for (const t of tokens) {
            try {
                const result = await computeScoreInternal(t.mint, t, false, db);
                const conviction = result.conviction || {};

                // Apply EMA smoothing to prevent wild swings
                const previousScore = t.k_score || 0;
                const smoothedScore = applyEMA(result.score, previousScore);

                await db.run(`
                    UPDATE tokens
                    SET k_score = $1,
                        last_k_score_update = $2,
                        conviction_score = $3,
                        conviction_accumulators = $4,
                        conviction_holders = $5,
                        conviction_reducers = $6,
                        conviction_extractors = $7,
                        conviction_analyzed = $8,
                        holders = $9,
                        last_holder_check = $10
                    WHERE mint = $11
                `, [
                    smoothedScore,
                    Date.now().toString(),
                    conviction.score || 0,
                    conviction.accumulators || 0,
                    conviction.holders || 0,
                    conviction.reducers || 0,
                    conviction.extractors || 0,
                    conviction.analyzed || 0,
                    conviction.realHoldersCount || 0,
                    Date.now().toString(),
                    t.mint
                ]);

                // Save holder history snapshot (daily)
                await saveHolderHistory(db, t.mint, conviction.totalHolders || 0, conviction.realHoldersCount || 0);

                // Save K-Score history snapshot (daily) for credit rating trajectory
                await saveKScoreHistory(db, t.mint, smoothedScore, conviction.score || 0, conviction.realHoldersCount || 0);

                // Broadcast K-Score update via WebSocket (use smoothed score)
                if (broadcast) {
                    broadcast.kscoreUpdate(t.mint, {
                        kScore: smoothedScore,
                        symbol: t.symbol,
                        conviction: conviction.score || 0,
                        accumulators: conviction.accumulators || 0,
                        holders: conviction.realHoldersCount || 0,
                        timestamp: Date.now()
                    });
                }

            } catch (err) {
                logger.warn(`[K-Score] Failed for ${t.mint}: ${err.message}`);
            }

            await sleep(500); // Slower between tokens (conviction is heavy)
        }

        logger.info("[K-Score v5] Cycle complete.");

    } catch (e) {
        logger.error("[K-Score] Cycle error", e);
    }
}

function start(deps) {
    if (!HELIUS_API_KEY) {
        logger.warn("[K-Score] No HELIUS_API_KEY - conviction analysis disabled");
    }

    // Run every 10 minutes
    setInterval(() => updateKScores(deps), 600000);
    // Run once after startup (delay 30s to let other services init)
    setTimeout(() => updateKScores(deps), 30000);
}

module.exports = {
    start,
    updateSingleToken,
    calculateTokenScore: async (mint) => {
        const result = await computeScoreInternal(mint, null, true);
        return result.score;
    },
    calculateConviction, // Exposed for testing
};
