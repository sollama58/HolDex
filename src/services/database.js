const { Pool } = require('pg');
const config = require('../config/env');
const logger = require('./logger');
const { getClient } = require('./redis');
// We require this dynamically inside functions if needed to avoid circular deps,
// or pass it in. For aggregateAndSaveToken, we need it.
const { getHolderCountFromRPC } = require('./solana');
const { signMarket, signKScore } = require('../utils/dataSignature'); 

let primaryPool = null;
let readPool = null; 
let dbWrapper = null;
let initPromise = null;

const pendingRequests = new Map();

async function initDB() {
    if (dbWrapper) return dbWrapper;
    if (initPromise) return initPromise;

    initPromise = (async () => {
        try {
            const isLocal = config.DATABASE_URL.includes('localhost') || config.DATABASE_URL.includes('127.0.0.1');

            // SECURITY: SSL Configuration (H1)
            // In production, prefer SSL verification. Set DB_SSL_REJECT_UNAUTHORIZED=true for strict mode.
            // Default to false for Render's managed Postgres (uses internal certs)
            const strictSsl = process.env.DB_SSL_REJECT_UNAUTHORIZED === 'true';
            let sslConfig;

            if (isLocal) {
                sslConfig = false;
            } else {
                sslConfig = { rejectUnauthorized: strictSsl };
                if (!strictSsl && config.NODE_ENV === 'production') {
                    logger.warn('⚠️ SECURITY: SSL certificate verification disabled (DB_SSL_REJECT_UNAUTHORIZED != true)');
                    logger.warn('⚠️ Set DB_SSL_REJECT_UNAUTHORIZED=true in production for MITM protection');
                }
            }

            // SCALABILITY FIX: Reduced max connections from 50 to 10.
            // With 3 services (API, Worker, Listener) running, 50 * 3 = 150 connections
            // which exceeds standard Render/Postgres limits (usually 100).
            primaryPool = new Pool({
                connectionString: config.DATABASE_URL,
                ssl: sslConfig,
                max: 10, 
                idleTimeoutMillis: 30000,
                connectionTimeoutMillis: 5000,
            });

            primaryPool.on('error', (err) => logger.error(`Unexpected error on Primary DB: ${err.message}`));
            const client = await primaryPool.connect();
            client.release();
            logger.info(`💾 Database: Connection Successful.`);

            if (process.env.READ_DATABASE_URL) {
                readPool = new Pool({ connectionString: process.env.READ_DATABASE_URL, ssl: sslConfig });
            } else {
                readPool = primaryPool;
            }

            // --- SCHEMA DEFINITIONS ---
            await primaryPool.query(`
                CREATE TABLE IF NOT EXISTS tokens (
                    mint TEXT PRIMARY KEY,
                    name TEXT,
                    symbol TEXT,
                    image TEXT,
                    supply TEXT,
                    decimals INTEGER DEFAULT 9,
                    priceUsd DOUBLE PRECISION,
                    liquidity DOUBLE PRECISION,
                    marketCap DOUBLE PRECISION,
                    volume24h DOUBLE PRECISION,
                    change24h DOUBLE PRECISION,
                    change1h DOUBLE PRECISION,
                    change5m DOUBLE PRECISION,
                    holders INTEGER DEFAULT 0,
                    last_holder_check BIGINT DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    -- K-Score Columns
                    k_score DOUBLE PRECISION DEFAULT 0,
                    last_k_score_update BIGINT DEFAULT 0,
                    
                    hasCommunityUpdate BOOLEAN DEFAULT FALSE,
                    metadata TEXT,
                    timestamp BIGINT
                );
                
                -- Stores daily snapshots of holder counts for Trend Calculation
                CREATE TABLE IF NOT EXISTS holders_history (
                    mint TEXT,
                    count INTEGER,
                    timestamp BIGINT,
                    PRIMARY KEY (mint, timestamp)
                );

                -- Detailed Pool Info for Liquidity Analysis
                CREATE TABLE IF NOT EXISTS pools (
                    address TEXT PRIMARY KEY,
                    mint TEXT,
                    dex TEXT,
                    token_a TEXT NOT NULL,
                    token_b TEXT NOT NULL,
                    reserve_a TEXT,
                    reserve_b TEXT,
                    price_usd DOUBLE PRECISION DEFAULT 0,
                    liquidity_usd DOUBLE PRECISION DEFAULT 0,
                    volume_24h DOUBLE PRECISION DEFAULT 0,
                    created_at BIGINT
                );
                
                CREATE TABLE IF NOT EXISTS candles_1m (
                    pool_address TEXT,
                    timestamp BIGINT,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume DOUBLE PRECISION,
                    PRIMARY KEY (pool_address, timestamp)
                );

                CREATE TABLE IF NOT EXISTS active_trackers ( pool_address TEXT PRIMARY KEY, priority INTEGER DEFAULT 1, last_check BIGINT DEFAULT 0 );
                CREATE TABLE IF NOT EXISTS token_updates ( id SERIAL PRIMARY KEY, mint TEXT, twitter TEXT, website TEXT, telegram TEXT, banner TEXT, description TEXT, submittedAt BIGINT, status TEXT DEFAULT 'pending', signature TEXT, payer TEXT );
                CREATE TABLE IF NOT EXISTS api_keys ( key_hash TEXT PRIMARY KEY, key_prefix TEXT, owner TEXT, wallet TEXT, tier TEXT DEFAULT 'free', requests_limit INTEGER DEFAULT 1000, requests_today INTEGER DEFAULT 0, last_reset BIGINT DEFAULT 0, is_active BOOLEAN DEFAULT TRUE, created_at BIGINT );

                CREATE TABLE IF NOT EXISTS holder_snapshots (
                    mint TEXT NOT NULL,
                    holder TEXT NOT NULL,
                    last_signature TEXT,
                    buy_count INTEGER DEFAULT 0,
                    sell_count INTEGER DEFAULT 0,
                    net_flow BIGINT DEFAULT 0,
                    conviction_class TEXT DEFAULT 'holder',
                    balance BIGINT DEFAULT 0,
                    updated_at BIGINT DEFAULT 0,
                    PRIMARY KEY (mint, holder)
                );

                CREATE TABLE IF NOT EXISTS webhooks (
                    id TEXT PRIMARY KEY,
                    mint TEXT NOT NULL,
                    webhook_id TEXT NOT NULL,
                    created_at BIGINT DEFAULT 0,
                    UNIQUE(mint)
                );

                -- K-Score history for trajectory analysis (30/60/90 day trends)
                CREATE TABLE IF NOT EXISTS k_score_history (
                    mint TEXT NOT NULL,
                    date DATE NOT NULL,
                    k_score DOUBLE PRECISION,
                    conviction_score DOUBLE PRECISION,
                    holders INTEGER,
                    PRIMARY KEY (mint, date)
                );

                -- Holder history for trend analysis
                CREATE TABLE IF NOT EXISTS holder_history (
                    mint TEXT NOT NULL,
                    date DATE NOT NULL,
                    holders INTEGER,
                    real_holders INTEGER,
                    PRIMARY KEY (mint, date)
                );

                -- Supply history for Mayhem Mode (mutable supply) tracking
                CREATE TABLE IF NOT EXISTS supply_history (
                    mint TEXT NOT NULL,
                    supply TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    source TEXT DEFAULT 'kscore',
                    change_percent DOUBLE PRECISION DEFAULT 0,
                    PRIMARY KEY (mint, timestamp)
                );

                -- Wallet credits for burn-based API access
                -- "Burn = Value Creation = Lifetime API Access"
                CREATE TABLE IF NOT EXISTS wallet_credits (
                    wallet TEXT PRIMARY KEY,
                    used_calls BIGINT DEFAULT 0,
                    last_call BIGINT,
                    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
                );

                -- ═══════════════════════════════════════════════════════════
                -- HARMONY SYSTEM: φ-Powered E-Score Infrastructure
                -- "Hold to enter. Burn to use. φ guides all ratios."
                -- ═══════════════════════════════════════════════════════════

                -- Participants: Unified E-Score tracking across ecosystem
                -- E-Score = geometric mean of 7 dimensions (hold, burn, use, build, run, refer, time)
                CREATE TABLE IF NOT EXISTS participants (
                    wallet TEXT PRIMARY KEY,
                    type TEXT DEFAULT 'user',              -- 'user' | 'holder' | 'burner' | 'dev' | 'infra'

                    -- Raw contribution metrics (source of truth)
                    -- Column names MUST match harmonyEngine.js expectations
                    holdings DOUBLE PRECISION DEFAULT 0,           -- Current $ASDF holdings
                    total_burned DOUBLE PRECISION DEFAULT 0,       -- Lifetime burned
                    api_calls_30d INTEGER DEFAULT 0,               -- Rolling 30-day API usage
                    apps_live INTEGER DEFAULT 0,                   -- Active applications built
                    nodes_active INTEGER DEFAULT 0,                -- Infrastructure nodes running
                    referrals_active INTEGER DEFAULT 0,            -- Active referrals

                    -- Cached E-Score (recalculated on demand)
                    cached_escore DOUBLE PRECISION DEFAULT 0,
                    cached_tier TEXT DEFAULT 'Newcomer',
                    cached_tier_icon TEXT DEFAULT '🌱',
                    escore_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    -- Timestamps
                    first_activity_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    -- Data integrity
                    sig_escore TEXT DEFAULT NULL
                );

                -- Contributions: Immutable audit trail for E-Score changes
                -- Every contribution is recorded for transparency
                CREATE TABLE IF NOT EXISTS contributions (
                    id SERIAL PRIMARY KEY,
                    wallet TEXT NOT NULL,
                    type TEXT NOT NULL,                    -- 'burn' | 'refer' | 'build' | 'run' | 'api_call'
                    amount DOUBLE PRECISION NOT NULL,
                    source TEXT NOT NULL,                  -- 'gasdf' | 'holdex' | 'direct'

                    -- Verification
                    tx_signature TEXT,                     -- Solana tx for burns
                    verified BOOLEAN DEFAULT FALSE,
                    verified_at BIGINT,

                    -- φ-Impact calculation
                    e_score_delta DOUBLE PRECISION DEFAULT 0,

                    -- Timestamps
                    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,

                    -- Prevent duplicates
                    UNIQUE(tx_signature)
                );

                -- Reward Distributions: φ-ratio fee distribution tracking
                -- 38.2% burn, 38.2% rewards, 23.6% treasury (based on φ²/φ³)
                CREATE TABLE IF NOT EXISTS reward_distributions (
                    id SERIAL PRIMARY KEY,
                    source_tx TEXT NOT NULL,               -- Original fee transaction
                    total_amount DOUBLE PRECISION NOT NULL,

                    -- φ-ratio splits
                    burn_amount DOUBLE PRECISION NOT NULL,      -- 38.2%
                    rewards_amount DOUBLE PRECISION NOT NULL,   -- 38.2%
                    treasury_amount DOUBLE PRECISION NOT NULL,  -- 23.6%

                    -- Distribution status
                    status TEXT DEFAULT 'pending',         -- 'pending' | 'distributed' | 'failed'
                    distributed_at BIGINT,

                    -- Timestamps
                    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
                );

                -- Operation Costs: Fee structure with efficiency floor
                -- minFee = (cost / 0.236) × 1.2 guarantees infrastructure sustainability
                CREATE TABLE IF NOT EXISTS operation_costs (
                    operation_type TEXT PRIMARY KEY,       -- 'gasdf_submit_standard', 'gasdf_submit_priority', etc.
                    base_fee DOUBLE PRECISION NOT NULL,    -- Base fee in $ASDF
                    actual_cost DOUBLE PRECISION NOT NULL, -- Infrastructure cost
                    min_fee DOUBLE PRECISION NOT NULL,     -- Efficiency floor: (cost / 0.236) × 1.2
                    max_discount DOUBLE PRECISION NOT NULL, -- Maximum discount % (e.g., 0.50 = 50%)
                    is_active BOOLEAN DEFAULT TRUE,        -- Enable/disable operations

                    -- Analytics
                    total_calls INTEGER DEFAULT 0,
                    total_revenue DOUBLE PRECISION DEFAULT 0,

                    -- Timestamps
                    updated_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
                );
            `);

            // Add new columns if they don't exist (migration-safe)
            const migrations = [
                // API Keys: Add columns for burn credits system
                `ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS wallet TEXT`,
                `ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS key_hash TEXT`,
                `ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS key_prefix TEXT`,
                // Wallet Credits: Add columns for cached burns
                `ALTER TABLE wallet_credits ADD COLUMN IF NOT EXISTS total_burned DOUBLE PRECISION DEFAULT 0`,
                `ALTER TABLE wallet_credits ADD COLUMN IF NOT EXISTS last_burn_check BIGINT DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS initial_supply TEXT`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS burned_amount DOUBLE PRECISION DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS burned_percent DOUBLE PRECISION DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS is_pump_fun BOOLEAN DEFAULT FALSE`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS bonding_curve_complete BOOLEAN DEFAULT FALSE`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS conviction_score DOUBLE PRECISION DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS conviction_accumulators INTEGER DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS conviction_holders INTEGER DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS conviction_reducers INTEGER DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS conviction_extractors INTEGER DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS conviction_analyzed INTEGER DEFAULT 0`,
                // Mayhem Mode (mutable supply) support
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS supply_last_check BIGINT DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS supply_change_24h DOUBLE PRECISION DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS is_mutable_supply BOOLEAN DEFAULT FALSE`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS mint_authority_revoked BOOLEAN DEFAULT FALSE`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS freeze_authority_revoked BOOLEAN DEFAULT FALSE`,
                // Security/LP Status caching (with 24h TTL for integrity)
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS security_last_check BIGINT DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS lp_last_check BIGINT DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS lp_burn_pct DOUBLE PRECISION DEFAULT NULL`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS lp_locked_pct DOUBLE PRECISION DEFAULT NULL`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS lp_status TEXT DEFAULT NULL`,
                // DATA INTEGRITY: 8-Category Signature System (42 colonnes - Controlled Chaos)
                // $asdfasdfa philosophy: Don't Trust, Verify
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS data_signature TEXT DEFAULT NULL`, // Legacy
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS sig_identity TEXT DEFAULT NULL`,   // name|symbol|image|decimals
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS sig_security TEXT DEFAULT NULL`,   // mint_auth|freeze_auth|mutable|verified
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS sig_lp TEXT DEFAULT NULL`,         // lp_burn|lp_locked|lp_status
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS sig_supply TEXT DEFAULT NULL`,     // supply|initial|burned
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS sig_kscore TEXT DEFAULT NULL`,     // k_score|conviction|holders
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS sig_market TEXT DEFAULT NULL`,     // price|mcap|liquidity
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS sig_origin TEXT DEFAULT NULL`,     // is_pump|bonding|timestamp|metadata
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS sig_full TEXT DEFAULT NULL`,       // HMAC(all sigs + chaos)
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS chaos_nonce TEXT DEFAULT NULL`,    // Random entropy - 42nd column
                // K-Score v9: Activity freshness tracking
                `ALTER TABLE holder_snapshots ADD COLUMN IF NOT EXISTS last_tx_timestamp BIGINT DEFAULT 0`,
                // Age column for tokens (avoid recalculating from timestamp)
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS age_days DOUBLE PRECISION DEFAULT 0`,
                // Holder snapshots integrity (same standard as other data)
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS sig_holders TEXT DEFAULT NULL`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS holders_snapshot_check BIGINT DEFAULT 0`,
                // Price/Market provenance columns (required by signMarket)
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS price_source TEXT DEFAULT 'unknown'`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS price_timestamp BIGINT DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS price_pool TEXT DEFAULT ''`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS mcap_calculated BOOLEAN DEFAULT FALSE`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS liquidity_source TEXT DEFAULT 'unknown'`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS liquidity_timestamp BIGINT DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS holders_source TEXT DEFAULT 'unknown'`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS holders_timestamp BIGINT DEFAULT 0`,
                // K-Score v10: Real holders tracking
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS real_holders INTEGER DEFAULT 0`,
                `ALTER TABLE tokens ADD COLUMN IF NOT EXISTS total_holders INTEGER DEFAULT 0`,
                // Harmony: Fix operation_costs schema if old version exists
                `ALTER TABLE operation_costs RENAME COLUMN operation TO operation_type`,
                `ALTER TABLE operation_costs RENAME COLUMN infrastructure_cost TO actual_cost`,
                `ALTER TABLE operation_costs ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE`,
                // Harmony: Fix participants schema if old version exists (add missing columns)
                `ALTER TABLE participants ADD COLUMN IF NOT EXISTS holdings DOUBLE PRECISION DEFAULT 0`,
                `ALTER TABLE participants ADD COLUMN IF NOT EXISTS api_calls_30d INTEGER DEFAULT 0`,
                `ALTER TABLE participants ADD COLUMN IF NOT EXISTS apps_live INTEGER DEFAULT 0`,
                `ALTER TABLE participants ADD COLUMN IF NOT EXISTS nodes_active INTEGER DEFAULT 0`,
                `ALTER TABLE participants ADD COLUMN IF NOT EXISTS referrals_active INTEGER DEFAULT 0`,
                `ALTER TABLE participants ADD COLUMN IF NOT EXISTS cached_escore DOUBLE PRECISION DEFAULT 0`,
                `ALTER TABLE participants ADD COLUMN IF NOT EXISTS cached_tier TEXT DEFAULT 'Newcomer'`,
                `ALTER TABLE participants ADD COLUMN IF NOT EXISTS cached_tier_icon TEXT DEFAULT '🌱'`,
                `ALTER TABLE participants ADD COLUMN IF NOT EXISTS escore_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`,
                `ALTER TABLE participants ADD COLUMN IF NOT EXISTS first_activity_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`,
                `ALTER TABLE participants ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`,
            ];

            // PERFORMANCE: Add indexes for frequently queried columns
            const indexMigrations = [
                // Index for verified tokens filter (hasCommunityUpdate = TRUE)
                `CREATE INDEX IF NOT EXISTS idx_tokens_community_update ON tokens (hasCommunityUpdate) WHERE hasCommunityUpdate = TRUE`,
                // Index for K-Score sorting (most common sort)
                `CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens (k_score DESC NULLS LAST)`,
                // Index for timestamp/age sorting
                `CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens (timestamp DESC NULLS LAST)`,
                // Index for market cap sorting
                `CREATE INDEX IF NOT EXISTS idx_tokens_marketcap ON tokens (marketCap DESC NULLS LAST)`,
                // Composite index for verified + k_score (most common query pattern)
                `CREATE INDEX IF NOT EXISTS idx_tokens_verified_kscore ON tokens (hasCommunityUpdate, k_score DESC NULLS LAST) WHERE hasCommunityUpdate = TRUE`,
                // Index for pools by mint (frequent JOIN)
                `CREATE INDEX IF NOT EXISTS idx_pools_mint ON pools (mint)`,
                // Index for holder_snapshots queries
                `CREATE INDEX IF NOT EXISTS idx_holder_snapshots_mint_balance ON holder_snapshots (mint, balance DESC)`,
                // Index for k_score_history date range queries
                `CREATE INDEX IF NOT EXISTS idx_kscore_history_mint_date ON k_score_history (mint, date DESC)`,
                // ═══════════════════════════════════════════════════════════
                // HARMONY SYSTEM INDEXES
                // ═══════════════════════════════════════════════════════════
                // E-Score leaderboard queries (most common)
                `CREATE INDEX IF NOT EXISTS idx_participants_escore ON participants (e_score DESC NULLS LAST)`,
                // Contributions by wallet (for E-Score calculation)
                `CREATE INDEX IF NOT EXISTS idx_contributions_wallet ON contributions (wallet, created_at DESC)`,
                // Contributions by type (analytics)
                `CREATE INDEX IF NOT EXISTS idx_contributions_type ON contributions (type, created_at DESC)`,
                // Pending reward distributions
                `CREATE INDEX IF NOT EXISTS idx_reward_distributions_status ON reward_distributions (status) WHERE status = 'pending'`,
            ];

            for (const sql of migrations) {
                try {
                    await primaryPool.query(sql);
                } catch (_e) {
                    // Column might already exist, ignore
                }
            }

            // Apply index migrations
            for (const sql of indexMigrations) {
                try {
                    await primaryPool.query(sql);
                } catch (e) {
                    // Index might already exist or column missing, ignore
                    logger.debug(`Index migration skipped: ${e.message?.slice(0, 50)}`);
                }
            }
            logger.info('💾 Database: Indexes verified');

            // Seed operation costs if empty (GASdf fee structure)
            // Based on φ-ratio efficiency floor: minFee = (cost / 0.236) × 1.2
            const existingCosts = await primaryPool.query('SELECT COUNT(*) as count FROM operation_costs');
            if (parseInt(existingCosts.rows[0].count) === 0) {
                const seedCosts = [
                    // operation_type, base_fee, actual_cost, min_fee, max_discount
                    ['gasdf_submit_standard', 100, 5, 25.42, 0.50],    // Standard submission
                    ['gasdf_submit_priority', 250, 15, 76.27, 0.40],   // Priority submission
                    ['gasdf_submit_instant', 500, 30, 152.54, 0.30],   // Instant submission
                    ['holdex_api_call', 1, 0.01, 0.05, 0.80],          // HolDex API call
                ];
                for (const [opType, baseFee, cost, minFee, maxDiscount] of seedCosts) {
                    await primaryPool.query(`
                        INSERT INTO operation_costs (operation_type, base_fee, actual_cost, min_fee, max_discount)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (operation_type) DO NOTHING
                    `, [opType, baseFee, cost, minFee, maxDiscount]);
                }
                logger.info('⚗️ Harmony: Seeded operation costs (φ-ratio efficiency floor)');
            }

            logger.info('⚗️ Harmony: E-Score tables ready (φ = 1.618)');

            dbWrapper = {
                query: (text, params) => (text.trim().toUpperCase().startsWith('SELECT') ? readPool : primaryPool).query(text, params),
                get: async (text, params) => { const res = await readPool.query(text, params); return res.rows[0]; },
                all: async (text, params) => { const res = await readPool.query(text, params); return res.rows; },
                run: async (text, params) => { const res = await primaryPool.query(text, params); return { rowCount: res.rowCount }; }
            };

            return dbWrapper;

        } catch (error) {
            logger.error(`Database Init Failed: ${error.message}`);
            initPromise = null;
            throw error;
        }
    })();

    return initPromise;
}

function getDB() { if (!dbWrapper) throw new Error("Database not initialized."); return dbWrapper; }

async function smartCache(key, ttlSeconds, fetchFn) {
    const redis = getClient();
    if (redis) { try { const cached = await redis.get(key); if (cached) return JSON.parse(cached); } catch (_e) { /* ignore */ } }
    if (pendingRequests.has(key)) return pendingRequests.get(key);
    const fetchPromise = (async () => {
        try { const data = await fetchFn(); if (redis && data) redis.set(key, JSON.stringify(data), 'EX', ttlSeconds).catch(() => {}); return data; } finally { pendingRequests.delete(key); }
    })();
    pendingRequests.set(key, fetchPromise);
    return fetchPromise;
}

async function enableIndexing(db, mint, poolData) {
    if (!poolData || !poolData.pairAddress) return;
    try {
        await db.run(`
            INSERT INTO pools (
                address, mint, dex, price_usd, liquidity_usd, volume_24h, created_at, token_a, token_b, reserve_a, reserve_b
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT(address) DO UPDATE SET
                price_usd = EXCLUDED.price_usd,
                liquidity_usd = EXCLUDED.liquidity_usd,
                volume_24h = EXCLUDED.volume_24h,
                reserve_a = EXCLUDED.reserve_a,
                reserve_b = EXCLUDED.reserve_b
        `, [
            poolData.pairAddress, mint, poolData.dexId, poolData.priceUsd || 0, poolData.liquidity?.usd || 0,
            poolData.volume?.h24 || 0, Date.now(), poolData.baseToken, poolData.quoteToken,
            poolData.reserve_a || null, poolData.reserve_b || null
        ]);
        await db.run(`INSERT INTO active_trackers (pool_address, priority, last_check) VALUES ($1, 10, 0) ON CONFLICT(pool_address) DO NOTHING`, [poolData.pairAddress]);
    } catch (_err) { /* ignore */ }
}

async function aggregateAndSaveToken(db, mint) {
    try {
        const pools = await db.all(`SELECT * FROM pools WHERE mint = $1`, [mint]);
        if (pools.length === 0) return;

        let totalLiq = 0; let totalVol = 0; let mainPool = pools[0]; 
        for (const p of pools) {
            const liq = parseFloat(p.liquidity_usd || 0);
            totalLiq += liq;
            totalVol += parseFloat(p.volume_24h || 0);
            if (liq > parseFloat(mainPool.liquidity_usd || 0)) mainPool = p;
        }

        const price = parseFloat(mainPool.price_usd || 0);
        let change24h = null, change1h = null, change5m = null;

        if (price > 0) {
            const getPriceAt = async (ts) => {
                const row = await db.get(`SELECT close FROM candles_1m WHERE pool_address = $1 AND timestamp <= $2 ORDER BY timestamp DESC LIMIT 1`, [mainPool.address, ts]);
                return row ? parseFloat(row.close) : null;
            };
            const now = Date.now();
            const [p24h, p1h, p5m] = await Promise.all([ getPriceAt(now - 86400000), getPriceAt(now - 3600000), getPriceAt(now - 300000) ]);
            if (p24h) change24h = ((price - p24h) / p24h) * 100;
            if (p1h) change1h = ((price - p1h) / p1h) * 100;
            if (p5m) change5m = ((price - p5m) / p5m) * 100;
        }

        // --- HOLDER CHECK LOGIC ---
        const now = Date.now();
        const tokenRow = await db.get(`SELECT last_holder_check, holders FROM tokens WHERE mint = $1`, [mint]);
        const lastCheck = parseInt(tokenRow?.last_holder_check || 0);
        
        let holderCount = null;
        // Check holders every 30 minutes (with 30s timeout to not block)
        if (now - lastCheck > 1800000) {
            try {
                // Fetch from RPC with timeout
                const timeoutPromise = new Promise((_, reject) =>
                    setTimeout(() => reject(new Error('Holder count timeout')), 30000)
                );
                const count = await Promise.race([
                    getHolderCountFromRPC(mint),
                    timeoutPromise
                ]);
                if (typeof count === 'number' && count > 0) {
                    holderCount = count;
                    // Save history (Snapshot normalized to start of day)
                    const today = Math.floor(now / (24 * 60 * 60 * 1000)) * (24 * 60 * 60 * 1000);
                    
                    await db.run(`
                        INSERT INTO holders_history (mint, count, timestamp) 
                        VALUES ($1, $2, $3)
                        ON CONFLICT (mint, timestamp) DO UPDATE SET count = EXCLUDED.count
                    `, [mint, holderCount, today]);
                }
            } catch (e) {
                logger.warn(`Holder check failed for ${mint}: ${e.message}`);
            }
        }

        // Build Update Query
        // FIX: Removed `timestamp` column update to preserve original creation time.
        // FIX: Added `updated_at = NOW()` to track recent updates properly.
        const params = [totalLiq, totalVol, price, mint]; 
        let query = `UPDATE tokens SET liquidity = $1, volume24h = $2, priceUsd = $3, updated_at = NOW(), marketCap = ($3 * CAST(supply AS DOUBLE PRECISION) / POWER(10, COALESCE(decimals, 9)))`;
        
        let idx = 5; // Start at 5 since we use $1-$4 above
        if (change24h !== null) { query += `, change24h = $${idx++}`; params.push(change24h); }
        if (change1h !== null) { query += `, change1h = $${idx++}`; params.push(change1h); }
        if (change5m !== null) { query += `, change5m = $${idx++}`; params.push(change5m); }
        if (holderCount !== null) { query += `, holders = $${idx++}, last_holder_check = $${idx++}`; params.push(holderCount); params.push(now); }

        // Include kscore fields in RETURNING if holders were updated (needed for sig_kscore)
        query += ` WHERE mint = $4 RETURNING mint, priceusd, marketcap, liquidity, price_source, price_timestamp, price_pool, liquidity_source, liquidity_timestamp, mcap_calculated, holders_source, holders_timestamp, age_days, k_score, conviction_score, conviction_accumulators, conviction_holders, conviction_reducers, conviction_extractors, conviction_analyzed, holders, last_k_score_update`;
        const result = await db.get(query, params);

        // Re-sign market data to maintain integrity
        if (result) {
            const sig_market = signMarket(result);

            // FIX: If holders were updated, also re-sign sig_kscore (holders is part of the kscore signature)
            if (holderCount !== null) {
                const sig_kscore = signKScore(result);
                await db.run(`UPDATE tokens SET sig_market = $1, sig_kscore = $2 WHERE mint = $3`, [sig_market, sig_kscore, mint]);
            } else {
                await db.run(`UPDATE tokens SET sig_market = $1 WHERE mint = $2`, [sig_market, mint]);
            }
        }

    } catch (err) { logger.error(`Aggregation Error ${mint}: ${err.message}`); }
}

module.exports = { initDB, getDB, smartCache, enableIndexing, aggregateAndSaveToken };
