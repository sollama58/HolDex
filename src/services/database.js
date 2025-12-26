const { Pool } = require('pg');
const config = require('../config/env');
const logger = require('./logger');

// SCALABILITY FIX: Limit max connections to prevent 'too_many_clients' errors
// on shared hosting (Render/Heroku/Railway).
const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: 10, // Limit pool size per container (Api + Worker + Indexer = ~30 connections total)
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Cache wrapper (Simple in-memory for DB results)
const cache = new Map();
async function smartCache(key, ttlSeconds, fetchFn) {
    if (cache.has(key)) {
        const { value, expires } = cache.get(key);
        if (Date.now() < expires) return value;
    }
    const value = await fetchFn();
    cache.set(key, { value, expires: Date.now() + (ttlSeconds * 1000) });
    return value;
}

const dbWrapper = {
  pool, 
  async query(text, params) {
    const start = Date.now();
    try {
      const res = await pool.query(text, params);
      return res;
    } catch (error) {
      logger.error('Database query error', { text, error: error.message });
      throw error;
    }
  },
  async get(text, params) { const res = await this.query(text, params); return res.rows[0]; },
  async all(text, params) { const res = await this.query(text, params); return res.rows; },
  async run(text, params) { const res = await this.query(text, params); return { rowCount: res.rowCount }; },
  async exec(text) { return this.query(text); }
};

const initDB = async () => {
  try {
    // 1. Core Token Tables
    await pool.query(`
      CREATE TABLE IF NOT EXISTS tokens (
        mint TEXT PRIMARY KEY,
        symbol TEXT,
        name TEXT,
        decimals INTEGER,
        supply TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        metadata JSONB,
        
        -- New Columns for Indexer/Sort
        k_score INTEGER DEFAULT 0,
        marketCap DOUBLE PRECISION DEFAULT 0,
        volume24h DOUBLE PRECISION DEFAULT 0,
        change24h DOUBLE PRECISION DEFAULT 0,
        change1h DOUBLE PRECISION DEFAULT 0,
        change5m DOUBLE PRECISION DEFAULT 0,
        priceUsd DOUBLE PRECISION DEFAULT 0,
        hasCommunityUpdate BOOLEAN DEFAULT FALSE,
        timestamp BIGINT DEFAULT 0
      );
    `);

    // 2. Token Updates (Community)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS token_updates (
        id SERIAL PRIMARY KEY,
        mint TEXT NOT NULL,
        twitter TEXT,
        website TEXT,
        telegram TEXT,
        banner TEXT,
        description TEXT,
        status TEXT DEFAULT 'pending', 
        signature TEXT UNIQUE,
        payer TEXT,
        submittedAt BIGINT
      );
    `);

    // 3. Indexer Tables
    await pool.query(`
      CREATE TABLE IF NOT EXISTS pools (
          address TEXT PRIMARY KEY,       
          mint TEXT NOT NULL,             
          dex TEXT NOT NULL,              
          token_a TEXT NOT NULL,          
          token_b TEXT NOT NULL,          
          created_at BIGINT DEFAULT 0,
          UNIQUE(mint, dex)
      );
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS candles_1m (
          pool_address TEXT NOT NULL,
          timestamp BIGINT NOT NULL,
          open DOUBLE PRECISION,
          high DOUBLE PRECISION,
          low DOUBLE PRECISION,
          close DOUBLE PRECISION,
          volume DOUBLE PRECISION DEFAULT 0,
          PRIMARY KEY (pool_address, timestamp)
      );
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS active_trackers (
          pool_address TEXT PRIMARY KEY,
          last_check BIGINT DEFAULT 0,
          priority INTEGER DEFAULT 1
      );
    `);

    // --- SCALABILITY FIX: INDEXES ---
    // These speed up the dashboard sorting queries significantly as table grows
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_mcap ON tokens(marketCap DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_volume ON tokens(volume24h DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens(timestamp DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_candles_pool_time ON candles_1m(pool_address, timestamp DESC);`);

    logger.info('Database initialized with Optimized Schema');
  } catch (error) {
    logger.error('Database initialization failed', error);
    process.exit(1);
  }
};

module.exports = {
  getDB: () => dbWrapper,
  initDB,
  smartCache,
  saveTokenData: async (db, mint, data, timestamp) => {
      // Helper to Upsert token data from DexScreener/Indexer
      const cols = [
          'name', 'symbol', 'image', 'marketCap', 'volume24h', 
          'priceUsd', 'change1h', 'change24h', 'change5m', 'timestamp'
      ];
      const vals = [
          data.name, data.ticker, data.image, data.marketCap, data.volume24h,
          data.priceUsd, data.change1h, data.change24h, data.change5m, timestamp
      ];
      
      await db.query(`
        INSERT INTO tokens (mint, ${cols.join(',')})
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT(mint) DO UPDATE SET
            name = EXCLUDED.name,
            symbol = EXCLUDED.symbol,
            image = COALESCE(EXCLUDED.image, tokens.image),
            marketCap = EXCLUDED.marketCap,
            volume24h = EXCLUDED.volume24h,
            priceUsd = EXCLUDED.priceUsd,
            change1h = EXCLUDED.change1h,
            change24h = EXCLUDED.change24h,
            change5m = EXCLUDED.change5m,
            timestamp = GREATEST(tokens.timestamp, EXCLUDED.timestamp)
      `, [mint, ...vals]);
  }
};
