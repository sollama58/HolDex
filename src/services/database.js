const { Pool } = require('pg');
const config = require('../config/env');
const logger = require('./logger');
const { getClient } = require('./redis'); // SCALABILITY: Use Redis

// SCALABILITY: Limit max connections to prevent 'too_many_clients' errors
const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: 20, // Increased to 20 to handle higher concurrency
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

// SCALABILITY: Redis-Based Caching
// Replaces in-memory Map to allow horizontal scaling (multiple servers)
async function smartCache(key, ttlSeconds, fetchFn) {
    const redis = getClient();
    
    // 1. Try Fetch from Redis
    if (redis && redis.status === 'ready') {
        try {
            const cached = await redis.get(key);
            if (cached) {
                return JSON.parse(cached);
            }
        } catch (e) {
            logger.warn(`Redis Cache Get Error: ${e.message}`);
        }
    }

    // 2. Execute Fetch Function (DB Query / API Call)
    const value = await fetchFn();

    // 3. Save to Redis
    if (value && redis && redis.status === 'ready') {
        try {
            // Set with Expiry (EX)
            await redis.set(key, JSON.stringify(value), 'EX', ttlSeconds);
        } catch (e) {
            logger.warn(`Redis Cache Set Error: ${e.message}`);
        }
    }

    return value;
}

const dbWrapper = {
  pool, // Expose pool
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
    // 1. ORIGINAL TABLES (Restored)
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
        
        -- Enhanced Columns for Sorting/Indexing
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

    await pool.query(`
      CREATE TABLE IF NOT EXISTS k_scores (
        mint TEXT PRIMARY KEY REFERENCES tokens(mint),
        score INTEGER,
        metrics JSONB,
        calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

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

    // 2. NEW INDEXER TABLES
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

    // 3. SCALABILITY: PERFORMANCE INDEXES
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_kscore ON tokens(k_score DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_mcap ON tokens(marketCap DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_volume ON tokens(volume24h DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_tokens_timestamp ON tokens(timestamp DESC);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_candles_pool_time ON candles_1m(pool_address, timestamp DESC);`);

    logger.info('Database initialized successfully with Optimized Schema');
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
      // Helper to Upsert token data
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
