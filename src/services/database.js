const { Pool } = require('pg');
const config = require('../config/env');
const logger = require('./logger');

// Database connection configuration
const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

const dbWrapper = {
  pool, // Expose pool for specialized operations if needed

  async query(text, params) {
    const start = Date.now();
    try {
      const res = await pool.query(text, params);
      const duration = Date.now() - start;
      // logger.debug('Executed query', { text, duration, rows: res.rowCount });
      return res;
    } catch (error) {
      logger.error('Database query error', { text, error: error.message });
      throw error;
    }
  },

  // SQLite-style compatibility wrappers for easier migration/usage
  async get(text, params) {
    const res = await this.query(text, params);
    return res.rows[0];
  },

  async all(text, params) {
    const res = await this.query(text, params);
    return res.rows;
  },

  async run(text, params) {
    const res = await this.query(text, params);
    return { lastID: null, changes: res.rowCount }; // Postgres doesn't return lastID easily in generic query
  },

  async exec(text) {
    return this.query(text);
  }
};

const initDB = async () => {
  try {
    // Existing Schema
    await pool.query(`
      CREATE TABLE IF NOT EXISTS tokens (
        mint TEXT PRIMARY KEY,
        symbol TEXT,
        name TEXT,
        decimals INTEGER,
        supply TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        metadata JSONB
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

    // --- NEW INDEXER SCHEMA ---
    
    // 1. POOLS: Links a Token to its Trading Pair
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

    // 2. CANDLES (1m): The foundational data for all charts
    // We use ON CONFLICT to allow re-running snapshots without errors
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
      CREATE INDEX IF NOT EXISTS idx_candles_pool_time ON candles_1m(pool_address, timestamp DESC);
    `);

    // 3. ACTIVE TRACKERS: Optimization to only scan "live" pools
    await pool.query(`
      CREATE TABLE IF NOT EXISTS active_trackers (
          pool_address TEXT PRIMARY KEY,
          last_check BIGINT DEFAULT 0,
          priority INTEGER DEFAULT 1
      );
    `);

    logger.info('Database initialized successfully with Indexer Schema');
  } catch (error) {
    logger.error('Database initialization failed', error);
    process.exit(1);
  }
};

module.exports = {
  getDB: () => dbWrapper,
  initDB
};
