/**
 * HARMONY MIGRATION SCRIPT
 *
 * Creates the database schema for the unified E-Score system.
 * Run: node src/scripts/migrate_harmony.js
 *
 * Tables created:
 * - participants: Stores E-Score and contribution data
 * - contributions: Audit trail of all contributions
 * - reward_distributions: History of reward distributions
 * - operation_costs: Cost definitions for efficiency floor
 */

'use strict';

require('dotenv').config();
const { Pool } = require('pg');
const harmony = require('../shared/harmony');

// Database connection
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DB_SSL_REJECT_UNAUTHORIZED === 'false'
        ? { rejectUnauthorized: false }
        : undefined
});

async function migrate() {
    const client = await pool.connect();

    try {
        console.log('\n🚀 Starting Harmony Migration...\n');

        await client.query('BEGIN');

        // ═══════════════════════════════════════════════════════════════
        // TABLE: participants
        // ═══════════════════════════════════════════════════════════════

        console.log('📦 Creating participants table...');

        await client.query(`
            CREATE TABLE IF NOT EXISTS participants (
                -- Identity
                wallet TEXT PRIMARY KEY,
                type TEXT DEFAULT 'user' CHECK (type IN ('user', 'holder', 'burner', 'dev', 'infra')),

                -- E-Score (computed)
                e_score DECIMAL(10,2) DEFAULT 0,
                tier TEXT DEFAULT 'Observer',
                tier_icon TEXT DEFAULT '👁️',

                -- Contribution counters (raw data for E-Score calculation)
                holdings BIGINT DEFAULT 0,
                total_burned BIGINT DEFAULT 0,
                api_calls_30d INTEGER DEFAULT 0,
                apps_live INTEGER DEFAULT 0,
                nodes_active INTEGER DEFAULT 0,
                referrals_active INTEGER DEFAULT 0,

                -- Rewards tracking
                rewards_pending DECIMAL(18,8) DEFAULT 0,
                rewards_lifetime DECIMAL(18,8) DEFAULT 0,
                rewards_claimed DECIMAL(18,8) DEFAULT 0,

                -- Timestamps
                first_activity_at TIMESTAMPTZ DEFAULT NOW(),
                last_activity_at TIMESTAMPTZ DEFAULT NOW(),
                e_score_updated_at TIMESTAMPTZ DEFAULT NOW(),
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        `);

        console.log('  ✓ participants table created');

        // ═══════════════════════════════════════════════════════════════
        // TABLE: contributions
        // ═══════════════════════════════════════════════════════════════

        console.log('📦 Creating contributions table...');

        await client.query(`
            CREATE TABLE IF NOT EXISTS contributions (
                id SERIAL PRIMARY KEY,
                wallet TEXT NOT NULL,

                -- Contribution details
                type TEXT NOT NULL CHECK (type IN ('hold', 'burn', 'use', 'build', 'run', 'refer')),
                amount DECIMAL(18,8) NOT NULL,

                -- Metadata
                source TEXT DEFAULT 'holdex',
                tx_signature TEXT,
                details JSONB,

                created_at TIMESTAMPTZ DEFAULT NOW(),

                -- Foreign key (soft - wallet may not exist yet)
                CONSTRAINT fk_wallet FOREIGN KEY (wallet)
                    REFERENCES participants(wallet)
                    ON DELETE CASCADE
                    DEFERRABLE INITIALLY DEFERRED
            )
        `);

        console.log('  ✓ contributions table created');

        // ═══════════════════════════════════════════════════════════════
        // TABLE: reward_distributions
        // ═══════════════════════════════════════════════════════════════

        console.log('📦 Creating reward_distributions table...');

        await client.query(`
            CREATE TABLE IF NOT EXISTS reward_distributions (
                id SERIAL PRIMARY KEY,

                -- Period
                period_start TIMESTAMPTZ NOT NULL,
                period_end TIMESTAMPTZ NOT NULL,

                -- Amounts
                total_fees_collected DECIMAL(18,8) NOT NULL,
                total_pool DECIMAL(18,8) NOT NULL,

                -- Distribution breakdown (φ ratios)
                burn_amount DECIMAL(18,8) NOT NULL,
                rewards_amount DECIMAL(18,8) NOT NULL,
                treasury_amount DECIMAL(18,8) NOT NULL,

                -- Stats
                recipients_count INTEGER NOT NULL DEFAULT 0,
                total_e_score DECIMAL(18,8) NOT NULL DEFAULT 0,

                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        `);

        console.log('  ✓ reward_distributions table created');

        // ═══════════════════════════════════════════════════════════════
        // TABLE: operation_costs
        // ═══════════════════════════════════════════════════════════════

        console.log('📦 Creating operation_costs table...');

        await client.query(`
            CREATE TABLE IF NOT EXISTS operation_costs (
                operation_type TEXT PRIMARY KEY,

                -- Pricing
                base_fee DECIMAL(18,8) NOT NULL,
                actual_cost DECIMAL(18,8) NOT NULL,

                -- Computed (for reference)
                min_fee DECIMAL(18,8) NOT NULL,
                max_discount DECIMAL(5,4) NOT NULL,

                -- Metadata
                description TEXT,
                service TEXT DEFAULT 'holdex',
                is_active BOOLEAN DEFAULT TRUE,

                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        `);

        console.log('  ✓ operation_costs table created');

        // ═══════════════════════════════════════════════════════════════
        // INDEXES
        // ═══════════════════════════════════════════════════════════════

        console.log('📦 Creating indexes...');

        // Participants indexes
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_participants_e_score
            ON participants(e_score DESC)
        `);

        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_participants_type
            ON participants(type)
        `);

        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_participants_tier
            ON participants(tier)
        `);

        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_participants_last_activity
            ON participants(last_activity_at DESC)
        `);

        // Contributions indexes
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_contributions_wallet
            ON contributions(wallet)
        `);

        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_contributions_type
            ON contributions(type)
        `);

        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_contributions_created
            ON contributions(created_at DESC)
        `);

        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_contributions_wallet_type
            ON contributions(wallet, type)
        `);

        // Reward distributions index
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_reward_distributions_period
            ON reward_distributions(period_end DESC)
        `);

        console.log('  ✓ Indexes created');

        // ═══════════════════════════════════════════════════════════════
        // SEED: Operation Costs
        // ═══════════════════════════════════════════════════════════════

        console.log('📦 Seeding operation costs...');

        const operations = [
            // HolDex operations
            { type: 'holdex_read_simple', baseFee: 25, cost: 2, desc: 'Simple token read', service: 'holdex' },
            { type: 'holdex_read_batch', baseFee: 100, cost: 8, desc: 'Batch token read', service: 'holdex' },
            { type: 'holdex_read_heavy', baseFee: 200, cost: 15, desc: 'Heavy read (verify/audit)', service: 'holdex' },
            { type: 'holdex_webhook', baseFee: 20, cost: 1, desc: 'Webhook setup', service: 'holdex' },
            { type: 'holdex_export', baseFee: 400, cost: 30, desc: 'Data export', service: 'holdex' },

            // GASdf operations
            { type: 'gasdf_quote', baseFee: 0, cost: 0.1, desc: 'Quote request (free)', service: 'gasdf' },
            { type: 'gasdf_submit_standard', baseFee: 500, cost: 17, desc: 'Standard gasless TX', service: 'gasdf' },
            { type: 'gasdf_submit_priority', baseFee: 1000, cost: 35, desc: 'Priority gasless TX', service: 'gasdf' },
        ];

        for (const op of operations) {
            const minFee = harmony.calculateMinimumFee(op.cost);
            const maxDiscount = harmony.calculateMaxDiscount(op.baseFee, op.cost);

            await client.query(`
                INSERT INTO operation_costs (
                    operation_type, base_fee, actual_cost, min_fee, max_discount,
                    description, service
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (operation_type) DO UPDATE SET
                    base_fee = EXCLUDED.base_fee,
                    actual_cost = EXCLUDED.actual_cost,
                    min_fee = EXCLUDED.min_fee,
                    max_discount = EXCLUDED.max_discount,
                    description = EXCLUDED.description,
                    service = EXCLUDED.service,
                    updated_at = NOW()
            `, [op.type, op.baseFee, op.cost, minFee, maxDiscount, op.desc, op.service]);
        }

        console.log('  ✓ Operation costs seeded');

        // ═══════════════════════════════════════════════════════════════
        // VERIFY
        // ═══════════════════════════════════════════════════════════════

        console.log('\n📊 Verifying operation costs...\n');

        const costs = await client.query(`
            SELECT operation_type, base_fee, actual_cost, min_fee,
                   ROUND(max_discount * 100, 1) as max_discount_pct,
                   CASE WHEN base_fee >= min_fee THEN '✓' ELSE '✗' END as viable
            FROM operation_costs
            ORDER BY service, operation_type
        `);

        console.log('  ┌────────────────────────┬──────────┬──────────┬──────────┬──────────┬────────┐');
        console.log('  │ Operation              │ Base Fee │ Cost     │ Min Fee  │ Max Disc │ Viable │');
        console.log('  ├────────────────────────┼──────────┼──────────┼──────────┼──────────┼────────┤');

        for (const row of costs.rows) {
            const op = row.operation_type.padEnd(22);
            const base = row.base_fee.toString().padStart(8);
            const cost = row.actual_cost.toString().padStart(8);
            const min = row.min_fee.toString().padStart(8);
            const disc = (row.max_discount_pct + '%').padStart(8);
            const viable = row.viable.padStart(6);
            console.log(`  │ ${op} │ ${base} │ ${cost} │ ${min} │ ${disc} │ ${viable} │`);
        }

        console.log('  └────────────────────────┴──────────┴──────────┴──────────┴──────────┴────────┘');

        await client.query('COMMIT');

        console.log('\n✅ Migration completed successfully!\n');

    } catch (error) {
        await client.query('ROLLBACK');
        console.error('\n❌ Migration failed:', error.message);
        throw error;
    } finally {
        client.release();
        await pool.end();
    }
}

// Run migration
migrate().catch(err => {
    console.error(err);
    process.exit(1);
});
