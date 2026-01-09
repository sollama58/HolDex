-- Migration: Distributed Polling Infrastructure
-- "N nodes, shared truth, verified consensus."
-- PostgreSQL = shared coordination layer, Local Redis = per-node cache only

-- ============================================
-- POLLING TASKS QUEUE
-- ============================================
CREATE TABLE IF NOT EXISTS polling_tasks (
    task_id SERIAL PRIMARY KEY,
    mint TEXT NOT NULL,
    priority INTEGER DEFAULT 100,
    reason TEXT NOT NULL DEFAULT 'scheduled',
    status TEXT NOT NULL DEFAULT 'pending', -- pending, claimed, completed, failed
    created_at BIGINT NOT NULL,
    created_by TEXT,
    claimed_by TEXT,
    claimed_at BIGINT,
    completed_at BIGINT,
    attempts INTEGER DEFAULT 0,
    last_error TEXT,
    k_score_result DOUBLE PRECISION
);

-- Partial unique index: only one pending task per mint
CREATE UNIQUE INDEX IF NOT EXISTS idx_polling_tasks_unique_pending
    ON polling_tasks (mint) WHERE status = 'pending';

-- Index for efficient task claiming (SELECT FOR UPDATE SKIP LOCKED)
CREATE INDEX IF NOT EXISTS idx_polling_tasks_pending_priority
    ON polling_tasks (priority ASC, created_at ASC)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_polling_tasks_status
    ON polling_tasks (status);

CREATE INDEX IF NOT EXISTS idx_polling_tasks_mint
    ON polling_tasks (mint, created_at DESC);

-- ============================================
-- TOKEN VERIFICATIONS (per-node verification tracking)
-- ============================================
CREATE TABLE IF NOT EXISTS token_verifications (
    id SERIAL PRIMARY KEY,
    mint TEXT NOT NULL,
    node_id TEXT NOT NULL,
    k_score DOUBLE PRECISION,
    node_signature TEXT,
    verified_at BIGINT NOT NULL,

    -- One verification per node per token
    UNIQUE(mint, node_id)
);

CREATE INDEX IF NOT EXISTS idx_token_verifications_mint
    ON token_verifications (mint, verified_at DESC);

CREATE INDEX IF NOT EXISTS idx_token_verifications_node
    ON token_verifications (node_id, verified_at DESC);

-- ============================================
-- NODE WORK HISTORY
-- ============================================
CREATE TABLE IF NOT EXISTS node_work_history (
    id SERIAL PRIMARY KEY,
    node_id TEXT NOT NULL,
    task_type TEXT NOT NULL DEFAULT 'polling',
    mint TEXT NOT NULL,
    completed_at BIGINT NOT NULL,
    duration_ms INTEGER,
    rpc_calls INTEGER DEFAULT 0,
    k_score_calculated BOOLEAN DEFAULT FALSE,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_node_work_history_node
    ON node_work_history (node_id, completed_at DESC);

CREATE INDEX IF NOT EXISTS idx_node_work_history_mint
    ON node_work_history (mint, completed_at DESC);

-- ============================================
-- CONSENSUS SNAPSHOTS
-- ============================================
CREATE TABLE IF NOT EXISTS consensus_snapshots (
    mint TEXT PRIMARY KEY,
    k_score_consensus DOUBLE PRECISION,
    agreeing_nodes INTEGER DEFAULT 0,
    total_nodes INTEGER DEFAULT 0,
    consensus_at BIGINT,
    confidence DECIMAL(5,4)
);

CREATE INDEX IF NOT EXISTS idx_consensus_snapshots_consensus_at
    ON consensus_snapshots (consensus_at DESC);

-- ============================================
-- ADD COLUMNS TO NODES TABLE
-- ============================================
DO $$
BEGIN
    -- Credits for φ-weighted task distribution
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'nodes' AND column_name = 'credits') THEN
        ALTER TABLE nodes ADD COLUMN credits DECIMAL(10,6) DEFAULT 1.618033988749895;
    END IF;

    -- Rename work_credits to credits if exists
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'nodes' AND column_name = 'work_credits') THEN
        ALTER TABLE nodes RENAME COLUMN work_credits TO credits;
    END IF;

    -- Last work timestamp
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'nodes' AND column_name = 'last_work_at') THEN
        ALTER TABLE nodes ADD COLUMN last_work_at BIGINT;
    END IF;

    -- Capabilities (JSON array)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'nodes' AND column_name = 'capabilities') THEN
        ALTER TABLE nodes ADD COLUMN capabilities JSONB DEFAULT '["polling", "webhooks", "verification"]'::jsonb;
    END IF;

    -- Total RPC calls (for cost tracking)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'nodes' AND column_name = 'total_rpc_calls') THEN
        ALTER TABLE nodes ADD COLUMN total_rpc_calls BIGINT DEFAULT 0;
    END IF;
END $$;

-- ============================================
-- ADD COLUMNS TO TOKENS TABLE
-- ============================================
DO $$
BEGIN
    -- K-Score source tracking
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'tokens' AND column_name = 'k_score_source') THEN
        ALTER TABLE tokens ADD COLUMN k_score_source TEXT DEFAULT 'placeholder';

        -- Update existing verified tokens
        UPDATE tokens SET k_score_source = 'calculated'
        WHERE hascommunityupdate = TRUE AND k_score > 0;
    END IF;

    -- Confidence level (0-1)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'tokens' AND column_name = 'k_score_confidence') THEN
        ALTER TABLE tokens ADD COLUMN k_score_confidence DECIMAL(5,4) DEFAULT 0;
    END IF;

    -- Consensus node count
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'tokens' AND column_name = 'k_score_consensus_nodes') THEN
        ALTER TABLE tokens ADD COLUMN k_score_consensus_nodes INTEGER DEFAULT 0;
    END IF;
END $$;

-- ============================================
-- VIEWS
-- ============================================

-- Network status view
CREATE OR REPLACE VIEW node_network_status AS
SELECT
    COUNT(*) FILTER (WHERE status = 'active' AND last_heartbeat > EXTRACT(EPOCH FROM NOW()) * 1000 - 30000) as nodes_active,
    COUNT(*) FILTER (WHERE status = 'degraded') as nodes_degraded,
    COUNT(*) FILTER (WHERE status = 'inactive' OR status = 'offline') as nodes_offline,
    COUNT(*) as nodes_total,
    COALESCE(SUM(verifications_24h), 0) as verifications_24h_total,
    COALESCE(SUM(total_rpc_calls), 0) as rpc_calls_total,
    COALESCE(AVG(credits), 1.618) as avg_credits
FROM nodes
WHERE approval_status = 'approved' OR is_genesis = TRUE;

-- Polling queue status view
CREATE OR REPLACE VIEW polling_queue_status AS
SELECT
    COUNT(*) FILTER (WHERE status = 'pending') as pending,
    COUNT(*) FILTER (WHERE status = 'claimed') as in_progress,
    COUNT(*) FILTER (WHERE status = 'completed' AND completed_at > EXTRACT(EPOCH FROM NOW()) * 1000 - 3600000) as completed_1h,
    COUNT(*) FILTER (WHERE status = 'failed') as failed,
    MIN(priority) FILTER (WHERE status = 'pending') as highest_priority
FROM polling_tasks;

-- ============================================
-- CLEANUP FUNCTION
-- ============================================
CREATE OR REPLACE FUNCTION cleanup_old_polling_tasks()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM polling_tasks
    WHERE status IN ('completed', 'failed')
      AND completed_at < EXTRACT(EPOCH FROM NOW()) * 1000 - 604800000; -- 7 days

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- COMMENTS
-- ============================================
COMMENT ON TABLE polling_tasks IS 'Distributed task queue for K-Score polling - PostgreSQL as coordination layer';
COMMENT ON TABLE token_verifications IS 'Per-node verification records for consensus tracking';
COMMENT ON TABLE node_work_history IS 'History of work done by each node for analytics';
COMMENT ON TABLE consensus_snapshots IS 'K-Score consensus records when φ⁻¹ nodes agree';
