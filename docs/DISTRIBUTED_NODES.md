# HolDex Distributed Node Architecture

> "N nodes, shared truth, verified consensus."
> Scale horizontally. Trust mathematically.

## Philosophy

```
                    φ = 1.618033988749895

    Consensus threshold = φ⁻¹ = 61.8% of active nodes
    Work distribution = φ-weighted random selection
    Credit regeneration = φ⁻² per cycle

    "Don't Trust, Verify" at network level
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              HELIUS WEBHOOKS                                │
│                         (FREE - Real-time source)                           │
│                                                                             │
│   Every transfer event → All subscribed nodes receive simultaneously        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                  REDIS                                       │
│                        (Coordination Layer)                                  │
│                                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │  Node Registry  │  │   Task Queue    │  │    Pub/Sub      │              │
│  │                 │  │                 │  │                 │              │
│  │ • heartbeats    │  │ • polling jobs  │  │ • k_score_update│              │
│  │ • work_credits  │  │ • claim locks   │  │ • node_join     │              │
│  │ • capabilities  │  │ • priorities    │  │ • consensus     │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
            ┌─────────────────────────┼─────────────────────────┐
            │                         │                         │
            ▼                         ▼                         ▼
     ┌─────────────┐           ┌─────────────┐           ┌─────────────┐
     │   Node 1    │           │   Node 2    │           │   Node N    │
     │             │           │             │           │             │
     │ Ed25519 key │           │ Ed25519 key │    ...    │ Ed25519 key │
     │ fingerprint │           │ fingerprint │           │ fingerprint │
     └─────────────┘           └─────────────┘           └─────────────┘
            │                         │                         │
            └─────────────────────────┼─────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              POSTGRESQL                                      │
│                         (Source of Truth)                                    │
│                                                                             │
│  • tokens (K-Score, signatures)                                             │
│  • holder_snapshots (conviction data)                                       │
│  • k_score_history (training data)                                          │
│  • nodes (registry, public keys)                                            │
│  • token_verifications (per-node signed proofs)                             │
│  • polling_tasks (distributed work queue)                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Node Lifecycle

### 1. Node Startup

```javascript
async function startNode() {
  // 1. Generate or load Ed25519 keypair
  const { publicKey, privateKey, fingerprint } = loadOrGenerateKeys();

  // 2. Register with PostgreSQL
  await db.query(`
    INSERT INTO nodes (node_id, node_public_key, node_key_fingerprint, ...)
    ON CONFLICT (node_id) DO UPDATE SET status = 'active', last_heartbeat = NOW()
  `);

  // 3. Register with Redis (ephemeral state)
  await redis.hset('nodes:live', nodeId, JSON.stringify({
    fingerprint,
    joined: Date.now(),
    capabilities: ['polling', 'webhooks', 'verification']
  }));
  await redis.hset('nodes:credits', nodeId, PHI); // Start with φ credits

  // 4. Subscribe to coordination channels
  await redis.subscribe('holdex:tasks', 'holdex:consensus', 'holdex:sync');

  // 5. Announce presence
  await redis.publish('holdex:nodes', JSON.stringify({
    event: 'node_join',
    node_id: nodeId,
    fingerprint,
    timestamp: Date.now()
  }));

  // 6. Start heartbeat loop
  setInterval(() => sendHeartbeat(), PHI * 1000); // Every φ seconds
}
```

### 2. Heartbeat System

```javascript
// Redis key: nodes:live (HASH)
// Redis key: nodes:credits (HASH)
// Redis key: nodes:last_work (HASH)

async function sendHeartbeat() {
  const now = Date.now();

  // Update Redis (fast, ephemeral)
  await redis.hset('nodes:live', nodeId, JSON.stringify({
    last_heartbeat: now,
    status: 'active'
  }));

  // Update PostgreSQL (persistent, less frequent)
  if (now - lastDbHeartbeat > 60000) { // Every minute
    await db.query(`
      UPDATE nodes SET last_heartbeat = $1, status = 'active'
      WHERE node_id = $2
    `, [now, nodeId]);
    lastDbHeartbeat = now;
  }

  // Regenerate work credits (φ⁻² per heartbeat)
  const currentCredits = await redis.hget('nodes:credits', nodeId);
  const newCredits = Math.min(PHI * PHI, parseFloat(currentCredits) + PHI_INV_SQ);
  await redis.hset('nodes:credits', nodeId, newCredits);
}

// Detect dead nodes (any node can do this)
async function pruneDeadNodes() {
  const nodes = await redis.hgetall('nodes:live');
  const now = Date.now();
  const DEAD_THRESHOLD = PHI * PHI * 1000 * 10; // ~26 seconds

  for (const [nodeId, data] of Object.entries(nodes)) {
    const { last_heartbeat } = JSON.parse(data);
    if (now - last_heartbeat > DEAD_THRESHOLD) {
      await redis.hdel('nodes:live', nodeId);
      await redis.hdel('nodes:credits', nodeId);
      await redis.publish('holdex:nodes', JSON.stringify({
        event: 'node_dead',
        node_id: nodeId,
        detected_by: currentNodeId
      }));
    }
  }
}
```

## Task Distribution

### Polling Task Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                     TASK LIFECYCLE                                │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. CREATION                                                     │
│     • Scheduled refresh (every φ³ minutes per token)             │
│     • Manual trigger (admin)                                     │
│     • Webhook anomaly (sudden activity spike)                    │
│                                                                  │
│  2. QUEUE                                                        │
│     Redis ZSET: tasks:polling                                    │
│     Score = priority (lower = more urgent)                       │
│     Value = {mint, created_at, reason, attempts}                 │
│                                                                  │
│  3. CLAIM                                                        │
│     Node pops task → SETNX lock → Execute                        │
│     Lock TTL = 60s (prevents stuck tasks)                        │
│                                                                  │
│  4. EXECUTE                                                      │
│     Poll Helius → Calculate K-Score → Sign → Write               │
│                                                                  │
│  5. COMPLETE                                                     │
│     Remove lock → Update credits → Publish result                │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### φ-Weighted Task Claiming

```javascript
const PHI = 1.618033988749895;
const PHI_INV = 1 / PHI;        // 0.618
const PHI_INV_SQ = 1 / (PHI*PHI); // 0.382

async function claimNextTask() {
  // 1. Get all live nodes and their credits
  const liveNodes = await redis.hgetall('nodes:live');
  const credits = await redis.hgetall('nodes:credits');

  const N = Object.keys(liveNodes).length;
  if (N === 0) return null;

  // 2. Calculate my claim probability
  const myCredits = parseFloat(credits[nodeId] || PHI);
  const totalCredits = Object.values(credits).reduce((s, c) => s + parseFloat(c), 0);
  const myProbability = myCredits / totalCredits;

  // 3. Random decision based on probability
  if (Math.random() > myProbability) {
    return null; // Let another node take it
  }

  // 4. Attempt to claim highest priority task
  const task = await redis.zpopmin('tasks:polling');
  if (!task) return null;

  const { mint, reason } = JSON.parse(task);
  const lockKey = `lock:poll:${mint}`;

  // 5. Atomic lock acquisition
  const locked = await redis.set(lockKey, nodeId, 'NX', 'EX', 60);
  if (!locked) {
    // Another node got it, put task back
    await redis.zadd('tasks:polling', Date.now(), task);
    return null;
  }

  // 6. Decrease my credits (I'm doing work)
  await redis.hincrbyfloat('nodes:credits', nodeId, -PHI_INV);

  return { mint, reason, lockKey };
}
```

### Consistent Token Assignment (Optional Optimization)

```javascript
// For predictable distribution, use consistent hashing
// Each node owns a "slot range" based on its position in the ring

function getTokenSlot(mint, totalSlots = 1024) {
  const hash = crypto.createHash('sha256').update(mint).digest();
  return hash.readUInt32BE(0) % totalSlots;
}

function getNodeForSlot(slot, liveNodes) {
  // Sort nodes by their fingerprint (deterministic order)
  const sorted = [...liveNodes].sort((a, b) =>
    a.fingerprint.localeCompare(b.fingerprint)
  );

  const slotsPerNode = Math.ceil(1024 / sorted.length);
  const nodeIndex = Math.floor(slot / slotsPerNode);

  return sorted[nodeIndex % sorted.length];
}

// Usage: "Primary" node for a token
// Other nodes can still verify (consensus)
async function shouldIPollThisToken(mint) {
  const slot = getTokenSlot(mint);
  const liveNodes = await getLiveNodes();
  const primaryNode = getNodeForSlot(slot, liveNodes);

  return primaryNode.node_id === nodeId;
}
```

## Consensus Mechanism

### K-Score Verification Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        CONSENSUS FLOW                                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Node A calculates K-Score for token X:                                 │
│                                                                         │
│  1. Calculate: K = 100 × ∛(D × O × L) = 72                              │
│                                                                         │
│  2. Sign verification:                                                  │
│     {mint, k_score: 72, timestamp, node_id}                             │
│     + Ed25519 signature                                                 │
│                                                                         │
│  3. Write to PostgreSQL:                                                │
│     INSERT INTO token_verifications (...)                               │
│                                                                         │
│  4. Broadcast to other nodes:                                           │
│     PUBLISH holdex:consensus {mint, k_score: 72, sig, node_id}          │
│                                                                         │
│  5. Other nodes receive, can:                                           │
│     a) Trust (if node A is reputable)                                   │
│     b) Verify independently (poll + calculate)                          │
│     c) Challenge (if score seems wrong)                                 │
│                                                                         │
│  6. Consensus reached when:                                             │
│     V ≥ N × φ⁻¹ nodes agree (within ±5 points)                          │
│     Where N = active nodes, V = verifying nodes                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Consensus Calculation

```javascript
async function calculateConsensus(mint) {
  const now = Date.now();
  const WINDOW = 24 * 60 * 60 * 1000; // 24h

  // Get all verifications in window
  const verifications = await db.query(`
    SELECT
      tv.node_id, tv.k_score, tv.verified_at, tv.node_signature,
      n.node_public_key, n.status as node_status
    FROM token_verifications tv
    JOIN nodes n ON tv.node_id = n.node_id
    WHERE tv.mint = $1
      AND tv.verified_at > $2
      AND n.status = 'active'
    ORDER BY tv.verified_at DESC
  `, [mint, now - WINDOW]);

  // Get active node count
  const activeNodes = await redis.hlen('nodes:live');

  // Filter to cryptographically valid verifications
  const valid = verifications.rows.filter(v => {
    if (!v.node_signature || !v.node_public_key) return false;
    return verifySignature(v, v.node_signature, v.node_public_key);
  });

  if (valid.length === 0) {
    return { status: 'unverified', confidence: 0 };
  }

  // Calculate score distribution
  const scores = valid.map(v => v.k_score);
  const median = scores.sort((a,b) => a-b)[Math.floor(scores.length/2)];
  const agreeing = scores.filter(s => Math.abs(s - median) <= 5);

  // φ-based consensus threshold
  const threshold = activeNodes * PHI_INV; // 61.8% of active nodes
  const consensusReached = agreeing.length >= threshold;

  // Confidence = agreeing / active (capped at φ⁻¹ + φ⁻² + φ⁻³ = 0.854)
  const confidence = Math.min(
    agreeing.length / activeNodes,
    PHI_INV + PHI_INV_SQ + (1/(PHI*PHI*PHI))
  );

  return {
    status: consensusReached ? 'consensus' : 'partial',
    k_score: median,
    verifications: valid.length,
    agreeing: agreeing.length,
    active_nodes: activeNodes,
    confidence,
    threshold_met: consensusReached
  };
}
```

## Database Schema Additions

```sql
-- Distributed polling tasks
CREATE TABLE IF NOT EXISTS polling_tasks (
  id SERIAL PRIMARY KEY,
  mint TEXT NOT NULL,
  priority INTEGER DEFAULT 100,  -- Lower = more urgent
  reason TEXT,                    -- 'scheduled', 'manual', 'anomaly'
  created_at BIGINT NOT NULL,
  claimed_by TEXT,                -- node_id
  claimed_at BIGINT,
  completed_at BIGINT,
  attempts INTEGER DEFAULT 0,
  last_error TEXT,

  CONSTRAINT unique_pending_task UNIQUE (mint, completed_at)
);

-- Index for efficient task claiming
CREATE INDEX idx_polling_tasks_pending
  ON polling_tasks (priority, created_at)
  WHERE completed_at IS NULL AND claimed_by IS NULL;

-- Node work history (for φ-weighted distribution)
CREATE TABLE IF NOT EXISTS node_work_history (
  id SERIAL PRIMARY KEY,
  node_id TEXT NOT NULL REFERENCES nodes(node_id),
  mint TEXT NOT NULL,
  work_type TEXT NOT NULL,  -- 'poll', 'verify', 'consensus'
  started_at BIGINT NOT NULL,
  completed_at BIGINT,
  rpc_calls INTEGER DEFAULT 0,
  success BOOLEAN,

  INDEX idx_node_work_recent (node_id, started_at DESC)
);

-- Consensus snapshots (for training data)
CREATE TABLE IF NOT EXISTS consensus_snapshots (
  mint TEXT NOT NULL,
  snapshot_date DATE NOT NULL,
  consensus_k_score INTEGER,
  verifications_count INTEGER,
  agreeing_count INTEGER,
  active_nodes INTEGER,
  confidence DECIMAL(5,4),

  PRIMARY KEY (mint, snapshot_date)
);
```

## Redis Keys Structure

```
# Node Registry (ephemeral)
nodes:live                    HASH    {node_id: {last_heartbeat, status, capabilities}}
nodes:credits                 HASH    {node_id: float}  # φ-based work credits
nodes:slots                   HASH    {slot: node_id}   # Consistent hashing (optional)

# Task Queue
tasks:polling                 ZSET    {priority: task_json}
lock:poll:{mint}              STRING  node_id (with TTL)

# Pub/Sub Channels
holdex:nodes                  CHANNEL  Node join/leave events
holdex:tasks                  CHANNEL  New task notifications
holdex:consensus              CHANNEL  K-Score verification broadcasts
holdex:sync                   CHANNEL  Data sync requests

# Metrics (for monitoring)
metrics:rpc_calls:{node_id}   STRING  count (with daily expiry)
metrics:verifications:{date}  HASH    {node_id: count}
```

## Configuration

### Environment Variables (per node)

```bash
# Node Identity
NODE_ID=gcrtrd                          # Unique node identifier
NODE_NAME="Genesis Node"                # Human-readable name
NODE_OPERATOR=asdfasdfa                 # Operator identifier
NODE_REGION=oregon                      # Geographic region
NODE_PRIVATE_KEY=base64_ed25519_key     # Ed25519 private key

# Shared Infrastructure
DATABASE_URL=postgresql://...           # Shared PostgreSQL
REDIS_URL=redis://...                   # Shared Redis

# Capabilities (what this node does)
NODE_CAP_POLLING=true                   # Participates in polling
NODE_CAP_WEBHOOKS=true                  # Receives webhooks
NODE_CAP_VERIFICATION=true              # Verifies other nodes' work

# Tuning
POLLING_INTERVAL_MS=1618                # φ seconds between poll attempts
HEARTBEAT_INTERVAL_MS=1618              # φ seconds between heartbeats
CONSENSUS_WINDOW_MS=86400000            # 24h verification window
```

### Scaling Guidelines

| Nodes | Tokens | Polling Interval | RPC Calls/Day | Consensus Threshold |
|-------|--------|------------------|---------------|---------------------|
| 1     | 1000   | 4.2h/token       | ~5,700        | N/A (single source) |
| 3     | 1000   | 4.2h/token       | ~1,900/node   | 2 nodes (61.8%)     |
| 5     | 1000   | 4.2h/token       | ~1,140/node   | 3 nodes (61.8%)     |
| 10    | 1000   | 4.2h/token       | ~570/node     | 6 nodes (61.8%)     |
| N     | T      | configurable     | T×6/N/day     | ceil(N×0.618)       |

## Implementation Phases

### Phase 1: Foundation (gcrtrd)
- [ ] Create `src/services/distributedPolling.js`
- [ ] Add Redis coordination keys
- [ ] Implement task queue (ZSET)
- [ ] Implement claim mechanism (SETNX)
- [ ] Test with single node (gcrtrd)

### Phase 2: Multi-Node
- [ ] Implement φ-weighted selection
- [ ] Add consensus broadcasting
- [ ] Create verification challenge system
- [ ] Test with 2-3 nodes

### Phase 3: Production
- [ ] Add monitoring/metrics
- [ ] Implement automatic rebalancing
- [ ] Add node reputation scoring
- [ ] Deploy N nodes

## Security Considerations

1. **Node Authentication**: Ed25519 signatures on all verifications
2. **Sybil Resistance**: New nodes start with limited credits
3. **Byzantine Tolerance**: 61.8% consensus resists up to 38.2% malicious nodes
4. **Work Verification**: Any node can challenge and re-verify
5. **Rate Limiting**: Credits prevent single node from dominating

## Monitoring

```javascript
// Prometheus metrics
const metrics = {
  nodes_active: new Gauge('holdex_nodes_active'),
  tasks_pending: new Gauge('holdex_tasks_pending'),
  rpc_calls_total: new Counter('holdex_rpc_calls_total'),
  consensus_reached: new Counter('holdex_consensus_reached'),
  verification_time: new Histogram('holdex_verification_seconds')
};
```

---

*"N nodes, one truth, verified by mathematics."*
