# Decentralization Path

> From single maintainer to federated network. Progressive, honest, measured.

## Current State

```
┌─────────────────────────────────────────────────────────────────┐
│                    2-NODE CONFIGURATION                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  NODE: zeyxx/Render              NODE: gcrtrd/Render             │
│  ┌──────────────────┐            ┌──────────────────┐            │
│  │ API + Calculator │            │ API + Calculator │            │
│  │ + Listener       │            │ + Listener       │            │
│  └────────┬─────────┘            └────────┬─────────┘            │
│           │                               │                      │
│           └───────────┬───────────────────┘                      │
│                       │                                          │
│            ┌──────────▼──────────┐                               │
│            │  SHARED PostgreSQL  │                               │
│            │  + Same SECRET      │                               │
│            │  + Same Helius Key  │                               │
│            └─────────────────────┘                               │
│                                                                  │
│  CENTRALIZED: Single DB, single signing key, single RPC source  │
│  DECENTRALIZED OPS: Two independent node operators              │
└─────────────────────────────────────────────────────────────────┘
```

## Trust Model

### Current Trust Assumptions

1. **Database Operator**: Trusts PostgreSQL host (Render shared DB)
2. **Signing Key**: Single `DATA_SIGNING_SECRET` shared by all nodes
3. **RPC Source**: Single Helius API key for on-chain data
4. **Calculation**: Any node can calculate K-Score, all produce same result

### Attack Vectors

| Vector | Current Mitigation | Future Mitigation |
|--------|-------------------|-------------------|
| DB tampering | Integrity watchdog auto-heals | Multi-node consensus |
| Key compromise | Rotate secret, re-sign all | Threshold signing (t-of-n) |
| RPC manipulation | Single source (Helius) | Multi-RPC consensus |
| Sybil nodes | Manual operator trust | E-Score stake + reputation |

## Node Registration

### Database Schema

```sql
-- Node registry
CREATE TABLE IF NOT EXISTS nodes (
    node_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    operator TEXT NOT NULL,           -- Operator wallet address

    -- Endpoints
    api_url TEXT,
    region TEXT,

    -- Health
    last_heartbeat BIGINT,
    uptime_30d DOUBLE PRECISION DEFAULT 0,

    -- Stats
    verifications_24h INTEGER DEFAULT 0,
    tokens_calculated INTEGER DEFAULT 0,
    consensus_rate DOUBLE PRECISION DEFAULT 1.0,

    -- Status
    status TEXT DEFAULT 'pending',    -- pending, active, degraded, offline
    joined_at BIGINT,

    -- Cryptographic identity (future)
    public_key TEXT,
    sig_node TEXT
);

-- Per-token verification log
CREATE TABLE IF NOT EXISTS token_verifications (
    mint TEXT NOT NULL,
    node_id TEXT NOT NULL,
    verified_at BIGINT NOT NULL,
    k_score DOUBLE PRECISION,
    signatures_valid BOOLEAN,
    PRIMARY KEY (mint, node_id)
);

-- Indexes
CREATE INDEX idx_nodes_status ON nodes(status);
CREATE INDEX idx_nodes_heartbeat ON nodes(last_heartbeat);
CREATE INDEX idx_verifications_mint ON token_verifications(mint);
```

### Heartbeat Protocol

Each node sends heartbeat every 60 seconds:

```javascript
// POST /internal/heartbeat
{
    node_id: "node-zeyxx-001",
    timestamp: Date.now(),
    version: "1.0.0",
    stats: {
        tokens_verified_1h: 1234,
        k_scores_calculated_1h: 567,
        uptime_seconds: 86400,
        memory_mb: 512,
        cpu_percent: 15
    },
    signature: "<HMAC of payload with shared secret>"
}

// Response
{
    acknowledged: true,
    nodes_active: 2,
    consensus_status: "healthy"
}
```

### Node Lifecycle

```
PENDING ──▶ ACTIVE ──▶ DEGRADED ──▶ OFFLINE
   │           │           │
   │           │           └── No heartbeat for 5 min
   │           │
   │           └── Heartbeat received, data synced
   │
   └── Registered, awaiting first heartbeat
```

## API Enhancement

### Current Token Response

```json
{
    "mint": "...",
    "name": "Token",
    "k_score": 75.4,
    "sig_full": "abc123..."
}
```

### Enhanced Token Response

```json
{
    "mint": "...",
    "name": "Token",
    "k_score": 75.4,
    "sig_full": "abc123...",
    "validation": {
        "nodes_active": 2,
        "nodes_agreed": 2,
        "consensus": "unanimous",
        "last_verified": 1704672000000,
        "i_score": 95.2,
        "verifiers": [
            {
                "node_id": "zeyxx",
                "verified_at": 1704672000000,
                "k_score": 75.4
            },
            {
                "node_id": "gcrtrd",
                "verified_at": 1704671900000,
                "k_score": 75.4
            }
        ]
    }
}
```

### New Endpoints

```
GET /api/nodes              -- List active nodes
GET /api/nodes/:id          -- Node details
GET /api/nodes/:id/stats    -- Node statistics
POST /internal/heartbeat    -- Node heartbeat (authenticated)
GET /api/token/:mint/verifications  -- All verifications for token
```

## I-Score: Infrastructure Health

The Integrity Score measures data trustworthiness:

```javascript
// I-Score formula: ∛(D × O × L)
// D: Coverage (signature completeness)
// O: Consistency (cross-node agreement)
// L: Recency (verification freshness)

function calculateIScore(token, nodes) {
    // D: What percentage of signatures are valid?
    const validCategories = countValidSignatures(token);
    const D = validCategories / 8;  // 8 signature categories

    // O: Do all nodes agree on K-Score?
    const kScores = nodes.map(n => n.k_score);
    const agreement = calculateAgreement(kScores, 5);  // ±5% tolerance
    const O = agreement;

    // L: How recent is the verification?
    const ageMs = Date.now() - token.last_verified;
    const halfLife = 24 * 60 * 60 * 1000;  // 24 hours
    const L = Math.max(0.1, Math.pow(0.5, ageMs / halfLife));

    return Math.cbrt(D * O * L) * 100;
}
```

### I-Score Thresholds

| Score | Status | Action |
|-------|--------|--------|
| 95-100 | Excellent | Fully verified, all nodes agree |
| 80-94 | Good | Minor gaps, acceptable |
| 60-79 | Warning | Needs attention, refresh recommended |
| 40-59 | Critical | Integrity breach, auto-heal triggered |
| 0-39 | Failed | Full recalculation required |

## Decentralization Roadmap

### Level 0: Bootstrap (Current)

- [x] Single maintainer (zeyxx)
- [x] Single signing key
- [x] Single RPC (Helius)
- [x] Shared PostgreSQL
- [x] Second node operator (gcrtrd)
- [ ] Node registration table
- [ ] Heartbeat system
- [ ] Node count display

### Level 1: Multi-Node Visibility

- [ ] Nodes table in database
- [ ] Heartbeat endpoint
- [ ] Display "Validated by N nodes" on tokens
- [ ] Expose I-Score publicly
- [ ] Track node operators via E-Score RUN dimension
- [ ] Multi-RPC fallback (Helius + public RPCs)

### Level 2: Federated Validation

- [ ] Multi-sig signing (2-of-3 nodes)
- [ ] Independent K-Score calculation per node
- [ ] Consensus requirement (agree within ±5%)
- [ ] Read replicas for database
- [ ] Node reputation scoring
- [ ] Byzantine fault detection

### Level 3: Permissionless

- [ ] Threshold signing (t-of-n)
- [ ] Own Solana RPC nodes
- [ ] On-chain K-Score anchoring (periodic)
- [ ] Stake-weighted consensus
- [ ] Permissionless node joining
- [ ] Slashing for bad behavior

## E-Score Integration

The RUN dimension incentivizes node operation:

```javascript
const MULTIPLIERS = {
    HOLD: 1.0,
    BURN: PHI,           // 1.618
    USE: 1.0,
    BUILD: PHI ** 2,     // 2.618
    RUN: PHI ** 2,       // 2.618  ← Node operators get φ² boost
    REFER: PHI,
    TIME: 1.0
};
```

### Node Operator Rewards

```
Run a node → nodes_active += 1 → E-Score increases (×φ²)
                                       ↓
                                 Fee discount increases
                                       ↓
                                 Operator benefits
                                       ↓
                                 More nodes join
                                       ↓
                                 Network more resilient
```

### Verification Flow

```
1. Wallet runs HolDex node
2. Node registers in database
3. Node sends heartbeat every 60s
4. Worker updates participant.nodes_active
5. E-Score recalculated with RUN dimension
6. Operator gets higher discount on GASdf fees
```

## Implementation Priority

### Sprint 1: Node Visibility

1. Add `nodes` table to database
2. Create heartbeat endpoint
3. Register current 2 nodes
4. Add node count to token responses

### Sprint 2: I-Score Exposure

1. Calculate I-Score per token
2. Add to API response
3. Create `/api/nodes` endpoint
4. Dashboard for node status

### Sprint 3: E-Score Integration

1. Track which wallets run nodes
2. Update `nodes_active` in participants
3. Recalculate E-Score for node operators
4. Verify discount application in GASdf

### Sprint 4: Multi-RPC

1. Add secondary RPC configuration
2. Implement fallback logic
3. Compare results between RPCs
4. Alert on divergence

## Security Considerations

### Node Authentication

```javascript
// Heartbeat must be signed with shared secret
function validateHeartbeat(payload, signature) {
    const expected = crypto
        .createHmac('sha256', DATA_SIGNING_SECRET)
        .update(JSON.stringify(payload))
        .digest('hex');

    return signature === expected;
}
```

### Operator Verification

Currently: Manual trust (invite-only)
Future: E-Score stake requirement + reputation

```javascript
// Future: Require minimum E-Score to run node
const MIN_ESCORE_FOR_NODE = 25;  // First φ threshold

if (operatorEScore < MIN_ESCORE_FOR_NODE) {
    throw new Error('Insufficient E-Score to operate node');
}
```

### Consensus Rules

```javascript
// K-Score consensus: all nodes must agree within tolerance
const KSCORE_TOLERANCE = 5;  // ±5 points

function checkConsensus(kScores) {
    const min = Math.min(...kScores);
    const max = Math.max(...kScores);
    return (max - min) <= KSCORE_TOLERANCE;
}
```

## Monitoring

### Health Metrics

```
# Node health
node_uptime_seconds{node_id="zeyxx"} 86400
node_last_heartbeat{node_id="zeyxx"} 1704672000000
node_verifications_24h{node_id="zeyxx"} 12345

# Consensus health
consensus_nodes_active 2
consensus_nodes_agreed 2
consensus_divergence_count 0

# I-Score distribution
iscore_excellent_count 8500
iscore_good_count 1200
iscore_warning_count 250
iscore_critical_count 50
```

### Alerts

| Condition | Severity | Action |
|-----------|----------|--------|
| Node offline > 5 min | Warning | Notify operator |
| Node offline > 1 hour | Critical | Exclude from consensus |
| K-Score divergence > 5 | Critical | Investigate, halt updates |
| I-Score < 60 for token | Warning | Queue recalculation |
| I-Score < 40 for token | Critical | Auto-heal from snapshot |

---

*Decentralization is not a destination—it's a direction. We move toward it progressively, measured by I-Score, incentivized by E-Score, validated by multiple nodes.*
