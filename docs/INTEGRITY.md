# Data Integrity System

HolDex implements a 9-category cryptographic signature system with self-healing capabilities and multi-node verification.

## Philosophy

> "Don't Trust, Verify" - $asdfasdfa

Every piece of data in the database is cryptographically signed. Tampering is detected and automatically healed. Multiple nodes can independently verify data integrity.

## 9-Category Signature System

### Categories

| # | Category | Signature | Protected Data |
|---|----------|-----------|----------------|
| 1 | Identity | `sig_identity` | mint, name, symbol, image, decimals |
| 2 | Security | `sig_security` | mint_authority_revoked, freeze_authority_revoked, is_mutable, verified |
| 3 | LP | `sig_lp` | lp_burn_pct, lp_locked_pct, lp_status |
| 4 | Supply | `sig_supply` | supply, initial_supply, burned_amount, burned_percent |
| 5 | K-Score | `sig_kscore` | k_score, conviction_*, holders, real_holders |
| 6 | Market | `sig_market` | priceusd, marketcap, liquidity, priceSource, priceTimestamp |
| 7 | Origin | `sig_origin` | is_pump_fun, bonding_curve_complete, timestamp |
| 8 | Holders | `sig_holders` | Top 20 holder balances from holder_snapshots |
| 9 | Full | `sig_full` | HMAC(signatures 1-7 + chaos_nonce) |

### Signature Generation

```javascript
const crypto = require('crypto');

function sign(category, data, secret) {
  const payload = JSON.stringify(data, Object.keys(data).sort());
  return crypto
    .createHmac('sha256', secret)
    .update(payload)
    .digest('hex');
}
```

**Key Points:**
- JSON keys are sorted for deterministic hashing
- Uses HMAC-SHA256
- Secret from `DATA_SIGNING_SECRET` env var

### Full Signature

The full signature chains all 7 category signatures:

```javascript
function signFull(token, chaosNonce, secret) {
  const payload = [
    token.sig_identity,
    token.sig_security,
    token.sig_lp,
    token.sig_supply,
    token.sig_kscore,
    token.sig_market,
    token.sig_origin,
    chaosNonce
  ].join('|');

  return crypto
    .createHmac('sha256', secret)
    .update(payload)
    .digest('hex');
}
```

## Chaos Nonce

Each token has a `chaos_nonce` field:

```sql
chaos_nonce TEXT  -- 16 random hex bytes
```

**Purpose:**
- Regenerated on every update
- Makes signatures unpredictable
- Prevents pattern analysis attacks
- Part of full signature calculation

**Generation:**
```javascript
const chaosNonce = crypto.randomBytes(16).toString('hex');
```

## Verification Process

```javascript
function verify(token) {
  const results = {};

  // Verify each category
  results.identity = verifyCategory('identity', token);
  results.security = verifyCategory('security', token);
  results.lp = verifyCategory('lp', token);
  results.supply = verifyCategory('supply', token);
  results.kscore = verifyCategory('kscore', token);
  results.market = verifyCategory('market', token);
  results.origin = verifyCategory('origin', token);

  // Verify full signature
  results.full = verifyFull(token);

  return results;
}
```

## Integrity Watchdog

### Location
`/src/tasks/integrityWatchdog.js`

### Function
Self-healing background task that:
1. Scans all tokens every 5 minutes
2. Verifies all 8 signatures
3. Detects tampering
4. Restores from Redis snapshot
5. Re-signs with fresh chaos_nonce

### Process

```
1. Load all tokens from database
       │
       ▼
2. For each token:
   - Verify all 8 signatures
   - Compare against stored values
       │
       ▼
3. If tampering detected:
   - Log incident
   - Alert to Discord
   - Fetch snapshot from Redis
       │
       ▼
4. Restore data:
   - Overwrite tampered fields
   - Generate new chaos_nonce
   - Re-sign all categories
       │
       ▼
5. Save healed record
```

### Resilience

- **Attack Cost**: Attacker modifies DB
- **Defense Cost**: We restore for free (snapshot read, no RPC)
- **Snapshot Lifetime**: 7-day TTL in Redis
- **Snapshot Frequency**: After every K-Score update

## Redis Snapshots

### Storage Format
```javascript
const key = `snapshot:${mint}`;
const value = JSON.stringify({
  data: tokenRecord,
  timestamp: Date.now(),
  signatures: {
    identity: sig_identity,
    security: sig_security,
    // ... all 8 signatures
  }
});

await redis.setex(key, 7 * 24 * 3600, value); // 7-day TTL
```

### Snapshot Trigger
- After successful K-Score calculation
- After security audit completion
- After LP verification

## Key Rotation

Supports zero-downtime key rotation:

```env
DATA_SIGNING_SECRET=new_secret_key
DATA_SIGNING_SECRET_PREVIOUS=old_secret_key
```

**Verification Order:**
1. Try current secret
2. If fails, try previous secret
3. If previous succeeds, re-sign with new secret

## API Endpoint

### GET /api/token/:mint/verify

Returns integrity report:

```json
{
  "mint": "9zB5wRar...",
  "integrity": {
    "identity": "valid",
    "security": "valid",
    "lp": "valid",
    "supply": "valid",
    "kscore": "valid",
    "market": "stale",
    "origin": "valid",
    "full": "valid"
  },
  "staleness": {
    "k_score": "2h ago",
    "price": "5m ago",
    "holders": "1h ago"
  },
  "chaos_nonce": "a1b2c3d4..."
}
```

### Status Values
- `valid`: Signature matches data
- `invalid`: Signature mismatch (tampering)
- `stale`: Data too old (needs refresh)
- `missing`: No signature present

## Alerting

Tampering triggers Discord/Telegram alerts:

```javascript
{
  title: "⚠️ Data Tampering Detected",
  fields: [
    { name: "Token", value: mint },
    { name: "Category", value: category },
    { name: "Expected", value: expectedSig },
    { name: "Found", value: actualSig },
    { name: "Action", value: "Auto-healing from snapshot" }
  ]
}
```

## Database Schema

```sql
-- Per-category signatures (1-7)
sig_identity TEXT,
sig_security TEXT,
sig_lp TEXT,
sig_supply TEXT,
sig_kscore TEXT,
sig_market TEXT,
sig_origin TEXT,

-- Holder snapshots signature (8)
sig_holders TEXT,
holders_snapshot_check BIGINT,  -- TTL tracking

-- Master signature (9)
sig_full TEXT,

-- Entropy
chaos_nonce TEXT
```

## Security Considerations

1. **Secret Storage**: Never commit secrets to repo
2. **Key Length**: Minimum 32 characters
3. **Rotation**: Rotate annually or after compromise
4. **Monitoring**: Alert on any verification failure
5. **Backup**: Redis snapshots are critical

## Ignored Categories

Some categories are ignored during healing to prevent loops:

```javascript
const IGNORED_CATEGORIES = ['market', 'full']; // Volatile data
```

Market signatures may be stale without triggering healing, since price data updates constantly.

---

## Multi-Node Architecture

HolDex supports distributed verification across multiple nodes. This enables the **Optimistic Burn Protocol** where independent nodes verify data integrity.

### Architecture Overview

```
                    ┌─────────────────────────────────────┐
                    │         SHARED INFRASTRUCTURE       │
                    │                                     │
                    │  ┌─────────────────────────────┐   │
                    │  │      PostgreSQL (Shared)     │   │
                    │  │   • Source of truth          │   │
                    │  │   • All tokens + signatures  │   │
                    │  │   • holder_snapshots         │   │
                    │  └─────────────────────────────┘   │
                    │                                     │
                    │  ┌─────────────────────────────┐   │
                    │  │   DATA_SIGNING_SECRET        │   │
                    │  │   (Shared across all nodes)  │   │
                    │  └─────────────────────────────┘   │
                    └─────────────────────────────────────┘
                                     │
           ┌─────────────────────────┼─────────────────────────┐
           │                         │                         │
           ▼                         ▼                         ▼
    ┌─────────────┐           ┌─────────────┐           ┌─────────────┐
    │   NODE 1    │           │   NODE 2    │           │   NODE N    │
    │             │           │             │           │             │
    │ ┌─────────┐ │           │ ┌─────────┐ │           │ ┌─────────┐ │
    │ │  Redis  │ │           │ │  Redis  │ │           │ │  Redis  │ │
    │ │ (Local) │ │           │ │ (Local) │ │           │ │ (Local) │ │
    │ └─────────┘ │           │ └─────────┘ │           │ └─────────┘ │
    │             │           │             │           │             │
    │ Watchdog    │           │ Watchdog    │           │ Watchdog    │
    │ verifies    │           │ verifies    │           │ verifies    │
    │ independently│          │ independently│          │ independently│
    └─────────────┘           └─────────────┘           └─────────────┘
```

### Why Separate Redis?

Each node maintains its own Redis instance for **independent verification**:

| Component | Shared? | Reason |
|-----------|---------|--------|
| PostgreSQL | ✅ Yes | Single source of truth for all data |
| DATA_SIGNING_SECRET | ✅ Yes | Identical signatures across nodes |
| Redis | ❌ No | Independent snapshot verification |
| HELIUS_API_KEY | Optional | Can share or use separate quotas |

**Benefits of separate Redis:**
1. **No single point of failure** - If one Redis fails, other nodes continue
2. **Independent healing** - Each node can detect/heal tampering independently
3. **Consensus detection** - Divergent snapshots indicate potential issues
4. **Geographic distribution** - Nodes can run in different regions

### Environment Variables

#### Shared (Must be identical)

```env
# CRITICAL: Same across ALL nodes
DATA_SIGNING_SECRET=your-32-char-minimum-secret
DATABASE_URL=postgresql://user:pass@host:5432/holdex

# If using Oracle/webhooks integration
ORACLE_WEBHOOK_SECRET=your-oracle-secret
WEBHOOK_SECRET=your-helius-webhook-secret
```

#### Per-Node (Each node has its own)

```env
# Each node's own Redis
REDIS_URL=redis://localhost:6379

# Can be shared or separate (API quotas)
HELIUS_API_KEY=your-helius-key

# Node-specific
ADMIN_PASSWORD=node-specific-password
NODE_ENV=production
PORT=3000
```

### Snapshot Format v3

Multi-node setups use snapshot v3 which includes holder data:

```javascript
{
  // Token data
  mint: "9zB5...",
  k_score: 75,
  holders: 1234,
  // ... all token fields

  // Metadata
  _snapshotTime: 1704067200000,
  _snapshotVersion: 3,

  // v3: Holder snapshots for complete restoration
  _holderSnapshots: [
    { holder: "wallet1...", balance: "1000000000" },
    { holder: "wallet2...", balance: "500000000" },
    // ... top 20 holders
  ]
}
```

### Verification Flow (Multi-Node)

```
Node 1                    Database                    Node 2
   │                          │                          │
   │──── Read token ─────────>│<──── Read token ────────│
   │                          │                          │
   │<─── Token + sigs ────────│────── Token + sigs ────>│
   │                          │                          │
   ▼                          │                          ▼
Verify locally                │                 Verify locally
   │                          │                          │
   │ sig_kscore ✓             │             sig_kscore ✓ │
   │ sig_holders ✓            │            sig_holders ✓ │
   │ sig_identity ✓           │           sig_identity ✓ │
   │                          │                          │
   ▼                          │                          ▼
Store snapshot               │                 Store snapshot
(own Redis)                   │                 (own Redis)
```

### Tampering Detection

When tampering is detected:

1. **Single node detects** → Heals from its own Redis snapshot
2. **Multiple nodes detect** → Each heals independently (convergent)
3. **Divergent detection** → Indicates potential attack or bug

```javascript
// Watchdog logs on each node
[Watchdog] TAMPERED: TOKEN (mint123...) - categories: kscore,holders
[Watchdog] Restoring 20 holder snapshots for mint123...
[Watchdog] HEALED: mint123... (tampered: kscore,holders)
```

### Holder Signature (sig_holders)

Protects the `holder_snapshots` table independently:

```javascript
function signHolders(mint, snapshots) {
  const sorted = snapshots
    .sort((a, b) => BigInt(b.balance) - BigInt(a.balance))
    .slice(0, 20);

  const data = [
    mint,
    sorted.length,
    ...sorted.map(s => `${s.holder}:${s.balance}`)
  ].join('|');

  return hmacSign(data);
}
```

**Verification requires holder_snapshots:**
```javascript
const result = verifyAllSignatures(token, {
  holderSnapshots: await db.all(
    'SELECT holder, balance FROM holder_snapshots WHERE mint = $1',
    [mint]
  )
});
```

### Node Setup Checklist

#### For a new node joining the network:

- [ ] Get `DATA_SIGNING_SECRET` from existing operator
- [ ] Get `DATABASE_URL` (same shared DB)
- [ ] Set up own Redis instance
- [ ] Configure `HELIUS_API_KEY` (shared or own)
- [ ] Set `NODE_ENV=production`
- [ ] Deploy and verify watchdog starts
- [ ] Check logs for successful signature verification

#### Verification command:
```bash
# Check if node is verifying correctly
curl https://your-node/api/token/MINT_ADDRESS/verify
```

### Optimistic Burn Protocol

The multi-node setup enables trustless burn verification:

1. **User burns tokens** → Transaction on-chain
2. **Node 1 detects** → Updates DB, signs data
3. **Node 2 verifies** → Same signature = consensus
4. **Credits issued** → After N confirmations

```
Burn TX ──> Node 1 signs ──> DB updated ──> Node 2 verifies ──> Consensus
                │                                   │
                └─── Same DATA_SIGNING_SECRET ──────┘
                     = Identical signatures
```

### Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| Signature mismatch between nodes | Different secrets | Verify `DATA_SIGNING_SECRET` is identical |
| Snapshots not found | Redis not connected | Check `REDIS_URL` configuration |
| Healing loops | Market data changing | Ensure `market` is in `IGNORED_CATEGORIES` |
| holder sig fails | Snapshots out of sync | Run deep refresh on affected tokens |
