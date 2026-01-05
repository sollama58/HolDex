# Data Integrity System

HolDex implements an 8-category cryptographic signature system with self-healing capabilities.

## Philosophy

> "Don't Trust, Verify"

Every piece of data in the database is cryptographically signed. Tampering is detected and automatically healed.

## 8-Category Signature System

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
| 8 | Full | `sig_full` | HMAC(all 7 signatures + chaos_nonce) |

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
-- Per-category signatures
sig_identity TEXT,
sig_security TEXT,
sig_lp TEXT,
sig_supply TEXT,
sig_kscore TEXT,
sig_market TEXT,
sig_origin TEXT,

-- Master signature
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
const IGNORED_CATEGORIES = ['market']; // Market data refreshes too frequently
```

Market signatures may be stale without triggering healing, since price data updates constantly.
