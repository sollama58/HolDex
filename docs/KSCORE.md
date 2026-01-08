# K-Score Algorithm v10

K-Score is a pure geometric mean formula that measures token conviction on a 0-100 scale.

## Philosophy

- **Simple**: One formula, no arbitrary weights
- **On-chain Pure**: All data from blockchain
- **Balanced**: Must excel on ALL dimensions
- **Transparent**: Open formula, verifiable

## Formula

```
K = 100 * ∛(D * O * L)
```

Where:
- **D** = Diamond Hands (conviction strength)
- **O** = Organic Growth (holder quality)
- **L** = Longevity (survival factor)

### Why Geometric Mean?

The geometric mean ensures:
1. Token must score well on ALL three pillars
2. A zero in any dimension = zero K-Score
3. Cannot game by maximizing only one metric
4. Natural 0-100 scale

## Pillar Breakdown

### D - Diamond Hands

Measures holder conviction through behavior analysis.

```
D = √(C * R * F)
```

| Component | Formula | Description |
|-----------|---------|-------------|
| C | (accumulators + TOP20) / analyzed | Conviction percentage |
| R | accumulators / (extractors + 1) | Accumulator/Extractor ratio |
| F | max(1/e, freshness) | Activity freshness (floor = 0.368) |

**Conviction Classes:**
- **Accumulator**: Net buyer (buy_count > sell_count)
- **Holder**: Neither buying nor selling
- **Reducer**: Slight net seller
- **Extractor**: Heavy net seller

### O - Organic Growth

Measures natural, distributed holder base.

```
O = √(H * T)
```

| Component | Formula | Description |
|-----------|---------|-------------|
| H | min(1, holders / TARGET) | Holder count score (TARGET = 1000) |
| T | 1 - (TOP20_holdings / circulating) | Top 20 distribution |

**Notes:**
- **Philosophy $asdfasdfa**: ALL unique holders count, no USD threshold
- Log normalization handles scale (1000 holders ≈ 0.70, 10K ≈ 0.82)
- T (top 20 distribution) handles quality/concentration
- TOP20 concentration penalizes whale dominance
- Target of 1000 holders for full score

### L - Longevity

Measures token survival and activity.

```
L = A * S
```

| Component | Formula | Description |
|-----------|---------|-------------|
| A | min(1, age_days / 365) | Age factor (approaches 1.0) |
| S | activity_score | Survival factor (0-1) |

**Survival Scoring:**
- Recent activity (< 7 days): High survival
- Moderate activity (7-30 days): Medium survival
- Low activity (> 30 days): Low survival
- Dead (> 90 days): Very low survival

## Score Interpretation

| K-Score | Tier | Icon | Quality | Description |
|---------|------|------|---------|-------------|
| 90-100 | Diamond | 💎 | Exceptional | Strong conviction, healthy distribution, proven survival |
| 80-89 | Platinum | 💠 | High Quality | Very good fundamentals across all metrics |
| 70-79 | Gold | 🥇 | Good Quality | Solid token with good holder behavior |
| 60-69 | Silver | 🥈 | Fair Quality | Average token, some positive indicators |
| 50-59 | Bronze | 🥉 | Speculative | Borderline - may have one weak metric |
| 40-49 | Copper | 🟤 | High Risk | Weak fundamentals, proceed with caution |
| 20-39 | Iron | ⚫ | Very High Risk | Multiple red flags, likely short-lived |
| 0-19 | Rust | 🔩 | Distressed | Failing or abandoned token |

## Acceptance Thresholds

For automated acceptance (GASdf, Oracle integrations):

| K-Score | Status | Action |
|---------|--------|--------|
| 70+ | Always Accepted | Gold tier or higher - high quality |
| 50-69 | Accepted | Silver/Bronze - acceptable quality |
| 30-49 | Review Required | Copper tier - manual review needed |
| < 30 | Rejected | Iron/Rust - not accepted |

**Hardcoded Accepts:** SOL, USDC, USDT, $ASDF (bypass K-Score check)

## Data Sources

### Helius API (Primary)
- **Holder Enumeration**: `getTokenLargestAccounts` (paginated)
- **Transfer History**: Webhook callbacks
- **Cost**: Paginated, no per-holder RPC cost

### Jupiter Price API (Market Data)
- Token prices (USD)
- Free tier via lite-api.jup.ag
- Not used for conviction (Helius-only)

### Raydium API (Pool Data)
- Liquidity, volume, pool info
- Free API (api-v3.raydium.io)
- Pool discovery and tracking

### On-chain RPC
- Supply, decimals
- Mint/freeze authority status

## Calculation Process

```
1. Fetch holder list (Helius, paginated, max 10K)
       │
       ▼
2. Analyze each holder:
   - Classify conviction (accumulator/holder/reducer/extractor)
   - Track buy/sell counts
   - Calculate net flow
       │
       ▼
3. Calculate D (Diamond Hands):
   - Conviction% from classifications
   - Accumulator/Extractor ratio
   - Activity freshness
       │
       ▼
4. Calculate O (Organic Growth):
   - Total unique holders
   - Top 20 concentration
       │
       ▼
5. Calculate L (Longevity):
   - Token age in days
   - Survival factor from activity
       │
       ▼
6. Combine: K = 100 * cubeRoot(D * O * L)
       │
       ▼
7. Sign data (8-category HMAC)
       │
       ▼
8. Store in database + Redis snapshot
```

## Update Frequency

| Priority | Volume Threshold | Staleness |
|----------|------------------|-----------|
| High | > $10K | 2 hours |
| Medium | $500 - $10K | 12 hours |
| Low | < $500 | 24 hours |

Community-verified tokens get priority updates.

## Database Fields

```sql
-- Core K-Score
k_score DOUBLE PRECISION,
last_k_score_update TIMESTAMP,

-- Conviction Data
conviction_score DOUBLE PRECISION,
conviction_accumulators INTEGER,
conviction_holders INTEGER,
conviction_reducers INTEGER,
conviction_extractors INTEGER,
conviction_analyzed INTEGER,

-- Holder Data
holders INTEGER,
real_holders INTEGER,
total_holders INTEGER,

-- Age Data
age_days DOUBLE PRECISION,
last_holder_check TIMESTAMP,

-- Signature
sig_kscore TEXT
```

## History Tracking

### k_score_history
Daily snapshots for trend analysis:
```sql
CREATE TABLE k_score_history (
    mint TEXT,
    date DATE,
    k_score DOUBLE PRECISION,
    conviction_score DOUBLE PRECISION,
    holders INTEGER,
    PRIMARY KEY (mint, date)
);
```

### Trends
- 30-day K-Score change
- 60-day K-Score change
- 90-day K-Score change
- Holder growth rate

## API Endpoints

### GET /api/token/:mint
Returns current K-Score with all components.

### GET /api/token/:mint/evolution
Returns historical K-Score trajectory.

### GET /oracle/kscore/:mint
Returns acceptance status for payments.

```json
{
  "k_score": 75,
  "tier": "Trusted",
  "accepted": true,
  "reason": "K-Score >= 50"
}
```

## Anti-Gaming Measures

1. **Geometric Mean**: Can't max one metric
2. **Top 20 Penalty**: Whale concentration hurts score
3. **Conviction Analysis**: Detects wash trading patterns
4. **Freshness Decay**: Old activity loses value
5. **Survival Factor**: Dead tokens score low
6. **Data Signatures**: Tamper detection

## Data Integrity System

### 8-Category Signature System

All K-Score data is cryptographically signed using HMAC-SHA256:

| Category | Signature | Protected Data |
|----------|-----------|----------------|
| Identity | `sig_identity` | name, symbol, image, decimals |
| Security | `sig_security` | mint/freeze authority, mutable |
| LP | `sig_lp` | LP burn %, locked %, status |
| Supply | `sig_supply` | supply, burned amount/% |
| K-Score | `sig_kscore` | k_score, conviction_*, holders |
| Market | `sig_market` | price, mcap, liquidity + provenance |
| Origin | `sig_origin` | is_pump_fun, bonding_complete |
| Holders | `sig_holders` | Top 20 holder balances (integrity) |
| Full | `sig_full` | HMAC of all sigs + chaos_nonce |

### Integrity Watchdog

- Scans all verified tokens every 5 minutes
- Detects tampering via signature verification
- Auto-restores from Redis snapshots (v3: includes holder data)
- Alerts on tampering detection/healing

### Philosophy $asdfasdfa

> "Don't Trust, Verify"

- All data cryptographically signed
- Snapshot v3 includes holder_snapshots for complete restoration
- Chaos nonce prevents signature prediction
- Key rotation support with zero downtime

## Version History

| Version | Changes |
|---------|---------|
| v10 | Pure geometric mean, simplified formula |
| v9 | Added freshness decay |
| v8 | Conviction class refinement |
| v7 | Top 20 distribution metric |
| v6 | Holder threshold removal |
| v5 | Initial production release |
