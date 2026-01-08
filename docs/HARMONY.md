# Harmony Integration

The Harmony system is a phi-based economic engine shared between HolDex and GASdf.

## Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      Harmony Ecosystem                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐                      ┌──────────────┐        │
│  │    GASdf     │◄────── Oracle ──────►│   HolDex     │        │
│  │              │        Queries        │              │        │
│  │ - Payments   │                       │ - K-Score    │        │
│  │ - Burns      │────── Webhooks ──────►│ - E-Score    │        │
│  │ - Fees       │       (burns)         │ - Discounts  │        │
│  └──────────────┘                       └──────────────┘        │
│                                                                 │
│                    Shared: harmony.js                           │
│                    (identical in both repos)                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## The Golden Ratio (phi)

All economic ratios are derived from phi (1.618033988749895):

```javascript
const PHI = 1.618033988749895;

const RATIOS = {
  BURN:     1 / (PHI * PHI),     // 0.382 (38.2%)
  REWARDS:  1 / (PHI * PHI),     // 0.382 (38.2%)
  TREASURY: 1 / (PHI * PHI * PHI) // 0.236 (23.6%)
};
```

**Why phi?**
- Natural balance found throughout nature
- Self-similar proportions (fractal economics)
- Aesthetically pleasing distribution
- Sum of BURN + REWARDS + TREASURY = 1.0

## E-Score System

### Formula

E-Score is the geometric mean of 7 contribution dimensions with a diversity bonus:

```
E = geometricMean(dimensions) * (1 + (activeCount - 1) * 0.1)
```

### Dimensions

| Dimension | Source | Multiplier | Weight |
|-----------|--------|------------|--------|
| HOLD | holdings | 1.0 | Normal |
| BURN | total_burned | phi (1.618) | High |
| USE | api_calls_30d | 1.0 | Normal |
| BUILD | apps_live | phi² (2.618) | Highest |
| RUN | nodes_active | phi² (2.618) | Highest |
| REFER | referrals_active | phi (1.618) | High |
| TIME | days_active/30 | 1.0 | Normal |

**Multiplier Philosophy:**
- BURN, BUILD, RUN have phi multipliers
- Rewards permanent commitment over temporary capital
- Building and running infrastructure is valued most

### Dimension Scoring

Each dimension is log-scaled:
```javascript
score = Math.log10(1 + rawValue) * multiplier * 10;
```

### Diversity Bonus

Active dimensions provide a bonus:
- 1 active: 1.0x (no bonus)
- 2 active: 1.1x
- 3 active: 1.2x
- 7 active: 1.6x (maximum)

## Tiers

| E-Score | Tier | Icon |
|---------|------|------|
| 100+ | Legendary | 👑 |
| 50-99 | Mythic | 🔮 |
| 30-49 | Epic | ⚡ |
| 15-29 | Rare | 💎 |
| 7-14 | Uncommon | 🌟 |
| 3-6 | Common | ✨ |
| 1-2 | Starter | 🌱 |
| 0.1-0.9 | Seedling | 🫘 |
| 0 | Observer | 👁️ |

**Note:** Tiers are cosmetic only - no gatekeeping.

## Discount Calculation

### Three-Layer System

1. **Theoretical Discount** (E-Score only)
```javascript
// Pure PHI formula - no magic numbers
discount = min(95%, 1 - φ^(-eScore / 25));

// Where 25 = 5² (5 is Fibonacci number - phi connection)
```
- Uses golden ratio (φ) - aligned with $asdfasdfa philosophy
- Each 25 E-Score = one power of φ in discount
- Gives EXACT phi ratios at milestones (see table below)

2. **Efficiency Floor** (cost-based constraint)
```javascript
minFee = (operationCost / RATIOS.TREASURY) * SAFETY_MARGIN;
// minFee = (cost / 0.236) * 1.2
maxDiscount = 1 - (minFee / baseFee);
```
- Treasury must cover infrastructure costs
- 20% safety margin above actual cost

3. **Effective Discount** (minimum of above)
```javascript
effectiveDiscount = Math.min(theoretical, maxAllowed);
finalFee = Math.max(minFee, baseFee * (1 - effectiveDiscount));
```

### Discount Table (Pure PHI Milestones)

| E-Score | Discount | Phi Relationship |
|---------|----------|------------------|
| 0 | 0% | - |
| 5 | 9.2% | - |
| 10 | 17.5% | - |
| 20 | 32.0% | - |
| **25** | **38.2%** | **= 1/φ² (same as burn rate)** |
| 30 | 43.9% | - |
| **50** | **61.8%** | **= 1/φ (golden cut)** |
| **75** | **76.4%** | **= 1-1/φ³** |
| **100** | **85.4%** | **= 1-1/φ⁴** |
| ∞ | 95% (cap) | - |

## Fee Distribution

After discount is applied, the fee is distributed:

```
Total Fee
    │
    ├─── 38.2% ──► Burn (permanent deflation)
    │
    ├─── 38.2% ──► Rewards (participant distribution)
    │
    └─── 23.6% ──► Treasury (operations)
```

## Operation Costs

| Operation | Base Fee | Max Discount | Min Fee |
|-----------|----------|--------------|---------|
| gasdf_submit_standard | 100 $ASDF | 50% | ~50 |
| gasdf_submit_priority | 250 $ASDF | 40% | ~127 |
| gasdf_submit_instant | 500 $ASDF | 30% | ~254 |
| holdex_api_call | 1 $ASDF | 80% | ~0.5 |

## Database Tables

### participants
```sql
CREATE TABLE participants (
    wallet TEXT PRIMARY KEY,
    type TEXT DEFAULT 'user',

    -- Raw contribution metrics
    holdings DOUBLE PRECISION DEFAULT 0,
    total_burned DOUBLE PRECISION DEFAULT 0,
    api_calls_30d INTEGER DEFAULT 0,
    apps_live INTEGER DEFAULT 0,
    nodes_active INTEGER DEFAULT 0,
    referrals_active INTEGER DEFAULT 0,

    -- Cached E-Score
    cached_escore DOUBLE PRECISION DEFAULT 0,
    cached_tier TEXT DEFAULT 'Newcomer',
    cached_tier_icon TEXT DEFAULT '🌱',
    escore_updated_at TIMESTAMP,

    -- Timestamps
    first_activity_at TIMESTAMP,
    last_activity_at TIMESTAMP,

    -- Integrity
    sig_escore TEXT
);
```

### contributions
```sql
CREATE TABLE contributions (
    id SERIAL PRIMARY KEY,
    wallet TEXT NOT NULL,
    type TEXT NOT NULL,  -- 'burn', 'refer', 'build', 'run', 'api_call'
    amount DOUBLE PRECISION,
    source TEXT,
    tx_signature TEXT UNIQUE,
    verified BOOLEAN DEFAULT FALSE,
    verified_at TIMESTAMP,
    e_score_delta DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### reward_distributions
```sql
CREATE TABLE reward_distributions (
    id SERIAL PRIMARY KEY,
    source_tx TEXT NOT NULL,
    total_amount DOUBLE PRECISION NOT NULL,
    burn_amount DOUBLE PRECISION NOT NULL,     -- 38.2%
    rewards_amount DOUBLE PRECISION NOT NULL,   -- 38.2%
    treasury_amount DOUBLE PRECISION NOT NULL,  -- 23.6%
    status TEXT DEFAULT 'pending',
    distributed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### operation_costs
```sql
CREATE TABLE operation_costs (
    operation_type TEXT PRIMARY KEY,
    base_fee DOUBLE PRECISION NOT NULL,
    actual_cost DOUBLE PRECISION NOT NULL,
    min_fee DOUBLE PRECISION,
    max_discount DOUBLE PRECISION,
    is_active BOOLEAN DEFAULT TRUE,
    total_calls INTEGER DEFAULT 0,
    total_revenue DOUBLE PRECISION DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);
```

## Oracle API

### GET /oracle/escore/:wallet
Returns E-Score with tier and benefits.

### GET /oracle/discount/:wallet/:operation
Calculates final fee with discount.

### GET /oracle/costs
Returns all operation costs and phi constants.

### POST /oracle/webhook/burns
Receives burn notifications from GASdf.

**Headers:**
- `x-holdex-signature`: HMAC-SHA256(JSON.stringify(body), ORACLE_WEBHOOK_SECRET)

**Body:**
```json
{
  "wallet": "WALLET_ADDRESS",
  "amount": 100,
  "txSignature": "TX_SIGNATURE",
  "source": "gasdf"
}
```

## Synchronization

### Shared File
`/src/shared/harmony.js` must be identical in both HolDex and GASdf.

Contains:
- PHI constant
- RATIOS (BURN, REWARDS, TREASURY)
- TIERS array
- calculateEScore() function
- calculateDiscount() function
- getTierForScore() function

### HMAC Secret
Both services share `ORACLE_WEBHOOK_SECRET` / `HOLDEX_WEBHOOK_SECRET`:
- HolDex: `ORACLE_WEBHOOK_SECRET`
- GASdf: `HOLDEX_WEBHOOK_SECRET`

Must be 32+ characters.

## Integration Flow

```
1. User submits on GASdf
       │
       ▼
2. GASdf calls HolDex Oracle
   GET /oracle/discount/:wallet/:operation
       │
       ▼
3. HolDex calculates:
   - Fetch E-Score for wallet
   - Apply theoretical discount
   - Apply efficiency floor
   - Return final fee
       │
       ▼
4. User pays fee on GASdf
       │
       ▼
5. GASdf burns portion and notifies HolDex
   POST /oracle/webhook/burns
       │
       ▼
6. HolDex updates:
   - participants.total_burned
   - contributions (audit trail)
   - Invalidates E-Score cache
```

## Philosophy

> "Hold to enter. Burn to use. phi guides all ratios."

The Harmony system rewards:
- **Commitment** over speculation (burn > hold)
- **Building** over consuming (build multiplier = phi²)
- **Diversity** over concentration (diversity bonus)
- **Sustainability** over exploitation (efficiency floor)
