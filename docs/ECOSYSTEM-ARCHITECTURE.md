# $asdfasdfa Ecosystem Architecture

> From chaos to clarity. One source of truth. Everything burns.

## Current State

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CURRENT ARCHITECTURE                               │
│                              (Pre-Monorepo)                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐       │
│  │      GASdf       │    │      HolDex      │    │  asdf-manifesto  │       │
│  │                  │    │                  │    │                  │       │
│  │ Gas abstraction  │───▶│ Token analytics  │    │ Philosophy docs  │       │
│  │ Fee burning      │    │ K-Score oracle   │    │ Economic model   │       │
│  │ Jupiter swaps    │    │ E-Score engine   │    │                  │       │
│  │                  │    │ Integrity sigs   │    │                  │       │
│  └────────┬─────────┘    └────────┬─────────┘    └──────────────────┘       │
│           │                       │                                          │
│           │   DUPLICATED CODE     │                                          │
│           ├───────────────────────┤                                          │
│           │                       │                                          │
│  harmony.js (650 lines)  harmony.js (580 lines)                              │
│  holder-tiers.js         geometric-quality.js                                │
│  token-gate.js           claude-phi.js                                       │
│                                                                              │
│  STATUS: Files diverge. Bugs waiting to happen.                              │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Repositories

| Repo | Purpose | Status | Lines of Code |
|------|---------|--------|---------------|
| **HolDex** | Token analytics, K-Score oracle, E-Score engine | Live | ~15,000 |
| **GASdf** | Gas abstraction, fee burning, Jupiter swaps | Live | ~12,000 |
| **asdf-manifesto** | Philosophy, economics, public docs | Live | ~1,500 |
| **Ignition** | Fair launch platform | Development | TBD |
| **ASDForecast** | Prediction markets | Development | TBD |

## The Problem

1. **Duplicated Code**: `harmony.js` exists in both GASdf and HolDex with different implementations
2. **Divergent Logic**: E-Score calculated differently in each repo
3. **No Shared Packages**: Each repo copies code instead of importing
4. **Inconsistent Principles**: φ constants defined in multiple places
5. **Decentralization Blocked**: Can't add nodes without clear architecture

## Target Architecture: Monorepo

```
asdfasdfa/
├── packages/                    # Shared NPM packages
│   ├── @asdf/phi/               # φ constants, ratios, calculations
│   ├── @asdf/harmony/           # E-Score, tiers, discounts
│   ├── @asdf/integrity/         # Signatures, verification, watchdog
│   ├── @asdf/geometric/         # D×O×L quality metrics
│   └── @asdf/holdex-client/     # HolDex API client
│
├── apps/                        # Deployable applications
│   ├── holdex-api/              # HolDex API server
│   ├── holdex-calculator/       # K-Score worker
│   ├── holdex-listener/         # Token discovery
│   ├── gasdf-api/               # GASdf API server
│   ├── gasdf-burn-worker/       # Burn execution
│   └── ignition-api/            # Ignition (future)
│
├── docs/                        # Public documentation
│   ├── manifesto/               # Philosophy
│   ├── economics/               # Economic model
│   └── api/                     # API references
│
├── infra/                       # Infrastructure
│   ├── docker/                  # Docker configs
│   ├── render/                  # Render.yaml templates
│   └── nginx/                   # Load balancer
│
└── tools/                       # Development tools
    ├── eslint-config/           # Shared ESLint
    └── tsconfig/                # Shared TypeScript
```

## Shared Packages

### @asdf/phi

The golden ratio foundation.

```javascript
// packages/phi/index.js
const PHI = 1.618033988749895;

module.exports = {
    PHI,
    PHI_INV: 1 / PHI,           // 0.618...
    PHI_SQ: PHI ** 2,           // 2.618...
    PHI_CUBE: PHI ** 3,         // 4.236...

    // Standard thresholds
    THRESHOLDS: {
        EXCELLENT: 80,           // 100 × φ⁻¹ × φ⁻¹
        GOOD: 60,                // 100 × φ⁻¹
        WARNING: 40,             // 100 × φ⁻²
        CRITICAL: 25             // 100 × φ⁻³
    },

    // Fee distribution
    RATIOS: {
        BURN: 1 - 1/PHI**3,      // 76.4%
        TREASURY: 1/PHI**3       // 23.6%
    }
};
```

### @asdf/harmony

E-Score engine. Single source of truth.

```javascript
// packages/harmony/index.js
const { PHI } = require('@asdf/phi');

const MULTIPLIERS = {
    HOLD: 1.0,
    BURN: PHI,           // 1.618
    USE: 1.0,
    BUILD: PHI ** 2,     // 2.618
    RUN: PHI ** 2,       // 2.618
    REFER: PHI,          // 1.618
    TIME: 1.0
};

function calculateEScore(contributions) {
    // ... single implementation
}

function calculateDiscount(eScore) {
    return Math.min(0.95, 1 - Math.pow(PHI, -eScore / 25));
}

module.exports = { calculateEScore, calculateDiscount, MULTIPLIERS };
```

### @asdf/geometric

D×O×L quality scoring pattern.

```javascript
// packages/geometric/index.js
function geometricMean(...values) {
    if (values.some(v => v <= 0)) return 0;
    const product = values.reduce((a, b) => a * b, 1);
    return Math.pow(product, 1 / values.length);
}

function qualityScore(dimensions) {
    const { D, O, L } = dimensions;
    return 100 * Math.cbrt(D * O * L);
}

module.exports = { geometricMean, qualityScore };
```

### @asdf/integrity

Cryptographic signatures and verification.

```javascript
// packages/integrity/index.js
const crypto = require('crypto');

const CATEGORIES = [
    'identity', 'security', 'lp', 'supply',
    'kscore', 'market', 'origin', 'holders', 'full'
];

function sign(data, secret) {
    return crypto.createHmac('sha256', secret)
        .update(JSON.stringify(data))
        .digest('hex');
}

function verify(data, signature, secret) {
    return sign(data, secret) === signature;
}

module.exports = { sign, verify, CATEGORIES };
```

## The Three Scores

```
                         K-SCORE
                      Token Health
                    100 × ∛(D×O×L)
                           △
                          ╱ ╲
                         ╱   ╲
                        ╱     ╲
                       ╱ TRUST ╲
                      ╱   CORE  ╲
                     ╱           ╲
                    ╱             ╲
           E-SCORE ─────────────── I-SCORE
        Participant              Infrastructure
        Contribution                Health
        φ-weighted                 ∛(D×O×L)
```

### K-Score: Token Health (0-100)

**What it measures:** Objective token quality from on-chain data

**Formula:** `K = 100 × ∛(D × O × L)`
- D: Diamond Hands (conviction patterns)
- O: Organic Growth (distribution quality)
- L: Longevity (survival over time)

**Package:** `@asdf/geometric` + HolDex kScoreUpdater

### E-Score: Participant Engagement

**What it measures:** User contribution to ecosystem

**Formula:** `E = geometricMean(dimensions) × diversityBonus`

Seven dimensions with φ multipliers:
- HOLD (1.0), BURN (φ), USE (1.0), BUILD (φ²), RUN (φ²), REFER (φ), TIME (1.0)

**Package:** `@asdf/harmony`

### I-Score: Infrastructure Health

**What it measures:** Data integrity and node consensus

**Formula:** `I = ∛(D × O × L)`
- D: Coverage (signature completeness)
- O: Consistency (cross-node agreement)
- L: Recency (verification freshness)

**Package:** `@asdf/integrity` + HolDex integrityWatchdog

## Migration Path

### Phase 1: Extract Packages (Week 1-2)

1. Create monorepo structure with Turborepo/Nx
2. Extract `@asdf/phi` from claude-phi.js
3. Extract `@asdf/harmony` from harmony.js (HolDex version = source of truth)
4. Extract `@asdf/geometric` from geometric-quality.js
5. Extract `@asdf/integrity` from dataSignature.js

### Phase 2: Migrate Apps (Week 3-4)

1. Create `apps/holdex-api` importing shared packages
2. Create `apps/gasdf-api` importing shared packages
3. Remove duplicated code from both
4. Update imports to use `@asdf/*`

### Phase 3: Node Registration (Week 5-6)

1. Add `nodes` table to database
2. Implement heartbeat system
3. Display node count on API responses
4. Track node operators via E-Score RUN dimension

### Phase 4: Documentation (Week 7)

1. Move asdf-manifesto to `docs/manifesto/`
2. Generate API docs from code
3. Update README files

## Node Architecture

### Current: 2 Nodes

```
NODE: zeyxx/Render              NODE: gcrtrd/Render
┌──────────────────┐            ┌──────────────────┐
│ HolDex API       │            │ HolDex API       │
│ + Calculator     │            │ + Calculator     │
│ + Listener       │            │ + Listener       │
└────────┬─────────┘            └────────┬─────────┘
         │                               │
         └───────────┬───────────────────┘
                     │
          ┌──────────▼──────────┐
          │  SHARED PostgreSQL  │
          │  + Same SECRET      │
          │  + Same Helius Key  │
          └─────────────────────┘
```

### Future: Federated Nodes

```
┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
│ Node 1  │ │ Node 2  │ │ Node 3  │ │ Node N  │
└────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘
     │           │           │           │
     └───────────┴─────┬─────┴───────────┘
                       │
              ┌────────▼────────┐
              │    CONSENSUS    │
              │  (2-of-3 sigs)  │
              └────────┬────────┘
                       │
          ┌────────────┴────────────┐
          │                         │
    ┌─────▼─────┐            ┌──────▼──────┐
    │ PostgreSQL │            │  On-Chain   │
    │  Primary   │            │   Anchor    │
    └───────────┘            └─────────────┘
```

## Core Principles

### 1. Single Source of Truth

Every calculation has ONE canonical implementation in a shared package.

```
BAD:  GASdf/harmony.js (650 lines) + HolDex/harmony.js (580 lines)
GOOD: @asdf/harmony (single package, imported by both)
```

### 2. φ (Phi) Consistency

All ratios derive from φ = 1.618033988749895

```javascript
const { PHI } = require('@asdf/phi');
// Never hardcode 1.618, always use PHI constant
```

### 3. Geometric Mean Everything

Quality = ∛(D × O × L). If any dimension is zero, the score collapses.

```javascript
const { qualityScore } = require('@asdf/geometric');
const k = qualityScore({ D: conviction, O: distribution, L: longevity });
```

### 4. Cryptographic Verification

All data is signed. Don't trust, verify.

```javascript
const { verify } = require('@asdf/integrity');
if (!verify(tokenData, signature, secret)) {
    throw new Error('Signature mismatch - data tampered');
}
```

### 5. 100% Burn

All fees burn. No exceptions. No treasury skim on $asdfasdfa payments.

```javascript
if (paymentToken === ASDF_MINT) {
    burnAmount = feeAmount;  // 100%
} else {
    burnAmount = feeAmount * RATIOS.BURN;  // 76.4%
}
```

## File Mapping

### Current → Monorepo

| Current Location | Target Package |
|------------------|----------------|
| `HolDex/src/shared/claude-phi.js` | `@asdf/phi` |
| `HolDex/src/shared/harmony.js` | `@asdf/harmony` |
| `GASdf/src/services/harmony.js` | DELETE (use @asdf/harmony) |
| `HolDex/src/shared/geometric-quality.js` | `@asdf/geometric` |
| `HolDex/src/utils/dataSignature.js` | `@asdf/integrity` |
| `HolDex/src/shared/holdexClient.js` | `@asdf/holdex-client` |
| `GASdf/src/services/holdex.js` | DELETE (use @asdf/holdex-client) |

## Database Schema

### Shared Tables (PostgreSQL)

```sql
-- Tokens with K-Score and signatures
CREATE TABLE tokens (...);

-- Holder conviction tracking
CREATE TABLE holder_snapshots (...);

-- Participant E-Score
CREATE TABLE participants (...);

-- Node registry (NEW)
CREATE TABLE nodes (
    node_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    operator TEXT NOT NULL,
    api_url TEXT,
    last_heartbeat BIGINT,
    status TEXT DEFAULT 'active'
);

-- Token verification per node (NEW)
CREATE TABLE token_verifications (
    mint TEXT,
    node_id TEXT,
    verified_at BIGINT,
    k_score DOUBLE PRECISION,
    PRIMARY KEY (mint, node_id)
);
```

## API Endpoints

### HolDex

| Endpoint | Purpose |
|----------|---------|
| `GET /api/tokens` | Paginated token list |
| `GET /api/token/:mint` | Token details + K-Score |
| `GET /api/token/:mint/verify` | Verify signatures |
| `GET /oracle/escore/:wallet` | E-Score lookup |
| `GET /oracle/kscore/:mint` | K-Score lookup |

### GASdf

| Endpoint | Purpose |
|----------|---------|
| `POST /v1/quote` | Get fee quote |
| `POST /v1/submit` | Submit transaction |
| `GET /v1/tokens` | Accepted tokens |
| `GET /v1/stats` | Burn statistics |

## Environment Variables

### Shared (all apps)

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | PostgreSQL connection |
| `REDIS_URL` | Yes | Redis connection |
| `DATA_SIGNING_SECRET` | Yes | HMAC secret (min 32 chars) |
| `HELIUS_API_KEY` | Yes | Solana RPC |
| `ASDF_MINT` | Yes | $asdfasdfa token address |

### HolDex-specific

| Variable | Required | Description |
|----------|----------|-------------|
| `ADMIN_PASSWORD` | Prod | Admin API access |
| `DEXSCREENER_API_KEY` | No | Premium data |

### GASdf-specific

| Variable | Required | Description |
|----------|----------|-------------|
| `FEE_PAYER_PRIVATE_KEY` | Yes | Fee payer wallet |
| `HOLDEX_API_URL` | No | HolDex endpoint |

---

*One codebase. Shared packages. Everything burns. This is the architecture.*
