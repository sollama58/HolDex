# $ASDFASDFA Ecosystem - Architecture & Workflow

> "Don't Trust, Verify. Don't Extract, Burn." - The philosophy behind everything we build.

## Core Principle: 100% Burn

**Every fee across the entire ecosystem burns $asdfasdfa.**

```
┌─────────────────────────────────────────────────────────────────┐
│                     ALL FEES → 100% BURN                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  GASdf        → Pay gas in any token  → Burns $asdfasdfa       │
│  HolDex       → API queries           → Burns $asdfasdfa       │
│  ASDForecast  → Prediction fees       → Burns $asdfasdfa       │
│  Ignition     → Launch fees           → Burns $asdfasdfa       │
│  [Future]     → Any app fees          → Burns $asdfasdfa       │
│                                                                 │
│  Zero extraction. Zero treasury skim. Zero team cut.           │
│  All value returns to holders via deflation.                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**See**: [asdf-manifesto](https://github.com/zeyxx/asdf-manifesto) for full philosophy and economics.

## Vision: HolDex LLM

The ultimate goal is an AI (HolDex LLM) capable of predicting liquidity movements and on-chain behavior BEFORE they happen. Every project in this ecosystem feeds training data toward that goal.

```
                    ┌─────────────────────────────────────────┐
                    │           ASDFASDFA ECOSYSTEM           │
                    │         "Friction is Training Data"      │
                    └─────────────────────────────────────────┘
                                       │
          ┌────────────────────────────┼────────────────────────────┐
          │                            │                            │
          ▼                            ▼                            ▼
    ┌──────────┐               ┌──────────────┐              ┌───────────┐
    │  GASdf   │◄─────────────►│   HolDex     │◄────────────►│   Apps    │
    │ (Physics)│               │  (Brain)     │              │(Products) │
    └──────────┘               └──────────────┘              └───────────┘
          │                            │                            │
          │  Token Validation          │  K-Score Oracle            │
          │  Fee Discounts             │  E-Score Tiers             │
          │  Burn Notifications        │  Harmony Economics         │
          │                            │                            │
          └────────────────────────────┴────────────────────────────┘
                                       │
                                       ▼
                            ┌──────────────────┐
                            │  asdf-burn-engine │
                            │   (On-Chain)      │
                            └──────────────────┘
```

---

## The 3 Layers

### Layer 1: GASdf (The Physics Layer)

**Purpose**: Pay gas fees with any token. Friction is fuel for training.

**Repository**: `zeyxx/GASdf`
**Tech**: Node.js/Express, Jupiter v6, Helius RPC
**Status**: Production at https://asdfasdfa.tech

**Key Flows**:
```
1. Quote → Validate token via HolDex K-Score (≥50)
2. Submit → Co-sign transaction, broadcast
3. Burn  → 100% of fee converted to $asdfasdfa and burned
```

**Integration Points**:
- `GET /api/token/:mint` → HolDex K-Score validation
- `POST /oracle/webhook/burns` → Notify HolDex of burns
- `GET /oracle/discount/:wallet/:operation` → E-Score fee discounts

---

### Layer 2: HolDex (The Intelligence Layer)

**Purpose**: K-Score algorithm + Harmony economics oracle

**Repository**: `zeyxx/HolDex` (this repo)
**Tech**: Node.js/Express, PostgreSQL/TimescaleDB, Redis
**Status**: Production (multiple services)

**K-Score Formula**: `K = 100 × ∛(D × O × L)`
- **D** (Diamond Hands): Conviction strength
- **O** (Organic Growth): Anti-sniper distribution
- **L** (Longevity): Survival factor

**Oracle API**:
```
GET  /oracle/kscore/:mint     → Token acceptance
GET  /oracle/escore/:wallet   → Engagement score & tier
GET  /oracle/discount/:wallet → PHI-based fee discounts
POST /oracle/webhook/burns    → Burn event notifications
```

**9-Category Signature System**:
- `sig_identity`, `sig_security`, `sig_lp`, `sig_supply`
- `sig_kscore`, `sig_market`, `sig_origin`, `sig_holders`
- `sig_full` (HMAC chain of all + chaos_nonce)

---

### Layer 3: Apps (The Interface Layer)

**Products built on the ecosystem**:

| App | Purpose | Fee Flow |
|-----|---------|----------|
| **Ignition** | Fair launch platform (pump.fun replacement) | 100% → BURN |
| **ASDForecast** | 15-min SOL/USD predictions | 100% → BURN |
| **ASDev** | Token launcher with airdrops | 100% → BURN |
| **asdf-oracle** | Real-time dashboard | Visualization only |

---

## On-Chain: asdf-burn-engine

**Program ID**: `ASDFc5hkEM2MF8mrAAtCPieV6x6h1B5BwjgztFt7Xbui`

**Three Revenue Channels**:
1. **Trading Volume** → Creator fees auto-buyback + burn
2. **Ecosystem Apps** → 99.448% burned, 0.552% rebated
3. **Token Hierarchy** → 44.8% to root treasury, 55.2% retained

**Account Types**:
- `DATState` - Global config (fee splits, admin, pause flags)
- `TokenStats` - Per-token burn metrics
- `RebatePool` - External app deposit rebates
- `ValidatorState` - Trustless fee tracking

---

## Golden Ratio Economics (PHI = 1.618...)

PHI ratios govern internal metrics and discounts:

```
PHI = 1.618033988749895

Fee Distribution:
└── 100% → BURN (no extraction, no treasury, no team)

Quality Thresholds (K-Score, E-Score):
├── 80  (φ⁻¹ × φ⁻¹) → Excellent
├── 60  (φ⁻¹)       → Good
├── 40  (φ⁻²)       → Warning
└── 25  (φ⁻³)       → Critical

E-Score Discount Formula:
discount = min(95%, 1 - φ^(-eScore / 25))
```

**E-Score Tiers**:
| Score | Tier | Emoji |
|-------|------|-------|
| 100+  | Legendary | 👑 |
| 50-99 | Mythic | 🔮 |
| 30-49 | Epic | ⚡ |
| 15-29 | Rare | 💎 |
| 7-14  | Uncommon | 🌟 |
| 3-6   | Common | ✨ |
| 1-2   | Starter | 🌱 |

---

## Repository Map

### Core Infrastructure
| Repo | Purpose | Language | Priority |
|------|---------|----------|----------|
| `HolDex` | K-Score engine, Oracle API | Node.js | **ACTIVE** |
| `GASdf` | Gasless transactions | Node.js | **ACTIVE** |
| `asdf-burn-engine` | On-chain burn program | Rust/TS | **ACTIVE** |

### Products
| Repo | Purpose | Status |
|------|---------|--------|
| `Ignition` | Fair launch platform | Development |
| `ASDForecast` | Prediction market | Production |
| `ASDev` | Token launcher | Development |
| `asdf-oracle` | Dashboard | Production |

### Utilities
| Repo | Purpose | Status |
|------|---------|--------|
| `asdf-validator` | Creator fee tracking | Archived? |
| `asdf-validator-lite` | Single-token PoH | Archived? |
| `solana-keychain` | Rust signing library | Library |
| `asdf-vanity-grinder` | Address generator | Tool |
| `transfer-hook-authority` | Transfer hooks | Research |
| `upptime-gasdf` | Status monitoring | Production |
| `asdf-manifesto` | Philosophy & economics docs | **ACTIVE** |

---

## Data Flow for LLM Training

Every transaction generates training data:

```
User Action
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│                    DATA COLLECTION                       │
├─────────────────────────────────────────────────────────┤
│ GASdf:                                                   │
│   - Token payment patterns                              │
│   - Wallet behavior clusters                            │
│   - Fee sensitivity by holder tier                      │
│                                                         │
│ HolDex:                                                 │
│   - Holder conviction over time                         │
│   - Sniper detection patterns                           │
│   - K-Score evolution trajectories                      │
│                                                         │
│ ASDForecast:                                            │
│   - Prediction accuracy by wallet                       │
│   - Market sentiment signals                            │
│   - Timing patterns                                     │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│                 STRUCTURED LOGGING (JSON)                │
│                                                         │
│  { wallet, action, token, k_score, timestamp,           │
│    conviction_class, holder_tier, outcome }             │
│                                                         │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│                   HolDex LLM (Future)                   │
│                                                         │
│  Predictions:                                           │
│  - "This wallet will likely panic sell within 24h"     │
│  - "Token X shows early diamond hand accumulation"     │
│  - "Liquidity withdrawal imminent for token Y"         │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## Critical Weaknesses Identified

### High Priority
| Area | Issue | Risk |
|------|-------|------|
| **burn-engine** | Single daemon SPOF | Fee collection fails if daemon down |
| **burn-engine** | Shared vault attribution | Race conditions with multiple tokens |
| **GASdf** | Redis not replicated | Quote state loss on failure |
| **HolDex** | Signature tampering loop | Healing can trigger on volatile data |

### Medium Priority
| Area | Issue | Risk |
|------|-------|------|
| **All** | Helius single provider | RPC dependency |
| **GASdf** | PostgreSQL optional | Audit log gaps |
| **burn-engine** | 5% slippage window | Flash loan manipulation |
| **ASDForecast** | Manual ASDF conversion | Fee → ASDF not automated |

### Low Priority
| Area | Issue | Risk |
|------|-------|------|
| **asdf-validator** | Possibly outdated | Redundant with HolDex |
| **solana-keychain** | Not integrated | Unused library |

---

## Recommended Next Steps

### Immediate (This Week)
1. [ ] Create shared `CLAUDE.md` for all repos
2. [ ] Document GASdf ↔ HolDex integration contract
3. [ ] Add Redis replication for production

### Short-term (This Month)
4. [ ] Implement burn-engine daemon hot standby
5. [ ] Multi-RPC failover across all services
6. [ ] Archive/deprecate asdf-validator repos

### Long-term (This Quarter)
7. [ ] Design structured logging format for LLM training
8. [ ] Build prediction model prototype
9. [ ] Consolidate repos where possible

---

## Quick Reference: Running the Stack

### HolDex
```bash
# In /workspaces/HolDex
npm start              # API server (port 3000)
npm run calculator     # K-Score worker
npm run worker         # Background jobs
npm run listener       # Real-time monitoring
```

### GASdf
```bash
# In /workspaces/GASdf (if cloned)
npm start              # Main server
npm run dev            # Development mode
```

### asdf-burn-engine
```bash
# Rust program deployment
anchor build && anchor deploy

# TypeScript daemon
npm run daemon
```

---

## Claude Code Instructions

When working on this ecosystem:

1. **DATA FIRST**: Log structured JSON for future training
2. **NO BLACK BOX**: Every K-Score change must be traceable
3. **ITERATIVE**: K-Score weights are modular, easy to tune
4. **PHI RATIOS**: Fee splits always follow golden ratio

Priority file locations:
- K-Score: `HolDex/src/tasks/kScoreUpdater.js`
- Harmony: `HolDex/src/services/harmonyEngine.js`
- Signatures: `HolDex/src/utils/dataSignature.js`
- Oracle: `HolDex/src/routes/oracle.js`
