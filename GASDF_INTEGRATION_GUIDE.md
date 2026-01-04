# GASdf Integration Guide: HolDex Harmony System

> **For Claude Code:** Copy this entire document into a Claude Code session connected to the GASdf repository. It contains everything needed to integrate the HolDex Harmony E-Score system.

---

## Quick Start for Claude Code

```
You are integrating GASdf with the HolDex Harmony system.

TASK: Integrate HolDex Oracle API into GASdf for:
1. K-Score token validation (before accepting payment tokens)
2. E-Score discount calculation (apply discounts based on user engagement)
3. Burn webhook notification (report burns to HolDex with HMAC signature)

FILES TO CREATE:
- src/shared/harmony.js (copy from section below)
- src/shared/holdexClient.js (copy from section below)

INTEGRATION POINTS:
- Token validation service: call checkKScore() before accepting tokens
- Fee calculation service: call getDiscount() to get E-Score discount
- Burn service: call notifyBurn() after successful burns (requires HMAC)

ENVIRONMENT VARIABLES TO ADD:
- HOLDEX_API_URL=https://holdex-api.onrender.com
- HOLDEX_WEBHOOK_SECRET=<shared secret for HMAC signing>
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                           GASdf                                  │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ Fee Service  │  │ Burn Service │  │ Token Validator      │  │
│  │              │  │              │  │                      │  │
│  │ getDiscount()│  │ notifyBurn() │  │ isTokenAccepted()    │  │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘  │
│         │                 │                      │              │
│         └─────────────────┼──────────────────────┘              │
│                           │                                      │
│                    ┌──────▼───────┐                             │
│                    │ HoldexClient │  ← src/shared/holdexClient.js│
│                    │              │                             │
│                    │ + HMAC Sign  │  ← Webhook security         │
│                    └──────┬───────┘                             │
└───────────────────────────┼─────────────────────────────────────┘
                            │ HTTPS + HMAC
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                     HolDex Oracle API                            │
│                                                                  │
│  GET  /oracle/kscore/:mint         → K-Score (token acceptance) │
│  GET  /oracle/escore/:wallet       → E-Score & benefits         │
│  GET  /oracle/discount/:wallet/:op → Fee with discount          │
│  GET  /oracle/costs                → Operation costs & constants│
│  POST /oracle/webhook/burns        → Burn notification (HMAC)   │
│  GET  /oracle/participant/:wallet  → Full participant profile   │
│  GET  /oracle/leaderboard          → Top E-Score participants   │
│  GET  /oracle/stats                → Ecosystem statistics       │
│                                                                  │
│  Base URL: https://holdex-api.onrender.com (beta)               │
│  Rate Limit: 100 req/min (read), 20 req/min (write)            │
└─────────────────────────────────────────────────────────────────┘
```

---

## File 1: `src/shared/harmony.js`

Copy this file to GASdf. Used for local calculations when HolDex is unreachable.

```javascript
/**
 * HARMONY MODULE - Philosophical Core
 *
 * φ (PHI) = 1.618033988749895 (Golden Ratio)
 *
 * This module contains all the mathematical formulas for the E-Score system.
 * Copy to GASdf for local fallback calculations.
 */

'use strict';

const PHI = 1.618033988749895;

const RATIOS = Object.freeze({
    BURN: 1 / (PHI ** 2),        // 0.382 (38.2%)
    REWARDS: 1 / (PHI ** 2),     // 0.382 (38.2%)
    TREASURY: 1 / (PHI ** 3),    // 0.236 (23.6%)
});

const MULTIPLIERS = Object.freeze({
    HOLD: 1.0,
    BURN: PHI,           // 1.618x
    USE: 1.0,
    BUILD: PHI ** 2,     // 2.618x
    RUN: PHI ** 2,       // 2.618x
    REFER: PHI,          // 1.618x
    TIME: 1.0,
});

const SAFETY_MARGIN = 1.2;
const MAX_DISCOUNT_CAP = 0.95;
const DISCOUNT_ASYMPTOTE = 100;

const TIERS = [
    { threshold: 0,    name: 'Observer',   icon: '👁️',  color: '#6B7280' },
    { threshold: 10,   name: 'Spark',      icon: '✨',  color: '#F59E0B' },
    { threshold: 25,   name: 'Flame',      icon: '🔥',  color: '#EF4444' },
    { threshold: 50,   name: 'Blaze',      icon: '🌟',  color: '#8B5CF6' },
    { threshold: 100,  name: 'Inferno',    icon: '💫',  color: '#EC4899' },
    { threshold: 250,  name: 'Nova',       icon: '⭐',  color: '#3B82F6' },
    { threshold: 500,  name: 'Supernova',  icon: '🌠',  color: '#10B981' },
    { threshold: 1000, name: 'Hypernova',  icon: '💎',  color: '#06B6D4' },
];

function calculateEScore(contributions) {
    const {
        holdings = 0, burned = 0, apiCalls30d = 0,
        appsLive = 0, nodesActive = 0, referralsActive = 0, daysActive = 0
    } = contributions;

    const dimensions = {
        hold:  Math.log(1 + holdings / 1000) * MULTIPLIERS.HOLD,
        burn:  Math.log(1 + burned / 100) * MULTIPLIERS.BURN,
        use:   Math.log(1 + apiCalls30d / 10) * MULTIPLIERS.USE,
        build: Math.log(1 + appsLive) * MULTIPLIERS.BUILD,
        run:   Math.log(1 + nodesActive) * MULTIPLIERS.RUN,
        refer: Math.log(1 + referralsActive) * MULTIPLIERS.REFER,
        time:  Math.log(1 + daysActive / 30) * MULTIPLIERS.TIME,
    };

    const activeDimensions = Object.values(dimensions).filter(v => v > 0).length;
    const diversityBonus = activeDimensions > 1 ? 1 + (activeDimensions - 1) * 0.1 : 1;

    const activeValues = Object.values(dimensions).filter(v => v > 0);
    let geometricMean = 0;
    if (activeValues.length > 0) {
        const product = activeValues.reduce((a, b) => a * b, 1);
        geometricMean = Math.pow(product, 1 / activeValues.length);
    }

    const score = Math.round(geometricMean * diversityBonus * 10 * 100) / 100;
    return { score, breakdown: dimensions, activeDimensions, diversityBonus: Math.round((diversityBonus - 1) * 100) };
}

function calculateMinimumFee(operationCost) {
    return operationCost / RATIOS.TREASURY * SAFETY_MARGIN;
}

function calculateMaxDiscount(baseFee, operationCost) {
    const minFee = calculateMinimumFee(operationCost);
    if (baseFee <= minFee) return 0;
    return Math.min((baseFee - minFee) / baseFee, MAX_DISCOUNT_CAP);
}

function calculateDiscount(eScore) {
    return MAX_DISCOUNT_CAP * (1 - Math.exp(-eScore / DISCOUNT_ASYMPTOTE));
}

function calculateFinalFee(eScore, baseFee, operationCost) {
    const theoreticalDiscount = calculateDiscount(eScore);
    const maxAllowedDiscount = calculateMaxDiscount(baseFee, operationCost);
    const effectiveDiscount = Math.min(theoreticalDiscount, maxAllowedDiscount);
    const minFee = calculateMinimumFee(operationCost);
    const discountedFee = baseFee * (1 - effectiveDiscount);
    const finalFee = Math.max(discountedFee, minFee);

    return {
        baseFee, operationCost,
        minFee: Math.round(minFee * 100) / 100,
        discounts: {
            theoretical: Math.round(theoreticalDiscount * 10000) / 100,
            maxAllowed: Math.round(maxAllowedDiscount * 10000) / 100,
            effective: Math.round(effectiveDiscount * 10000) / 100,
            limited: effectiveDiscount < theoreticalDiscount
        },
        finalFee: Math.round(finalFee * 100) / 100,
        savings: Math.round((baseFee - finalFee) * 100) / 100,
        isViable: baseFee >= minFee
    };
}

function getTier(eScore) {
    for (let i = TIERS.length - 1; i >= 0; i--) {
        if (eScore >= TIERS[i].threshold) return TIERS[i];
    }
    return TIERS[0];
}

function calculateBenefits(eScore) {
    const discount = calculateDiscount(eScore);
    const tier = getTier(eScore);
    return {
        benefits: {
            discount: Math.round(discount * 10000) / 100,
            freeCalls: Math.floor(eScore * 10),
            rateLimit: Math.floor(100 + eScore * 5),
            priority: Math.round(Math.min(eScore / 10, 10) * 10) / 10
        },
        tier
    };
}

function distributeFee(totalFee) {
    return {
        total: totalFee,
        burn: Math.round(totalFee * RATIOS.BURN * 100000000) / 100000000,
        rewards: Math.round(totalFee * RATIOS.REWARDS * 100000000) / 100000000,
        treasury: Math.round(totalFee * RATIOS.TREASURY * 100000000) / 100000000
    };
}

module.exports = {
    PHI, RATIOS, MULTIPLIERS, SAFETY_MARGIN, MAX_DISCOUNT_CAP, DISCOUNT_ASYMPTOTE, TIERS,
    calculateEScore, getTier, calculateMinimumFee, calculateMaxDiscount,
    calculateDiscount, calculateFinalFee, calculateBenefits, distributeFee
};
```

---

## File 2: `src/shared/holdexClient.js`

Copy this file to GASdf. Handles all communication with HolDex Oracle API.

```javascript
/**
 * HOLDEX ORACLE CLIENT
 *
 * Client for GASdf to communicate with HolDex Oracle API.
 * Includes HMAC-SHA256 signing for webhook security.
 *
 * Usage:
 * const { getHoldexClient } = require('./shared/holdexClient');
 * const client = getHoldexClient({
 *     baseUrl: process.env.HOLDEX_API_URL,
 *     webhookSecret: process.env.HOLDEX_WEBHOOK_SECRET
 * });
 */

'use strict';

const crypto = require('crypto');

const DEFAULT_BASE_URL = 'https://holdex-api.onrender.com';
const DEFAULT_TIMEOUT = 10000;

class HoldexClient {
    constructor(options = {}) {
        this.baseUrl = (options.baseUrl || DEFAULT_BASE_URL).replace(/\/$/, '');
        this.timeout = options.timeout || DEFAULT_TIMEOUT;
        this.apiKey = options.apiKey || null;
        this.webhookSecret = options.webhookSecret || null;
        this.logger = options.logger || console;
    }

    /**
     * Create canonical JSON for HMAC signing
     * CRITICAL: Key order must match exactly on both sides (GASdf + HolDex)
     * Order: amount, source, txSignature, wallet (alphabetical)
     */
    _canonicalBurnPayload(payload) {
        return JSON.stringify({
            amount: payload.amount,
            source: payload.source,
            txSignature: payload.txSignature,
            wallet: payload.wallet
        });
    }

    /**
     * SECURITY: Sign payload with HMAC-SHA256
     */
    _signPayload(payload) {
        if (!this.webhookSecret) {
            throw new Error('webhookSecret is required for webhook operations');
        }
        const signature = crypto
            .createHmac('sha256', this.webhookSecret)
            .update(this._canonicalBurnPayload(payload))
            .digest('hex');
        return `sha256=${signature}`;
    }

    async _request(method, path, body = null) {
        const url = `${this.baseUrl}/oracle${path}`;
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout);

        try {
            const options = {
                method,
                headers: {
                    'Content-Type': 'application/json',
                    'User-Agent': 'GASdf/1.0'
                },
                signal: controller.signal
            };

            if (this.apiKey) {
                options.headers['x-api-key'] = this.apiKey;
            }

            if (body) {
                options.body = JSON.stringify(body);
            }

            const response = await fetch(url, options);
            const data = await response.json();

            if (!data.success) {
                throw new Error(data.error || 'Request failed');
            }

            return data.data;
        } catch (error) {
            if (error.name === 'AbortError') {
                throw new Error(`HolDex Oracle timeout: ${path}`);
            }
            throw error;
        } finally {
            clearTimeout(timeoutId);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // K-SCORE: Token Acceptance
    // ═══════════════════════════════════════════════════════════════

    /**
     * Check K-Score for a token mint address
     * Returns: { mint, k_score, tier, accepted, reason }
     */
    async checkKScore(mint) {
        return this._request('GET', `/kscore/${mint}`);
    }

    /**
     * Quick boolean check if token is accepted
     */
    async isTokenAccepted(mint) {
        try {
            const result = await this.checkKScore(mint);
            return result.accepted;
        } catch (error) {
            this.logger.warn(`[HoldexClient] K-Score check failed: ${error.message}`);
            return false; // Fail closed
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // E-SCORE: Discount Calculation
    // ═══════════════════════════════════════════════════════════════

    /**
     * Get E-Score and benefits for a wallet
     * Returns: { wallet, e_score, tier, benefits, display }
     */
    async getEScore(wallet) {
        return this._request('GET', `/escore/${wallet}`);
    }

    /**
     * Calculate discount for a specific operation
     * Returns: { wallet, operation, e_score, discounts, finalFee, baseFee }
     */
    async getDiscount(wallet, operation) {
        return this._request('GET', `/discount/${wallet}/${operation}`);
    }

    /**
     * Get all operation costs (cache this locally)
     * Returns: { operations, constants, acceptance }
     */
    async getOperationCosts() {
        return this._request('GET', '/costs');
    }

    // ═══════════════════════════════════════════════════════════════
    // BURN WEBHOOK (HMAC-SHA256 Signed)
    // ═══════════════════════════════════════════════════════════════

    /**
     * Notify HolDex of a burn transaction
     * SECURITY: Requires webhookSecret for HMAC signing
     *
     * @param {string} wallet - Wallet that burned tokens
     * @param {number} amount - Amount burned (positive number)
     * @param {string} txSignature - Solana transaction signature
     */
    async notifyBurn(wallet, amount, txSignature) {
        const payload = {
            wallet,
            amount,
            txSignature,
            source: 'gasdf'
        };

        // Sign the payload
        const signature = this._signPayload(payload);

        const url = `${this.baseUrl}/oracle/webhook/burns`;
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout);

        try {
            const response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'User-Agent': 'GASdf/1.0',
                    'x-holdex-signature': signature  // REQUIRED
                },
                body: JSON.stringify(payload),
                signal: controller.signal
            });

            const data = await response.json();

            if (!data.success) {
                throw new Error(data.error || 'Burn notification failed');
            }

            return data.data;
        } catch (error) {
            if (error.name === 'AbortError') {
                throw new Error('HolDex Oracle timeout: burn notification');
            }
            throw error;
        } finally {
            clearTimeout(timeoutId);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // PARTICIPANT & STATS
    // ═══════════════════════════════════════════════════════════════

    async getParticipant(wallet) {
        return this._request('GET', `/participant/${wallet}`);
    }

    async registerParticipant(wallet, type = 'user') {
        return this._request('POST', '/participant/register', { wallet, type });
    }

    async getLeaderboard(limit = 50) {
        return this._request('GET', `/leaderboard?limit=${limit}`);
    }

    async getStats() {
        return this._request('GET', '/stats');
    }

    async healthCheck() {
        try {
            await this.getOperationCosts();
            return true;
        } catch (_error) {
            return false;
        }
    }
}

// Singleton
let instance = null;

function getHoldexClient(options = {}) {
    if (!instance) {
        instance = new HoldexClient(options);
    }
    return instance;
}

module.exports = { HoldexClient, getHoldexClient };
```

---

## Integration Examples

### 1. Token Validation (Before Accepting Payment)

```javascript
const { getHoldexClient } = require('./shared/holdexClient');

async function validatePaymentToken(mint) {
    const client = getHoldexClient({
        baseUrl: process.env.HOLDEX_API_URL
    });

    const result = await client.checkKScore(mint);

    if (!result.accepted) {
        throw new Error(`Token rejected: ${result.reason}`);
    }

    return {
        mint,
        kScore: result.k_score,
        tier: result.tier
    };
}
```

### 2. Fee Calculation with E-Score Discount

```javascript
const { getHoldexClient } = require('./shared/holdexClient');

async function calculateTransactionFee(wallet) {
    const client = getHoldexClient({
        baseUrl: process.env.HOLDEX_API_URL
    });

    // Operation types: 'gasdf_submit_standard' or 'gasdf_submit_priority'
    const result = await client.getDiscount(wallet, 'gasdf_submit_standard');

    return {
        baseFee: result.baseFee,           // 500 $ASDF
        discount: result.discounts.effective, // e.g., 45.2%
        finalFee: result.finalFee,         // e.g., 274 $ASDF
        eScore: result.e_score
    };
}
```

### 3. Burn Notification (After Successful Burn)

```javascript
const { getHoldexClient } = require('./shared/holdexClient');

async function notifyBurnComplete(wallet, burnAmount, txSignature) {
    const client = getHoldexClient({
        baseUrl: process.env.HOLDEX_API_URL,
        webhookSecret: process.env.HOLDEX_WEBHOOK_SECRET  // REQUIRED
    });

    try {
        const result = await client.notifyBurn(wallet, burnAmount, txSignature);

        console.log(`Burn recorded: ${wallet} now has E-Score ${result.newEScore}`);
        return result;

    } catch (error) {
        // Log but don't fail the transaction
        console.error(`Failed to notify HolDex: ${error.message}`);
    }
}
```

### 4. Fallback with Local Calculation

```javascript
const { getHoldexClient } = require('./shared/holdexClient');
const harmony = require('./shared/harmony');

async function getDiscountWithFallback(wallet, baseFee, cost) {
    const client = getHoldexClient({
        baseUrl: process.env.HOLDEX_API_URL
    });

    try {
        return await client.getDiscount(wallet, 'gasdf_submit_standard');
    } catch (error) {
        console.warn(`HolDex unavailable, using local fallback`);
        // Fallback: E-Score 0 = no discount
        return harmony.calculateFinalFee(0, baseFee, cost);
    }
}
```

---

## Operation Types & Pricing

| Operation | Base Fee ($ASDF) | Cost | Max Discount | Description |
|-----------|------------------|------|--------------|-------------|
| `gasdf_quote` | 0 | 0.1 | 0% | Quote request (free) |
| `gasdf_submit_standard` | 500 | 17 | 85.7% | Standard gasless TX |
| `gasdf_submit_priority` | 1000 | 35 | 82.2% | Priority gasless TX |

---

## Environment Variables

Add to GASdf `.env`:

```bash
# HolDex Oracle API
HOLDEX_API_URL=https://holdex-api.onrender.com
HOLDEX_TIMEOUT=10000

# SECURITY: HMAC Secret for Burn Webhooks
# Generate: openssl rand -hex 32
# Must match ORACLE_WEBHOOK_SECRET on HolDex
HOLDEX_WEBHOOK_SECRET=your-64-character-hex-secret-here
```

---

## Security Requirements

| Requirement | Details |
|-------------|---------|
| **HMAC Signing** | All burn webhooks must include `x-holdex-signature` header |
| **Signature Format** | `sha256=<hex-encoded-hmac>` |
| **Secret Length** | Minimum 32 characters |
| **Rate Limiting** | 100 req/min read, 20 req/min write |
| **Address Validation** | Base58 Solana addresses only |

Without valid HMAC signature, burn webhooks return **401 Unauthorized**.

---

## Testing

```javascript
async function testHoldexIntegration() {
    const client = getHoldexClient({
        baseUrl: process.env.HOLDEX_API_URL,
        webhookSecret: process.env.HOLDEX_WEBHOOK_SECRET
    });

    // 1. Health check
    console.log('Health:', await client.healthCheck());

    // 2. K-Score check (SOL is always accepted)
    const sol = await client.checkKScore('So11111111111111111111111111111111111111112');
    console.log('SOL:', sol.k_score, sol.accepted);

    // 3. Get operation costs
    const costs = await client.getOperationCosts();
    console.log('Operations:', Object.keys(costs.operations));

    // 4. E-Score lookup (will return 0 for unknown wallets)
    const escore = await client.getEScore('YourWalletAddressHere');
    console.log('E-Score:', escore.e_score, escore.tier?.name);
}
```

---

## Summary Checklist

- [ ] Copy `harmony.js` to `src/shared/harmony.js`
- [ ] Copy `holdexClient.js` to `src/shared/holdexClient.js`
- [ ] Add environment variables to `.env`
- [ ] Generate shared webhook secret: `openssl rand -hex 32`
- [ ] Configure same secret on HolDex as `ORACLE_WEBHOOK_SECRET`
- [ ] Integrate `checkKScore()` in token validation
- [ ] Integrate `getDiscount()` in fee calculation
- [ ] Integrate `notifyBurn()` after successful burns
- [ ] Add fallback using local `harmony.js` calculations
