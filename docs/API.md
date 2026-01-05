# API Reference

Base URL: `https://holdex-api.onrender.com`

## Rate Limits

| Type | Limit | Scope |
|------|-------|-------|
| Global | 500 req/min | Per IP |
| API Key Creation | 5/hour | Per IP |
| Proxy Endpoints | 30 req/min | Per IP |
| Public Endpoints | 100 req/min | Per IP |
| Oracle Endpoints | 100 req/min | Per IP |

---

## Token Endpoints

### GET /api/tokens
List verified tokens (paginated).

**Query Parameters:**
- `page` (int): Page number (default: 1)
- `limit` (int): Results per page (default: 20, max: 100)
- `sort` (string): Sort field (k_score, marketcap, volume24h)
- `order` (string): asc | desc

**Response:**
```json
{
  "tokens": [...],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 1234
  }
}
```

### GET /api/token/:mint
Get detailed token information.

**Response:**
```json
{
  "mint": "9zB5wRar...",
  "name": "Token Name",
  "symbol": "TKN",
  "k_score": 75,
  "conviction_score": 82,
  "holders": 1500,
  "real_holders": 1200,
  "priceusd": 0.00123,
  "marketcap": 1234567,
  "liquidity": 50000,
  "volume24h": 25000,
  "lp_burn_pct": 100,
  "mint_authority_revoked": true,
  "freeze_authority_revoked": true,
  "age_days": 45.5,
  "hasCommunityUpdate": true
}
```

### GET /api/token/:mint/public
Public-friendly format (subset of fields).

### GET /api/token/:mint/top-holders
Get top holder rankings with conviction classification.

**Response:**
```json
{
  "holders": [
    {
      "wallet": "ABC...",
      "balance": 1000000,
      "percentage": 5.2,
      "conviction_class": "accumulator",
      "buy_count": 15,
      "sell_count": 2
    }
  ]
}
```

### GET /api/token/:mint/candles
Get OHLCV chart data.

**Query Parameters:**
- `resolution` (string): 5m | 1h | 4h | 1d
- `from` (int): Unix timestamp
- `to` (int): Unix timestamp

**Response:**
```json
{
  "candles": [
    {
      "timestamp": 1704067200,
      "open": 0.001,
      "high": 0.0012,
      "low": 0.0009,
      "close": 0.0011,
      "volume": 5000
    }
  ]
}
```

### GET /api/token/:mint/evolution
Historical K-Score trajectory.

**Response:**
```json
{
  "history": [
    {"date": "2025-01-01", "k_score": 65, "holders": 1000},
    {"date": "2025-01-02", "k_score": 68, "holders": 1050}
  ],
  "trends": {
    "k_score_30d": "+12%",
    "holders_30d": "+25%"
  }
}
```

### GET /api/token/:mint/card.png
Generate K-Score card image (Canvas rendered).

---

## Verification Endpoints

### GET /api/token/:mint/verify
Get integrity report with signature verification.

**Response:**
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
  }
}
```

### GET /api/stale-tokens
List tokens with outdated conviction data.

### GET /api/rpc-status
RPC provider health check.

---

## API Key Management

### POST /api/request-api-key
Generate new API key.

**Body:**
```json
{
  "wallet": "WALLET_ADDRESS",
  "signature": "SIGNED_MESSAGE"
}
```

**Response:**
```json
{
  "apiKey": "hx_live_abc123...",
  "tier": "free",
  "limit": 1000
}
```

### POST /api/request-my-keys
Get user's API keys.

**Body:**
```json
{
  "wallet": "WALLET_ADDRESS",
  "signature": "SIGNED_MESSAGE"
}
```

### GET /api/credits/:wallet
Check burn credits for wallet.

**Response:**
```json
{
  "wallet": "ABC...",
  "total_burned": 5000,
  "used_calls": 150,
  "remaining_calls": 4850,
  "tier": "pro"
}
```

---

## Oracle Endpoints

### GET /oracle/kscore/:mint
Token acceptance status for payments.

**Response:**
```json
{
  "success": true,
  "data": {
    "mint": "9zB5wRar...",
    "k_score": 75,
    "tier": "Trusted",
    "accepted": true,
    "reason": "K-Score >= 50"
  }
}
```

**Hardcoded Accepts:** SOL, USDC, USDT, $ASDF (always accepted)

### GET /oracle/escore/:wallet
Participant E-Score and benefits.

**Response:**
```json
{
  "success": true,
  "data": {
    "wallet": "ABC...",
    "e_score": 25.5,
    "tier": {
      "name": "Rare",
      "icon": "💎",
      "threshold": 15
    },
    "benefits": {
      "max_discount": "63%",
      "priority_support": true
    },
    "progress": {
      "next_tier": "Epic",
      "points_needed": 4.5
    },
    "dimensions": {
      "hold": 45,
      "burn": 30,
      "use": 20,
      "build": 0,
      "run": 0,
      "refer": 15,
      "time": 25
    }
  }
}
```

### GET /oracle/discount/:wallet/:operation
Calculate fee with discount.

**Parameters:**
- `wallet`: Solana wallet address
- `operation`: Operation type (gasdf_submit_standard, etc.)

**Response:**
```json
{
  "success": true,
  "data": {
    "operation": "gasdf_submit_standard",
    "base_fee": 100,
    "discounts": {
      "theoretical": 0.45,
      "maxAllowed": 0.50,
      "effective": 0.45
    },
    "finalFee": 55,
    "isViable": true,
    "breakdown": {
      "burn": 21.01,
      "rewards": 21.01,
      "treasury": 12.98
    }
  }
}
```

### GET /oracle/costs
Get all operation costs and constants.

**Response:**
```json
{
  "success": true,
  "data": {
    "operations": [
      {
        "operation_type": "gasdf_submit_standard",
        "base_fee": 100,
        "actual_cost": 10,
        "min_fee": 50.85,
        "max_discount": 0.50,
        "is_active": true
      }
    ],
    "constants": {
      "PHI": 1.618033988749895,
      "RATIOS": {
        "BURN": 0.382,
        "REWARDS": 0.382,
        "TREASURY": 0.236
      },
      "SAFETY_MARGIN": 1.2
    }
  }
}
```

### POST /oracle/webhook/burns
Receive burn notifications from GASdf.

**Headers:**
- `x-holdex-signature`: HMAC-SHA256 signature

**Body:**
```json
{
  "wallet": "WALLET_ADDRESS",
  "amount": 100,
  "txSignature": "TX_SIG...",
  "source": "gasdf"
}
```

---

## Webhook Endpoints

### POST /webhook/transfers
Helius transfer webhook receiver.

**Headers:**
- `Authorization`: Bearer WEBHOOK_SECRET

**Security:**
- HMAC-SHA256 signature verification
- Replay attack prevention (5min window)

---

## Admin Endpoints

All admin endpoints require `x-admin-auth` header with ADMIN_PASSWORD.

### GET /api/admin/updates
View pending community metadata submissions.

### POST /api/admin/approve-update
Approve metadata submission.

**Body:**
```json
{
  "updateId": 123
}
```

### POST /api/admin/reject-update
Reject metadata submission.

### POST /api/admin/refresh-kscore
Force K-Score recalculation.

**Body:**
```json
{
  "mint": "TOKEN_MINT"
}
```

### API Key Admin

- `GET /api/admin/keys` - List all API keys
- `POST /api/admin/generate-key` - Create key
- `POST /api/admin/update-key` - Modify key
- `POST /api/admin/revoke-key` - Disable key

---

## Health & Status

### GET /api/health
System health check.

**Response:**
```json
{
  "status": "ok",
  "services": {
    "database": "ok",
    "redis": "ok",
    "helius": {
      "circuitBreaker": "closed",
      "rateLimitRemaining": 1234
    }
  },
  "memory": {
    "heapUsed": 512,
    "rss": 1024
  }
}
```

---

## Error Responses

All errors follow this format:

```json
{
  "success": false,
  "error": "Error message",
  "code": "ERROR_CODE"
}
```

Common codes:
- `INVALID_MINT` - Invalid token address
- `NOT_FOUND` - Token not in database
- `RATE_LIMITED` - Rate limit exceeded
- `UNAUTHORIZED` - Invalid API key or signature
- `INTERNAL_ERROR` - Server error
