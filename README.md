# HolDex

**Solana Token Analytics Engine** - K-Score conviction analysis with Harmony economic integration.

## Overview

HolDex is a sophisticated token analytics platform that provides:

- **K-Score v10**: Geometric mean-based conviction scoring (0-100)
- **E-Score**: Participant engagement scoring with phi-based economics
- **Oracle API**: Token acceptance and discount calculation for GASdf
- **Real-time Monitoring**: Helius webhooks + pool snapshots
- **Data Integrity**: 8-category cryptographic signatures with self-healing

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         HolDex System                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │  API Server │  │ Calculator  │  │   Listener Worker       │ │
│  │  (index.js) │  │             │  │                         │ │
│  │             │  │ - K-Score   │  │ - Token Discovery       │ │
│  │ - REST API  │  │ - Metadata  │  │ - Real-time Scoring     │ │
│  │ - WebSocket │  │ - Grower    │  │ - Pump.fun Monitor      │ │
│  │ - Oracle    │  │   Scanner   │  │                         │ │
│  └──────┬──────┘  └──────┬──────┘  └───────────┬─────────────┘ │
│         │                │                      │               │
│         └────────────────┼──────────────────────┘               │
│                          │                                      │
│                    ┌─────▼─────┐                                │
│                    │ PostgreSQL│                                │
│                    │  + Redis  │                                │
│                    └───────────┘                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Services

| Service | Command | Purpose |
|---------|---------|---------|
| API Server | `npm start` | REST API + WebSocket (port 3000) |
| Calculator | `npm run calculator` | K-Score computation worker |
| Worker | `npm run worker` | Token processing queue |
| Listener | `npm run listener` | Real-time monitoring |

## Quick Start

```bash
# Install dependencies
npm install

# Configure environment
cp .env.example .env
# Edit .env with your values

# Start API server
npm start

# Or start calculator (separate process)
npm run calculator
```

## Environment Variables

### Required
```env
DATABASE_URL=postgresql://user:pass@host:5432/holdex
ADMIN_PASSWORD=<strong_password>
DATA_SIGNING_SECRET=<32+ char random>
HELIUS_API_KEY=<api_key>
```

### Optional
```env
REDIS_URL=redis://localhost:6379
WEBHOOK_SECRET=<for Helius webhooks>
ORACLE_WEBHOOK_SECRET=<for GASdf integration>
PORT=3000
NODE_ENV=production
```

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for full list.

## Documentation

| Document | Description |
|----------|-------------|
| [API Reference](docs/API.md) | All REST endpoints |
| [K-Score Algorithm](docs/KSCORE.md) | Conviction scoring formula |
| [Harmony Integration](docs/HARMONY.md) | E-Score and Oracle system |
| [Data Integrity](docs/INTEGRITY.md) | 8-category signatures |
| [Configuration](docs/CONFIGURATION.md) | Environment variables |

## Key Features

### K-Score v10
Pure geometric mean formula:
```
K = 100 * cubeRoot(D * O * L)
```
- **D (Diamond Hands)**: Conviction, accumulator ratio, freshness
- **O (Organic Growth)**: Holder count, distribution
- **L (Longevity)**: Age, survival factor

### Harmony System
Phi-based (1.618) economic engine:
- **E-Score**: 7-dimension engagement scoring
- **Discounts**: Up to 90% based on E-Score
- **Fee Distribution**: 38.2% burn / 38.2% rewards / 23.6% treasury

### Data Integrity
8-category HMAC signatures:
- Identity, Security, LP, Supply, K-Score, Market, Origin, Full
- Self-healing database with integrity watchdog
- Chaos nonce for unpredictability

## API Examples

```bash
# Get token K-Score
curl https://holdex-api.onrender.com/api/token/9zB5wRar...

# Oracle: Check token acceptance
curl https://holdex-api.onrender.com/oracle/kscore/9zB5wRar...

# Oracle: Get discount for wallet
curl https://holdex-api.onrender.com/oracle/discount/WALLET.../gasdf_submit_standard
```

## External Integrations

- **Helius**: RPC + Webhooks (holder enumeration, transfers, token metadata)
- **Jupiter**: Price API (free tier via lite-api.jup.ag)
- **Raydium**: Pool/liquidity data (free API)
- **CoinGecko**: SOL/USD price
- **Metaplex**: Token metadata

## Database Schema

Main tables:
- `tokens` (42 columns) - Token data + K-Score
- `participants` - E-Score tracking
- `contributions` - Burn/activity audit trail
- `holder_snapshots` - Conviction analysis
- `pools` / `candles_1m` - DEX data

## License

MIT

## Links

- Production: https://holdex-api.onrender.com
- Related: [GASdf](https://github.com/zeyxx/GASdf) - Fee payment integration
