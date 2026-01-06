# HolDex - Claude Code Context

## Project Overview
HolDex is a Solana blockchain analytics engine that calculates K-Score (token health metrics) with cryptographic data integrity. Multi-service architecture: API server, calculator worker, listener worker, background worker.

## Tech Stack
- **Runtime**: Node.js 18+ (CommonJS modules)
- **Framework**: Express.js + Socket.io
- **Database**: PostgreSQL + TimescaleDB (time-series)
- **Cache**: Redis (ioredis)
- **Blockchain**: @solana/web3.js, Helius API
- **Security**: HMAC-SHA256 signatures, Helmet, rate limiting

## Project Structure
```
src/
├── index.js           # API server entry (port 3000)
├── calculator.js      # K-Score worker entry
├── worker.js          # Background worker entry
├── listener_worker.js # Real-time listener entry
├── config/env.js      # Environment validation
├── routes/            # API endpoints (tokens, oracle, space, webhooks)
├── services/          # Business logic (database, redis, solana, etc.)
├── tasks/             # Background jobs (kScoreUpdater, integrityWatchdog)
├── middleware/        # Auth, rate limiting, caching
├── utils/             # Helpers (signatures, validation, metaplex)
├── indexer/           # Token indexing system
└── shared/            # Shared modules (harmony.js, holdexClient.js)
docs/                  # API.md, KSCORE.md, HARMONY.md, INTEGRITY.md
```

## Key Commands
```bash
npm start              # API server
npm run dev            # API with nodemon
npm run calculator     # K-Score calculation worker
npm run worker         # Token processing worker
npm run listener       # Real-time monitoring
npm run lint           # ESLint check
npm run lint:fix       # Auto-fix linting
```

## Architecture Patterns
- **Service layer**: Business logic in `/services/`
- **Background workers**: Redis-backed job queue
- **Event-driven**: WebSocket for real-time updates
- **Graceful shutdown**: SIGTERM/SIGINT handlers
- **Circuit breaker**: API resilience pattern

## Code Conventions
- CommonJS: `require()` / `module.exports`
- Async/await for all async operations
- camelCase variables, UPPER_CASE constants
- Console logging with emoji indicators (✅ ❌ ⚠️ 💾)
- Try-catch with detailed error logging
- Memory cleanup: nullify large objects after use

## K-Score Algorithm (v10)
```
K = 100 × ∛(D × O × L)
```
- **D (Diamond Hands)**: Conviction strength from holder behavior
- **O (Organic Growth)**: Distribution quality (anti-sniper)
- **L (Longevity)**: Survival factor over time

## Data Integrity System
8 signature categories per token (HMAC-SHA256):
- `sig_identity`, `sig_security`, `sig_lp`, `sig_supply`
- `sig_kscore`, `sig_market`, `sig_origin`, `sig_full`
- `chaos_nonce` for unpredictability

## Main API Routes
- `GET /api/tokens` - Paginated token list
- `GET /api/token/:mint` - Token details + K-Score
- `GET /api/token/:mint/verify` - Integrity verification
- `GET /api/token/:mint/card.png` - Generated K-Score card
- `POST /webhook/transfers` - Helius webhooks

## Environment Variables (Required)
- `DATABASE_URL` - PostgreSQL connection string
- `ADMIN_PASSWORD` - Admin API access (production)
- `DATA_SIGNING_SECRET` - HMAC secret (min 32 chars)
- `HELIUS_API_KEY` - Solana RPC + webhooks
- `REDIS_URL` - Cache connection

## Database Tables
- `tokens` - Main token data (42 columns)
- `participants` - E-Score wallet tracking
- `holder_snapshots` - Conviction analysis
- `candles_1m` - OHLCV price data
- `k_score_history` - Daily snapshots

## Key Files by Size
- `tasks/kScoreUpdater.js` (3,424 lines) - Core K-Score logic
- `routes/tokens.js` (2,271 lines) - Token endpoints
- `services/cardGenerator.js` (1,104 lines) - PNG rendering
- `routes/space.js` (830 lines) - Marketplace

## Security Considerations
- All K-Score data cryptographically signed
- Rate limiting: 500 req/min global
- Webhook signature verification required
- Input validation on all forms
- Integrity watchdog auto-heals tampering

## Deployment
- **Render.com**: render.yaml config
- **Docker**: docker-compose.yml (Nginx LB, 3x API, workers, TimescaleDB, Redis)

## Documentation
See `/docs/` for detailed documentation:
- `API.md` - REST API reference
- `KSCORE.md` - Algorithm details
- `HARMONY.md` - Economic system (E-Score, Phi ratios)
- `INTEGRITY.md` - Signature verification
