# Configuration

All configuration is via environment variables.

## Required Variables

### Database
```env
DATABASE_URL=postgresql://user:pass@host:5432/holdex
```
PostgreSQL connection string. Required for all operations.

### Security
```env
ADMIN_PASSWORD=<strong_password>
```
Password for admin API endpoints. Required in production.

```env
DATA_SIGNING_SECRET=<32+ char random>
```
Secret for HMAC data signatures. Required in production.

### RPC
```env
HELIUS_API_KEY=<api_key>
```
Helius API key for Solana RPC and webhooks.

## Optional Variables

### Database (Advanced)
```env
READ_DATABASE_URL=postgresql://...
```
Read replica connection for load distribution.

```env
DB_SSL_REJECT_UNAUTHORIZED=false
```
Disable SSL verification (common for managed DBs).

### Redis
```env
REDIS_URL=redis://localhost:6379
```
Redis connection for caching. Falls back to in-memory if not set.

### Server
```env
PORT=3000
```
HTTP server port (default: 3000).

```env
NODE_ENV=production
```
Environment mode. Set to `production` for:
- Stricter validation
- Required secrets enforcement
- Performance optimizations

```env
API_URL=https://holdex-api.onrender.com
```
Public API URL for webhook callbacks.

### CORS
```env
CORS_ORIGINS=https://custom.domain.com,https://other.com
```
Additional allowed CORS origins (comma-separated).

Default patterns:
- `*.alonisthe.dev`
- `localhost:*`
- `*.github.dev`

### Webhooks
```env
WEBHOOK_SECRET=<32+ char>
```
Secret for Helius webhook signature verification.

```env
ORACLE_WEBHOOK_SECRET=<32+ char>
```
Shared secret with GASdf for burn notifications.

### RPC URLs
```env
SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=...
```
Solana HTTP RPC endpoint.

```env
SOLANA_WSS_URL=wss://mainnet.helius-rpc.com/?api-key=...
```
Solana WebSocket endpoint.

### Payment
```env
TREASURY_WALLET=<solana_address>
```
Wallet for receiving fees.

```env
FEE_SOL=0.1
```
SOL fee amount.

```env
FEE_TOKEN_AMOUNT=5000
```
Token fee amount.

```env
FEE_TOKEN_MINT=9zB5wRarXMj86MymwLumSKA1Dx35zPqqKfcZtK1Spump
```
$ASDF token mint address.

### Intervals
```env
METADATA_UPDATE_INTERVAL=300000
```
Metadata refresh interval in ms (default: 5 min).

```env
HOLDER_SCAN_INTERVAL=300000
```
Holder scan interval in ms (default: 5 min).

### Rate Limiting
```env
ORACLE_RATE_LIMIT=100
```
Oracle endpoint rate limit per minute (default: 100).

### Features
```env
ENABLE_RPC_HOLDER_CHECK=true
```
Enable on-chain holder verification for burn credits.

### Key Rotation
```env
DATA_SIGNING_SECRET_PREVIOUS=<old_secret>
```
Previous signing secret for zero-downtime rotation.

## Validation

On startup, the system validates:

### Critical (Always Required)
- `DATABASE_URL`

### Critical (Production Only)
- `ADMIN_PASSWORD`
- `DATA_SIGNING_SECRET`

### High Priority
- `WEBHOOK_SECRET` (if webhooks enabled)

### Security Checks
- `ORACLE_WEBHOOK_SECRET` length >= 32 chars

### Warnings
- Missing `HELIUS_API_KEY`
- Missing `REDIS_URL`

## Example .env

```env
# Database
DATABASE_URL=postgresql://user:pass@host:5432/holdex

# Security
ADMIN_PASSWORD=super_secure_password_123
DATA_SIGNING_SECRET=at_least_32_characters_random_string
WEBHOOK_SECRET=another_32_char_random_string
ORACLE_WEBHOOK_SECRET=shared_secret_with_gasdf_32chars

# RPC
HELIUS_API_KEY=your_helius_api_key
SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=KEY
SOLANA_WSS_URL=wss://mainnet.helius-rpc.com/?api-key=KEY

# Redis
REDIS_URL=redis://localhost:6379

# Server
PORT=3000
NODE_ENV=production
API_URL=https://holdex-api.onrender.com

# Payment
TREASURY_WALLET=YourTreasuryWalletAddress
FEE_TOKEN_MINT=9zB5wRarXMj86MymwLumSKA1Dx35zPqqKfcZtK1Spump
FEE_TOKEN_AMOUNT=5000
FEE_SOL=0.1
```

## Render Environment

For Render deployment, set these in the service's Environment tab:

### Secret (Hide from logs)
- `DATABASE_URL`
- `ADMIN_PASSWORD`
- `DATA_SIGNING_SECRET`
- `WEBHOOK_SECRET`
- `ORACLE_WEBHOOK_SECRET`
- `HELIUS_API_KEY`

### Plain
- `PORT`
- `NODE_ENV`
- `API_URL`
- `CORS_ORIGINS`

## Services Configuration

### holdex-api
```
Build Command: npm install --ignore-optional
Start Command: npm start
```

### holdex-calculator
```
Build Command: npm install --ignore-optional
Start Command: npm run calculator
```

Both services share the same environment variables.
