# HolDex - Todo List

## K-Score Fixes (Completed)
- [x] Fix conviction_class NULL handling in webhook mode (877a10a)
  - Bug: NULL conviction_class → acc=0, ext=0 → R=0% → Diamond Hands = 3%
  - Fix: Default case classifies based on buy_count/sell_count

## RPC Credit Protection (Post K-Score Fix)

### P0 - Critical
- [x] Protect PnL endpoints - Add Redis cache (60s TTL) ✅ 25d8e4b
- [x] Protect PnL endpoints - Cache-aware burn credits ✅ b0aedec
  - Cache HIT = FREE, Cache MISS = 1 credit (philosophy $asdfasdfa)

### P1 - Important
- [x] Add global wallet_tx_cache table (cross-token reuse) ✅ 16b095b
  - Fetch once, analyze many tokens - saves RPC when same wallet in multiple top 20s
- [x] Cache token supply in Redis (1h TTL) ✅ 1db0d3f
  - Supply rarely changes, safe to cache aggressively

### P2 - Nice to Have
- [x] Rate limit PnL endpoints specifically (5 req/min/wallet) ✅ 9cfd6c3

## API Access Control

### Whitelist for Trusted Services
- [x] Add API key whitelist for trusted services (GASdf) ✅ 415cd0d
  - `WHITELISTED_API_KEYS` env var (comma-separated)
  - Bypasses anti-sybil gate (no 10K $ASDF requirement)
  - No credit deduction, no rate limiting
  - `X-Whitelisted: true` response header

---

## Notes

### Attack Vectors Identified
- PnL endpoints: 0 cache, 0 burn credits, 50 RPC calls/request
- Top 20 re-analysis: Same wallets analyzed multiple times across tokens
- Token supply: Recalculated every update instead of cached
