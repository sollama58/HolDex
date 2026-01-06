# HolDex - Todo List

## K-Score Fixes (Completed)
- [x] Fix conviction_class NULL handling in webhook mode (877a10a)
  - Bug: NULL conviction_class → acc=0, ext=0 → R=0% → Diamond Hands = 3%
  - Fix: Default case classifies based on buy_count/sell_count

## RPC Credit Protection (Post K-Score Fix)

### P0 - Critical
- [ ] Protect PnL endpoints - Add Redis cache (60s TTL)
- [ ] Protect PnL endpoints - Require burn credits before RPC calls
- [ ] Reduce PnL maxPages default from 50 to 10

### P1 - Important
- [ ] Add global wallet_analysis cache table (cross-token reuse)
- [ ] Cache token supply in Redis (1h TTL)

### P2 - Nice to Have
- [ ] Rate limit PnL endpoints specifically (5 req/min/wallet)

---

## Notes

### Attack Vectors Identified
- PnL endpoints: 0 cache, 0 burn credits, 50 RPC calls/request
- Top 20 re-analysis: Same wallets analyzed multiple times across tokens
- Token supply: Recalculated every update instead of cached
