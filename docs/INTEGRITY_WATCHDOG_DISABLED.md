# Integrity Watchdog Disabled

## Date: 2026-01-08

## Why Disabled

The integrity watchdog was designed for a **multi-node distributed network** with Byzantine fault tolerance, where multiple untrusted nodes need to verify each other's data. However, HolDex currently runs as a **single trusted backend** on Render.

### Problems with Watchdog in Single-Backend Setup

1. **False Positives**: Legitimate data updates (bug fixes, indexer updates) flagged as "tampering"
2. **Noisy Logs**: Constant `⛔ TAMPERED` warnings for normal operations
3. **Healing Loops**: Attempts to restore from Redis snapshots that also have outdated signatures
4. **Wrong Architecture**: Designed for preventing malicious database tampering in trustless networks, not for trusted backends

### Example False Positives

Recent bug fixes that triggered false tampering alerts:
- **Token Indexer Fix** ([indexer.js:68-90](../src/services/indexer.js#L68-L90)) - Updated token name/symbol/image conditionally
- **BIGINT Type Fix** ([kScoreUpdater.js:378-381](../src/tasks/kScoreUpdater.js#L378-L381)) - Converted decimal values to integers
- **K-Score Updates** - Every 12-hour refresh updates data without regenerating signatures immediately

These are **legitimate operations**, not tampering attempts.

## What We Disabled

**File**: [src/index.js:485-486](../src/index.js#L485-L486)

```javascript
// BEFORE (causing issues):
integrityWatchdog.start({ db: getDB() });
integrityWatchdog.startNodeWatchdog({ db: getDB() });

// AFTER (disabled):
// integrityWatchdog.start({ db: getDB() });
// integrityWatchdog.startNodeWatchdog({ db: getDB() });
```

## What Still Works

✅ **Signatures are still generated** - K-Score updater generates HMAC signatures for all data
✅ **Verification endpoint available** - `GET /api/token/:mint/verify` still works
✅ **Snapshots still saved** - Redis snapshots created after K-Score calculations
✅ **Data signing secret** - Still using `DATA_SIGNING_SECRET` for signature generation

## What Doesn't Work

❌ **Automatic tampering detection** - No background scanning for signature mismatches
❌ **Auto-healing** - No automatic restoration from Redis snapshots
❌ **Tampering alerts** - No Discord/Telegram alerts for signature failures

## Security Implications

### Before (Multi-Node Model)
- Multiple independent nodes verify each other
- Malicious database modification detected and healed automatically
- Trustless verification across distributed network

### After (Single-Backend Model)
- Single trusted backend controls all data
- Database tampering requires compromising Render infrastructure (already catastrophic)
- Signatures provide audit trail but no automatic enforcement

### Risk Assessment

**Risk**: Someone gains database write access and modifies K-Scores
**Before**: Watchdog detects and heals within 5 minutes
**After**: Manual verification required via `/verify` endpoint

**Verdict**: If your database is compromised, the attacker already has full control regardless of signatures. The watchdog doesn't meaningfully improve security in a single-backend architecture.

## When to Re-Enable

Re-enable the integrity watchdog if/when:

1. **Multi-node deployment** - Running multiple independent verification nodes
2. **Shared database** - Multiple services writing to the same PostgreSQL instance
3. **Trustless architecture** - Need Byzantine fault tolerance for distributed consensus
4. **Public verification network** - External nodes verifying your data

## Alternative: Manual Verification

If you want to check integrity occasionally without the noisy automated scanning:

```bash
# Check a specific token's integrity
curl http://localhost:3000/api/token/MINT_ADDRESS/verify

# Response shows signature status:
{
  "integrity": {
    "identity": "valid",
    "kscore": "valid",
    ...
  }
}
```

## Re-signing After Updates

If you make code changes that update database fields, you should re-sign tokens to avoid stale signatures:

```bash
# Re-sign all verified tokens (updates DB + Redis snapshots)
node src/scripts/resignAllTokens.js
```

This prevents signature mismatches when using the manual verification endpoint.

## Code References

- [src/index.js:485-486](../src/index.js#L485-L486) - Watchdog disabled here
- [src/tasks/integrityWatchdog.js](../src/tasks/integrityWatchdog.js) - Full watchdog implementation
- [src/utils/dataSignature.js](../src/utils/dataSignature.js) - Signature generation functions
- [docs/INTEGRITY.md](./INTEGRITY.md) - Full integrity system documentation

## Related Changes

- **RPC Optimizations** ([RPC_OPTIMIZATIONS.md](./RPC_OPTIMIZATIONS.md)) - K-Score refresh interval changed to 12 hours
- **Token Indexer Fix** ([TOKEN_INDEXING_BUG_FIX.md](./TOKEN_INDEXING_BUG_FIX.md)) - Conditional data updates
- **BIGINT Type Fix** ([WALLET_TX_CACHE_FIX.md](./WALLET_TX_CACHE_FIX.md)) - Type conversions

---

**Summary**: The integrity watchdog is a sophisticated multi-node verification system, but it's overkill for a single trusted backend. Disabling it removes false positive warnings without meaningfully reducing security.
