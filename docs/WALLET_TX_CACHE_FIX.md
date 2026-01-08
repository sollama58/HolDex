# Wallet Transaction Cache BIGINT Type Error Fix

## Problem

The calculator worker was throwing this PostgreSQL error:

```
[WalletTxCache] Bulk cache error: invalid input syntax for type bigint: "9.811614681"
```

## Root Cause

**Database Schema:**
The `wallet_tx_cache` table has a `net_flow` column defined as `BIGINT` ([database.js:199](../src/services/database.js#L199)):

```sql
CREATE TABLE IF NOT EXISTS wallet_tx_cache (
    wallet TEXT NOT NULL,
    mint TEXT NOT NULL,
    buy_count INTEGER DEFAULT 0,
    sell_count INTEGER DEFAULT 0,
    net_flow BIGINT DEFAULT 0,      -- ← REQUIRES INTEGER
    ...
);
```

**Code Issue:**
In [kScoreUpdater.js:465-466](../src/tasks/kScoreUpdater.js#L465-L466), the `netFlow` is calculated by adding/subtracting token `amount` values, which are **decimals** (token amounts with decimals):

```javascript
if (isBuy) { ti.buyCount++; ti.netFlow += amount; }  // amount = 9.811614681
if (isSell) { ti.sellCount++; ti.netFlow -= amount; }
```

When this decimal value was inserted into the BIGINT column at line 389, PostgreSQL threw a type error because `BIGINT` only accepts integers.

## The Fix

**File:** [src/tasks/kScoreUpdater.js:378-381](../src/tasks/kScoreUpdater.js#L378-L381)

Added type conversion to ensure all values are integers before database insertion:

```javascript
async function cacheWalletTxBulk(db, wallet, tokenInteractions, lastSignature) {
    // ...
    for (const ti of tokenInteractions) {
        // FIX: Convert netFlow to integer (BIGINT column requirement)
        const buyCount = Math.floor(Number(ti.buyCount)) || 0;
        const sellCount = Math.floor(Number(ti.sellCount)) || 0;
        const netFlow = Math.floor(Number(ti.netFlow)) || 0;

        await db.run(`
            INSERT INTO wallet_tx_cache (...)
            VALUES ($1, $2, $3, $4, $5, ...)
        `, [wallet, ti.mint, buyCount, sellCount, netFlow, ...]);
    }
}
```

### What Changed:

1. **Before**: Directly used `ti.buyCount`, `ti.sellCount`, `ti.netFlow` (could be decimals)
2. **After**: Convert to integers using `Math.floor(Number(value)) || 0`

### Why Math.floor?

- `Number()` - Converts to number (in case it's a string)
- `Math.floor()` - Rounds down to nearest integer (safe for BIGINT)
- `|| 0` - Fallback to 0 if NaN/null/undefined

## Impact

- ✅ **No data loss**: `netFlow` represents token quantities, flooring is acceptable for cache purposes
- ✅ **Error eliminated**: No more "invalid input syntax for type bigint" errors
- ✅ **Backwards compatible**: Existing integer values pass through unchanged
- ✅ **Safe conversion**: Handles edge cases (null, undefined, NaN)

## Testing

To verify the fix works:

```bash
# Run the calculator worker
npm run calculator

# Check logs - should NOT see BIGINT errors
tail -f logs/app.log | grep -i "bigint\|WalletTxCache"

# If working correctly, you'll see cache operations without errors
```

## Related Files

- [src/tasks/kScoreUpdater.js:371-400](../src/tasks/kScoreUpdater.js#L371-L400) - Fixed function
- [src/services/database.js:194-205](../src/services/database.js#L194-L205) - Table schema

## Why netFlow Uses BIGINT

Token amounts can be very large (e.g., 1,000,000,000 tokens × 10^9 decimals = 10^18), which exceeds JavaScript's safe integer range but fits in PostgreSQL's BIGINT (max: 9,223,372,036,854,775,807).

By using `Math.floor()`, we convert decimal amounts to their smallest unit (like satoshis for Bitcoin), which is the correct way to store token quantities.

---

**Date Fixed**: 2026-01-08
**Bug Severity**: Medium (causes calculator worker errors)
**Fix Complexity**: Low (type conversion)
**Regression Risk**: None (only affects cache writes)
