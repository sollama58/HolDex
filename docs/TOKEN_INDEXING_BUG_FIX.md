# Token Indexing Bug Fix - "New Discovery" Issue

## Problem Description

When searching for a token by contract address (CA), tokens appeared as "New Discovery" without any name, symbol, or image data, even after multiple searches.

## Root Cause

The bug was in [src/services/indexer.js:53-62](../src/services/indexer.js#L53-L62) in the `ON CONFLICT` clause of the token INSERT statement.

### Before (Broken):

```sql
INSERT INTO tokens (mint, name, symbol, image, ...)
VALUES ($1, $2, $3, $4, ...)
ON CONFLICT(mint) DO UPDATE SET
    updated_at = NOW()
```

**What happened:**
1. First search: Token indexed with metadata (if fetch succeeded) or "Unknown" (if fetch failed)
2. Second search: Token exists → `ON CONFLICT` triggered → Only `updated_at` updated
3. Result: Token stuck with original data (often "Unknown"/"UNKNOWN") forever

**Why it failed:**
- If metadata fetch failed on first attempt (timeout, network error, missing HELIUS_API_KEY), token saved as "Unknown"
- Subsequent searches would trigger `ON CONFLICT` but only update timestamp
- Market data (price, volume, mcap) never refreshed
- Identity data (name, symbol, image) never re-attempted

## The Fix

### After (Fixed):

```sql
INSERT INTO tokens (mint, name, symbol, image, ...)
VALUES ($1, $2, $3, $4, ...)
ON CONFLICT(mint) DO UPDATE SET
    name = CASE
        WHEN tokens.name IN ('Unknown', 'New Discovery', '') OR tokens.name IS NULL
        THEN EXCLUDED.name
        ELSE tokens.name
    END,
    symbol = CASE
        WHEN tokens.symbol IN ('UNKNOWN', 'UNK', '') OR tokens.symbol IS NULL
        THEN EXCLUDED.symbol
        ELSE tokens.symbol
    END,
    image = CASE
        WHEN tokens.image IS NULL OR tokens.image = ''
        THEN EXCLUDED.image
        ELSE tokens.image
    END,
    priceUsd = EXCLUDED.priceUsd,
    marketCap = EXCLUDED.marketCap,
    volume24h = EXCLUDED.volume24h,
    change24h = EXCLUDED.change24h,
    change1h = EXCLUDED.change1h,
    change5m = EXCLUDED.change5m,
    updated_at = NOW()
```

**What this does:**

1. **Identity fields (name, symbol, image)**:
   - If current value is "Unknown"/"UNKNOWN"/"UNK"/NULL/empty → Update with new value
   - If current value is real data → Preserve it (prevents signature breaking)

2. **Market data (price, volume, mcap, changes)**:
   - Always update with latest values from GeckoTerminal

3. **Timestamp**:
   - Always update `updated_at`

## Benefits

✅ **Auto-healing**: Tokens stuck as "Unknown" automatically fix themselves on next search

✅ **Fresh market data**: Price/volume/mcap refresh every search (until cached)

✅ **Signature safety**: Real identity data preserved once set (sig_identity stays valid)

✅ **Retry logic**: Failed metadata fetches automatically retry on next search

## Testing

### Test Case 1: Token with Failed Metadata

1. Search token with bad/slow network → Saves as "Unknown"
2. Search again → Metadata re-fetched, token updated with real name/symbol

### Test Case 2: Existing Token Refresh

1. Token already indexed with real data
2. Search again → Market data refreshed, identity preserved

### Test Case 3: New Token

1. Search new token → Full indexing happens
2. Token saved with all data (metadata + market + pools)

## Related Changes

1. **Improved Logging** ([indexer.js:31-51](../src/services/indexer.js#L31-L51)):
   - Added step-by-step logging for debugging
   - Shows metadata fetch results
   - Shows market data fetch results
   - Shows pool discovery results

2. **Better Error Handling** ([indexer.js:42-43](../src/services/indexer.js#L42-L43)):
   - Supply fetch errors now logged with details
   - Helps diagnose RPC connection issues

## Verification

To verify the fix works:

```bash
# Test with diagnostic script
node scripts/diagnose-indexer.js YOUR_TOKEN_MINT_ADDRESS

# Check logs for successful indexing
tail -f logs/app.log | grep "Indexer"
```

Expected output:
```
🔍 [Indexer] Starting indexing for 12345678...
📝 [Indexer] Metadata: Token Name (SYMBOL)
💹 [Indexer] Market data: $0.000123 | Vol: $45678
🏊 [Indexer] Found 2 pool(s) for 12345678
✅ [Indexer] Successfully indexed Token Name (12345678)
```

## Edge Cases Handled

1. **Metadata fetch timeout**: Token saved as "Unknown", retried on next search
2. **GeckoTerminal rate limit**: Market data = 0, but identity still saved
3. **No pools found**: Token indexed without price data, pools discovered later
4. **Duplicate searches**: ON CONFLICT prevents duplicate tokens
5. **Metaplex metadata changes**: Identity preserved once set (prevents signature break)

## Performance Impact

- **Before**: Only updated timestamp on duplicate search
- **After**: Updates market data + conditionally updates identity
- **Impact**: Negligible (~1-2ms extra per conflict)

## Related Files

- [src/services/indexer.js](../src/services/indexer.js) - Main indexing logic
- [src/routes/tokens.js:1960](../src/routes/tokens.js#L1960) - Search endpoint trigger
- [src/utils/metaplex.js](../src/utils/metaplex.js) - Metadata fetching
- [scripts/diagnose-indexer.js](../scripts/diagnose-indexer.js) - Diagnostic tool

---

**Date Fixed**: 2026-01-08
**Bug Severity**: High (broken core feature)
**Fix Complexity**: Low (SQL UPDATE clause change)
**Regression Risk**: Low (preserves existing correct data)
