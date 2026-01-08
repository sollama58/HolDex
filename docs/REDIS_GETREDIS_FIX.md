# Redis getRedis() Function Error Fix

## Problem

The calculator worker was throwing this error:

```
Cache write failed: getRedis is not a function
```

## Root Cause

Multiple files were trying to import `getRedis` from `src/services/redis.js`:

```javascript
const { getRedis } = require('./redis');
```

However, [redis.js:60-65](../src/services/redis.js#L60-L65) only exported:
- `connectRedis`
- `initRedis` (alias for connectRedis)
- `getClient`
- `getSubscriber`

**Missing**: `getRedis` function was not exported!

## Files Affected

These files were importing the non-existent `getRedis`:

1. [src/services/solana.js](../src/services/solana.js) - Holder count caching (lines 4, 145, 229)
2. [src/services/pool_finder.js](../src/services/pool_finder.js) - Pool reserve caching

## The Fix

Added `getRedis` as an alias to `getClient` in [redis.js:64](../src/services/redis.js#L64):

```javascript
module.exports = {
    connectRedis,
    initRedis: connectRedis, // ALIAS for backwards compatibility
    getClient,
    getRedis: getClient,     // ALIAS for backwards compatibility (NEW)
    getSubscriber
};
```

### Why an Alias?

1. **Backwards Compatibility**: Multiple files already use `getRedis()` - changing them all would be risky
2. **Consistency**: Both names (`getClient` and `getRedis`) refer to the same function
3. **Future-proof**: New code can use either name

## How It Works

`getRedis()` is a **synchronous** function that returns the Redis client instance:

```javascript
// Returns the client if connected, or null if not
const redis = getRedis();

if (redis) {
    // Use redis client
    await redis.get(key);
    await redis.set(key, value, 'EX', ttl);
}
```

**Important**: Unlike `initRedis()` (which is async and connects), `getRedis()` just returns the existing client.

## Correct Usage Pattern

```javascript
const { getRedis } = require('./redis');

async function someFunction() {
    try {
        const redis = getRedis(); // Synchronous - no await
        if (redis) {
            const value = await redis.get('key'); // Async operation
        }
    } catch (e) {
        console.error('Redis operation failed:', e);
    }
}
```

## Testing

To verify the fix:

```bash
# Start the calculator worker
npm run calculator

# Check logs - should NOT see "getRedis is not a function"
tail -f logs/app.log | grep -i redis

# Test holder count caching
curl http://localhost:3000/api/token/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v

# Check Redis for cached data
redis-cli --scan --pattern "holdex:holders:count:*"
```

## Related Files

- [src/services/redis.js](../src/services/redis.js) - Redis client initialization
- [src/services/solana.js](../src/services/solana.js) - Holder count caching
- [src/services/pool_finder.js](../src/services/pool_finder.js) - Pool reserve caching
- [src/services/rpcMonitor.js](../src/services/rpcMonitor.js) - RPC usage tracking

---

**Date Fixed**: 2026-01-08
**Bug Severity**: High (blocking calculator worker)
**Fix Complexity**: Low (1-line alias addition)
**Regression Risk**: None (pure addition, no breaking changes)
