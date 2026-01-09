# HolDex Troubleshooting Guide

## New Token Indexer Not Working

### Symptoms
- New tokens are not being automatically added to the database
- Searching by contract address (CA) returns no results
- Token listener logs show no activity

### Root Causes & Solutions

#### 1. Listener Worker Not Running

**Check if listener is running**:
```bash
# Check running processes
ps aux | grep listener_worker

# Check Docker containers (if using Docker)
docker ps | grep listener

# Check PM2 processes (if using PM2)
pm2 list | grep listener
```

**Solution**: Start the listener worker
```bash
# Local development
npm run listener

# Docker
docker-compose up -d listener

# PM2
pm2 start src/listener_worker.js --name listener
```

---

#### 2. WebSocket Connection Issues

**Check logs for connection errors**:
```bash
# Local
tail -f logs/app.log | grep "Listener"

# Docker
docker logs -f holdex-listener | grep "🔌\|❌\|⚠️"
```

**Common errors**:
- `WSS endpoint not configured` - Missing SOLANA_WSS_URL
- `Connection timeout` - Firewall blocking WebSocket connections
- `Rate limited` - Too many concurrent connections

**Solution A: Verify WebSocket URL**:
```bash
# Check .env file
cat .env | grep WSS

# Should have one of:
SOLANA_WSS_URL=wss://mainnet.helius-rpc.com/?api-key=YOUR_KEY
# OR
HELIUS_API_KEY=your_api_key_here  # Auto-generates WSS URL
```

**Solution B: Test WebSocket connection**:
```javascript
// Run this in Node.js REPL
const { Connection } = require('@solana/web3.js');
const conn = new Connection('https://mainnet.helius-rpc.com', {
    wsEndpoint: 'wss://mainnet.helius-rpc.com/?api-key=YOUR_KEY'
});

conn.onLogs(
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', // Raydium
    (logs) => console.log('✅ Logs received:', logs.signature),
    'processed'
);

// Should see logs within 30 seconds during market hours
```

---

#### 3. Redis Not Connected

**Check Redis status**:
```bash
# Test Redis connection
redis-cli ping
# Should return: PONG

# Check Redis URL in .env
cat .env | grep REDIS_URL
```

**Solution**: Fix Redis connection
```bash
# Start Redis (local)
redis-server

# Docker
docker-compose up -d redis

# Verify connection from app
node -e "const redis = require('./src/services/redis'); redis.initRedis().then(() => console.log('✅ Redis OK')).catch(e => console.error('❌', e))"
```

---

#### 4. Database Connection Issues

**Check database connectivity**:
```bash
# Test PostgreSQL connection
psql $DATABASE_URL -c "SELECT COUNT(*) FROM tokens;"

# Check for missing tables
psql $DATABASE_URL -c "\dt"
```

**Solution**: Run migrations if tables are missing
```bash
# Check if tokens table exists
psql $DATABASE_URL -c "\d tokens"

# If missing, reinitialize database
node src/scripts/seed_data.js
```

---

#### 5. Listener Watchdog Triggering

**Symptom**: Logs show `⚠️ LISTENERS DEAD? Reconnecting...` every 2 minutes

**Cause**: No activity detected for 120 seconds (markets closed or subscription failed)

**Solution A**: Check market hours
- Pump.fun and Raydium are most active during US/EU market hours (14:00-22:00 UTC)
- Test during active hours or use manual token addition

**Solution B**: Force trigger a test
```javascript
// Add test token manually
const { indexTokenOnChain } = require('./src/services/indexer');
indexTokenOnChain('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v').then(console.log);
```

---

#### 6. Token Search by CA Not Working

**Symptom**: Searching by contract address returns empty results

**Debug**:
```bash
# Test indexer directly
node -e "
const { indexTokenOnChain } = require('./src/services/indexer');
const mint = 'YOUR_TOKEN_ADDRESS_HERE';
indexTokenOnChain(mint).then(result => {
    console.log('✅ Token indexed:', result);
    process.exit(0);
}).catch(err => {
    console.error('❌ Indexing failed:', err.message);
    process.exit(1);
});
"
```

**Check API route**:
```bash
# Test search endpoint
curl "http://localhost:3000/api/tokens?search=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
```

**Solution**: Verify indexer is called
```javascript
// In src/routes/tokens.js:1959
if (rows.length === 0) {
    await indexTokenOnChain(search);  // This should trigger
    rows = await db.all(`SELECT * FROM tokens WHERE mint = $1`, [search]);
}
```

**Common causes**:
1. Invalid Solana address (not base58, wrong length)
2. Network error fetching metadata
3. Token has no liquidity pools
4. GeckoTerminal API rate limit

---

## Diagnostic Commands

### Check All Services

```bash
#!/bin/bash
# Save as: scripts/check-services.sh

echo "=== HolDex Service Health Check ==="

echo ""
echo "1. Redis Status:"
redis-cli ping 2>&1 || echo "❌ Redis not responding"

echo ""
echo "2. PostgreSQL Status:"
psql $DATABASE_URL -c "SELECT 1" 2>&1 | grep -q "1" && echo "✅ Database OK" || echo "❌ Database connection failed"

echo ""
echo "3. Token Count:"
psql $DATABASE_URL -c "SELECT COUNT(*) as total_tokens FROM tokens;" 2>&1

echo ""
echo "4. Listener Process:"
ps aux | grep -v grep | grep listener_worker && echo "✅ Listener running" || echo "❌ Listener not running"

echo ""
echo "5. Recent Token Additions:"
psql $DATABASE_URL -c "SELECT mint, symbol, name, timestamp FROM tokens ORDER BY timestamp DESC LIMIT 5;" 2>&1

echo ""
echo "6. WebSocket Subscriptions (check listener logs):"
tail -20 logs/app.log 2>/dev/null | grep "Subscribed to" || echo "No subscription logs found"
```

### Manual Token Addition Test

```bash
# Test adding a known token (USDC)
curl -X POST http://localhost:3000/api/tokens \
  -H "Content-Type: application/json" \
  -d '{"mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"}'

# Should return token data if indexer works
```

### Check Listener Logs

```bash
# View last 100 listener logs
tail -100 logs/app.log | grep -E "🔍|🎧|💓|✅|❌|⚠️"

# Filter for new token discoveries
tail -100 logs/app.log | grep "POTENTIAL NEW POOL"

# Check subscription status
tail -100 logs/app.log | grep "Subscribed to"
```

---

## Environment Variables Checklist

Required for new token indexer:

```bash
# Core services
DATABASE_URL=postgresql://user:pass@localhost:5432/holdex
REDIS_URL=redis://localhost:6379

# Solana RPC (REQUIRED for listener)
HELIUS_API_KEY=your_helius_api_key_here

# OR manually specify WebSocket endpoint
SOLANA_RPC_URL=https://mainnet.helius-rpc.com
SOLANA_WSS_URL=wss://mainnet.helius-rpc.com/?api-key=YOUR_KEY

# Optional
USE_WEBHOOKS=false  # Set to true if using Helius webhooks instead of listeners
```

---

## Known Issues & Workarounds

### Issue 1: Listener stops after 2 hours
**Cause**: Helius WebSocket connections timeout after prolonged inactivity
**Workaround**: Automatic reconnection is built-in (see watchdog at line 252 in newTokenListener.js)
**Manual fix**: Restart listener worker

### Issue 2: Token added but K-Score is 0
**Cause**: K-Score only calculated for tokens with `hasCommunityUpdate = TRUE`
**Solution**: Approve token for community updates
```sql
UPDATE tokens SET hasCommunityUpdate = TRUE WHERE mint = 'YOUR_MINT_ADDRESS';
```
Or use admin panel:
```bash
curl -X POST http://localhost:3000/api/admin/refresh-kscore \
  -H "x-admin-password: YOUR_ADMIN_PASSWORD" \
  -H "Content-Type: application/json" \
  -d '{"mint": "YOUR_MINT_ADDRESS", "force": true}'
```

### Issue 3: Duplicate token entries
**Cause**: Race condition between listener and manual indexing
**Fix**: The `ON CONFLICT (mint) DO NOTHING` clause prevents duplicates
**Verify**:
```sql
SELECT mint, COUNT(*) FROM tokens GROUP BY mint HAVING COUNT(*) > 1;
```

### Issue 4: Token metadata missing (name/symbol/image)
**Cause**: Metaplex metadata fetch failed or token has no metadata
**Solution**: Manually update
```sql
UPDATE tokens
SET name = 'Token Name', symbol = 'SYMBOL', image = 'https://...'
WHERE mint = 'YOUR_MINT_ADDRESS';
```

---

## Performance Optimization

If listener is slow or consuming too many resources:

### 1. Reduce RPC Call Frequency
```javascript
// In newTokenListener.js:69
// Increase delay between retries
await new Promise(r => setTimeout(r, 2000 + (i * 1000)));  // Was 1000
```

### 2. Limit Processed Signatures Cache
```javascript
// In newTokenListener.js:47
if (processedSigs.size > 5000) {  // Reduced from 10000
```

### 3. Disable Grower Scanner (if not needed)
```javascript
// In listener_worker.js:111-114
// Comment out grower scanner start
// if (growerScanner && typeof growerScanner.start === 'function') {
//     growerScanner.start({ db });
// }
```

---

## Monitoring & Alerts

### Set up monitoring for listener health

```bash
# Add to crontab (check every 5 minutes)
*/5 * * * * /path/to/scripts/check-listener.sh

# scripts/check-listener.sh:
#!/bin/bash
if ! ps aux | grep -v grep | grep listener_worker > /dev/null; then
    echo "❌ Listener worker is down! Restarting..."
    cd /path/to/holdex && npm run listener &
    # Send alert (email, Slack, etc.)
    curl -X POST YOUR_WEBHOOK_URL -d "Listener worker restarted"
fi
```

---

## FAQ

**Q: How long does it take for a new token to appear?**
A: Immediately if listener is running. The token is added to DB within 1-2 seconds of pool creation, then indexed (metadata, pools) within 5-10 seconds.

**Q: Why are some tokens missing K-Scores?**
A: K-Scores only calculate for tokens with `hasCommunityUpdate = TRUE`. New tokens default to `FALSE` and K-Score = 10 placeholder.

**Q: Can I manually add a token?**
A: Yes, search by contract address in the UI, or use the API:
```bash
curl "http://localhost:3000/api/tokens?search=YOUR_MINT_ADDRESS"
```
This will trigger `indexTokenOnChain()` automatically.

**Q: How do I verify the listener is working?**
A: Check logs for heartbeat:
```bash
tail -f logs/app.log | grep "💓 Listener Status"
```
Should show activity counter increasing every 30 seconds during market hours.

**Q: Token was added but has no price/liquidity data**
A: This happens if:
1. Token has no liquidity pools (fresh launch)
2. Jupiter/Raydium doesn't have data yet (token too new)
3. Pool discovery failed (check `findPoolsOnChain()` logs)

**Solution**: Wait 5-10 minutes and refresh, or trigger manual pool discovery:
```javascript
const { findPoolsOnChain } = require('./src/services/pool_finder');
findPoolsOnChain('YOUR_MINT').then(console.log);
```

---

## Related Files

- [src/listener_worker.js](../src/listener_worker.js) - Main listener entry point
- [src/tasks/newTokenListener.js](../src/tasks/newTokenListener.js) - WebSocket subscriptions
- [src/services/indexer.js](../src/services/indexer.js) - Token indexing logic
- [src/routes/tokens.js:1959](../src/routes/tokens.js#L1959) - Search by CA trigger
- [src/services/pool_finder.js](../src/services/pool_finder.js) - Pool discovery

---

## Getting Help

If troubleshooting doesn't resolve the issue:

1. **Check logs**: Full context in `logs/app.log`
2. **Enable debug logging**: Set `LOG_LEVEL=debug` in `.env`
3. **Test in isolation**: Run diagnostic commands above
4. **Check GitHub issues**: [github.com/your-repo/issues](https://github.com)

**Collect this info when reporting issues**:
- Listener logs (last 100 lines with timestamps)
- Environment variables (masked sensitive values)
- Output of diagnostic commands
- Steps to reproduce
