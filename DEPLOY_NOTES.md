# PR#2 - Deployment Notes for Maintainer

## Required Environment Variables (Render)

Add these to your Render Web Service environment:

```bash
# Required for webhook mode (99% RPC cost reduction)
API_URL=https://your-backend.onrender.com
WEBHOOK_SECRET=<generate-a-32-byte-hex-secret>

# Generate secret with:
# node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
```

## What happens after deploy

1. **Startup logs will show:**
   ```
   [Webhook] Initializing master webhook → https://your-backend.onrender.com/webhook/transfers
   [Webhook] Master webhook created with X tokens
   🟢 K-Score Updater Started - Mode: WEBHOOK (0 RPC), Interval: 60min
   ```

2. **Helius will start pushing transfer events** to `/webhook/transfers`

3. **K-Score updates** will use cached data (0 RPC) instead of polling

## RPC Cost Comparison

| Mode | RPC calls/hour | Monthly cost |
|------|----------------|--------------|
| Before (polling every 10min) | ~100,000 | $$$$$ |
| After (webhook + hourly light) | ~500 | $ |

## If webhooks are NOT configured

The system falls back gracefully to polling mode (30min interval instead of 10min).
Still a 3x improvement, but not the full 99% reduction.

## New Features

- **K-Score Card Image API**: `GET /api/token/:mint/card.png`
  - Generates shareable Twitter cards
  - 1200x628 PNG with K-Score, conviction breakdown, grade badge

## Questions?

Contact the PR author or check the code comments in:
- `src/index.js` (webhook initialization)
- `src/tasks/kScoreUpdater.js` (K-Score light/deep modes)
- `src/services/cardGenerator.js` (image generation)
