# HolDex Genesis Node Setup - gcrtrd

**For: sollama58**
**Node ID: gcrtrd**
**Role: Genesis Node (Consensus Authority)**

---

## What This Node Does

You are one of **2 Genesis Nodes** in the HolDex verification network. Your node will:

- Calculate K-Scores for Solana tokens
- Sign every calculation with your Ed25519 private key
- Participate in consensus (61.8% agreement required)
- Earn φ credits for work completed

As a Genesis node, you have **permanent approval status** - no one can remove you from the network.

---

## Quick Start (Render.com)

### 1. Fork the Repository

Fork `https://github.com/zeyxx/HolDex` to your GitHub account.

### 2. Create Background Worker on Render

1. Go to https://dashboard.render.com
2. Click **New** → **Background Worker**
3. Connect your forked repository
4. Configure:
   - **Name**: `holdex-calculator-gcrtrd`
   - **Branch**: `main`
   - **Build Command**: `npm install --ignore-optional`
   - **Start Command**: `npm run calculator`
   - **Plan**: Starter ($7/month) or higher

### 3. Set Environment Variables

Add these environment variables in Render dashboard:

```
NODE_ID=gcrtrd
NODE_NAME=Genesis Prod (sollama58)
NODE_PRIVATE_KEY=<sent via secure channel>
USE_DISTRIBUTED_POLLING=true
DATABASE_URL=<will be provided>
REDIS_URL=<will be provided>
HELIUS_API_KEY=<your Helius API key>
```

### 4. Deploy

Click **Create Background Worker**. The node will automatically:
- Connect to the shared PostgreSQL database
- Register as genesis node `gcrtrd`
- Start calculating and signing K-Scores
- Send heartbeats every ~8 seconds

---

## Environment Variables Reference

| Variable | Required | Description |
|----------|----------|-------------|
| `NODE_ID` | Yes | Must be exactly `gcrtrd` |
| `NODE_NAME` | Yes | Display name for your node |
| `NODE_PRIVATE_KEY` | Yes | Your Ed25519 private key (see below) |
| `USE_DISTRIBUTED_POLLING` | Yes | Set to `true` for network mode |
| `DATABASE_URL` | Yes | Shared PostgreSQL connection string |
| `REDIS_URL` | Yes | Your Redis instance (can be shared or local) |
| `HELIUS_API_KEY` | Yes | For Solana RPC access |

---

## Your Credentials

**Node ID:** `gcrtrd`

Your **private key** has been sent to you via secure channel (DM/encrypted).
**NEVER commit your private key to git or share it publicly.**

Your **public key** is hardcoded in `src/config/genesis.js`. This means:
- Anyone can verify your signatures
- Only you can create valid signatures
- Your node is permanently recognized as a genesis authority

---

## Verification

After deployment, check the logs for:

```
🔑 Node: gcrtrd (Genesis Prod (sollama58))
   Mode: DISTRIBUTED
✅ Genesis Nodes Ready
✅ Node registered: gcrtrd
🔐 Node gcrtrd is GENESIS - can participate in consensus
✅ Distributed Polling Ready
🧠 Calculator Brain ACTIVE
   Mode: DISTRIBUTED
```

You should see heartbeats:
```
[Heartbeat] gcrtrd: +0.382 credits
```

---

## Network Status

Once running, your node will appear in the network:

```
🌐 Network: 2 nodes | X tasks | credits: Y.YY
```

Both genesis nodes (asdfasdfa + gcrtrd) need to agree for consensus on K-Scores.

---

## Troubleshooting

### "Node not found or not approved"
Your node wasn't registered. Check that `NODE_ID=gcrtrd` exactly.

### "Invalid signature"
Wrong private key. Copy the exact key from this document.

### "Foreign key constraint violation"
Database issue. Contact asdfasdfa to verify database state.

### Node shows "offline" status
Check that:
1. The service is running
2. `USE_DISTRIBUTED_POLLING=true` is set
3. Database connection is working

---

## Security Notes

1. **Never share your private key** - it's your identity
2. **Never commit the private key to git** - use environment variables only
3. **Your signatures are permanent** - every K-Score you sign is recorded forever
4. **You cannot be removed** - Genesis nodes are hardcoded in the source

---

## Contact

Questions? Reach out to asdfasdfa (primary genesis node operator).

---

*"Don't Trust, Verify" - Every K-Score is cryptographically signed.*
