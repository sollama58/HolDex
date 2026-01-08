# K-Score Explained: Simple Quality Ratings for Tokens

## What is K-Score?

**K-Score** is a simple, on-chain quality rating for Solana tokens. Think of it like a credit score, but for memecoins.

Instead of complex formulas with dozens of metrics, K-Score focuses on three things that actually matter:

1. **💎 Diamond Hands** - Do holders believe in this token?
2. **🌱 Organic Growth** - Is the distribution healthy, or controlled by a few wallets?
3. **⏰ Longevity** - Has this token survived, or is it brand new?

### The Formula (Simple Version)

```
K-Score = 100 × ∛(Diamond Hands × Organic Growth × Longevity)
```

All three components are equally weighted. A token must excel on **all three** to score high.

---

## K-Score Tiers

K-Scores range from 0-100. Here's what each tier means:

| Tier | Score Range | Icon | Quality | What It Means |
|------|-------------|------|---------|---------------|
| **Diamond** | 90-100 | 💎 | Exceptional Quality | Strong conviction, healthy distribution, proven survival. Rare. |
| **Platinum** | 80-89 | 💠 | High Quality | Very good fundamentals across all metrics. |
| **Gold** | 70-79 | 🥇 | Good Quality | Solid token with good holder behavior. |
| **Silver** | 60-69 | 🥈 | Fair Quality | Average token, some positive indicators. |
| **Bronze** | 50-59 | 🥉 | Speculative | Borderline - may have one weak metric. |
| **Copper** | 40-49 | 🟤 | High Risk | Weak fundamentals. Proceed with caution. |
| **Iron** | 20-39 | ⚫ | Very High Risk | Multiple red flags. Likely short-lived. |
| **Rust** | 0-19 | 🔩 | Distressed | Failing or abandoned. Avoid. |

---

## How the Three Pillars Work

### 1. 💎 Diamond Hands (Conviction)

**What it measures**: Do holders accumulate over time, or do they dump?

**How we calculate it**:
- We analyze the **top 20 holders** (excluding pools/DEX addresses)
- We look at their transaction history over the past 30-90 days
- We classify each holder as:
  - **Accumulator** 📈: Buying more over time
  - **Holder** 🤝: Stable balance (no major sells)
  - **Reducer** 📉: Selling some, but still holding
  - **Extractor** 🚪: Dumped most/all of their position

**Conviction Score** = `(Accumulators + Holders) / Total Analyzed × 100`

**Example**:
- If 15 out of 20 top holders are accumulating or holding steady → **High conviction (75%)**
- If only 5 out of 20 are holding → **Low conviction (25%)**

**Why it matters**: Tokens with high conviction (diamond hands) tend to survive longer and have more stable prices.

---

### 2. 🌱 Organic Growth (Distribution Quality)

**What it measures**: Is the token widely distributed, or controlled by a few whales?

**How we calculate it**:
1. **Total Holders** - More holders = better
2. **Top 20 Concentration** - Lower concentration = better
   - If the top 20 wallets own 80% of supply → **Centralized (bad)**
   - If the top 20 wallets own 20% of supply → **Distributed (good)**

**Organic Growth Score** = `√(Holders × (1 - Top20Concentration))`

**Example**:
- **Good**: 5,000 holders, top 20 own 25% → High organic growth
- **Bad**: 100 holders, top 20 own 90% → Low organic growth (likely sniped launch)

**Why it matters**: Widely distributed tokens have more resilient communities and less dump risk.

---

### 3. ⏰ Longevity (Survival Factor)

**What it measures**: How long has the token existed, and is it still active?

**How we calculate it**:
- **Age Factor**: Tokens gain credit score over time
  - 1 day old → Low score
  - 7 days old → Moderate score
  - 30+ days old → High score
  - Uses asymptotic curve (diminishing returns after 90 days)

- **Activity Factor**: Recent trading activity boosts the score
  - Dead (no volume) → Penalty
  - Active trading → Bonus

**Longevity Score** = `Age Factor × Activity Multiplier`

**Why it matters**: Most failed tokens die within the first week. Surviving 30+ days is a strong signal.

---

## Real Examples

### Example 1: Diamond Tier (K = 95)

```
Diamond Hands: 85% (17 of 20 top holders accumulating)
Organic Growth: High (3,200 holders, top 20 own 18%)
Longevity: 45 days old, active trading

K = 100 × ∛(0.85 × HighOrganic × 0.95) ≈ 95
```

**Interpretation**: Exceptional token. Strong community conviction, healthy distribution, proven survival.

---

### Example 2: Bronze Tier (K = 52)

```
Diamond Hands: 40% (8 of 20 top holders accumulating)
Organic Growth: Medium (800 holders, top 20 own 45%)
Longevity: 12 days old, moderate activity

K = 100 × ∛(0.40 × MediumOrganic × 0.65) ≈ 52
```

**Interpretation**: Speculative. Some holders are dumping, moderate concentration, still relatively new.

---

### Example 3: Rust Tier (K = 15)

```
Diamond Hands: 10% (2 of 20 top holders remaining)
Organic Growth: Very Low (50 holders, top 20 own 95%)
Longevity: 3 days old, declining volume

K = 100 × ∛(0.10 × VeryLowOrganic × 0.20) ≈ 15
```

**Interpretation**: Distressed/Dying. Mass exodus, highly concentrated (likely insider dump), failing fast.

---

## What K-Score is NOT

❌ **Not a price prediction** - K-Score measures quality, not future price movement

❌ **Not a buy/sell signal** - High K-Score doesn't mean "buy now"

❌ **Not gaming-proof** - Sophisticated actors could manipulate metrics, but it's expensive

❌ **Not for trading bots** - Use for research and due diligence, not automated trading

---

## What K-Score IS

✅ **Quality filter** - Quickly identify which tokens have healthy fundamentals

✅ **Research starting point** - Use K-Score to filter 10,000 tokens down to 100 worth researching

✅ **Red flag detector** - Low K-Score (< 40) = multiple warning signs

✅ **Community health indicator** - High Diamond Hands score = strong community

---

## How to Use K-Score

### For Investors

1. **Filter by tier**: Start with Diamond/Platinum tier tokens
2. **Check the pillars**: Look at Diamond Hands, Organic Growth, and Longevity individually
   - Strong on all 3? → Worth deeper research
   - Weak on 1-2? → Understand why before investing
3. **Track trajectory**: Is K-Score improving or declining over time?
4. **Combine with other research**: K-Score + team + product + market = full picture

### For Traders

- **High K-Score (70+)**: Lower volatility, more likely to survive dumps
- **Medium K-Score (50-69)**: Moderate risk, watch for trend changes
- **Low K-Score (< 50)**: High risk, potential for quick dumps

### For Communities

- **Improve your K-Score** by:
  1. Encouraging long-term holding (builds Diamond Hands score)
  2. Broadening distribution (airdrops, fair launches)
  3. Maintaining consistent activity (trading, utility)

---

## Common Questions

**Q: Why is my token's K-Score low even though the price is up?**

A: K-Score measures **quality fundamentals**, not price. A token can pump on hype but have poor fundamentals (low holder conviction, concentrated ownership). Price follows narratives short-term, but fundamentals matter long-term.

---

**Q: Can K-Score be gamed?**

A: Yes, but it's expensive. To fake Diamond Hands conviction, you'd need to:
- Create dozens of wallets
- Fund them all
- Execute hundreds of organic-looking transactions over weeks
- Maintain activity

This costs significant SOL in fees and capital lockup. Most scammers won't bother.

---

**Q: My favorite token has a low K-Score. Is it a scam?**

A: Not necessarily. Low K-Score means weak fundamentals **right now**. Reasons could be:
- Very new token (low Longevity)
- Recent dump event (low Diamond Hands)
- Concentrated airdrop (low Organic Growth)

Check the **individual pillars** to understand which metric is dragging down the score.

---

**Q: Do all good tokens have high K-Scores?**

A: No. K-Score is one metric. A token could have:
- Amazing product but low K-Score (too new)
- Strong team but low K-Score (concentrated pre-sale)
- Great narrative but low K-Score (paper hands community)

Use K-Score as a **filter**, not the final decision.

---

**Q: How often does K-Score update?**

A: Every 12 hours for verified tokens (tokens with community updates enabled). New tokens get their first K-Score immediately when added to the database.

---

**Q: What's a "good" K-Score?**

A: Depends on your risk tolerance:
- **Conservative investors**: 70+ (Gold tier or higher)
- **Moderate risk**: 60-70 (Silver tier)
- **Speculators**: 50-60 (Bronze tier)
- **High risk/YOLO**: < 50 (proceed with extreme caution)

For context, **less than 5% of tokens reach Diamond tier (90+)**.

---

## Technical Details

### Data Sources

All K-Score data comes from **on-chain sources only**:
- Solana blockchain (transaction history, holder balances)
- DEX reserves (liquidity data)
- Token program data (supply, metadata)

**No off-chain data** (Twitter followers, Telegram members, etc.) is used.

### Cryptographic Verification

Every K-Score is **cryptographically signed** using HMAC-SHA256. This prevents tampering and ensures data integrity.

See [docs/INTEGRITY.md](./INTEGRITY.md) for verification details.

### Update Frequency

- **Scheduled updates**: Every 12 hours for verified tokens
- **On-demand updates**: When token is first added or manually refreshed by admin
- **Price updates**: Every 30 seconds (separate from K-Score calculation)

---

## Comparison to Other Metrics

| Metric | What It Measures | K-Score Equivalent |
|--------|------------------|-------------------|
| Market Cap | Token value × supply | Not measured (price-based) |
| Volume 24h | Trading activity | Part of Longevity (activity factor) |
| Holder Count | Number of wallets | Part of Organic Growth |
| Liquidity | DEX pool depth | Not measured (manipulable) |
| Age | Time since launch | Part of Longevity |

**Key difference**: K-Score combines **behavioral data** (are holders accumulating?) with **structural data** (distribution, age) to create a holistic quality score.

---

## Limitations

1. **New tokens penalized**: Longevity factor means brand new tokens (< 7 days) will have lower scores even if fundamentals are strong.

2. **Whale games**: A sophisticated team could create fake "diamond hands" with multiple wallets. Cost-prohibitive but possible.

3. **No product analysis**: K-Score doesn't evaluate the product, team, or roadmap. A technically strong token with a bad product could score high.

4. **Market conditions ignored**: During market-wide dumps, even Diamond tier tokens can see massive sell-offs.

5. **Not predictive**: K-Score is descriptive (current fundamentals) not predictive (future price).

---

## Conclusion

**K-Score is a quality filter, not a crystal ball.**

Use it to:
- ✅ Quickly identify tokens worth researching
- ✅ Avoid obvious scams (Rust/Iron tier)
- ✅ Track community health over time
- ✅ Combine with other research for full due diligence

Don't use it to:
- ❌ Make automated trading decisions
- ❌ Replace fundamental analysis
- ❌ Predict short-term price movements

**The best way to use K-Score**: Start with Diamond/Platinum tiers, research deeply, and understand why the metrics are what they are.

---

## Related Documentation

- [docs/KSCORE.md](./KSCORE.md) - Technical implementation details
- [docs/INTEGRITY.md](./INTEGRITY.md) - Cryptographic verification
- [docs/API.md](./API.md) - API endpoints for K-Score data
- [docs/TROUBLESHOOTING.md](./TROUBLESHOOTING.md) - Common issues

---

*Last updated: 2026-01-08*
