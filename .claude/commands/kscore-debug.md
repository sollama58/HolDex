# /kscore-debug

Debug K-Score calculation issues for a specific token.

## Usage
```
/kscore-debug <mint_address>
/kscore-debug <symbol>
```

## Instructions

Debug the K-Score calculation for the specified token.

### K-Score v10 Formula
```
K = 100 * cbrt(D * O * L)
```

Where:
- **D (Diamond Hands)**: Conviction strength from holder behavior (50% weight)
- **O (Organic Growth)**: Distribution quality, anti-sniper (35% weight)
- **L (Longevity)**: Survival factor over time (15% weight)

### Debug Steps

1. **Fetch current state**
   ```sql
   SELECT mint, symbol, k_score, conviction_score,
          conviction_accumulators, conviction_holders,
          conviction_reducers, conviction_extractors,
          holders, real_holders, last_k_score_update
   FROM tokens WHERE mint = $1 OR symbol = $1;
   ```

2. **Check conviction breakdown**
   - Accumulators should be high for healthy tokens
   - Extractors indicate selling pressure
   - Real holders ($1+) vs total holders ratio

3. **Verify holder snapshots**
   ```sql
   SELECT holder, balance, conviction_class, buy_count, sell_count
   FROM holder_snapshots WHERE mint = $1
   ORDER BY balance DESC LIMIT 20;
   ```

4. **Check history trend**
   ```sql
   SELECT date, k_score, conviction_score, holders
   FROM k_score_history WHERE mint = $1
   ORDER BY date DESC LIMIT 7;
   ```

### Output Format
```
## K-Score Debug: [SYMBOL]

### Current Score: [X]/100
- Diamond Hands (D): [value]
- Organic Growth (O): [value]
- Longevity (L): [value]

### Conviction Breakdown
- Accumulators: [N] ([%])
- Holders: [N] ([%])
- Reducers: [N] ([%])
- Extractors: [N] ([%])

### Issues Found
[Any anomalies or problems]

### Recommendations
[Suggested actions]
```

### Common Issues
- NULL conviction_class in webhook mode
- Stale last_k_score_update
- Missing holder_snapshots
- Low real_holders ratio
