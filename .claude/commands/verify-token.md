# /verify-token

Quick integrity verification for a token's cryptographic signatures.

## Usage
```
/verify-token <mint_address>
```

## Instructions

Verify all 9 signature categories for the specified token.

### Verification Process

1. **Fetch token with signatures**
   ```sql
   SELECT *,
     sig_identity IS NOT NULL as has_sig_i,
     sig_security IS NOT NULL as has_sig_s,
     sig_lp IS NOT NULL as has_sig_l,
     sig_supply IS NOT NULL as has_sig_su,
     sig_kscore IS NOT NULL as has_sig_k,
     sig_market IS NOT NULL as has_sig_m,
     sig_origin IS NOT NULL as has_sig_o,
     sig_holders IS NOT NULL as has_sig_h,
     sig_full IS NOT NULL as has_sig_f
   FROM tokens WHERE mint = $1;
   ```

2. **Check API verification endpoint**
   ```bash
   curl -s "https://holdex-api.onrender.com/api/token/$MINT/verify"
   ```

3. **Verify holder snapshots exist** (for sig_holders)
   ```sql
   SELECT COUNT(*) FROM holder_snapshots WHERE mint = $1;
   ```

### Output Format
```
## Integrity Report: [MINT]

### Signature Status
| Category | Status | Notes |
|----------|--------|-------|
| Identity | [valid/invalid/missing] | |
| Security | [valid/invalid/missing] | |
| LP | [valid/invalid/missing] | |
| Supply | [valid/invalid/missing] | |
| K-Score | [valid/invalid/missing] | |
| Market | [valid/stale/missing] | |
| Origin | [valid/invalid/missing] | |
| Holders | [valid/invalid/missing] | |
| Full | [valid/invalid/missing] | |

### Staleness
- K-Score: [time since update]
- Price: [time since update]
- Holders: [time since update]

### Issues
[Any problems found]
```

### Expected Behavior
- Stable categories (identity, security, lp, supply, kscore, origin) should always be `valid`
- Market category may be `stale` (updates every 30s)
- Holders category evolves with trades
- Full category is volatile (composite of all)

### Philosophy
"Don't Trust, Verify" - Every piece of data is cryptographically signed.
