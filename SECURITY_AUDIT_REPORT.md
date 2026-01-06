# HolDex Security Audit Report
**Date:** 2026-01-03
**Auditor:** Helius RPC Engineering Team
**Scope:** Full-stack security audit (Frontend → API → Database → RPC Integration)

---

## Executive Summary

| Severity | Count | Status |
|----------|-------|--------|
| CRITICAL | 3 | Requires immediate attention |
| HIGH | 6 | Should be fixed before next release |
| MEDIUM | 12 | Should be addressed in roadmap |
| LOW | 8 | Minor improvements |

**Overall Assessment:** The codebase shows evidence of significant security hardening (Phase 1-3 commits visible), with many best practices implemented. However, several critical XSS vulnerabilities in the frontend and fail-open middleware patterns require immediate remediation.

---

## CRITICAL Issues (3)

### C1. Stored XSS via Token Data Rendering
**File:** `homepage.html:1542-1577`
**Impact:** Complete account takeover, session hijacking

```javascript
tbody.innerHTML = displayTokens.map((t, idx) => {
    return `<span class="font-bold">${t.ticker}</span>
            <span class="text-[#737373]">${t.name}</span>`;
}).join('');
```

**Attack Vector:** Attacker creates token with `ticker: "<img src=x onerror=alert(document.cookie)>"`. All users viewing dashboard execute malicious JavaScript.

**Fix:**
```javascript
function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}
// Use: ${escapeHtml(t.ticker)}
```

---

### C2. XSS via Social Links (javascript: protocol)
**File:** `homepage.html:1736-1744`

```javascript
if(t.twitter) links += `<a href="${t.twitter}" target="_blank">`;
```

**Attack Vector:** Token with `twitter: "javascript:alert(1)"` executes on click.

**Fix:** Validate URLs allow only `http:` and `https:` protocols.

---

### C3. Fail-Open in Rate Limiter (Complete Bypass)
**File:** `src/middleware/burnRateLimiter.js:102-106`

```javascript
} catch (e) {
    // Fail open for now (don't break API during transition)
    next();
}
```

**Impact:** Any error (Redis down, DB error) allows unlimited unauthenticated access.

**Fix:** Implement fail-closed behavior with strict fallback rate limiting.

---

## HIGH Issues (6)

### H1. SSL Certificate Verification Disabled
**Files:** `src/services/database.js:23` + 13 scripts

```javascript
const sslConfig = isLocal ? false : { rejectUnauthorized: false };
```

**Impact:** Man-in-the-Middle attacks on database connections possible.

**Fix:** Enable certificate verification or use internal network.

---

### H2. Rate Limit Bypass When Redis Down
**File:** `src/middleware/rateLimiter.js:82-85`

```javascript
} else {
    logger.warn('Redis unavailable for rate limiting. Request allowed.');
}
```

**Impact:** DoS attacks possible, attackers could target Redis to disable limits.

---

### H3. XSS in Top Holders Table
**File:** `homepage.html:2068-2090`

```javascript
<a href="${h.orbUrl}" target="_blank">
<button onclick="copyToClipboard('${h.address}')">
```

**Impact:** JavaScript injection via crafted holder addresses.

---

### H4. XSS in Description Preview
**File:** `homepage.html:2325`

```javascript
prevContainer.innerHTML = `<p>${desc || 'No description...'}</p>`;
```

---

### H5. XSS in API Keys Display
**File:** `homepage.html:2376-2406`

```javascript
${canUpgrade ? `<button onclick="upgradeApiTier('${k.key_id}', '${nextTier}')">` : ''}
```

---

### H6. Raw Error Messages Exposed (20+ locations)
**File:** `src/routes/tokens.js`

```javascript
} catch (e) { res.status(500).json({ error: e.message }); }
```

**Impact:** Stack traces, file paths, internal function names exposed.

**Fix:** Use `sanitizeError()` consistently (already exists in webhooks.js).

---

## MEDIUM Issues (12)

### M1. API Key in Query Parameter
**Files:** `burnRateLimiter.js:36`, `rateLimiter.js:11`, `unifiedRateLimiter.js:27`

```javascript
const apiKey = req.headers['x-api-key'] || req.query.api_key;
```

**Risk:** Query parameters logged in access logs, browser history.

---

### M2. Missing Rate Limits on 12 Endpoints
| Endpoint | Risk |
|----------|------|
| `GET /proxy/balance/:wallet` | RPC consumption |
| `GET /token/:mint/card.png` | CPU exhaustion |
| `POST /request-update` | Spam submissions |
| `GET /health` | Info disclosure |
| `GET /stale-tokens` | Info disclosure |
| `GET /rpc-status` | Info disclosure |
| `GET /config/fees` | Info disclosure |
| `POST /request-my-keys` | Enumeration |
| `GET /credits/:wallet` | Enumeration |
| `GET /api-pricing` | No auth needed |
| `GET /token/:mint/evolution` | DB load |
| `GET /track-record` | No limit |

---

### M3. X-Forwarded-For Rate Limit Bypass
**File:** `src/routes/tokens.js:18`

```javascript
keyGenerator: (req) => req.headers['x-forwarded-for'] || req.ip
```

**Risk:** Spoofable header bypasses rate limits.

---

### M4. Missing Input Validation on /request-update
**File:** `src/routes/tokens.js:508-531`

- URLs not validated beyond https prefix
- Description length not limited
- userPublicKey not validated as Solana address

---

### M5. Default Database Credentials in Fallback
**File:** `src/config/env.js:103`

```javascript
DATABASE_URL: process.env.DATABASE_URL || 'postgresql://user:password@localhost:5432/holdex',
```

---

### M6. DATA_SIGNING_SECRET Not Enforced in Production
**File:** `src/config/env.js:151`

Unlike WEBHOOK_SECRET, no production validation exists.

---

### M7. Redis Graceful Degradation Allows Bypass
**File:** `src/services/redis.js:42-46`

Combined with M2 rate limiter issues, allows full bypass.

---

### M8. Legacy Plaintext API Key Support
**File:** `src/middleware/unifiedRateLimiter.js:45-49`

```javascript
keyRecord = await db.get('SELECT ... FROM api_keys WHERE key = $1', [apiKey]);
```

---

### M9. ILIKE Pattern Injection
**File:** `src/routes/tokens.js:1516, 1612`

```javascript
WHERE (symbol ILIKE $1 OR name ILIKE $1)  // %${search}%
```

`escapeLikePattern()` exists but not used for search.

---

### M10. Dynamic ORDER BY SQL Interpolation
**File:** `src/routes/tokens.js:1541-1650`

```javascript
ORDER BY COALESCE(${sortCol}, 0) ${dir}
```

Currently whitelisted but fragile to future changes.

---

### M11. LP Pairs XSS
**File:** `homepage.html:1753`

```javascript
lpBody.innerHTML = t.pairs.map(p => `<td>${p.dexId}</td>`).join('');
```

---

### M12. PnL Table XSS
**File:** `homepage.html:2706-2739`

---

## LOW Issues (8)

### L1. Health Endpoint Exposes Internal State
**File:** `src/routes/tokens.js:277-328`

Memory usage, service states exposed without auth.

### L2. ADMIN_PASSWORD Only Required in Production
Development environments may have no admin auth.

### L3. Helius API Key in URL (Logging Risk)
`?api-key=${heliusKey}` may appear in logs.

### L4. Migration Errors Silently Swallowed
`src/services/database.js:238-244`

### L5. No Redis Authentication Warning
No validation for password in REDIS_URL.

### L6. Webhook Signature Format Not Validated
`src/routes/webhooks.js:128-135`

### L7. Partial Key Hash LIKE Matching
`src/routes/tokens.js:908-934` - Birthday attack scenario.

### L8. Timing Attack in Length Check
`src/routes/tokens.js:203-205`

---

## Positive Security Measures Observed

| Control | Location | Status |
|---------|----------|--------|
| Timing-safe password comparison | tokens.js:207 | Implemented |
| API key hashing (SHA-256) | apiKeyHash.js | Implemented |
| Parameterized SQL queries | All routes | Implemented |
| LIKE pattern escaping | tokens.js:896 | Exists (not always used) |
| Webhook replay protection | webhooks.js:50-58 | Implemented |
| HMAC signature verification | webhooks.js:101-108 | Implemented |
| Helmet security headers | index.js:37 | Implemented |
| CORS strict configuration | index.js:85-93 | Implemented |
| Server-side XSS escaping | index.js:26-34 | Implemented |
| 8-Category Data Signatures | dataSignature.js | Implemented |
| Self-Healing Watchdog | integrityWatchdog.js | Implemented |

---

## Recommendations by Priority

### Immediate (This Week)
1. Fix all XSS vulnerabilities in homepage.html (C1, C2, H3-H5, M11-M12)
2. Change burnRateLimiter to fail-closed (C3)
3. Enable SSL certificate verification (H1)
4. Apply sanitizeError() to all catch blocks (H6)

### Short-term (This Month)
5. Add rate limiting to unprotected endpoints (M2)
6. Deprecate API key in query parameters (M1)
7. Implement in-memory fallback rate limiting (H2, M7)
8. Add DATA_SIGNING_SECRET production enforcement (M6)
9. Use escapeLikePattern() for search (M9)
10. Complete legacy plaintext key migration (M8)

### Medium-term
11. Enable Content Security Policy (CSP) headers
12. Add input validation on /request-update (M4)
13. Remove default credentials from fallbacks (M5)
14. Configure trusted proxy for X-Forwarded-For (M3)

---

## Live Endpoint Test Results

| Endpoint | Auth | Rate Limited | Status |
|----------|------|--------------|--------|
| `GET /api/health` | No | No | Info disclosure |
| `GET /api/tokens/public` | No | Yes | OK |
| `GET /api/tokens` | Yes | Yes | OK |
| `GET /api/admin/updates` | Yes | N/A | Protected |
| `GET /api/config/fees` | No | No | treasury=null exposed |
| `GET /api/proxy/balance/:wallet` | No | No | RPC risk |
| `GET /api/rpc-status` | No | No | OK (no key leak) |

---

## Conclusion

HolDex demonstrates strong security awareness with implementations like:
- 8-category cryptographic signatures
- Self-healing integrity watchdog
- Webhook HMAC verification
- Timing-safe comparisons

However, the **frontend XSS vulnerabilities are critical** and require immediate attention. The fail-open middleware patterns also present significant risk.

**Recommendation:** Prioritize XSS fixes before any public launch or marketing push.

---

*Report generated by Helius RPC Engineering Team*
*Audit methodology: Static analysis + Dynamic testing + Code review*
