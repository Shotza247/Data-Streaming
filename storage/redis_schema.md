# storage/redis_schema.md
# Redis Key Naming Conventions and TTL Policies
# ─────────────────────────────────────────────────────────────────────────────
# All keys use colon-delimited namespacing following the Redis recommended
# convention: `<domain>:<entity>:<identifier>`.
# Written by: scripts/kpi_to_redis.py (poller) and future agent writes.
# ─────────────────────────────────────────────────────────────────────────────

## Key Families

### 1. `tax:kpi:window:<window_start_iso>` — KPI Window Hash

**Type:** HASH
**TTL:** 900 seconds (15 minutes)
**Written by:** `scripts/kpi_to_redis.py` — polls PostgreSQL `tax_kpi_windows` every 30 s
**Read by:** `dashboard/tabs/tab_tax_kpi.py` — live KPI cards and trend chart

**Fields:**

| Field | Type | Example | Description |
|---|---|---|---|
| `window_start` | string (ISO 8601) | `2024-01-15T10:05:00` | Tumbling window open timestamp |
| `window_end` | string (ISO 8601) | `2024-01-15T10:10:00` | Tumbling window close timestamp |
| `total_count` | integer string | `"142"` | Total applications in window |
| `avg_income` | decimal string (2 dp) | `"284500.50"` | Average taxable income |
| `distinct_customers` | integer string | `"138"` | Unique customers in window |
| `fraud_count` | integer string | `"7"` | Applications flagged as fraud |
| `fraud_rate` | decimal string (4 dp) | `"0.0493"` | fraud_count / total_count |
| `top_province` | string | `"GAUTENG"` | Lexicographically max province (proxy for most active) |

**Example:**
```
HGETALL tax:kpi:window:2024-01-15T10:05:00
1) "window_start"       2) "2024-01-15T10:05:00"
3) "window_end"         4) "2024-01-15T10:10:00"
5) "total_count"        6) "142"
7) "avg_income"         8) "284500.50"
9) "distinct_customers" 10) "138"
11) "fraud_count"       12) "7"
13) "fraud_rate"        14) "0.0493"
15) "top_province"      16) "GAUTENG"
```

**Scan pattern:**
```
SCAN 0 MATCH "tax:kpi:window:*" COUNT 100
```

---

### 2. `tax:kpi:latest` — Latest Window Summary Hash

**Type:** HASH
**TTL:** 900 seconds (15 minutes)
**Written by:** `scripts/kpi_to_redis.py` — updated every poll cycle alongside per-window keys
**Read by:** `dashboard/tabs/tab_tax_kpi.py` — single-read for KPI card headlines

**Fields:**

| Field | Type | Example | Description |
|---|---|---|---|
| `window_start` | string (ISO 8601) | `"2024-01-15T10:05:00"` | Most recent window start |
| `total_count` | integer string | `"142"` | Applications in latest window |
| `avg_income` | decimal string (2 dp) | `"284500.50"` | Average income in latest window |
| `fraud_rate` | decimal string (4 dp) | `"0.0493"` | Fraud rate in latest window |
| `top_province` | string | `"GAUTENG"` | Leading province |
| `updated_at` | string (ISO 8601 UTC) | `"2024-01-15T10:07:33Z"` | When the poller last wrote this key |

**Example:**
```
HGETALL tax:kpi:latest
```

---

### 3. `tax:agent:session:<session_id>` — Agent Chat Session List

**Type:** LIST (most recent first via LPUSH)
**TTL:** 3600 seconds (1 hour)
**Written by:** `dashboard/tabs/tab_agent_chat.py` — push on each user/agent message exchange
**Read by:** `dashboard/tabs/tab_agent_chat.py` — replay session history on page reload

**Element JSON schema:**
```json
{
  "role": "user | assistant",
  "content": "text of the message",
  "timestamp": "ISO 8601",
  "sources": ["optional list of source references"]
}
```

**Example:**
```
LRANGE tax:agent:session:abc123 0 -1
```

---

### 4. `tax:fraud:alert:<customer_id>` — Active Fraud Alert Flag

**Type:** STRING (value = severity level)
**TTL:** 1800 seconds (30 minutes)
**Written by:** LangGraph fraud agent (Sub-Task 7) after high-confidence fraud detection
**Read by:** `dashboard/tabs/tab_fraud.py` — highlights flagged customer IDs in table

**Value:** One of `LOW`, `MEDIUM`, `HIGH`, `CRITICAL`

**Example:**
```
GET tax:fraud:alert:CUST-001
-> "HIGH"
```

---

## TTL Policy Summary

| Key Pattern | TTL | Rationale |
|---|---|---|
| `tax:kpi:window:*` | 900 s (15 min) | KPI windows are replaced every 5 min; 15 min gives 3 windows of history in cache |
| `tax:kpi:latest` | 900 s (15 min) | Aligned with window TTL; always refreshed by poller before expiry |
| `tax:agent:session:*` | 3600 s (1 hr) | Session state; long enough for a demo session |
| `tax:fraud:alert:*` | 1800 s (30 min) | Short-lived alert; re-evaluated on next agent run |

---

## Operational Notes

- Redis is configured without persistence in this platform (in-memory only).  
  On container restart all keys are lost — the kpi-poller will repopulate within one poll cycle (30 s).

- Use `redis-cli KEYS "tax:*"` for debugging (note: `KEYS` is O(N) — use `SCAN` in production).

- Monitor key count: `redis-cli DBSIZE`

- Flush all keys (reset): `redis-cli FLUSHDB` (destructive — all sessions lost)
