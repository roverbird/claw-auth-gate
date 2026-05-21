# auth-gate

_"Every website deserves an AI Agent Chatbot, but who will pay the LLM bill?"_

A self-contained Rust binary that provides Google authentication, per-user prompt
quotas, and voucher-based top-ups for WebSocket-based AI chatbot deployments.

Designed to sit between **Caddy** and **ZeroClaw** — but works in front of any
WebSocket backend. It allows to gate website chatbot users with prompt limits and
has a logic to sell prompt quotas as vouchers.

---

## What it does

- Verifies Google JWT tokens during the WebSocket handshake
- Tracks per-user prompt usage in SQLite
- Enforces a configurable free tier limit
- Rate limits prompts per user per minute (optional)
- Accepts voucher codes to extend a user's quota (pay-as-you-go)
- Forwards authenticated, within-quota sessions to the upstream AI backend
- Emits structured JSON events to the frontend for limit and redemption states

No Python. No Node. No runtime dependencies. Single binary, single config file.

---

## Architecture

```
Browser
  │  Google Sign-In → JWT
  │  wss://yourdomain.com/ws/chat?token=<JWT>
  ▼
Caddy :443          — TLS termination, static file serving
  ▼
auth-gate :9090     — auth, rate limit, quota, voucher logic
  ▼
ZeroClaw :123456    — LLM inference
```

All three services run on a single Debian VPS. Only Caddy is internet-facing.

---

## Database

SQLite. Two tables.

```sql
-- one row per user, created on first login
CREATE TABLE usage (
    email  TEXT PRIMARY KEY,
    count  INTEGER NOT NULL DEFAULT 0,   -- prompts used
    quota  INTEGER NOT NULL DEFAULT 0    -- prompts allowed
);

-- one row per voucher code
CREATE TABLE vouchers (
    code      TEXT PRIMARY KEY,
    prompts   INTEGER NOT NULL,
    used_by   TEXT DEFAULT NULL,         -- set on redemption
    used_at   TEXT DEFAULT NULL          -- UTC timestamp
);
```

New users get `quota = free_limit` from config. Voucher redemption adds to `quota`.
Gate blocks when `count >= quota`.

---

## Configuration

```toml
# /etc/auth-gate/config.toml

listen_port            = 9090
google_client_id       = "YOUR_CLIENT_ID.apps.googleusercontent.com"
upstream_url           = "ws://127.0.0.1:42616/ws/chat"
upstream_token         = "your-upstream-token"   # optional
free_limit             = 3
payment_url            = "https://your-payment-link"
db_path                = "/var/lib/auth-gate/usage.db"
rate_limit_per_minute  = 6                        # optional — omit to disable
# test_mode            = true                     # dev only — disables JWT
```

### All config parameters

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `listen_port` | u16 | yes | Port to bind on localhost |
| `google_client_id` | string | yes | Google OAuth2 client ID |
| `upstream_url` | string | yes | Upstream WebSocket URL |
| `free_limit` | u64 | yes | Free prompts given to every new user |
| `payment_url` | string | yes | Shown in `system_limit` event |
| `db_path` | string | yes | SQLite file path |
| `upstream_token` | string | no | Bearer token injected into upstream requests |
| `rate_limit_per_minute` | u32 | no | Max prompts per user per 60s window |
| `test_mode` | bool | no | Bypass JWT verification (dev only) |

---

## CLI

```bash
# Run (production)
auth-gate /etc/auth-gate/config.toml

# Run without JWT verification (dev)
auth-gate config.toml --test

# Generate voucher codes
auth-gate config.toml --generate N PROMPTS

# Help
auth-gate --help
```

### Voucher generation

```bash
auth-gate config.toml --generate 20 50
#   DUCK-7842-XKPQ-3NMF  (50 prompts)
#   WQRT-4JHN-BVZK-92LC  (50 prompts)
#   ...
# Done. Codes are live in the database.
```

Codes are inserted directly into the database and ready to use immediately.
Format: `XXXX-XXXX-XXXX-XXXX`, unambiguous character set, case-insensitive on entry.

---

## WebSocket protocol

auth-gate intercepts two message types. Everything else passes through untouched.

**Prompt** (client → gate → upstream):
```json
{ "type": "message", "content": "user message here" }
```

**Voucher redemption** (client → gate, does not reach upstream):
```json
{ "type": "redeem", "code": "DUCK-7842-XKPQ-3NMF" }
```

**Gate responses to client:**

```json
{ "type": "system_limit",  "content": "...", "payment_url": "..." }
{ "type": "rate_limited",  "content": "Too many messages. Please wait a moment.", "retry_after_secs": 60 }
{ "type": "redeem_ok",     "prompts_added": 50, "prompts_used": 3, "prompts_total": 53 }
{ "type": "redeem_error",  "content": "invalid code" }
{ "type": "system_error",  "content": "Service unavailable" }
```

**Message check order** (per incoming prompt):
1. Rate limit — if exceeded → `rate_limited`, block
2. Quota — if exhausted → `system_limit`, block
3. Forward to upstream → increment count

---

## Operations

```bash
# View all users
sqlite3 /var/lib/auth-gate/usage.db \
  "SELECT email, count, quota, quota-count AS remaining FROM usage ORDER BY count DESC;"

# View voucher inventory
sqlite3 /var/lib/auth-gate/usage.db \
  "SELECT code, prompts, COALESCE(used_by,'—') AS used_by FROM vouchers;"

# Reset a user's count
sqlite3 /var/lib/auth-gate/usage.db \
  "UPDATE usage SET count=0 WHERE email='user@example.com';"

# Add prompts directly (no voucher)
sqlite3 /var/lib/auth-gate/usage.db \
  "UPDATE usage SET quota=quota+50 WHERE email='user@example.com';"

# Daily backup (add to cron)
sqlite3 /var/lib/auth-gate/usage.db \
  ".backup /var/backups/auth-gate-$(date +%Y%m%d).db"

# Update binary
cargo build --release
sudo cp target/release/auth-gate /usr/local/bin/auth-gate
sudo systemctl restart auth-gate
```

---

## Build

```bash
sudo apt install build-essential pkg-config libssl-dev -y
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
cargo build --release
```

---

## Full deployment

Full deployment — including Caddy configuration, ZeroClaw setup, systemd units,
frontend integration, and the voucher-based SaaS flow — is available as a private
install script and documentation.

Contact: [kibervarnost](mailto:kibervarnost@proton.me)

---

## License

MIT
