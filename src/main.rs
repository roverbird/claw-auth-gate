use std::{fs, sync::Arc, time::{Duration, Instant}};

use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Query, State},
    http::{HeaderValue, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::{mpsc, RwLock}};
use dashmap::DashMap;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as TungMessage};

// ── Config ────────────────────────────────────────────────────────────────────

#[derive(Deserialize, Clone)]
struct Config {
    listen_port:      u16,
    google_client_id: String,
    upstream_url:     String,
    free_limit:       u64,
    stripe_url:       String,   // any payment / info URL shown at limit
    db_path:          String,
    test_mode:             Option<bool>,
    upstream_token:        Option<String>,
    rate_limit_per_minute: Option<u32>,   // max prompts per user per 60s window
}

impl Config {
    fn load(path: &str) -> Self {
        let raw = fs::read_to_string(path)
            .unwrap_or_else(|_| panic!("Cannot read config: {path}"));
        toml::from_str(&raw)
            .unwrap_or_else(|e| panic!("Bad config: {e}"))
    }
}

// ── Database ──────────────────────────────────────────────────────────────────

type DbPool = Pool<SqliteConnectionManager>;

fn db_init(path: &str) -> DbPool {
    if let Some(parent) = std::path::Path::new(path).parent() {
        fs::create_dir_all(parent).ok();
    }
    let manager = SqliteConnectionManager::file(path).with_init(|conn| {
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;

             -- per-user prompt tracking
             CREATE TABLE IF NOT EXISTS usage (
                 email      TEXT PRIMARY KEY,
                 count      INTEGER NOT NULL DEFAULT 0,
                 quota      INTEGER NOT NULL DEFAULT 0
             );

             -- voucher codes
             CREATE TABLE IF NOT EXISTS vouchers (
                 code       TEXT PRIMARY KEY,
                 prompts    INTEGER NOT NULL,
                 used_by    TEXT    DEFAULT NULL,
                 used_at    TEXT    DEFAULT NULL
             );"
        )
    });
    Pool::builder()
        .max_size(8)
        .build(manager)
        .expect("Cannot build DB pool")
}

// Ensure user row exists with the free quota on first connection
fn db_ensure_user(pool: &DbPool, email: &str, free_limit: u64) {
    let conn = pool.get().expect("DB pool exhausted");
    conn.execute(
        "INSERT OR IGNORE INTO usage(email, count, quota) VALUES(?1, 0, ?2)",
        rusqlite::params![email, free_limit],
    ).expect("db_ensure_user failed");
}

// Returns (count, quota) in one query
fn db_get_usage(pool: &DbPool, email: &str) -> (u64, u64) {
    let conn = pool.get().expect("DB pool exhausted");
    conn.query_row(
        "SELECT count, quota FROM usage WHERE email = ?1",
        [email],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).unwrap_or((0, 0))
}

fn db_increment(pool: &DbPool, email: &str) {
    let conn = pool.get().expect("DB pool exhausted");
    conn.execute(
        "UPDATE usage SET count = count + 1 WHERE email = ?1",
        [email],
    ).expect("db_increment failed");
}

// Redeem a voucher: validates, marks used, adds prompts to quota — all in one transaction
// Returns Ok(prompts_added) or Err(reason)
fn db_redeem(pool: &DbPool, email: &str, code: &str) -> Result<u64, &'static str> {
    let mut conn = pool.get().expect("DB pool exhausted");

    // Fetch voucher
    let result: rusqlite::Result<(u64, Option<String>)> = conn.query_row(
        "SELECT prompts, used_by FROM vouchers WHERE code = ?1",
        [code],
        |row| Ok((row.get(0)?, row.get(1)?)),
    );

    match result {
        Err(_)           => Err("invalid code"),
        Ok((_, Some(_))) => Err("code already used"),
        Ok((prompts, None)) => {
            // Explicit transaction — both updates succeed or neither does
            let tx = conn.transaction().expect("redeem: begin failed");
            tx.execute(
                "UPDATE vouchers SET used_by=?1, used_at=datetime('now') WHERE code=?2",
                rusqlite::params![email, code],
            ).expect("redeem: mark voucher failed");
            tx.execute(
                "UPDATE usage SET quota=quota+?1 WHERE email=?2",
                rusqlite::params![prompts, email],
            ).expect("redeem: update quota failed");
            tx.commit().expect("redeem: commit failed");
            Ok(prompts)
        }
    }
}

// ── Voucher generation (CLI) ──────────────────────────────────────────────────

fn generate_vouchers(pool: &DbPool, count: usize, prompts: u64) {
    use std::collections::HashSet;
    let conn = pool.get().expect("DB pool exhausted");
    let mut generated: HashSet<String> = HashSet::new();

    println!("Generating {count} vouchers × {prompts} prompts each:\n");

    while generated.len() < count {
        let code = random_code();
        if generated.contains(&code) { continue; }

        conn.execute(
            "INSERT OR IGNORE INTO vouchers(code, prompts) VALUES(?1, ?2)",
            rusqlite::params![code, prompts],
        ).expect("insert voucher failed");

        println!("  {code}  ({prompts} prompts)");
        generated.insert(code);
    }

    println!("\nDone. Codes are live in the database.");
}

fn random_code() -> String {
    // Four groups of 4 uppercase alphanum chars: XXXX-XXXX-XXXX-XXXX
    let charset: Vec<char> = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789".chars().collect();
    let mut code = String::with_capacity(19);
    for group in 0..4 {
        if group > 0 { code.push('-'); }
        for _ in 0..4 {
            let idx = (pseudo_rand() as usize) % charset.len();
            code.push(charset[idx]);
        }
    }
    code
}

// Simple LCG — good enough for non-crypto voucher codes
fn pseudo_rand() -> u64 {
    use std::time::SystemTime;
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let c = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    (seed.wrapping_mul(6364136223846793005).wrapping_add(c)).wrapping_add(1442695040888963407)
}

// ── Rate limiting ─────────────────────────────────────────────────────────────

struct RateBucket {
    count:        u32,
    window_start: Instant,
}

// Returns true if the request is allowed, false if rate limit exceeded
fn rate_check(map: &DashMap<String, RateBucket>, email: &str, limit: u32) -> bool {
    let now = Instant::now();
    let window = Duration::from_secs(60);

    let mut bucket = map.entry(email.to_string()).or_insert(RateBucket {
        count:        0,
        window_start: now,
    });

    // Reset window if a full minute has passed
    if bucket.window_start.elapsed() >= window {
        bucket.count        = 0;
        bucket.window_start = now;
    }

    if bucket.count >= limit {
        return false; // exceeded
    }

    bucket.count += 1;
    true
}



#[derive(Debug, Deserialize, Clone)]
struct Jwk { kid: String, n: String, e: String }

#[derive(Clone)]
struct CachedCerts { keys: Vec<Jwk>, fetched_at: Instant }

const CERT_TTL: Duration = Duration::from_secs(300);
type CertsCache = Arc<RwLock<Option<CachedCerts>>>;

async fn get_certs(cache: &CertsCache, http: &HttpClient) -> Result<Vec<Jwk>, &'static str> {
    {
        let g = cache.read().await;
        if let Some(ref c) = *g {
            if c.fetched_at.elapsed() < CERT_TTL { return Ok(c.keys.clone()); }
        }
    }
    let mut g = cache.write().await;
    if let Some(ref c) = *g {
        if c.fetched_at.elapsed() < CERT_TTL { return Ok(c.keys.clone()); }
    }
    #[derive(Deserialize)] struct GoogleCerts { keys: Vec<Jwk> }
    let fetched: GoogleCerts = http
        .get("https://www.googleapis.com/oauth2/v3/certs")
        .send().await.map_err(|_| "google certs fetch failed")?
        .json().await.map_err(|_| "google certs parse failed")?;
    *g = Some(CachedCerts { keys: fetched.keys.clone(), fetched_at: Instant::now() });
    Ok(fetched.keys)
}

// ── JWT verification ──────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
struct GoogleClaims { email: String, aud: serde_json::Value, iss: String }

async fn verify_google_jwt(
    token: &str, client_id: &str, cache: &CertsCache, http: &HttpClient,
) -> Result<String, &'static str> {
    let kid = decode_header(token).map_err(|_| "bad token header")?.kid.ok_or("missing kid")?;
    let keys = get_certs(cache, http).await?;
    let jwk  = keys.iter().find(|k| k.kid == kid).ok_or("no matching signing key")?;
    let key  = DecodingKey::from_rsa_components(&jwk.n, &jwk.e).map_err(|_| "bad RSA key")?;
    let mut v = Validation::new(Algorithm::RS256);
    v.set_audience(&[client_id]);
    v.set_issuer(&["accounts.google.com", "https://accounts.google.com"]);
    decode::<GoogleClaims>(token, &key, &v)
        .map(|d| d.claims.email)
        .map_err(|_| "JWT verification failed")
}

// ── App state ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    config:       Config,
    db:           DbPool,
    http:         HttpClient,
    certs:        CertsCache,
    rate_buckets: Arc<DashMap<String, RateBucket>>,
}

// ── WebSocket handler ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct TokenQuery { token: Option<String> }

async fn ws_handler(
    ws:           WebSocketUpgrade,
    Query(q):     Query<TokenQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    if state.config.test_mode == Some(true) {
        println!("⚠  TEST MODE — using test@example.com");
        return ws.on_upgrade(move |socket| handle_socket(socket, "test@example.com".into(), state));
    }
    let token = match q.token {
        Some(t) => t,
        None    => return (StatusCode::UNAUTHORIZED, "missing token").into_response(),
    };
    match verify_google_jwt(&token, &state.config.google_client_id, &state.certs, &state.http).await {
        Ok(email) => {
            println!("✓ {email}");
            ws.on_upgrade(move |socket| handle_socket(socket, email, state))
        }
        Err(e) => {
            println!("✗ JWT: {e}");
            (StatusCode::UNAUTHORIZED, e).into_response()
        }
    }
}

async fn handle_socket(client_ws: WebSocket, email: String, state: AppState) {
    // Ensure user exists in DB with free quota
    db_ensure_user(&state.db, &email, state.config.free_limit);

    // Build upstream request
    let mut req = state.config.upstream_url.as_str()
        .into_client_request().expect("invalid upstream URL");
    if let Some(token) = &state.config.upstream_token {
        req.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
    }

    let (upstream, _) = match connect_async(req).await {
        Ok(c)  => c,
        Err(e) => {
            eprintln!("✗ upstream: {e}");
            let (mut tx, _) = client_ws.split();
            let _ = tx.send(Message::Text(
                serde_json::json!({"type":"system_error","content":"Service unavailable"})
                    .to_string().into()
            )).await;
            return;
        }
    };

    let (mut up_tx, mut up_rx) = upstream.split();
    let (cl_tx, mut cl_rx)     = client_ws.split();

    // mpsc: both upstream forwarder and softgate notices write here
    let (tx, mut rx) = mpsc::channel::<Message>(32);
    let tx2 = tx.clone();

    // Task A: upstream → channel
    tokio::spawn(async move {
        while let Some(Ok(msg)) = up_rx.next().await {
            let out = match msg {
                TungMessage::Text(t)   => Message::Text(t.into()),
                TungMessage::Binary(b) => Message::Binary(b.into()),
                TungMessage::Close(_)  => break,
                _                      => continue,
            };
            if tx2.send(out).await.is_err() { break; }
        }
    });

    // Task B: channel → client (sole owner of cl_tx)
    let mut cl_tx = cl_tx;
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if cl_tx.send(msg).await.is_err() { break; }
        }
    });

    // Main loop: client → gate → upstream
    while let Some(Ok(msg)) = cl_rx.next().await {
        let text = match msg {
            Message::Text(t)  => t.to_string(),
            Message::Close(_) => break,
            _                 => continue,
        };

        let payload: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v)  => v,
            Err(_) => continue,
        };

        match payload["type"].as_str() {

            // ── Prompt ────────────────────────────────────────────────────────
            Some("message") => {
                // Rate limit check (if configured)
                if let Some(limit) = state.config.rate_limit_per_minute {
                    if !rate_check(&state.rate_buckets, &email, limit) {
                        println!("🚦 {email} rate limited");
                        let _ = tx.send(Message::Text(serde_json::json!({
                            "type":              "rate_limited",
                            "content":           "Too many messages. Please wait a moment.",
                            "retry_after_secs":  60,
                        }).to_string().into())).await;
                        continue;
                    }
                }
                let (count, quota) = db_get_usage(&state.db, &email);

                if count >= quota {
                    println!("⛔ {email} limit ({count}/{quota})");
                    let _ = tx.send(Message::Text(serde_json::json!({
                        "type":        "system_limit",
                        "content":     format!("You've used all {quota} prompts."),
                        "payment_url": state.config.stripe_url,
                    }).to_string().into())).await;
                    continue;
                }

                if up_tx.send(TungMessage::Text(text.into())).await.is_err() { break; }
                db_increment(&state.db, &email);
                println!("→ {email} {}/{quota}", count + 1);
            }

            // ── Voucher redemption ────────────────────────────────────────────
            Some("redeem") => {
                let code = payload["code"].as_str().unwrap_or("").trim().to_uppercase();
                let reply = match db_redeem(&state.db, &email, &code) {
                    Ok(prompts) => {
                        println!("🎟  {email} redeemed {code} (+{prompts})");
                        let (count, quota) = db_get_usage(&state.db, &email);
                        serde_json::json!({
                            "type":            "redeem_ok",
                            "prompts_added":   prompts,
                            "prompts_used":    count,
                            "prompts_total":   quota,
                        })
                    }
                    Err(reason) => {
                        println!("✗ redeem {code} for {email}: {reason}");
                        serde_json::json!({
                            "type":    "redeem_error",
                            "content": reason,
                        })
                    }
                };
                let _ = tx.send(Message::Text(reply.to_string().into())).await;
            }

            // ── Everything else passes through ────────────────────────────────
            _ => {
                if up_tx.send(TungMessage::Text(text.into())).await.is_err() { break; }
            }
        }
    }

    println!("⊗ {email} disconnected");
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn print_usage() {
    println!("Usage:");
    println!("  auth-gate [config.toml]              — run the server");
    println!("  auth-gate [config.toml] --test        — run in test mode (no JWT)");
    println!("  auth-gate [config.toml] --generate N PROMPTS");
    println!("                                        — generate N voucher codes");
    println!("                                          worth PROMPTS each");
    println!();
    println!("Examples:");
    println!("  auth-gate /etc/auth-gate/config.toml --generate 10 50");
    println!("    → generates 10 codes, each adding 50 prompts");
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_usage();
        return;
    }

    let config_path = args.iter().skip(1)
        .find(|a| !a.starts_with("--"))
        .cloned()
        .unwrap_or_else(|| "/etc/auth-gate/config.toml".to_string());

    let mut config = Config::load(&config_path);
    let db = db_init(&config.db_path);

    // ── --generate N PROMPTS ──────────────────────────────────────────────────
    if let Some(pos) = args.iter().position(|a| a == "--generate") {
        let count = args.get(pos + 1)
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or_else(|| { eprintln!("--generate requires N PROMPTS"); std::process::exit(1); });
        let prompts = args.get(pos + 2)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(|| { eprintln!("--generate requires N PROMPTS"); std::process::exit(1); });
        generate_vouchers(&db, count, prompts);
        return;
    }

    // ── --test flag ───────────────────────────────────────────────────────────
    if args.iter().any(|a| a == "--test") {
        config.test_mode = Some(true);
    }

    let port = config.listen_port;

    println!("🚪 auth-gate on 127.0.0.1:{port}");
    println!("→  upstream : {}", config.upstream_url);
    println!("→  limit    : {} prompts", config.free_limit);
    println!("→  rate     : {}", config.rate_limit_per_minute
        .map(|r| format!("{r} prompts/min"))
        .unwrap_or_else(|| "unlimited".into()));
    println!("→  mode     : {}", if config.test_mode == Some(true) { "TEST (no JWT)" } else { "production" });

    let state = AppState {
        db,
        http:         HttpClient::new(),
        certs:        Arc::new(RwLock::new(None)),
        rate_buckets: Arc::new(DashMap::new()),
        config,
    };

    let app = Router::new()
        .route("/ws/{*path}", get(ws_handler))
        .with_state(state);

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await
        .unwrap_or_else(|e| panic!("Cannot bind port {port}: {e}"));

    axum::serve(listener, app).await.expect("server error");
}
