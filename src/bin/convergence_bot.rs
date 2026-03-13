//! Convergence Strategy Bot for Polymarket Binary Markets
//!
//! Exploits the lag between our Black-Scholes FV model (instant BTC reaction)
//! and the Polymarket order book midpoint (slower to update).
//!
//! When |FV - book_mid| > min_divergence, takes at the ask.
//! When divergence closes to take_profit, sells at the bid.
//!
//! Run:
//!   cargo run --release --bin convergence_bot                      # paper mode
//!   cargo run --release --bin convergence_bot -- --live            # live mode
//!   cargo run --release --bin convergence_bot -- --live --dry-run  # live book, no orders

#![allow(dead_code)]

use std::collections::VecDeque;
use std::convert::Infallible;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use axum::{
    extract::State as AxumState,
    response::{
        sse::{Event, KeepAlive, Sse},
        Html,
    },
    routing::get,
    Router,
};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tracing::{error, info, warn};

use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as _;

use polymarket_client_sdk::clob::types::{OrderType, Side, SignatureType};
use polymarket_client_sdk::clob::{self, Config as ClobConfig};
use polymarket_client_sdk::gamma;
use polymarket_client_sdk::gamma::types::request::EventsRequest;
use polymarket_client_sdk::rtds;
use polymarket_client_sdk::types::Address as SdkAddress;
use polymarket_client_sdk::POLYGON;

/// SDK builder-promoted auth state
type BuilderAuthState = polymarket_client_sdk::auth::state::Authenticated<
    polymarket_client_sdk::auth::builder::Builder,
>;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// ── Constants ──

const CLOB_HOST: &str = "https://clob.polymarket.com";
const CLOB_BOOK_URL: &str = "https://clob.polymarket.com/book";
const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@trade";

// ── Config ──

#[derive(Debug, Clone)]
struct ConvergenceConfig {
    // Strategy parameters (from backtest tuning)
    min_divergence: f64,     // 0.04 — minimum |FV - mid| to enter
    take_profit_div: f64,    // 0.01 — close when divergence < this (and in profit)
    stop_loss_cents: f64,    // 0.10 — sell when bid < avg - this (price-based stoploss)
    profit_take_cents: f64,  // 0.05 — sell when bid > avg + this (lock profit regardless of div)
    order_size: Decimal,     // 20 shares per trade
    max_position: Decimal,   // 80 shares max per side
    max_cost: f64,           // 0.60 — don't buy extreme FV (cap downside risk)
    min_remaining_secs: f64, // 30.0 — stop entering with < 30s left
    force_sell_secs: f64,    // 30.0 — force sell all positions with < 30s left

    // Execution
    tick_size: Decimal,         // 0.01
    poll_interval_ms: u64,      // 1000ms between book polls
    market_discovery_secs: u64, // 15s between market scans
    max_start_delay_secs: f64,  // 30.0 — skip period if started > this many secs ago
    entry_cooldown_secs: f64,   // 10.0 — minimum seconds between entries per side
    max_stoplosses: u32,        // 2 — stop trading after this many stoplosses per period
    min_sigma: f64,             // 0.000020 — floor for sigma to prevent FV hypersensitivity
    warmup_secs: u64,           // 30 — seconds to wait for vol calculation

    // Mode
    live: bool,    // false = paper (log only, no orders)
    dry_run: bool, // true = live book data but no order submission
}

impl Default for ConvergenceConfig {
    fn default() -> Self {
        Self {
            min_divergence: 0.06,
            take_profit_div: 0.01,
            stop_loss_cents: 0.10,
            profit_take_cents: 0.08,
            order_size: dec!(20),
            max_position: dec!(60),
            max_cost: 0.55,
            min_remaining_secs: 30.0,
            force_sell_secs: 30.0,
            tick_size: dec!(0.01),
            poll_interval_ms: 1000,
            market_discovery_secs: 15,
            max_start_delay_secs: 30.0,
            entry_cooldown_secs: 10.0,
            max_stoplosses: 1,
            min_sigma: 0.000020,
            warmup_secs: 30,
            live: false,
            dry_run: false,
        }
    }
}

// ── Market & Book Types ──

#[derive(Clone, Debug)]
struct Market {
    condition_id: String,
    token_id_yes: String,
    token_id_no: String,
    question: String,
    end_date: DateTime<Utc>,
    start_date: DateTime<Utc>,
    tick_size: Decimal,
    neg_risk: bool,
}

#[derive(Clone, Debug)]
struct BookSnapshot {
    best_bid: Decimal,
    best_ask: Decimal,
}

impl BookSnapshot {
    fn mid(&self) -> f64 {
        if self.best_bid > Decimal::ZERO && self.best_ask > Decimal::ZERO {
            ((self.best_bid + self.best_ask) / dec!(2))
                .to_string()
                .parse()
                .unwrap_or(0.0)
        } else {
            0.0
        }
    }
}

// ── Position Tracking ──

#[derive(Clone, Debug, Default)]
struct Position {
    up_qty: Decimal,
    up_cost: Decimal,
    down_qty: Decimal,
    down_cost: Decimal,
    // Track FV at entry for convergence vs reversal detection
    up_entry_fv: f64,
    down_entry_fv: f64,
}

impl Position {
    fn avg_up(&self) -> Decimal {
        if self.up_qty > Decimal::ZERO {
            self.up_cost / self.up_qty
        } else {
            Decimal::ZERO
        }
    }
    fn avg_down(&self) -> Decimal {
        if self.down_qty > Decimal::ZERO {
            self.down_cost / self.down_qty
        } else {
            Decimal::ZERO
        }
    }
}

// ── Shared State ──

struct SharedState {
    btc_price: RwLock<f64>,       // Binance price (fast signal for FV)
    chainlink_price: RwLock<f64>, // Chainlink price (resolution source for btc_open)
    btc_open: RwLock<Option<f64>>,
    sigma: RwLock<f64>,
    // Rolling price buffer for vol calculation (Binance — fastest updates)
    price_buffer: RwLock<Vec<(Instant, f64)>>,
}

impl SharedState {
    fn new() -> Self {
        Self {
            btc_price: RwLock::new(0.0),
            chainlink_price: RwLock::new(0.0),
            btc_open: RwLock::new(None),
            sigma: RwLock::new(0.0),
            price_buffer: RwLock::new(Vec::with_capacity(1024)),
        }
    }

    /// Update from Binance (fast signal for FV computation + sigma)
    fn update_price(&self, price: f64) {
        let now = Instant::now();
        *self.btc_price.write() = price;

        let mut buf = self.price_buffer.write();
        buf.push((now, price));

        // Keep last 5 minutes of data for vol calculation
        let cutoff = now - Duration::from_secs(300);
        buf.retain(|(t, _)| *t >= cutoff);

        // Compute realized vol (per-second sigma)
        if buf.len() >= 10 {
            let mut log_returns = Vec::new();
            for i in 1..buf.len() {
                let dt = buf[i].0.duration_since(buf[i - 1].0).as_secs_f64();
                if dt > 0.0 {
                    let lr = (buf[i].1 / buf[i - 1].1).ln();
                    log_returns.push((lr, dt));
                }
            }
            if log_returns.len() >= 5 {
                let total_dt: f64 = log_returns.iter().map(|(_, dt)| dt).sum();
                let variance: f64 = log_returns.iter().map(|(lr, _)| lr * lr).sum();
                let vol_per_sec = (variance / total_dt).sqrt();
                *self.sigma.write() = vol_per_sec;
            }
        }
    }

    /// Update from Chainlink RTDS (matches Polymarket resolution source)
    fn update_chainlink(&self, price: f64) {
        *self.chainlink_price.write() = price;
    }

    /// Get Chainlink price for btc_open (falls back to Binance if Chainlink not yet connected)
    fn chainlink_or_binance(&self) -> f64 {
        let cl = *self.chainlink_price.read();
        if cl > 0.0 {
            cl
        } else {
            *self.btc_price.read()
        }
    }
}

// ── FV Model (matches Rust orchestrator) ──

fn normal_cdf(x: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.2316419 * x.abs());
    let d = 0.3989422804014327_f64;
    let poly = t
        * (0.31938153 + t * (-0.35656378 + t * (1.78147794 + t * (-1.82125598 + t * 1.33027443))));
    let p = d * (-x * x / 2.0).exp() * poly;
    if x >= 0.0 {
        1.0 - p
    } else {
        p
    }
}

fn fair_value_up(btc_open: f64, btc_current: f64, sigma_per_sec: f64, remaining_secs: f64) -> f64 {
    if btc_open <= 0.0 || btc_current <= 0.0 || remaining_secs <= 0.0 {
        return 0.5;
    }
    let log_return = (btc_current / btc_open).ln();
    let remaining_vol = sigma_per_sec * remaining_secs.sqrt();
    if remaining_vol <= 1e-12 {
        return if log_return > 0.0 {
            0.95
        } else if log_return < 0.0 {
            0.05
        } else {
            0.5
        };
    }
    let z = log_return / remaining_vol;
    normal_cdf(z).clamp(0.02, 0.98)
}

// ── CLOB REST Book Fetch ──

#[derive(Deserialize)]
struct ClobBookResponse {
    bids: Vec<ClobLevel>,
    asks: Vec<ClobLevel>,
}

#[derive(Deserialize)]
struct ClobLevel {
    price: String,
    size: String,
}

fn fetch_book(token_id: &str) -> Option<BookSnapshot> {
    let url = format!("{CLOB_BOOK_URL}?token_id={token_id}");
    let resp = ureq::get(&url).call().ok()?;
    let body = resp.into_body().read_to_string().ok()?;
    let book: ClobBookResponse = serde_json::from_str(&body).ok()?;

    let best_bid = book
        .bids
        .iter()
        .filter_map(|l| l.price.parse::<Decimal>().ok())
        .max()
        .unwrap_or(Decimal::ZERO);

    let best_ask = book
        .asks
        .iter()
        .filter_map(|l| l.price.parse::<Decimal>().ok())
        .min()
        .unwrap_or(Decimal::ZERO);

    Some(BookSnapshot { best_bid, best_ask })
}

// ── Market Discovery ──

fn parse_duration_minutes(question: &str) -> Option<u32> {
    let q = question;
    for (i, _) in q.match_indices(':') {
        if i < 1 || i + 7 > q.len() {
            continue;
        }
        let after_colon = &q[i + 1..];
        if after_colon.len() < 5 {
            continue;
        }
        let start_mins: u32 = match after_colon[..2].parse() {
            Ok(m) => m,
            Err(_) => continue,
        };
        let ampm_end = if after_colon.len() > 2
            && (after_colon[2..].starts_with("AM") || after_colon[2..].starts_with("PM"))
        {
            2
        } else {
            continue;
        };

        let rest = &after_colon[ampm_end + 2..];
        if !rest.starts_with('-') {
            continue;
        }
        let rest = &rest[1..];
        let colon2 = rest.find(':')?;
        let end_mins_str = &rest[colon2 + 1..];
        if end_mins_str.len() < 4 {
            continue;
        }
        let end_mins: u32 = match end_mins_str[..2].parse() {
            Ok(m) => m,
            Err(_) => continue,
        };

        let hour_start: u32 = q[i - 1..i].parse().ok().or_else(|| {
            if i >= 2 {
                q[i - 2..i].parse().ok()
            } else {
                None
            }
        })?;
        let is_pm_start = after_colon[2..].starts_with("PM");
        let hour_end: u32 = rest[..colon2].parse().ok()?;
        let is_pm_end = end_mins_str[2..].starts_with("PM");

        let to_24h = |h: u32, m: u32, pm: bool| -> u32 {
            let h24 = if pm && h != 12 {
                h + 12
            } else if !pm && h == 12 {
                0
            } else {
                h
            };
            h24 * 60 + m
        };

        let s = to_24h(hour_start, start_mins, is_pm_start);
        let e = to_24h(hour_end, end_mins, is_pm_end);
        let diff = if e > s { e - s } else { e + 1440 - s };
        return Some(diff);
    }
    None
}

/// CLOB-based market discovery — scans the CLOB `/markets` paginated endpoint.
///
/// The Gamma API returns market stubs with empty condition_ids for BTC Up/Down markets.
/// The CLOB has the same markets fully provisioned. We scan from a high cursor offset
/// and parse the `market_slug` field (format: `btc-updown-{dur}-{start_timestamp_utc}`)
/// to extract start/end times and duration.
fn scan_clob_markets(start_cursor: &str, allowed_durations: &[u32]) -> (Vec<Market>, String) {
    let mut result = Vec::new();
    let mut cursor = start_cursor.to_string();
    let now = Utc::now();

    for _batch in 0..30 {
        let url = format!("{CLOB_HOST}/markets?limit=1000&next_cursor={cursor}");
        let resp = match ureq::get(&url).call() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[clob_scan] HTTP error: {e}");
                break;
            }
        };
        let body = match resp.into_body().read_to_string() {
            Ok(b) => b,
            Err(_) => break,
        };
        let parsed: serde_json::Value = match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(_) => break,
        };

        let data = match parsed["data"].as_array() {
            Some(d) if !d.is_empty() => d,
            _ => break,
        };

        let next = parsed["next_cursor"].as_str().unwrap_or("LTE=").to_string();

        for m in data {
            let question = m["question"].as_str().unwrap_or("");
            if !question.to_lowercase().contains("bitcoin up or down") {
                continue;
            }
            if m["accepting_orders"].as_bool() != Some(true) {
                continue;
            }

            // Parse market_slug: btc-updown-{dur}-{start_timestamp}
            let slug = m["market_slug"].as_str().unwrap_or("");
            let parts: Vec<&str> = slug.split('-').collect();
            // slug format: "btc-updown-5m-1772390400"
            // parts: ["btc", "updown", "5m", "1772390400"]
            if parts.len() < 4 {
                continue;
            }

            let dur_mins = match parts[2] {
                "5m" => 5u32,
                "15m" => 15,
                "1h" => 60,
                "4h" => 240,
                _ => continue,
            };
            if !allowed_durations.contains(&dur_mins) {
                continue;
            }

            let start_ts: i64 = match parts[3].parse() {
                Ok(t) => t,
                Err(_) => continue,
            };

            let start_date = match DateTime::from_timestamp(start_ts, 0) {
                Some(d) => d,
                None => continue,
            };
            let end_date = start_date + chrono::Duration::minutes(dur_mins as i64);

            // Skip markets that ended more than 5 min ago
            if end_date < now - chrono::Duration::minutes(5) {
                continue;
            }

            let cid = m["condition_id"].as_str().unwrap_or("");
            if cid.is_empty() {
                continue;
            }

            // Parse tokens — outcomes are "Up" and "Down"
            let tokens = match m["tokens"].as_array() {
                Some(t) if t.len() >= 2 => t,
                _ => continue,
            };

            let mut up_id = String::new();
            let mut down_id = String::new();
            for tok in tokens {
                let outcome = tok["outcome"].as_str().unwrap_or("");
                let tid = tok["token_id"].as_str().unwrap_or("");
                match outcome {
                    "Up" => up_id = tid.to_string(),
                    "Down" => down_id = tid.to_string(),
                    _ => {}
                }
            }
            if up_id.is_empty() || down_id.is_empty() {
                continue;
            }

            let neg_risk = m["neg_risk"].as_bool().unwrap_or(false);

            result.push(Market {
                condition_id: cid.to_string(),
                token_id_yes: up_id,
                token_id_no: down_id,
                question: question.to_string(),
                end_date,
                start_date,
                tick_size: dec!(0.01),
                neg_risk,
            });
        }

        if next == "LTE=" || data.len() < 1000 {
            cursor = next;
            break;
        }
        cursor = next;
    }

    (result, cursor)
}

/// Estimate a starting CLOB cursor for scanning recent markets.
/// Uses March 1 2026 as baseline (~520k markets), growing ~1500/day.
fn estimate_clob_start_cursor() -> String {
    use chrono::TimeZone;
    let baseline = 520_000u64;
    let baseline_date = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    let days_since = (Utc::now() - baseline_date).num_days().max(0) as u64;
    let estimated_total = baseline + days_since * 1500;
    // Start 25k before estimated total to ensure we don't miss anything
    let start = estimated_total.saturating_sub(25_000);
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(start.to_string())
}

/// Gamma API fallback — original discovery logic.
/// NOTE: Gamma often returns market stubs with empty condition_ids,
/// making this unreliable for BTC Up/Down 5-min markets.
async fn discover_btc_markets_gamma(allowed_durations: &[u32]) -> Result<Vec<Market>> {
    let gamma_client = gamma::Client::default();
    let now = Utc::now();
    let req = EventsRequest::builder()
        .limit(200)
        .active(true)
        .closed(false)
        .end_date_min(now)
        .tag_slug("bitcoin".to_string())
        .build();

    let events = gamma_client
        .events(&req)
        .await
        .context("Gamma API query failed")?;

    let mut results = Vec::new();
    let prefix = "bitcoin up or down";

    for event in &events {
        let markets = match &event.markets {
            Some(m) => m,
            None => continue,
        };
        let neg_risk = event.neg_risk.unwrap_or(false);

        for market in markets {
            let question = match &market.question {
                Some(q) => q.clone(),
                None => continue,
            };
            if !question.to_lowercase().contains(prefix) {
                continue;
            }
            if market.active != Some(true) {
                continue;
            }
            let end_date = match market.end_date {
                Some(d) if d > now => d,
                _ => continue,
            };
            let duration_mins = match parse_duration_minutes(&question) {
                Some(d) if allowed_durations.contains(&d) => d,
                _ => continue,
            };
            let start_date = end_date - chrono::Duration::minutes(duration_mins as i64);
            let condition_id = match market.condition_id {
                Some(cid) => format!("{cid}"),
                None => continue,
            };
            let token_ids = match &market.clob_token_ids {
                Some(ids) if ids.len() >= 2 => ids.clone(),
                _ => continue,
            };
            let tick_size = market.order_price_min_tick_size.unwrap_or(dec!(0.01));

            results.push(Market {
                condition_id,
                token_id_yes: token_ids[0].to_string(),
                token_id_no: token_ids[1].to_string(),
                question,
                end_date,
                start_date,
                tick_size,
                neg_risk,
            });
        }
    }

    Ok(results)
}

// ── Chainlink RTDS Feed (for btc_open — matches Polymarket resolution source) ──

async fn chainlink_feed(state: Arc<SharedState>, shutdown: Arc<Notify>) {
    use futures_util::StreamExt;

    let mut backoff = Duration::from_secs(2);
    let max_backoff = Duration::from_secs(30);

    loop {
        info!("[chainlink] Subscribing to RTDS btc/usd oracle...");

        let client = rtds::Client::default();
        let stream = match client.subscribe_chainlink_prices(Some("btc/usd".to_owned())) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "[chainlink] Subscribe failed: {e}, retrying in {}s",
                    backoff.as_secs()
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };
        info!("[chainlink] Connected");
        backoff = Duration::from_secs(2);

        tokio::pin!(stream);

        loop {
            tokio::select! {
                item = stream.next() => {
                    match item {
                        Some(Ok(price)) => {
                            let p: f64 = price.value.to_string().parse().unwrap_or(0.0);
                            if p > 0.0 {
                                state.update_chainlink(p);
                            }
                        }
                        Some(Err(e)) => {
                            warn!("[chainlink] Stream error: {e}");
                            break;
                        }
                        None => {
                            warn!("[chainlink] Stream ended");
                            break;
                        }
                    }
                }
                _ = shutdown.notified() => {
                    info!("[chainlink] Shutting down");
                    return;
                }
            }
        }

        warn!(
            "[chainlink] Disconnected, reconnecting in {}s",
            backoff.as_secs()
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

// ── Binance WebSocket Feed (fallback) ──

async fn binance_feed(state: Arc<SharedState>, shutdown: Arc<Notify>) {
    use futures_util::StreamExt;
    use tokio_tungstenite::connect_async;

    loop {
        info!("[binance] Connecting...");
        let ws_result = connect_async(BINANCE_WS_URL).await;
        let (ws_stream, _) = match ws_result {
            Ok(v) => v,
            Err(e) => {
                warn!("[binance] Connection failed: {e}, retrying in 2s");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        info!("[binance] Connected");

        let (_, mut read) = ws_stream.split();

        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                            // Parse: {"p":"98234.50", ...}
                            if let Some(price) = parse_binance_price(&text) {
                                state.update_price(price);
                            }
                        }
                        Some(Ok(_)) => {} // ping/pong/binary
                        Some(Err(e)) => {
                            warn!("[binance] WS error: {e}");
                            break;
                        }
                        None => {
                            warn!("[binance] Stream ended");
                            break;
                        }
                    }
                }
                _ = shutdown.notified() => {
                    info!("[binance] Shutting down");
                    return;
                }
            }
        }

        warn!("[binance] Disconnected, reconnecting in 1s");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn parse_binance_price(text: &str) -> Option<f64> {
    // Fast path: find "p":"..." in the JSON
    let idx = text.find("\"p\":\"")?;
    let start = idx + 5;
    let end = text[start..].find('"')? + start;
    text[start..end].parse().ok()
}

// ── Order Execution ──

struct Executor {
    clob: clob::Client<BuilderAuthState>,
    signer: PrivateKeySigner,
    dry_run: bool,
}

impl Executor {
    async fn place_fok(
        &self,
        token_id: &str,
        side: Side,
        price: Decimal,
        size: Decimal,
        tick_size: Decimal,
        label: &str,
    ) -> Result<String> {
        if self.dry_run {
            info!("[DRY-RUN] Would {label} FOK: token={token_id} price={price} size={size}");
            return Ok("dry-run".to_string());
        }

        let token = U256::from_str(token_id).context("Invalid token_id")?;

        let rounded_price = round_to_tick(price, tick_size);

        // Build → sign → post (explicit types to help inference)
        let signable = self
            .clob
            .limit_order()
            .token_id(token)
            .side(side)
            .price(rounded_price)
            .size(size)
            .order_type(OrderType::FOK)
            .build()
            .await
            .context(format!("FOK {label} build failed"))?;

        let signed = self
            .clob
            .sign(&self.signer, signable)
            .await
            .context(format!("FOK {label} sign failed"))?;

        let resp = tokio::time::timeout(Duration::from_secs(5), self.clob.post_order(signed))
            .await
            .context(format!("FOK {label} timed out"))?
            .context(format!("FOK {label} post failed"))?;

        if !resp.success {
            let msg = resp.error_msg.as_deref().unwrap_or("unknown");
            bail!("FOK {label} rejected: {msg}");
        }
        Ok(resp.order_id)
    }

    async fn buy_fok(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
        tick_size: Decimal,
    ) -> Result<String> {
        self.place_fok(token_id, Side::Buy, price, size, tick_size, "buy")
            .await
    }

    async fn sell_fok(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
        tick_size: Decimal,
    ) -> Result<String> {
        self.place_fok(token_id, Side::Sell, price, size, tick_size, "sell")
            .await
    }

    /// Verify CLOB order matched, then force CLOB to refresh its cached balance
    /// and poll until the CLOB recognizes our token holdings. This is the correct
    /// approach because the CLOB's internal ledger (not on-chain state) determines
    /// whether a sell order will be accepted.
    async fn wait_for_fill(&self, order_id: &str, token_id: &str, timeout_secs: u64) -> bool {
        use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
        use polymarket_client_sdk::clob::types::{AssetType, OrderStatusType};

        let deadline = Instant::now() + Duration::from_secs(timeout_secs);

        // Step 1: Verify CLOB shows Matched
        match self.clob.order(order_id).await {
            Ok(order) => {
                let status = &order.status;
                let matched = order.size_matched;
                let original = order.original_size;
                info!("[FILL CHECK] CLOB status={status:?}, matched={matched}/{original}");
                match status {
                    OrderStatusType::Matched => {}
                    OrderStatusType::Canceled | OrderStatusType::Unmatched => {
                        warn!("[FILL CHECK] Order NOT matched — buy failed");
                        return false;
                    }
                    _ => {
                        warn!("[FILL CHECK] CLOB status not Matched ({status:?})");
                        return false;
                    }
                }
            }
            Err(e) => {
                warn!("[FILL CHECK] CLOB query failed: {e:#}");
                return false;
            }
        }

        let token = U256::from_str(token_id).unwrap();

        // Step 2: Force CLOB to refresh its cached balance from on-chain
        // This is critical — the CLOB's internal ledger may not auto-update after
        // a FOK buy settles on-chain. We must explicitly trigger a refresh.
        info!("[FILL CHECK] CLOB matched — forcing balance refresh...");
        let mut attempt = 0u32;

        while Instant::now() < deadline {
            attempt += 1;

            // Force the CLOB to re-read on-chain balances
            let update_req = BalanceAllowanceRequest::builder()
                .asset_type(AssetType::Conditional)
                .token_id(token)
                .build();
            if let Err(e) = self.clob.update_balance_allowance(update_req).await {
                warn!("[FILL CHECK] Balance update request failed: {e:#} (attempt {attempt})");
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }

            // Small delay to let the CLOB process the refresh
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Query the CLOB's cached balance
            let query_req = BalanceAllowanceRequest::builder()
                .asset_type(AssetType::Conditional)
                .token_id(token)
                .build();
            match self.clob.balance_allowance(query_req).await {
                Ok(resp) => {
                    let bal = resp.balance;
                    if bal > Decimal::ZERO {
                        info!(
                            "[FILL CHECK] CLOB balance={bal} — ready to sell! (attempt {attempt})"
                        );
                        return true;
                    }
                    if attempt % 3 == 0 {
                        info!("[FILL CHECK] CLOB balance={bal}, waiting... (attempt {attempt})");
                    }
                }
                Err(e) => {
                    warn!("[FILL CHECK] Balance query failed: {e:#} (attempt {attempt})");
                }
            }

            tokio::time::sleep(Duration::from_secs(3)).await;
        }

        warn!("[FILL CHECK] Timeout after {timeout_secs}s — CLOB never recognized balance");
        false
    }
}

fn round_to_tick(price: Decimal, tick_size: Decimal) -> Decimal {
    if tick_size <= Decimal::ZERO {
        return price;
    }
    (price / tick_size).floor() * tick_size
}

// ── Period Stats ──

#[derive(Default)]
struct PeriodStats {
    entries: u32,
    exits: u32,
    buys_up: Decimal,
    buys_down: Decimal,
    sells_up: Decimal,
    sells_down: Decimal,
    sell_pnl: f64,
    stoplosses: u32,                   // count of stoploss exits
    last_stoploss_at: Option<Instant>, // prevent re-entry after stoploss
}

// ── Dashboard State ──

#[derive(Clone, Serialize)]
struct TradeFeedEntry {
    time: String,
    action: String, // BUY, PROFIT, STOPLOSS, CONVERGE, FORCE, SETTLE
    side: String,   // UP, DN, or empty
    qty: f64,
    price: f64,
    pnl: f64,
}

#[derive(Clone, Serialize)]
struct ConvergenceDashboard {
    // Header
    btc_price: f64,
    sigma: f64,
    mode: String, // "LIVE", "PAPER", "DRY-RUN"
    session_pnl: f64,
    period_count: u32,
    wins: u32,
    losses: u32,
    win_rate: f64,

    // Current period
    period_name: String,
    remaining_secs: f64,
    btc_open: f64,
    fv_up: f64,
    fv_down: f64,
    mid_up: f64,
    mid_down: f64,
    div_up: f64,
    div_down: f64,
    ask_up: f64,
    ask_down: f64,
    bid_up: f64,
    bid_down: f64,

    // Position
    position_side: String, // "UP", "DN", or "FLAT"
    position_qty: f64,
    position_avg_cost: f64,
    current_bid: f64,
    unrealized_pnl: f64,
    fill_confirmed: bool,

    // Period stats
    period_entries: u32,
    period_exits: u32,
    period_stoplosses: u32,
    period_sell_pnl: f64,

    // Totals
    total_entries: u32,
    total_exits: u32,
    total_stoplosses: u32,

    // Trade feed (last 50)
    trades: VecDeque<TradeFeedEntry>,

    // Redeem status
    last_redeem_at: String,
    last_redeem_result: String,
}

impl Default for ConvergenceDashboard {
    fn default() -> Self {
        Self {
            btc_price: 0.0,
            sigma: 0.0,
            mode: "PAPER".to_string(),
            session_pnl: 0.0,
            period_count: 0,
            wins: 0,
            losses: 0,
            win_rate: 0.0,
            period_name: String::new(),
            remaining_secs: 0.0,
            btc_open: 0.0,
            fv_up: 0.0,
            fv_down: 0.0,
            mid_up: 0.0,
            mid_down: 0.0,
            div_up: 0.0,
            div_down: 0.0,
            ask_up: 0.0,
            ask_down: 0.0,
            bid_up: 0.0,
            bid_down: 0.0,
            position_side: "FLAT".to_string(),
            position_qty: 0.0,
            position_avg_cost: 0.0,
            current_bid: 0.0,
            unrealized_pnl: 0.0,
            fill_confirmed: false,
            period_entries: 0,
            period_exits: 0,
            period_stoplosses: 0,
            period_sell_pnl: 0.0,
            total_entries: 0,
            total_exits: 0,
            total_stoplosses: 0,
            trades: VecDeque::new(),
            last_redeem_at: String::new(),
            last_redeem_result: String::new(),
        }
    }
}

impl ConvergenceDashboard {
    fn push_trade(&mut self, entry: TradeFeedEntry) {
        self.trades.push_front(entry);
        while self.trades.len() > 50 {
            self.trades.pop_back();
        }
    }
}

type SharedConvergenceDashboard = Arc<RwLock<ConvergenceDashboard>>;

// ── Web Server ──

const CONVERGENCE_HTML: &str = include_str!("convergence_dashboard.html");

#[derive(Clone)]
struct ConvergenceWebState {
    dashboard: SharedConvergenceDashboard,
    shutdown: Arc<AtomicBool>,
}

async fn start_convergence_web(dashboard: SharedConvergenceDashboard, port: u16) {
    let shutdown = Arc::new(AtomicBool::new(false));
    let web_state = ConvergenceWebState {
        dashboard,
        shutdown,
    };

    let app = Router::new()
        .route("/", get(conv_index))
        .route("/api/stream", get(conv_sse))
        .with_state(web_state);

    let addr = format!("0.0.0.0:{port}");
    info!("[web] Convergence dashboard at http://localhost:{port}");
    eprintln!("\n  Dashboard: http://localhost:{port}\n");

    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("[web] Failed to bind on {addr}: {e} — dashboard disabled");
            return;
        }
    };
    if let Err(e) = axum::serve(listener, app).await {
        error!("[web] Server error: {e}");
    }
}

async fn conv_index() -> Html<&'static str> {
    Html(CONVERGENCE_HTML)
}

async fn conv_sse(
    AxumState(state): AxumState<ConvergenceWebState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    use tokio_stream::StreamExt;
    let stream = tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(
        Duration::from_millis(500),
    ))
    .map(move |_| {
        let snap = state.dashboard.read().clone();
        let json = serde_json::to_string(&snap).unwrap_or_default();
        Ok(Event::default().data(json))
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

// ── Auto-Redemption ──

struct RedeemContext {
    signer: PrivateKeySigner,
    wallet_address: String,
    rpc_url: String,
}

async fn redeem_sweep(ctx: &RedeemContext, dashboard: &SharedConvergenceDashboard) {
    use alloy::primitives::{Address, B256};
    use alloy::providers::ProviderBuilder;
    use polymarket_client_sdk::ctf;
    use polymarket_client_sdk::ctf::types::RedeemPositionsRequest;
    use polymarket_client_sdk::data;
    use polymarket_client_sdk::data::types::request::PositionsRequest;
    use std::collections::HashMap;

    const USDC_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

    let wallet_addr = match Address::from_str(&ctx.wallet_address) {
        Ok(a) => a,
        Err(e) => {
            warn!("[redeem] Invalid wallet address: {e}");
            return;
        }
    };

    let data_client = data::Client::default();
    let req = PositionsRequest::builder()
        .user(wallet_addr)
        .redeemable(true)
        .size_threshold(Decimal::new(1, 2))
        .limit(500)
        .expect("invalid limit")
        .build();

    let positions = match data_client.positions(&req).await {
        Ok(p) => p,
        Err(e) => {
            warn!("[redeem] Query failed: {e}");
            dashboard.write().last_redeem_result = format!("Query failed: {e}");
            return;
        }
    };

    if positions.is_empty() {
        info!("[redeem] No redeemable positions");
        dashboard.write().last_redeem_result = "None found".to_string();
        dashboard.write().last_redeem_at = Utc::now().format("%H:%M:%S").to_string();
        return;
    }

    // Group by condition_id
    let mut by_cond: HashMap<B256, (String, bool)> = HashMap::new();
    for pos in &positions {
        if pos.size <= Decimal::ZERO {
            continue;
        }
        by_cond
            .entry(pos.condition_id)
            .or_insert_with(|| (pos.title.clone(), pos.negative_risk));
    }

    info!("[redeem] Found {} redeemable markets", by_cond.len());

    let usdc_addr = Address::from_str(USDC_ADDRESS).unwrap();
    let mut ok = 0u32;
    let mut fail = 0u32;

    for (condition_id, (title, neg_risk)) in &by_cond {
        let url = match ctx.rpc_url.parse() {
            Ok(u) => u,
            Err(_) => {
                fail += 1;
                continue;
            }
        };
        let provider = ProviderBuilder::new()
            .wallet(ctx.signer.clone())
            .connect_http(url);

        let ctf_client = if *neg_risk {
            ctf::Client::with_neg_risk(provider, POLYGON)
        } else {
            ctf::Client::new(provider, POLYGON)
        };
        let ctf_client = match ctf_client {
            Ok(c) => c,
            Err(e) => {
                warn!("[redeem] CTF client failed for {title}: {e}");
                fail += 1;
                continue;
            }
        };

        let req = RedeemPositionsRequest::for_binary_market(usdc_addr, *condition_id);
        match ctf_client.redeem_positions(&req).await {
            Ok(resp) => {
                info!("[redeem] OK {title} tx={:?}", resp.transaction_hash);
                ok += 1;
            }
            Err(e) => {
                warn!("[redeem] FAILED {title}: {e}");
                fail += 1;
            }
        }
    }

    let result = format!("{ok} redeemed, {fail} failed");
    info!("[redeem] {result}");
    let mut d = dashboard.write();
    d.last_redeem_at = Utc::now().format("%H:%M:%S").to_string();
    d.last_redeem_result = result;
}

// ── CSV Trade Logger ──

struct TradeLogger {
    writer: std::io::BufWriter<std::fs::File>,
}

impl TradeLogger {
    fn new(path: &str) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let is_empty = file.metadata()?.len() == 0;
        let mut writer = std::io::BufWriter::new(file);
        if is_empty {
            use std::io::Write;
            writeln!(writer, "timestamp,period,action,side,qty,price,avg_cost,fv,mid,div,pnl,remaining_secs,sigma,btc")?;
        }
        Ok(Self { writer })
    }

    fn log(
        &mut self,
        period: &str,
        action: &str,
        side: &str,
        qty: Decimal,
        price: Decimal,
        avg_cost: Decimal,
        fv: f64,
        mid: f64,
        div: f64,
        pnl: f64,
        remaining_secs: f64,
        sigma: f64,
        btc: f64,
    ) {
        use std::io::Write;
        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let _ = writeln!(
            self.writer,
            "{},{},{},{},{},{},{},{:.4},{:.4},{:.4},{:.2},{:.0},{:.8},{:.2}",
            ts,
            period,
            action,
            side,
            qty,
            price,
            avg_cost,
            fv,
            mid,
            div,
            pnl,
            remaining_secs,
            sigma,
            btc
        );
        let _ = self.writer.flush();
    }

    fn log_settlement(
        &mut self,
        period: &str,
        sell_pnl: f64,
        settle_pnl: f64,
        entries: u32,
        exits: u32,
        stoplosses: u32,
    ) {
        use std::io::Write;
        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let _ = writeln!(
            self.writer,
            "{},{},SETTLE,,,,,,,,{:.2},,,",
            ts,
            period,
            sell_pnl + settle_pnl
        );
        let _ = writeln!(
            self.writer,
            "# entries={} exits={} stoplosses={} sell_pnl={:.2} settle_pnl={:.2}",
            entries, exits, stoplosses, sell_pnl, settle_pnl
        );
        let _ = self.writer.flush();
    }
}

// ── Main Event Loop ──

async fn run_convergence(
    config: ConvergenceConfig,
    executor: Option<Executor>,
    state: Arc<SharedState>,
    dashboard: SharedConvergenceDashboard,
    redeem_ctx: Option<Arc<RedeemContext>>,
) -> Result<()> {
    let allowed_durations: Vec<u32> = vec![5]; // 5-min only — 15-min has too much reversal risk
    let poll_interval = Duration::from_millis(config.poll_interval_ms);

    let mut current_market: Option<Market> = None;
    let mut position = Position::default();
    let mut stats = PeriodStats::default();
    let mut last_discovery = Instant::now() - Duration::from_secs(60);
    let mut period_count = 0u32;
    let mut total_pnl = 0.0_f64;
    let mut period_name = String::new();
    let mut fill_confirmed = false; // true once buy FOK is confirmed matched + CLOB balance verified
    let mut wins = 0u32;
    let mut losses = 0u32;
    let mut last_redeem = Instant::now() - Duration::from_secs(300); // allow immediate first sweep

    // CLOB market cache — scanned at startup, refreshed periodically
    let clob_start_cursor = estimate_clob_start_cursor();
    info!("[discovery] Initial CLOB scan starting...");

    let mut clob_market_cache: Vec<Market>;
    {
        let cursor_clone = clob_start_cursor.clone();
        let dur_clone = allowed_durations.clone();
        let (initial_markets, _end_cursor) =
            tokio::task::spawn_blocking(move || scan_clob_markets(&cursor_clone, &dur_clone))
                .await
                .context("Initial CLOB scan failed")?;
        let now_markets: Vec<_> = initial_markets
            .iter()
            .filter(|m| m.end_date > Utc::now())
            .collect();
        info!(
            "[discovery] Initial CLOB scan: {} markets found ({} upcoming/active)",
            initial_markets.len(),
            now_markets.len()
        );
        clob_market_cache = initial_markets;
    }
    let mut last_clob_refresh = Instant::now();

    // Set dashboard mode
    {
        let mut d = dashboard.write();
        d.mode = if config.live && !config.dry_run {
            "LIVE".to_string()
        } else if config.live && config.dry_run {
            "DRY-RUN".to_string()
        } else {
            "PAPER".to_string()
        };
    }

    // CSV trade log
    let log_path = format!(
        "convergence_trades_{}.csv",
        Utc::now().format("%Y%m%d_%H%M%S")
    );
    let mut trade_log = TradeLogger::new(&log_path)?;
    info!("Trade log: {}", log_path);

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║          CONVERGENCE BOT — FV-Midpoint Strategy            ║");
    println!(
        "║  div={:.2} profit={}c stop={}c pos={} cost<={:.2}     ║",
        config.min_divergence,
        (config.profit_take_cents * 100.0) as u32,
        (config.stop_loss_cents * 100.0) as u32,
        config.max_position,
        config.max_cost
    );
    println!(
        "║  mode: {}{}                                          ║",
        if config.live { "LIVE" } else { "PAPER" },
        if config.dry_run { " (dry-run)" } else { "" }
    );
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    loop {
        let now = Utc::now();

        // ── Market Discovery ──
        if last_discovery.elapsed() > Duration::from_secs(config.market_discovery_secs) {
            last_discovery = Instant::now();

            // Check if current market expired
            if let Some(ref m) = current_market {
                if now >= m.end_date {
                    // Period ended — settle
                    let settle_pnl = settle_position(&position, &stats);
                    let period_pnl = settle_pnl + stats.sell_pnl;
                    total_pnl += period_pnl;
                    period_count += 1;
                    if period_pnl > 0.0 {
                        wins += 1;
                    } else if period_pnl < 0.0 {
                        losses += 1;
                    }

                    println!("  ╰─ SETTLED: sell_pnl=${:.2} settle_pnl=${:.2} total=${:.2} stoplosses={}",
                        stats.sell_pnl, settle_pnl, period_pnl, stats.stoplosses);
                    println!(
                        "  ╰─ Entries={} Exits={} | Running total: {} periods, avg=${:.2}\n",
                        stats.entries,
                        stats.exits,
                        period_count,
                        if period_count > 0 {
                            total_pnl / period_count as f64
                        } else {
                            0.0
                        }
                    );

                    trade_log.log_settlement(
                        &period_name,
                        stats.sell_pnl,
                        settle_pnl,
                        stats.entries,
                        stats.exits,
                        stats.stoplosses,
                    );

                    // Dashboard: push settle trade + update totals
                    {
                        let mut d = dashboard.write();
                        d.push_trade(TradeFeedEntry {
                            time: Utc::now().format("%H:%M:%S").to_string(),
                            action: "SETTLE".to_string(),
                            side: String::new(),
                            qty: 0.0,
                            price: 0.0,
                            pnl: period_pnl,
                        });
                        d.total_entries += stats.entries;
                        d.total_exits += stats.exits;
                        d.total_stoplosses += stats.stoplosses;
                        d.session_pnl = total_pnl;
                        d.period_count = period_count;
                        d.wins = wins;
                        d.losses = losses;
                        d.win_rate = if period_count > 0 {
                            wins as f64 / period_count as f64 * 100.0
                        } else {
                            0.0
                        };
                        // Reset period fields
                        d.period_name.clear();
                        d.remaining_secs = 0.0;
                        d.position_side = "FLAT".to_string();
                        d.position_qty = 0.0;
                        d.position_avg_cost = 0.0;
                        d.current_bid = 0.0;
                        d.unrealized_pnl = 0.0;
                        d.fill_confirmed = false;
                        d.period_entries = 0;
                        d.period_exits = 0;
                        d.period_stoplosses = 0;
                        d.period_sell_pnl = 0.0;
                    }

                    // Trigger redemption sweep after settlement (10s delay)
                    if let Some(ref ctx) = redeem_ctx {
                        let ctx = Arc::clone(ctx);
                        let dash = Arc::clone(&dashboard);
                        last_redeem = Instant::now();
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_secs(10)).await;
                            redeem_sweep(&ctx, &dash).await;
                        });
                    }

                    current_market = None;
                    position = Position::default();
                    stats = PeriodStats::default();
                    fill_confirmed = false;
                }
            }

            // Discover new market if none active
            if current_market.is_none() {
                // Periodically refresh CLOB cache (every 60s)
                if last_clob_refresh.elapsed() > Duration::from_secs(60) {
                    last_clob_refresh = Instant::now();
                    let cursor_clone = clob_start_cursor.clone();
                    let dur_clone = allowed_durations.clone();
                    match tokio::task::spawn_blocking(move || {
                        scan_clob_markets(&cursor_clone, &dur_clone)
                    })
                    .await
                    {
                        Ok((new_markets, _)) if !new_markets.is_empty() => {
                            info!(
                                "[discovery] CLOB cache refreshed: {} markets",
                                new_markets.len()
                            );
                            clob_market_cache = new_markets;
                        }
                        Ok(_) => {} // Keep existing cache if scan returns empty
                        Err(e) => warn!("[discovery] CLOB refresh failed: {e}"),
                    }
                }

                // Use cached CLOB markets (primary) or Gamma fallback
                let markets = if !clob_market_cache.is_empty() {
                    // Filter to still-relevant markets
                    clob_market_cache
                        .iter()
                        .filter(|m| m.end_date > now - chrono::Duration::minutes(1))
                        .cloned()
                        .collect::<Vec<_>>()
                } else {
                    // Fallback to Gamma if CLOB cache is empty
                    match discover_btc_markets_gamma(&allowed_durations).await {
                        Ok(m) => m,
                        Err(e) => {
                            warn!("Gamma fallback also failed: {e}");
                            Vec::new()
                        }
                    }
                };

                if markets.is_empty() {
                    info!("No BTC Up/Down markets found, waiting...");
                } else {
                    let active: Vec<_> = markets
                        .iter()
                        .filter(|m| m.end_date > now && m.start_date <= now)
                        .collect();
                    info!(
                        "Found {} active / {} total BTC markets",
                        active.len(),
                        markets.len()
                    );
                }
                // Pick a period that JUST started (within max_start_delay)
                // OR the next upcoming period to wait for
                let recently_started: Vec<_> = markets
                    .iter()
                    .filter(|m| {
                        let started = m.start_date <= now;
                        let age_secs = (now - m.start_date).num_seconds() as f64;
                        started && m.end_date > now && age_secs <= config.max_start_delay_secs
                    })
                    .collect();

                if let Some(best) = recently_started.first() {
                    println!("╭─ NEW PERIOD: {}", best.question);
                    let remaining = (best.end_date - now).num_seconds();
                    let age = (now - best.start_date).num_seconds();
                    println!(
                        "│  ends in {}s | started {}s ago | cond={}",
                        remaining,
                        age,
                        &best.condition_id[..best.condition_id.len().min(12)]
                    );

                    // Set btc_open from Chainlink (matches Polymarket resolution source)
                    // Falls back to Binance if Chainlink not yet connected
                    let btc = state.chainlink_or_binance();
                    let btc_src = if *state.chainlink_price.read() > 0.0 {
                        "chainlink"
                    } else {
                        "binance"
                    };
                    if btc > 0.0 {
                        *state.btc_open.write() = Some(btc);
                        println!("│  BTC open: ${:.2} ({})", btc, btc_src);
                    }

                    period_name = best.question.clone();
                    current_market = Some((*best).clone());

                    // Dashboard: new period
                    {
                        let mut d = dashboard.write();
                        d.period_name = period_name.clone();
                        d.btc_open = btc;
                    }
                } else {
                    // Show next upcoming period
                    let upcoming: Vec<_> = markets.iter().filter(|m| m.start_date > now).collect();
                    if let Some(next) = upcoming.iter().min_by_key(|m| m.start_date) {
                        let wait_secs = (next.start_date - now).num_seconds();
                        let in_progress: Vec<_> = markets
                            .iter()
                            .filter(|m| m.start_date <= now && m.end_date > now)
                            .collect();
                        if !in_progress.is_empty() {
                            info!("Skipping {} in-progress period(s) (stale btc_open), next starts in {}s",
                                in_progress.len(), wait_secs);
                        } else {
                            info!("No active period, next starts in {}s", wait_secs);
                        }
                    }
                }
            }
        }

        // ── Periodic redemption sweep (every 5 min when idle) ──
        if last_redeem.elapsed() > Duration::from_secs(300) {
            if let Some(ref ctx) = redeem_ctx {
                let ctx = Arc::clone(ctx);
                let dash = Arc::clone(&dashboard);
                last_redeem = Instant::now();
                tokio::spawn(async move {
                    redeem_sweep(&ctx, &dash).await;
                });
            }
        }

        // ── Update dashboard header even when idle ──
        {
            let mut d = dashboard.write();
            d.btc_price = *state.btc_price.read();
            d.sigma = (*state.sigma.read()).max(config.min_sigma);
        }

        // ── No market → wait ──
        let market = match &current_market {
            Some(m) => m.clone(),
            None => {
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        // ── Fetch Book ──
        let (yes_book, no_book) = {
            let yes_tok = market.token_id_yes.clone();
            let no_tok = market.token_id_no.clone();
            tokio::task::spawn_blocking(move || (fetch_book(&yes_tok), fetch_book(&no_tok)))
                .await
                .unwrap_or((None, None))
        };

        let yes_book = match yes_book {
            Some(b) => b,
            None => {
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };
        let no_book = match no_book {
            Some(b) => b,
            None => {
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        // ── Compute FV ──
        let btc_current = *state.btc_price.read();
        let btc_open = match *state.btc_open.read() {
            Some(o) => o,
            None => {
                // First tick: use Chainlink as open (matches Polymarket resolution)
                let open = state.chainlink_or_binance();
                *state.btc_open.write() = Some(open);
                open
            }
        };
        let raw_sigma = *state.sigma.read();
        let sigma = raw_sigma.max(config.min_sigma); // Apply floor
        let remaining_secs = (market.end_date - now).num_seconds() as f64;

        if remaining_secs < config.min_remaining_secs || btc_current <= 0.0 || raw_sigma <= 0.0 {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        let fv_up = fair_value_up(btc_open, btc_current, sigma, remaining_secs);
        let fv_down = 1.0 - fv_up;

        let mid_up = yes_book.mid();
        let mid_down = no_book.mid();

        if mid_up <= 0.0 || mid_down <= 0.0 {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        let div_up = fv_up - mid_up;
        let div_down = fv_down - mid_down;

        // ── Check Exits First ──
        // Exit conditions (in priority order):
        //   1. Force sell: < force_sell_secs remaining → sell everything at market
        //   2. Profit-take: bid > avg + profit_take_cents → lock in profit
        //   3. Convergence: div ≤ take_profit AND in profit → mid caught up to FV
        //   4. Stop-loss: div < stop_loss → FV reversed deeply against us
        let force_sell = remaining_secs < config.force_sell_secs;

        // Only attempt sells if buy fill has been confirmed by CLOB
        let can_sell = fill_confirmed || executor.is_none(); // paper mode always allows sells

        // Sell UP position
        if position.up_qty > Decimal::ZERO && yes_book.best_bid > Decimal::ZERO && can_sell {
            let current_div = fv_up - mid_up;
            let sell_price = yes_book.best_bid;
            let sell_qty = position.up_qty;
            let avg = position.avg_up();
            let sell_f = sell_price.to_string().parse::<f64>().unwrap_or(0.0);
            let avg_f = avg.to_string().parse::<f64>().unwrap_or(0.0);
            let profit_per_share = sell_f - avg_f;

            let profit_take = profit_per_share >= config.profit_take_cents;
            let convergence = current_div <= config.take_profit_div && profit_per_share >= 0.0;
            let stop_loss = profit_per_share <= -config.stop_loss_cents; // price-based stoploss

            if force_sell || profit_take || convergence || stop_loss {
                let reason = if force_sell {
                    "FORCE"
                } else if profit_take {
                    "PROFIT"
                } else if convergence {
                    "CONVERGE"
                } else {
                    "STOPLOSS"
                };
                let pnl_est = profit_per_share * sell_qty.to_string().parse::<f64>().unwrap_or(0.0);

                let success = if let Some(ref exec) = executor {
                    match exec
                        .sell_fok(&market.token_id_yes, sell_price, sell_qty, market.tick_size)
                        .await
                    {
                        Ok(oid) => {
                            info!("[SELL UP {reason}] qty={sell_qty} price={sell_price} div={current_div:.4} pnl~${pnl_est:.2} oid={oid}");
                            true
                        }
                        Err(e) => {
                            warn!("[SELL UP] Failed: {e:#}");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            false
                        }
                    }
                } else {
                    println!("│  [SELL UP {reason}] qty={sell_qty} bid={sell_price} avg={avg:.2} div={current_div:.4} pnl~${pnl_est:.2}");
                    true
                };
                if success {
                    trade_log.log(
                        &period_name,
                        reason,
                        "UP",
                        sell_qty,
                        sell_price,
                        avg,
                        fv_up,
                        mid_up,
                        current_div,
                        pnl_est,
                        remaining_secs,
                        sigma,
                        btc_current,
                    );
                    dashboard.write().push_trade(TradeFeedEntry {
                        time: Utc::now().format("%H:%M:%S").to_string(),
                        action: reason.to_string(),
                        side: "UP".to_string(),
                        qty: sell_qty.to_string().parse().unwrap_or(0.0),
                        price: sell_price.to_string().parse().unwrap_or(0.0),
                        pnl: pnl_est,
                    });
                    stats.sell_pnl += pnl_est;
                    stats.sells_up += sell_qty;
                    stats.exits += 1;
                    if stop_loss {
                        stats.stoplosses += 1;
                        stats.last_stoploss_at = Some(Instant::now());
                    }
                    position.up_cost = Decimal::ZERO;
                    position.up_qty = Decimal::ZERO;
                    position.up_entry_fv = 0.0;
                    fill_confirmed = false; // reset for re-entry
                }
            }
        }

        // Sell DOWN position
        if position.down_qty > Decimal::ZERO && no_book.best_bid > Decimal::ZERO && can_sell {
            let current_div = fv_down - mid_down;
            let sell_price = no_book.best_bid;
            let sell_qty = position.down_qty;
            let avg = position.avg_down();
            let sell_f = sell_price.to_string().parse::<f64>().unwrap_or(0.0);
            let avg_f = avg.to_string().parse::<f64>().unwrap_or(0.0);
            let profit_per_share = sell_f - avg_f;

            let profit_take = profit_per_share >= config.profit_take_cents;
            let convergence = current_div <= config.take_profit_div && profit_per_share >= 0.0;
            let stop_loss = profit_per_share <= -config.stop_loss_cents; // price-based stoploss

            if force_sell || profit_take || convergence || stop_loss {
                let reason = if force_sell {
                    "FORCE"
                } else if profit_take {
                    "PROFIT"
                } else if convergence {
                    "CONVERGE"
                } else {
                    "STOPLOSS"
                };
                let pnl_est = profit_per_share * sell_qty.to_string().parse::<f64>().unwrap_or(0.0);

                let success = if let Some(ref exec) = executor {
                    match exec
                        .sell_fok(&market.token_id_no, sell_price, sell_qty, market.tick_size)
                        .await
                    {
                        Ok(oid) => {
                            info!("[SELL DN {reason}] qty={sell_qty} price={sell_price} div={current_div:.4} pnl~${pnl_est:.2} oid={oid}");
                            true
                        }
                        Err(e) => {
                            warn!("[SELL DN] Failed: {e:#}");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            false
                        }
                    }
                } else {
                    println!("│  [SELL DN {reason}] qty={sell_qty} bid={sell_price} avg={avg:.2} div={current_div:.4} pnl~${pnl_est:.2}");
                    true
                };
                if success {
                    trade_log.log(
                        &period_name,
                        reason,
                        "DN",
                        sell_qty,
                        sell_price,
                        avg,
                        fv_down,
                        mid_down,
                        current_div,
                        pnl_est,
                        remaining_secs,
                        sigma,
                        btc_current,
                    );
                    dashboard.write().push_trade(TradeFeedEntry {
                        time: Utc::now().format("%H:%M:%S").to_string(),
                        action: reason.to_string(),
                        side: "DN".to_string(),
                        qty: sell_qty.to_string().parse().unwrap_or(0.0),
                        price: sell_price.to_string().parse().unwrap_or(0.0),
                        pnl: pnl_est,
                    });
                    stats.sell_pnl += pnl_est;
                    stats.sells_down += sell_qty;
                    stats.exits += 1;
                    if stop_loss {
                        stats.stoplosses += 1;
                        stats.last_stoploss_at = Some(Instant::now());
                    }
                    position.down_cost = Decimal::ZERO;
                    position.down_qty = Decimal::ZERO;
                    position.down_entry_fv = 0.0;
                    fill_confirmed = false; // reset for re-entry
                }
            }
        }

        // ── Check Entries (with cooldown) ──
        let ask_up_f64: f64 = yes_book.best_ask.to_string().parse().unwrap_or(0.0);
        let ask_down_f64: f64 = no_book.best_ask.to_string().parse().unwrap_or(0.0);

        // Only enter if no position on opposite side (prevent whipsaw)
        // AND not in force-sell zone
        // AND not recently stopped out (30s cooldown after stoploss)
        let has_up = position.up_qty > Decimal::ZERO;
        let has_down = position.down_qty > Decimal::ZERO;
        let min_entry_remaining = config.force_sell_secs + 60.0; // need at least 60s + force_sell buffer
        let stoploss_cooldown = stats
            .last_stoploss_at
            .map(|t| t.elapsed() < Duration::from_secs(30))
            .unwrap_or(false);
        let stoploss_maxed = stats.stoplosses >= config.max_stoplosses;

        // Buy UP if FV > mid by min_divergence AND no DOWN position
        if div_up >= config.min_divergence
            && remaining_secs > min_entry_remaining
            && ask_up_f64 > 0.0
            && ask_up_f64 <= config.max_cost
            && position.up_qty < config.max_position
            && !has_down  // don't enter opposite side
            && !stoploss_cooldown  // wait after stoploss
            && !stoploss_maxed
        // stop trading after max stoplosses
        {
            let buy_price = yes_book.best_ask;
            // Buy full position at once (reduces churn vs incremental)
            let buy_qty = config.max_position - position.up_qty;

            if let Some(ref exec) = executor {
                match exec
                    .buy_fok(&market.token_id_yes, buy_price, buy_qty, market.tick_size)
                    .await
                {
                    Ok(oid) => {
                        info!("[BUY UP] qty={buy_qty} ask={buy_price} fv={fv_up:.4} mid={mid_up:.4} div={div_up:.4} oid={oid}");
                        // Verify CLOB match + force CLOB balance refresh before committing position
                        if exec.wait_for_fill(&oid, &market.token_id_yes, 60).await {
                            info!("[BUY UP] Fill confirmed — tokens available for selling");
                            if position.up_qty == Decimal::ZERO {
                                position.up_entry_fv = fv_up;
                            }
                            position.up_cost += buy_price * buy_qty;
                            position.up_qty += buy_qty;
                            stats.buys_up += buy_qty;
                            stats.entries += 1;
                            fill_confirmed = true;
                            trade_log.log(
                                &period_name,
                                "BUY",
                                "UP",
                                buy_qty,
                                buy_price,
                                Decimal::ZERO,
                                fv_up,
                                mid_up,
                                div_up,
                                0.0,
                                remaining_secs,
                                sigma,
                                btc_current,
                            );
                            dashboard.write().push_trade(TradeFeedEntry {
                                time: Utc::now().format("%H:%M:%S").to_string(),
                                action: "BUY".to_string(),
                                side: "UP".to_string(),
                                qty: buy_qty.to_string().parse().unwrap_or(0.0),
                                price: buy_price.to_string().parse().unwrap_or(0.0),
                                pnl: 0.0,
                            });
                        } else {
                            warn!("[BUY UP] Fill NOT confirmed — phantom order, skipping position");
                        }
                    }
                    Err(e) => {
                        warn!("[BUY UP] Failed: {e:#}");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            } else {
                println!("│  [BUY UP] qty={buy_qty} ask={buy_price} fv={fv_up:.4} mid={mid_up:.4} div={div_up:.4}");
                if position.up_qty == Decimal::ZERO {
                    position.up_entry_fv = fv_up;
                }
                position.up_cost += buy_price * buy_qty;
                position.up_qty += buy_qty;
                stats.buys_up += buy_qty;
                stats.entries += 1;
                trade_log.log(
                    &period_name,
                    "BUY",
                    "UP",
                    buy_qty,
                    buy_price,
                    Decimal::ZERO,
                    fv_up,
                    mid_up,
                    div_up,
                    0.0,
                    remaining_secs,
                    sigma,
                    btc_current,
                );
                dashboard.write().push_trade(TradeFeedEntry {
                    time: Utc::now().format("%H:%M:%S").to_string(),
                    action: "BUY".to_string(),
                    side: "UP".to_string(),
                    qty: buy_qty.to_string().parse().unwrap_or(0.0),
                    price: buy_price.to_string().parse().unwrap_or(0.0),
                    pnl: 0.0,
                });
            }
        }

        // Buy DOWN if FV > mid by min_divergence AND no UP position
        if div_down >= config.min_divergence
            && remaining_secs > min_entry_remaining
            && ask_down_f64 > 0.0
            && ask_down_f64 <= config.max_cost
            && position.down_qty < config.max_position
            && !has_up  // don't enter opposite side
            && !stoploss_cooldown  // wait after stoploss
            && !stoploss_maxed
        // stop trading after max stoplosses
        {
            let buy_price = no_book.best_ask;
            let buy_qty = config.max_position - position.down_qty;

            if let Some(ref exec) = executor {
                match exec
                    .buy_fok(&market.token_id_no, buy_price, buy_qty, market.tick_size)
                    .await
                {
                    Ok(oid) => {
                        info!("[BUY DN] qty={buy_qty} ask={buy_price} fv={fv_down:.4} mid={mid_down:.4} div={div_down:.4} oid={oid}");
                        // Verify CLOB match + force CLOB balance refresh before committing position
                        if exec.wait_for_fill(&oid, &market.token_id_no, 60).await {
                            info!("[BUY DN] Fill confirmed — tokens available for selling");
                            if position.down_qty == Decimal::ZERO {
                                position.down_entry_fv = fv_down;
                            }
                            position.down_cost += buy_price * buy_qty;
                            position.down_qty += buy_qty;
                            stats.buys_down += buy_qty;
                            stats.entries += 1;
                            fill_confirmed = true;
                            trade_log.log(
                                &period_name,
                                "BUY",
                                "DN",
                                buy_qty,
                                buy_price,
                                Decimal::ZERO,
                                fv_down,
                                mid_down,
                                div_down,
                                0.0,
                                remaining_secs,
                                sigma,
                                btc_current,
                            );
                            dashboard.write().push_trade(TradeFeedEntry {
                                time: Utc::now().format("%H:%M:%S").to_string(),
                                action: "BUY".to_string(),
                                side: "DN".to_string(),
                                qty: buy_qty.to_string().parse().unwrap_or(0.0),
                                price: buy_price.to_string().parse().unwrap_or(0.0),
                                pnl: 0.0,
                            });
                        } else {
                            warn!("[BUY DN] Fill NOT confirmed — phantom order, skipping position");
                        }
                    }
                    Err(e) => {
                        warn!("[BUY DN] Failed: {e:#}");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            } else {
                println!("│  [BUY DN] qty={buy_qty} ask={buy_price} fv={fv_down:.4} mid={mid_down:.4} div={div_down:.4}");
                if position.down_qty == Decimal::ZERO {
                    position.down_entry_fv = fv_down;
                }
                position.down_cost += buy_price * buy_qty;
                position.down_qty += buy_qty;
                stats.buys_down += buy_qty;
                stats.entries += 1;
                trade_log.log(
                    &period_name,
                    "BUY",
                    "DN",
                    buy_qty,
                    buy_price,
                    Decimal::ZERO,
                    fv_down,
                    mid_down,
                    div_down,
                    0.0,
                    remaining_secs,
                    sigma,
                    btc_current,
                );
                dashboard.write().push_trade(TradeFeedEntry {
                    time: Utc::now().format("%H:%M:%S").to_string(),
                    action: "BUY".to_string(),
                    side: "DN".to_string(),
                    qty: buy_qty.to_string().parse().unwrap_or(0.0),
                    price: buy_price.to_string().parse().unwrap_or(0.0),
                    pnl: 0.0,
                });
            }
        }

        // ── Dashboard Update (every tick) ──
        {
            let mut d = dashboard.write();
            d.btc_price = btc_current;
            d.sigma = sigma;
            d.remaining_secs = remaining_secs;
            d.fv_up = fv_up;
            d.fv_down = fv_down;
            d.mid_up = mid_up;
            d.mid_down = mid_down;
            d.div_up = div_up;
            d.div_down = div_down;
            d.ask_up = yes_book.best_ask.to_string().parse().unwrap_or(0.0);
            d.ask_down = no_book.best_ask.to_string().parse().unwrap_or(0.0);
            d.bid_up = yes_book.best_bid.to_string().parse().unwrap_or(0.0);
            d.bid_down = no_book.best_bid.to_string().parse().unwrap_or(0.0);
            d.fill_confirmed = fill_confirmed;
            d.period_entries = stats.entries;
            d.period_exits = stats.exits;
            d.period_stoplosses = stats.stoplosses;
            d.period_sell_pnl = stats.sell_pnl;

            // Position info
            if position.up_qty > Decimal::ZERO {
                let avg = position.avg_up();
                let bid_f: f64 = yes_book.best_bid.to_string().parse().unwrap_or(0.0);
                let avg_f: f64 = avg.to_string().parse().unwrap_or(0.0);
                let qty_f: f64 = position.up_qty.to_string().parse().unwrap_or(0.0);
                d.position_side = "UP".to_string();
                d.position_qty = qty_f;
                d.position_avg_cost = avg_f;
                d.current_bid = bid_f;
                d.unrealized_pnl = (bid_f - avg_f) * qty_f;
            } else if position.down_qty > Decimal::ZERO {
                let avg = position.avg_down();
                let bid_f: f64 = no_book.best_bid.to_string().parse().unwrap_or(0.0);
                let avg_f: f64 = avg.to_string().parse().unwrap_or(0.0);
                let qty_f: f64 = position.down_qty.to_string().parse().unwrap_or(0.0);
                d.position_side = "DN".to_string();
                d.position_qty = qty_f;
                d.position_avg_cost = avg_f;
                d.current_bid = bid_f;
                d.unrealized_pnl = (bid_f - avg_f) * qty_f;
            } else {
                d.position_side = "FLAT".to_string();
                d.position_qty = 0.0;
                d.position_avg_cost = 0.0;
                d.current_bid = 0.0;
                d.unrealized_pnl = 0.0;
            }
        }

        // ── Status Line (every ~10s to reduce noise, or every tick with position) ──
        let rem = remaining_secs as i64;
        let has_pos = position.up_qty > Decimal::ZERO || position.down_qty > Decimal::ZERO;
        if has_pos || rem % 10 == 0 {
            let cl = *state.chainlink_price.read();
            let cl_delta = if cl > 0.0 { btc_current - cl } else { 0.0 };
            print!("│  BTC=${btc_current:.0} CL=${cl:.0}({cl_delta:+.0}) σ={sigma:.6} rem={rem}s fv={fv_up:.3}/{fv_down:.3} mid={mid_up:.3}/{mid_down:.3} div={div_up:+.3}/{div_down:+.3}");
            if has_pos {
                print!(
                    " | pos: UP={}@{:.2} DN={}@{:.2}",
                    position.up_qty,
                    position.avg_up(),
                    position.down_qty,
                    position.avg_down()
                );
            }
            println!();
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Estimate settlement PnL for remaining position (paper mode).
/// In live, we'd sell remaining shares before period end.
fn settle_position(position: &Position, _stats: &PeriodStats) -> f64 {
    // We don't know resolution in advance.
    // For paper tracking: assume we sell at last known FV (approximate).
    // Real PnL is from sell_pnl + any remaining exposure.
    let up_exposure = if position.up_qty > Decimal::ZERO {
        let cost_f = position.up_cost.to_string().parse::<f64>().unwrap_or(0.0);
        -cost_f // worst case: UP goes to zero
    } else {
        0.0
    };
    let down_exposure = if position.down_qty > Decimal::ZERO {
        let cost_f = position.down_cost.to_string().parse::<f64>().unwrap_or(0.0);
        -cost_f
    } else {
        0.0
    };
    // Conservative: mark unsold positions as loss
    up_exposure + down_exposure
}

// ── Entry Point ──

#[tokio::main]
async fn main() -> Result<()> {
    // TLS crypto provider
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(
                    "convergence_bot=info,polymarket_client_sdk=error,warn",
                )
            }),
        )
        .init();

    dotenvy::dotenv().ok();

    // Parse args
    let args: Vec<String> = std::env::args().collect();
    let live = args.iter().any(|a| a == "--live");
    let dry_run = args.iter().any(|a| a == "--dry-run");

    let mut config = ConvergenceConfig::default();
    config.live = live;
    config.dry_run = dry_run;

    // Parse optional config overrides from args
    for (i, arg) in args.iter().enumerate() {
        match arg.as_str() {
            "--div" if i + 1 < args.len() => {
                config.min_divergence = args[i + 1].parse().context("Invalid --div value")?;
            }
            "--tp" if i + 1 < args.len() => {
                config.take_profit_div = args[i + 1].parse().context("Invalid --tp value")?;
            }
            "--size" if i + 1 < args.len() => {
                config.order_size = args[i + 1].parse().context("Invalid --size value")?;
            }
            "--max-pos" if i + 1 < args.len() => {
                config.max_position = args[i + 1].parse().context("Invalid --max-pos value")?;
            }
            _ => {}
        }
    }

    // SDK auth (live mode only)
    let mut redeem_ctx: Option<Arc<RedeemContext>> = None;
    let executor = if live {
        let private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
            .context("POLYMARKET_PRIVATE_KEY not set in .env")?;
        let wallet_address =
            std::env::var("WALLET_ADDRESS").context("WALLET_ADDRESS not set in .env")?;
        let builder_key = std::env::var("POLY_BUILDER_KEY").context("POLY_BUILDER_KEY not set")?;
        let builder_secret =
            std::env::var("POLY_BUILDER_SECRET").context("POLY_BUILDER_SECRET not set")?;
        let builder_passphrase =
            std::env::var("POLY_BUILDER_PASSPHRASE").context("POLY_BUILDER_PASSPHRASE not set")?;
        let rpc_url = std::env::var("POLYGON_RPC_URL")
            .unwrap_or_else(|_| "https://polygon-rpc.com".to_string());

        let signer = PrivateKeySigner::from_str(&private_key)
            .context("Invalid private key")?
            .with_chain_id(Some(POLYGON));

        // Clone signer for redemption before it moves into Executor
        redeem_ctx = Some(Arc::new(RedeemContext {
            signer: signer.clone(),
            wallet_address: wallet_address.clone(),
            rpc_url,
        }));

        let _wallet_addr =
            SdkAddress::from_str(&wallet_address).context("Invalid wallet address")?;

        info!("Authenticating CLOB client...");
        let clob = clob::Client::new(
            CLOB_HOST,
            ClobConfig::builder()
                .heartbeat_interval(Duration::from_secs(10))
                .build(),
        )
        .context("Failed to create CLOB client")?
        .authentication_builder(&signer)
        .signature_type(SignatureType::Eoa)
        .authenticate()
        .await
        .context("CLOB authentication failed")?;

        info!("CLOB client authenticated");

        // Promote to builder
        let builder_uuid = polymarket_client_sdk::auth::Uuid::parse_str(&builder_key)
            .context("Invalid POLY_BUILDER_KEY (must be UUID)")?;
        let builder_creds = polymarket_client_sdk::auth::Credentials::new(
            builder_uuid,
            builder_secret,
            builder_passphrase,
        );
        let builder_config = polymarket_client_sdk::auth::builder::Config::local(builder_creds);
        let promoted = clob
            .promote_to_builder(builder_config)
            .await
            .context("Builder promotion failed")?;

        info!("CLOB client promoted to Builder mode");

        Some(Executor {
            clob: promoted,
            signer,
            dry_run,
        })
    } else {
        None
    };

    // Dashboard
    let dashboard: SharedConvergenceDashboard =
        Arc::new(RwLock::new(ConvergenceDashboard::default()));
    let dash_clone = Arc::clone(&dashboard);
    tokio::spawn(async move {
        start_convergence_web(dash_clone, 3001).await;
    });

    // Shared state for price feeds
    let state = Arc::new(SharedState::new());
    let shutdown = Arc::new(Notify::new());

    // Start Binance feed (fast signal for FV computation + sigma)
    let state_clone = Arc::clone(&state);
    let shutdown_clone = Arc::clone(&shutdown);
    tokio::spawn(async move {
        binance_feed(state_clone, shutdown_clone).await;
    });

    // Start Chainlink RTDS feed (for btc_open — matches Polymarket resolution)
    let state_clone = Arc::clone(&state);
    let shutdown_clone = Arc::clone(&shutdown);
    tokio::spawn(async move {
        chainlink_feed(state_clone, shutdown_clone).await;
    });

    // Wait for first BTC price from Binance (fast)
    info!("Waiting for Binance BTC price...");
    loop {
        if *state.btc_price.read() > 0.0 {
            let price = *state.btc_price.read();
            let cl = *state.chainlink_price.read();
            if cl > 0.0 {
                info!(
                    "BTC price: ${:.2} (Binance) / ${:.2} (Chainlink)",
                    price, cl
                );
            } else {
                info!(
                    "BTC price: ${:.2} (Binance, Chainlink connecting...)",
                    price
                );
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Wait for vol calculation to accumulate (need enough ticks for reliable sigma)
    info!("Warming up volatility ({}s)...", config.warmup_secs);
    tokio::time::sleep(Duration::from_secs(config.warmup_secs)).await;
    let sigma = *state.sigma.read();
    info!(
        "Initial sigma: {:.8} (floor: {:.8})",
        sigma, config.min_sigma
    );

    // Run main loop
    run_convergence(config, executor, state, dashboard, redeem_ctx).await
}
