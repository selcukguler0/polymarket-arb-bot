//! Shared strategy infrastructure: market discovery, book fetching, executor.
//!
//! Extracted from convergence_bot.rs so that multiple strategy binaries can
//! reuse the same CLOB scanning, orderbook polling, and FOK execution logic.

use std::str::FromStr;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as _;

use polymarket_client_sdk::clob::types::{OrderType, Side, SignatureType};
use polymarket_client_sdk::clob::{self, Config as ClobConfig};
use polymarket_client_sdk::POLYGON;

// ── Constants ──

pub const CLOB_HOST: &str = "https://clob.polymarket.com";
pub const CLOB_BOOK_URL: &str = "https://clob.polymarket.com/book";

/// USDC contract address on Polygon.
pub const USDC_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

// ── Type aliases ──

/// CLOB client with Builder auth for order attribution + weekly USDC rewards.
pub type BuilderAuthState = polymarket_client_sdk::auth::state::Authenticated<
    polymarket_client_sdk::auth::builder::Builder,
>;

// ── Market ──

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Market {
    pub condition_id: String,
    pub token_id_up: String,
    pub token_id_down: String,
    pub question: String,
    pub start_date: DateTime<Utc>,
    pub end_date: DateTime<Utc>,
    pub tick_size: Decimal,
    pub neg_risk: bool,
}

impl Market {
    /// Duration of this market period in seconds.
    pub fn duration_secs(&self) -> i64 {
        (self.end_date - self.start_date).num_seconds()
    }

    /// Seconds since this market resolved (negative if still active).
    pub fn secs_since_end(&self) -> i64 {
        (Utc::now() - self.end_date).num_seconds()
    }

    /// Whether this market's period has ended.
    pub fn is_resolved(&self) -> bool {
        Utc::now() >= self.end_date
    }

    /// Whether this market is currently in its active trading window.
    pub fn is_active(&self) -> bool {
        let now = Utc::now();
        now >= self.start_date && now < self.end_date
    }
}

// ── Book Snapshot ──

#[derive(Clone, Debug, Default)]
pub struct BookSnapshot {
    pub best_bid: Decimal,
    pub best_ask: Decimal,
    pub bid_size: Decimal,
    pub ask_size: Decimal,
}

impl BookSnapshot {
    pub fn mid(&self) -> Decimal {
        if self.best_bid > Decimal::ZERO && self.best_ask > Decimal::ZERO {
            (self.best_bid + self.best_ask) / dec!(2)
        } else {
            Decimal::ZERO
        }
    }
}

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

/// Fetch the order book for a single token from the CLOB REST API.
/// Returns None on network/parse errors.
pub fn fetch_book(token_id: &str) -> Option<BookSnapshot> {
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
    let bid_size = book
        .bids
        .iter()
        .filter_map(|l| {
            let p = l.price.parse::<Decimal>().ok()?;
            let s = l.size.parse::<Decimal>().ok()?;
            if p == best_bid {
                Some(s)
            } else {
                None
            }
        })
        .sum::<Decimal>();

    let best_ask = book
        .asks
        .iter()
        .filter_map(|l| l.price.parse::<Decimal>().ok())
        .min()
        .unwrap_or(Decimal::ZERO);
    let ask_size = book
        .asks
        .iter()
        .filter_map(|l| {
            let p = l.price.parse::<Decimal>().ok()?;
            let s = l.size.parse::<Decimal>().ok()?;
            if p == best_ask {
                Some(s)
            } else {
                None
            }
        })
        .sum::<Decimal>();

    Some(BookSnapshot {
        best_bid,
        best_ask,
        bid_size,
        ask_size,
    })
}

/// Fetch books for both Up and Down tokens of a market.
/// Returns (up_book, down_book), either of which may be None on error.
pub fn fetch_market_books(market: &Market) -> (Option<BookSnapshot>, Option<BookSnapshot>) {
    let up = fetch_book(&market.token_id_up);
    let down = fetch_book(&market.token_id_down);
    (up, down)
}

// ── CLOB Market Discovery ──

/// Scan CLOB `/markets` paginated endpoint for BTC Up/Down markets.
///
/// Returns markets matching the allowed durations (in minutes: 5, 15, 60, 240).
/// Unlike Gamma API, CLOB always has fully provisioned condition_id and token_ids.
///
/// When `include_resolved` is true, markets with `accepting_orders=false` are also
/// returned (needed for post-resolution strategies that buy after period end).
pub fn scan_clob_markets(
    start_cursor: &str,
    allowed_durations: &[u32],
    include_resolved: bool,
) -> (Vec<Market>, String) {
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
            if !include_resolved && m["accepting_orders"].as_bool() != Some(true) {
                continue;
            }

            // Parse market_slug: btc-updown-{dur}-{start_timestamp}
            let slug = m["market_slug"].as_str().unwrap_or("");
            let parts: Vec<&str> = slug.split('-').collect();
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

            // Skip markets that ended more than 10 min ago (wider window for post-resolution)
            if end_date < now - chrono::Duration::minutes(10) {
                continue;
            }

            let cid = m["condition_id"].as_str().unwrap_or("");
            if cid.is_empty() {
                continue;
            }

            // Parse tokens — outcomes are "Up" and "Down" (NOT "Yes"/"No")
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
                token_id_up: up_id,
                token_id_down: down_id,
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
/// Uses March 9, 2026 as baseline (~560k markets), growing ~5000/day.
/// We only look back 5K markets (about 1 day) since we only need recent/active ones.
pub fn estimate_clob_start_cursor() -> String {
    use chrono::TimeZone;
    let baseline = 560_000u64;
    let baseline_date = Utc.with_ymd_and_hms(2026, 3, 9, 0, 0, 0).unwrap();
    let days_since = (Utc::now() - baseline_date).num_days().max(0) as u64;
    let estimated_total = baseline + days_since * 5000;
    // Only look back 5K markets — we only need the most recent ones.
    // 30 batches * 1000 = 30K scan range, so starting 5K back is plenty.
    let start = estimated_total.saturating_sub(5_000);
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(start.to_string())
}

// ── Executor ──

/// Minimal executor for FOK order placement, shared across strategies.
pub struct Executor {
    pub clob: clob::Client<BuilderAuthState>,
    pub signer: PrivateKeySigner,
    pub dry_run: bool,
}

impl Executor {
    /// Authenticate and create an Executor with Builder-promoted CLOB client.
    pub async fn new(
        private_key: &str,
        builder_key: &str,
        builder_secret: &str,
        builder_passphrase: &str,
        dry_run: bool,
    ) -> Result<Self> {
        let signer = PrivateKeySigner::from_str(private_key)
            .context("Invalid private key")?
            .with_chain_id(Some(POLYGON));

        let builder_uuid = polymarket_client_sdk::auth::Uuid::parse_str(builder_key)
            .context("Invalid POLY_BUILDER_KEY (must be UUID)")?;

        let mut last_err = String::new();
        let mut attempt = 0u32;
        let clob = loop {
            attempt += 1;
            info!(attempt, "Authenticating CLOB client...");

            let clob_normal = clob::Client::new(
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

            let builder_creds = polymarket_client_sdk::auth::Credentials::new(
                builder_uuid,
                builder_secret.to_string(),
                builder_passphrase.to_string(),
            );
            let builder_config = polymarket_client_sdk::auth::builder::Config::local(builder_creds);
            match clob_normal.promote_to_builder(builder_config).await {
                Ok(promoted) => {
                    info!("CLOB client promoted to Builder mode");
                    break promoted;
                }
                Err(e) => {
                    last_err = format!("{e}");
                    if attempt >= 3 {
                        bail!("Builder promotion failed after {attempt} attempts: {last_err}");
                    }
                    warn!(attempt, error = %e, "Builder promotion failed, retrying...");
                    tokio::time::sleep(Duration::from_secs(2 * attempt as u64)).await;
                }
            }
        };

        Ok(Self {
            clob,
            signer,
            dry_run,
        })
    }

    /// Place a Fill-or-Kill order. Returns order_id on success.
    pub async fn place_fok(
        &self,
        token_id: &str,
        side: Side,
        price: Decimal,
        size: Decimal,
        tick_size: Decimal,
        label: &str,
    ) -> Result<String> {
        if self.dry_run {
            info!("[DRY-RUN] Would {label} FOK: token={token_id} side={side:?} price={price} size={size}");
            return Ok("dry-run".to_string());
        }

        let token = U256::from_str(token_id).context("Invalid token_id")?;
        let rounded_price = round_to_tick(price, tick_size);

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

    /// Place a FOK buy order.
    pub async fn buy_fok(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
        tick_size: Decimal,
    ) -> Result<String> {
        self.place_fok(token_id, Side::Buy, price, size, tick_size, "buy")
            .await
    }

    /// Place a GTC (Good-Till-Cancelled) limit order. Returns order_id on success.
    pub async fn place_gtc(
        &self,
        token_id: &str,
        side: Side,
        price: Decimal,
        size: Decimal,
        tick_size: Decimal,
        label: &str,
    ) -> Result<String> {
        if self.dry_run {
            let fake_id = format!("dry-run-{}", chrono::Utc::now().timestamp_millis());
            info!("[DRY-RUN] Would {label} GTC: token={token_id} side={side:?} price={price} size={size}");
            return Ok(fake_id);
        }

        let token = U256::from_str(token_id).context("Invalid token_id")?;
        let rounded_price = round_to_tick(price, tick_size);

        let signable = self
            .clob
            .limit_order()
            .token_id(token)
            .side(side)
            .price(rounded_price)
            .size(size)
            .order_type(OrderType::GTC)
            .build()
            .await
            .context(format!("GTC {label} build failed"))?;

        let signed = self
            .clob
            .sign(&self.signer, signable)
            .await
            .context(format!("GTC {label} sign failed"))?;

        let resp = tokio::time::timeout(Duration::from_secs(5), self.clob.post_order(signed))
            .await
            .context(format!("GTC {label} timed out"))?
            .context(format!("GTC {label} post failed"))?;

        if !resp.success {
            let msg = resp.error_msg.as_deref().unwrap_or("unknown");
            bail!("GTC {label} rejected: {msg}");
        }
        Ok(resp.order_id)
    }

    /// Query open orders for a specific token. Returns all open orders (paginated).
    pub async fn get_open_orders(&self, token_id: &str) -> Result<Vec<OpenOrderResponse>> {
        use polymarket_client_sdk::clob::types::request::OrdersRequest;

        let token = U256::from_str(token_id).context("Invalid token_id")?;
        let req = OrdersRequest::builder().asset_id(token).build();

        let mut all = Vec::new();
        let mut cursor: Option<String> = None;

        for _ in 0..5 {
            let page = self
                .clob
                .orders(&req, cursor.clone())
                .await
                .context("Open orders query failed")?;

            all.extend(page.data);

            if page.next_cursor == "LTE=" || page.count == 0 {
                break;
            }
            cursor = Some(page.next_cursor);
        }

        Ok(all)
    }

    /// Cancel specific orders by ID (batch). Returns cancelled order IDs.
    pub async fn cancel_orders(&self, order_ids: &[&str]) -> Result<Vec<String>> {
        if order_ids.is_empty() {
            return Ok(vec![]);
        }
        if self.dry_run {
            info!("[DRY-RUN] Would cancel {} orders", order_ids.len());
            return Ok(order_ids.iter().map(|s| s.to_string()).collect());
        }

        let resp = self
            .clob
            .cancel_orders(order_ids)
            .await
            .context("Batch cancel failed")?;

        Ok(resp.canceled)
    }

    /// Place multiple GTC orders in a single batch HTTP call.
    /// Each entry: (token_id, side, price, size, tick_size).
    /// Returns Vec of (original_index, order_id) for successfully posted orders.
    pub async fn place_batch_gtc(
        &self,
        orders: &[(String, Side, Decimal, Decimal, Decimal)],
    ) -> Result<Vec<(usize, String)>> {
        if orders.is_empty() {
            return Ok(vec![]);
        }
        if self.dry_run {
            let ts = chrono::Utc::now().timestamp_millis();
            let results: Vec<(usize, String)> = orders
                .iter()
                .enumerate()
                .map(|(i, (token_id, side, price, size, _))| {
                    info!(
                        "[DRY-RUN] Would batch GTC: token={} side={side:?} price={price} size={size}",
                        &token_id[..12.min(token_id.len())]
                    );
                    (i, format!("dry-run-{ts}-{i}"))
                })
                .collect();
            return Ok(results);
        }

        let mut signed_orders = Vec::new();
        let mut valid_indices = Vec::new();

        for (i, (token_id, side, price, size, tick_size)) in orders.iter().enumerate() {
            let token = match U256::from_str(token_id) {
                Ok(v) => v,
                Err(e) => {
                    warn!("[executor] Batch GTC order {i} skipped: invalid token_id: {e}");
                    continue;
                }
            };
            let rounded_price = round_to_tick(*price, *tick_size);

            let signable = match self
                .clob
                .limit_order()
                .token_id(token)
                .side(*side)
                .price(rounded_price)
                .size(*size)
                .order_type(OrderType::GTC)
                .build()
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!("[executor] Batch GTC order {i} build failed: {e}");
                    continue;
                }
            };

            let signed = match self.clob.sign(&self.signer, signable).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("[executor] Batch GTC order {i} sign failed: {e}");
                    continue;
                }
            };

            signed_orders.push(signed);
            valid_indices.push(i);
        }

        if signed_orders.is_empty() {
            return Ok(vec![]);
        }

        let responses = tokio::time::timeout(
            Duration::from_secs(10),
            self.clob.post_orders(signed_orders),
        )
        .await
        .context("Batch GTC post_orders timed out")?
        .context("Batch GTC post_orders failed")?;

        let mut result = Vec::new();
        for (resp_idx, resp) in responses.iter().enumerate() {
            if resp.success {
                let orig_idx = valid_indices.get(resp_idx).copied().unwrap_or(resp_idx);
                result.push((orig_idx, resp.order_id.clone()));
            } else {
                let msg = resp.error_msg.as_deref().unwrap_or("unknown");
                warn!(
                    "[executor] Batch GTC order {} rejected: {msg}",
                    valid_indices.get(resp_idx).copied().unwrap_or(resp_idx)
                );
            }
        }

        info!(
            "[executor] Batch GTC: {}/{} orders posted",
            result.len(),
            orders.len()
        );
        Ok(result)
    }

    /// Place a FOK sell order (for liquidating unmatched positions).
    pub async fn sell_fok(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
        tick_size: Decimal,
    ) -> Result<String> {
        self.place_fok(token_id, Side::Sell, price, size, tick_size, "sell")
            .await
    }
}

/// Re-export for strategies that need to check order fill status.
pub use polymarket_client_sdk::clob::types::response::OpenOrderResponse;

/// Round price to nearest tick.
pub fn round_to_tick(price: Decimal, tick_size: Decimal) -> Decimal {
    if tick_size.is_zero() {
        return price;
    }
    (price / tick_size).round() * tick_size
}

// ── Redemption Helpers ──

/// Context needed for on-chain redemption.
pub struct RedeemContext {
    pub signer: PrivateKeySigner,
    pub wallet_address: String,
    pub rpc_url: String,
}

/// Sweep all redeemable positions for the wallet.
/// Returns (success_count, fail_count).
pub async fn redeem_sweep(ctx: &RedeemContext) -> (u32, u32) {
    use alloy::primitives::{Address, B256};
    use alloy::providers::ProviderBuilder;
    use polymarket_client_sdk::ctf;
    use polymarket_client_sdk::ctf::types::RedeemPositionsRequest;
    use polymarket_client_sdk::data;
    use polymarket_client_sdk::data::types::request::PositionsRequest;
    use std::collections::HashMap;

    let wallet_addr = match Address::from_str(&ctx.wallet_address) {
        Ok(a) => a,
        Err(e) => {
            warn!("[redeem] Invalid wallet address: {e}");
            return (0, 0);
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
            return (0, 0);
        }
    };

    if positions.is_empty() {
        info!("[redeem] No redeemable positions");
        return (0, 0);
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

    info!("[redeem] Sweep: {ok} redeemed, {fail} failed");
    (ok, fail)
}

// ── CSV Trade Logger ──

pub struct TradeLogger {
    writer: std::io::BufWriter<std::fs::File>,
}

impl TradeLogger {
    /// Create a new CSV trade logger. Writes header if file is new.
    pub fn new(path: &str, header: &str) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let is_empty = file.metadata()?.len() == 0;
        let mut writer = std::io::BufWriter::new(file);
        if is_empty {
            use std::io::Write;
            writeln!(writer, "{header}")?;
        }
        Ok(Self { writer })
    }

    /// Write a raw CSV line.
    pub fn write_line(&mut self, line: &str) {
        use std::io::Write;
        let _ = writeln!(self.writer, "{line}");
        let _ = self.writer.flush();
    }
}
