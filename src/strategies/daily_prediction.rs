//! Daily Prediction Market Pair-Arb Strategy
//!
//! Targets binary-outcome daily prediction markets on Polymarket:
//!   - "Will the price of Bitcoin be above $68,000 on March 9?"
//!   - "Will the price of Bitcoin be between $66,000 and $68,000 on March 9?"
//!   - "Will the price of Ethereum be above $2,000 on March 9?"
//!
//! Strategy: Buy both Yes + No tokens when combined ask < $1.00, then
//! hold until resolution and redeem matched pairs at $1.00 per pair.
//!
//! Based on wallet analysis (0xa42f12, 0xde17f): combined costs $0.76-0.95,
//! yielding 5-24% risk-free margin. No directional exposure needed.
//!
//! Run:
//!   cargo run --release --bin daily_prediction_bot                      # paper mode
//!   cargo run --release --bin daily_prediction_bot -- --live            # live mode
//!   cargo run --release --bin daily_prediction_bot -- --live --dry-run  # live book, no orders

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use tracing::{info, warn};

use super::core::{self, Executor, RedeemContext, TradeLogger};

// ── Daily Market Type ──

#[derive(Clone, Debug)]
pub struct DailyMarket {
    pub condition_id: String,
    pub token_id_yes: String,
    pub token_id_no: String,
    pub question: String,
    pub end_date: DateTime<Utc>,
    pub tick_size: Decimal,
    pub neg_risk: bool,
}

impl DailyMarket {
    /// Whether this market has resolved (end date passed).
    pub fn is_resolved(&self) -> bool {
        Utc::now() >= self.end_date
    }

    /// Whether this market is still active for trading.
    pub fn is_active(&self) -> bool {
        Utc::now() < self.end_date
    }

    /// Hours until resolution.
    pub fn hours_to_resolution(&self) -> f64 {
        let secs = (self.end_date - Utc::now()).num_seconds().max(0);
        secs as f64 / 3600.0
    }
}

// ── Config ──

#[derive(Debug, Clone)]
pub struct DailyPredictionConfig {
    /// Maximum combined cost (Yes ask + No ask) to enter a pair.
    /// E.g., 0.97 means we need at least 3% margin.
    pub max_combined_cost: Decimal,
    /// Minimum margin after fees to consider a trade worthwhile.
    pub min_margin: Decimal,
    /// Shares to buy per side per trade.
    pub order_size: Decimal,
    /// Maximum total matched pairs to hold across all markets simultaneously.
    pub max_total_pairs: Decimal,
    /// How often to poll books (ms).
    pub poll_interval_ms: u64,
    /// How often to scan for new markets (seconds).
    pub market_discovery_secs: u64,
    /// How often to trigger redemption sweeps (seconds).
    pub redeem_interval_secs: u64,
    /// Minimum available book depth on both sides to trade.
    pub min_book_size: Decimal,
    /// Minimum hours until resolution to trade (skip markets about to resolve).
    pub min_hours_to_resolution: f64,
    /// Which coins to trade. Lowercase: "bitcoin", "ethereum".
    pub coins: Vec<String>,
    /// Live mode flag.
    pub live: bool,
    /// Dry-run: live book data but no order submission.
    pub dry_run: bool,
}

impl Default for DailyPredictionConfig {
    fn default() -> Self {
        Self {
            max_combined_cost: dec!(0.97),
            min_margin: dec!(0.03),
            order_size: dec!(100),
            max_total_pairs: dec!(2000),
            poll_interval_ms: 5000,
            market_discovery_secs: 60,
            redeem_interval_secs: 300,
            min_book_size: dec!(10),
            min_hours_to_resolution: 1.0,
            coins: vec!["bitcoin".to_string(), "ethereum".to_string()],
            live: false,
            dry_run: false,
        }
    }
}

// ── Market Discovery ──

/// Check if a CLOB question is a daily prediction market we want to trade.
fn is_daily_prediction_question(question: &str) -> bool {
    let q = question.to_lowercase();
    // Must mention a coin
    let has_coin = q.contains("bitcoin") || q.contains("ethereum");
    // Must be a threshold or range question
    let has_type = q.contains("above") || q.contains("between") || q.contains("below");
    // Must be a "will the price" question (excludes btc-updown, etc.)
    let has_price = q.contains("price");
    has_coin && has_type && has_price
}

/// Estimate CLOB cursor for daily prediction market scanning.
///
/// Daily prediction markets are created days before their resolution date,
/// so we scan back further (20K markets ~4 days) compared to btc-updown.
pub fn estimate_daily_cursor() -> String {
    use chrono::TimeZone;
    let baseline = 560_000u64;
    let baseline_date = Utc.with_ymd_and_hms(2026, 3, 9, 0, 0, 0).unwrap();
    let days_since = (Utc::now() - baseline_date).num_days().max(0) as u64;
    let estimated_total = baseline + days_since * 5000;
    // Look back 20K markets (~4 days) to catch daily markets created earlier
    let start = estimated_total.saturating_sub(20_000);
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(start.to_string())
}

/// Scan CLOB `/markets` endpoint for daily prediction markets.
///
/// Returns markets with Yes/No outcomes that match daily prediction patterns.
/// Only returns active (accepting_orders) markets.
pub fn scan_daily_prediction_markets(start_cursor: &str) -> (Vec<DailyMarket>, String) {
    let mut result = Vec::new();
    let mut cursor = start_cursor.to_string();
    let now = Utc::now();

    for _batch in 0..50 {
        let url = format!(
            "{}/markets?limit=1000&next_cursor={}",
            core::CLOB_HOST,
            cursor
        );
        let resp = match ureq::get(&url).call() {
            Ok(r) => r,
            Err(e) => {
                warn!("[daily-scan] HTTP error: {e}");
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
            if !is_daily_prediction_question(question) {
                continue;
            }

            // Must be accepting orders (active market)
            if m["accepting_orders"].as_bool() != Some(true) {
                continue;
            }

            let cid = m["condition_id"].as_str().unwrap_or("");
            if cid.is_empty() {
                continue;
            }

            // Parse end date
            let end_str = m["end_date_iso"].as_str().unwrap_or("");
            let end_date = if !end_str.is_empty() {
                match end_str.parse::<DateTime<Utc>>() {
                    Ok(d) => d,
                    Err(_) => {
                        // Try parsing date-only format (YYYY-MM-DD) — add end of day
                        match chrono::NaiveDate::parse_from_str(end_str, "%Y-%m-%d") {
                            Ok(d) => d.and_hms_opt(23, 59, 59).unwrap().and_utc(),
                            Err(_) => continue,
                        }
                    }
                }
            } else {
                continue;
            };

            // Skip already resolved markets
            if end_date < now {
                continue;
            }

            // Parse tokens — outcomes are "Yes" and "No"
            let tokens = match m["tokens"].as_array() {
                Some(t) if t.len() >= 2 => t,
                _ => continue,
            };

            let mut yes_id = String::new();
            let mut no_id = String::new();
            for tok in tokens {
                let outcome = tok["outcome"].as_str().unwrap_or("");
                let tid = tok["token_id"].as_str().unwrap_or("");
                match outcome {
                    "Yes" => yes_id = tid.to_string(),
                    "No" => no_id = tid.to_string(),
                    _ => {}
                }
            }
            if yes_id.is_empty() || no_id.is_empty() {
                continue;
            }

            let tick_str = m["minimum_tick_size"].as_str().unwrap_or("0.01");
            let tick_size = tick_str.parse::<Decimal>().unwrap_or(dec!(0.01));
            let neg_risk = m["neg_risk"].as_bool().unwrap_or(false);

            result.push(DailyMarket {
                condition_id: cid.to_string(),
                token_id_yes: yes_id,
                token_id_no: no_id,
                question: question.to_string(),
                end_date,
                tick_size,
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

// ── Position Tracking ──

#[derive(Debug, Clone, Default)]
struct PairPosition {
    yes_qty: Decimal,
    yes_cost: Decimal,
    no_qty: Decimal,
    no_cost: Decimal,
    matched_pairs: Decimal,
    question: String,
}

impl PairPosition {
    fn avg_yes_cost(&self) -> Decimal {
        if self.yes_qty.is_zero() {
            Decimal::ZERO
        } else {
            self.yes_cost / self.yes_qty
        }
    }

    fn avg_no_cost(&self) -> Decimal {
        if self.no_qty.is_zero() {
            Decimal::ZERO
        } else {
            self.no_cost / self.no_qty
        }
    }

    fn combined_cost_per_pair(&self) -> Decimal {
        self.avg_yes_cost() + self.avg_no_cost()
    }

    fn profit_per_pair(&self) -> Decimal {
        Decimal::ONE - self.combined_cost_per_pair()
    }

    fn complete_pairs(&self) -> Decimal {
        self.yes_qty.min(self.no_qty)
    }

    fn total_invested(&self) -> Decimal {
        self.yes_cost + self.no_cost
    }
}

// ── Dashboard State ──

#[derive(Debug, Clone, Default, Serialize)]
pub struct Dashboard {
    pub mode: String,
    pub uptime_secs: u64,
    pub markets_discovered: usize,
    pub markets_traded: usize,
    pub total_pairs_bought: u32,
    pub total_pairs_redeemed: u32,
    pub session_pnl: f64,
    pub total_invested: f64,
    pub avg_combined_cost: f64,
    pub avg_margin: f64,
    pub last_trade_at: String,
    pub last_redeem_at: String,
}

pub type SharedDashboard = Arc<RwLock<Dashboard>>;

// ── Main Strategy Loop ──

pub async fn run(
    config: DailyPredictionConfig,
    executor: Option<Executor>,
    redeem_ctx: Option<Arc<RedeemContext>>,
) -> Result<()> {
    let start_time = Instant::now();
    let dashboard: SharedDashboard = Arc::new(RwLock::new(Dashboard {
        mode: if config.live && !config.dry_run {
            "LIVE".to_string()
        } else if config.live && config.dry_run {
            "DRY-RUN".to_string()
        } else {
            "PAPER".to_string()
        },
        ..Default::default()
    }));

    let poll_interval = Duration::from_millis(config.poll_interval_ms);

    // State
    let mut positions: HashMap<String, PairPosition> = HashMap::new();
    let mut last_discovery = Instant::now() - Duration::from_secs(120);
    let mut last_redeem = Instant::now() - Duration::from_secs(config.redeem_interval_secs);
    let mut last_status = Instant::now();
    let mut total_pairs_bought = 0u32;
    let mut total_pnl = 0.0_f64;
    let mut total_invested_dec = Decimal::ZERO;

    // CLOB market cache — use wider lookback for daily markets
    let clob_start_cursor = estimate_daily_cursor();
    info!("[daily] Initial CLOB scan for daily prediction markets...");
    let mut market_cache: Vec<DailyMarket>;
    {
        let cursor = clob_start_cursor.clone();
        let (markets, _) =
            tokio::task::spawn_blocking(move || scan_daily_prediction_markets(&cursor))
                .await
                .context("Initial CLOB scan failed")?;
        info!("[daily] Found {} daily prediction markets", markets.len());
        for m in &markets {
            info!(
                "[daily]   {} | resolves in {:.1}h | neg_risk={}",
                m.question,
                m.hours_to_resolution(),
                m.neg_risk
            );
        }
        market_cache = markets;
    }

    // Trade log
    let log_dir = "strategies/daily_prediction/logs";
    std::fs::create_dir_all(log_dir).ok();
    let log_path = format!(
        "{}/trades_{}.csv",
        log_dir,
        Utc::now().format("%Y%m%d_%H%M%S")
    );
    let mut trade_log = TradeLogger::new(
        &log_path,
        "timestamp,condition_id,question,action,yes_price,no_price,combined_cost,margin,size,expected_pnl",
    )?;
    info!("[daily] Trade log: {}", log_path);

    println!("\n{}", "=".repeat(70));
    println!("  DAILY PREDICTION PAIR-ARB STRATEGY");
    println!(
        "  max_cost={} min_margin={} size={} max_pairs={}",
        config.max_combined_cost, config.min_margin, config.order_size, config.max_total_pairs
    );
    println!(
        "  coins: {:?} | min_hours={:.1} | min_book_size={}",
        config.coins, config.min_hours_to_resolution, config.min_book_size
    );
    println!(
        "  mode: {}{}",
        if config.live { "LIVE" } else { "PAPER" },
        if config.dry_run { " (dry-run)" } else { "" }
    );
    println!("{}\n", "=".repeat(70));

    loop {
        // ── Market Discovery ──
        if last_discovery.elapsed() > Duration::from_secs(config.market_discovery_secs) {
            last_discovery = Instant::now();

            let cursor = clob_start_cursor.clone();
            let coins = config.coins.clone();
            match tokio::task::spawn_blocking(move || {
                let (markets, next) = scan_daily_prediction_markets(&cursor);
                // Filter by configured coins
                let filtered: Vec<DailyMarket> = markets
                    .into_iter()
                    .filter(|m| {
                        let q = m.question.to_lowercase();
                        coins.iter().any(|c| q.contains(c))
                    })
                    .collect();
                (filtered, next)
            })
            .await
            {
                Ok((new_markets, _)) => {
                    if new_markets.len() != market_cache.len() {
                        info!(
                            "[daily] Market cache updated: {} markets (was {})",
                            new_markets.len(),
                            market_cache.len()
                        );
                    }
                    market_cache = new_markets;
                }
                Err(e) => warn!("[daily] CLOB scan failed: {e}"),
            }
        }

        // ── Evaluate Markets ──
        let current_total_pairs: Decimal = positions.values().map(|p| p.complete_pairs()).sum();

        // Filter to active, tradeable markets
        let tradeable: Vec<&DailyMarket> = market_cache
            .iter()
            .filter(|m| {
                m.is_active()
                    && m.hours_to_resolution() >= config.min_hours_to_resolution
                    && !positions.contains_key(&m.condition_id)
            })
            .collect();

        for market in &tradeable {
            if current_total_pairs >= config.max_total_pairs {
                info!(
                    "[daily] Max total pairs ({}) reached, skipping",
                    config.max_total_pairs
                );
                break;
            }

            // Fetch books for both Yes and No tokens
            let yes_tid = market.token_id_yes.clone();
            let no_tid = market.token_id_no.clone();
            let (yes_book, no_book) = tokio::task::spawn_blocking(move || {
                let yes = core::fetch_book(&yes_tid);
                let no = core::fetch_book(&no_tid);
                (yes, no)
            })
            .await
            .context("Book fetch task failed")?;

            let yes_book = match yes_book {
                Some(b) if b.best_ask > Decimal::ZERO => b,
                _ => continue,
            };
            let no_book = match no_book {
                Some(b) if b.best_ask > Decimal::ZERO => b,
                _ => continue,
            };

            let combined_ask = yes_book.best_ask + no_book.best_ask;
            let margin = Decimal::ONE - combined_ask;
            let available_size = yes_book
                .ask_size
                .min(no_book.ask_size)
                .min(config.order_size);

            // Check book depth
            if available_size < config.min_book_size {
                continue;
            }

            // Check profitability
            if combined_ask > config.max_combined_cost {
                continue;
            }
            if margin < config.min_margin {
                continue;
            }

            // ── Execute pair buy ──
            let trade_size = available_size.min(config.order_size);
            let margin_pct: f64 = (margin * dec!(100)).to_string().parse().unwrap_or(0.0);

            println!(
                "--- OPPORTUNITY: {} | {:.1}h to resolution",
                market.question,
                market.hours_to_resolution()
            );
            println!(
                "    Yes ask={} ({} avail) | No ask={} ({} avail)",
                yes_book.best_ask, yes_book.ask_size, no_book.best_ask, no_book.ask_size
            );
            println!(
                "    combined={} | margin={:.1}% | size={}",
                combined_ask, margin_pct, trade_size
            );

            if let Some(ref exec) = executor {
                // Buy Yes side
                let yes_result = exec
                    .buy_fok(
                        &market.token_id_yes,
                        yes_book.best_ask,
                        trade_size,
                        market.tick_size,
                    )
                    .await;

                match yes_result {
                    Ok(yes_oid) => {
                        info!("[daily] Yes buy OK: order={yes_oid}");

                        // Buy No side
                        let no_result = exec
                            .buy_fok(
                                &market.token_id_no,
                                no_book.best_ask,
                                trade_size,
                                market.tick_size,
                            )
                            .await;

                        match no_result {
                            Ok(no_oid) => {
                                info!("[daily] No buy OK: order={no_oid}");

                                let pos = PairPosition {
                                    yes_qty: trade_size,
                                    yes_cost: yes_book.best_ask * trade_size,
                                    no_qty: trade_size,
                                    no_cost: no_book.best_ask * trade_size,
                                    matched_pairs: trade_size,
                                    question: market.question.clone(),
                                };

                                let pnl = pos.profit_per_pair() * trade_size;
                                let pnl_f64: f64 = pnl.to_string().parse().unwrap_or(0.0);
                                total_pnl += pnl_f64;
                                total_pairs_bought += 1;
                                total_invested_dec += pos.total_invested();

                                println!(
                                    "    PAIRED: {} pairs | cost={}/pair | profit=${:.2}",
                                    trade_size,
                                    pos.combined_cost_per_pair(),
                                    pnl_f64
                                );

                                let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
                                trade_log.write_line(&format!(
                                    "{},{},{},BUY_PAIR,{},{},{},{},{},{:.2}",
                                    ts,
                                    market.condition_id,
                                    market.question.replace(',', ";"),
                                    yes_book.best_ask,
                                    no_book.best_ask,
                                    pos.combined_cost_per_pair(),
                                    margin,
                                    trade_size,
                                    pnl_f64
                                ));

                                positions.insert(market.condition_id.clone(), pos);
                            }
                            Err(e) => {
                                warn!("[daily] No buy FAILED: {e}");
                                println!("    PARTIAL: Yes bought but No failed: {e}");

                                // Partial position — we have Yes tokens only
                                let pos = PairPosition {
                                    yes_qty: trade_size,
                                    yes_cost: yes_book.best_ask * trade_size,
                                    no_qty: Decimal::ZERO,
                                    no_cost: Decimal::ZERO,
                                    matched_pairs: Decimal::ZERO,
                                    question: market.question.clone(),
                                };
                                total_invested_dec += pos.yes_cost;
                                positions.insert(market.condition_id.clone(), pos);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("[daily] Yes buy FAILED: {e}");
                        println!("    SKIP: Yes buy failed: {e}");
                    }
                }
            } else {
                // Paper mode — simulate fill
                let pos = PairPosition {
                    yes_qty: trade_size,
                    yes_cost: yes_book.best_ask * trade_size,
                    no_qty: trade_size,
                    no_cost: no_book.best_ask * trade_size,
                    matched_pairs: trade_size,
                    question: market.question.clone(),
                };

                let pnl = pos.profit_per_pair() * trade_size;
                let pnl_f64: f64 = pnl.to_string().parse().unwrap_or(0.0);
                total_pnl += pnl_f64;
                total_pairs_bought += 1;
                total_invested_dec += pos.total_invested();

                println!(
                    "    [PAPER] PAIRED: {} pairs | cost={}/pair | profit=${:.2}",
                    trade_size,
                    pos.combined_cost_per_pair(),
                    pnl_f64
                );

                let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
                trade_log.write_line(&format!(
                    "{},{},{},BUY_PAIR,{},{},{},{},{},{:.2}",
                    ts,
                    market.condition_id,
                    market.question.replace(',', ";"),
                    yes_book.best_ask,
                    no_book.best_ask,
                    pos.combined_cost_per_pair(),
                    margin,
                    trade_size,
                    pnl_f64
                ));

                positions.insert(market.condition_id.clone(), pos);
            }
        }

        // ── Retry partial positions ──
        // If we have Yes but not No (or vice versa), try to complete
        let partials: Vec<(String, DailyMarket)> = positions
            .iter()
            .filter(|(_, p)| {
                p.matched_pairs.is_zero() && (p.yes_qty > Decimal::ZERO || p.no_qty > Decimal::ZERO)
            })
            .filter_map(|(cid, _)| {
                market_cache
                    .iter()
                    .find(|m| m.condition_id == *cid && m.is_active())
                    .map(|m| (cid.clone(), m.clone()))
            })
            .collect();

        for (cid, market) in partials {
            let pos = match positions.get(&cid) {
                Some(p) => p.clone(),
                None => continue,
            };

            // Need No side
            if pos.yes_qty > Decimal::ZERO && pos.no_qty.is_zero() {
                let no_tid = market.token_id_no.clone();
                let no_book = tokio::task::spawn_blocking(move || core::fetch_book(&no_tid))
                    .await
                    .ok()
                    .flatten();

                if let Some(no_book) = no_book {
                    if no_book.best_ask > Decimal::ZERO {
                        let combined = pos.avg_yes_cost() + no_book.best_ask;
                        let margin = Decimal::ONE - combined;
                        if combined <= config.max_combined_cost && margin >= config.min_margin {
                            let size = pos.yes_qty.min(no_book.ask_size);
                            if size >= config.min_book_size {
                                if let Some(ref exec) = executor {
                                    if let Ok(oid) = exec
                                        .buy_fok(
                                            &market.token_id_no,
                                            no_book.best_ask,
                                            size,
                                            market.tick_size,
                                        )
                                        .await
                                    {
                                        info!("[daily] Partial completion: No buy OK {oid}");
                                        let p = positions.get_mut(&cid).unwrap();
                                        p.no_qty = size;
                                        p.no_cost = no_book.best_ask * size;
                                        p.matched_pairs = p.yes_qty.min(p.no_qty);
                                        let pnl_f64: f64 = (p.profit_per_pair() * p.matched_pairs)
                                            .to_string()
                                            .parse()
                                            .unwrap_or(0.0);
                                        total_pnl += pnl_f64;
                                        total_invested_dec += p.no_cost;
                                        println!(
                                            "    COMPLETED partial: {} | cost={}/pair",
                                            market.question,
                                            p.combined_cost_per_pair()
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Need Yes side
            if pos.no_qty > Decimal::ZERO && pos.yes_qty.is_zero() {
                let yes_tid = market.token_id_yes.clone();
                let yes_book = tokio::task::spawn_blocking(move || core::fetch_book(&yes_tid))
                    .await
                    .ok()
                    .flatten();

                if let Some(yes_book) = yes_book {
                    if yes_book.best_ask > Decimal::ZERO {
                        let combined = yes_book.best_ask + pos.avg_no_cost();
                        let margin = Decimal::ONE - combined;
                        if combined <= config.max_combined_cost && margin >= config.min_margin {
                            let size = pos.no_qty.min(yes_book.ask_size);
                            if size >= config.min_book_size {
                                if let Some(ref exec) = executor {
                                    if let Ok(oid) = exec
                                        .buy_fok(
                                            &market.token_id_yes,
                                            yes_book.best_ask,
                                            size,
                                            market.tick_size,
                                        )
                                        .await
                                    {
                                        info!("[daily] Partial completion: Yes buy OK {oid}");
                                        let p = positions.get_mut(&cid).unwrap();
                                        p.yes_qty = size;
                                        p.yes_cost = yes_book.best_ask * size;
                                        p.matched_pairs = p.yes_qty.min(p.no_qty);
                                        let pnl_f64: f64 = (p.profit_per_pair() * p.matched_pairs)
                                            .to_string()
                                            .parse()
                                            .unwrap_or(0.0);
                                        total_pnl += pnl_f64;
                                        total_invested_dec += p.yes_cost;
                                        println!(
                                            "    COMPLETED partial: {} | cost={}/pair",
                                            market.question,
                                            p.combined_cost_per_pair()
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // ── Periodic Redemption Sweep ──
        if last_redeem.elapsed() > Duration::from_secs(config.redeem_interval_secs) {
            if let Some(ref ctx) = redeem_ctx {
                let ctx = Arc::clone(ctx);
                last_redeem = Instant::now();
                tokio::spawn(async move {
                    let (ok, fail) = core::redeem_sweep(&ctx).await;
                    if ok > 0 || fail > 0 {
                        info!("[daily] Redeem sweep: {ok} ok, {fail} failed");
                    }
                });
            }
        }

        // ── Dashboard Update ──
        {
            let mut d = dashboard.write();
            d.uptime_secs = start_time.elapsed().as_secs();
            d.markets_discovered = market_cache.len();
            d.markets_traded = positions.len();
            d.total_pairs_bought = total_pairs_bought;
            d.session_pnl = total_pnl;
            d.total_invested = total_invested_dec.to_string().parse().unwrap_or(0.0);
            if total_pairs_bought > 0 {
                let avg_cost: f64 = (total_invested_dec / Decimal::from(total_pairs_bought))
                    .to_string()
                    .parse()
                    .unwrap_or(0.0);
                d.avg_combined_cost = avg_cost;
                d.avg_margin = 1.0 - avg_cost;
            }
        }

        // ── Periodic Status ──
        if last_status.elapsed() > Duration::from_secs(30) {
            last_status = Instant::now();
            let matched: usize = positions
                .values()
                .filter(|p| p.matched_pairs > Decimal::ZERO)
                .count();
            let partial: usize = positions
                .values()
                .filter(|p| p.matched_pairs.is_zero())
                .count();
            let active_markets = market_cache.iter().filter(|m| m.is_active()).count();

            println!(
                "  [STATUS] markets={}/{} | pairs={} matched={} partial={} | pnl=${:.2} invested=${:.2} | uptime={}s",
                active_markets,
                market_cache.len(),
                total_pairs_bought,
                matched,
                partial,
                total_pnl,
                total_invested_dec.to_string().parse::<f64>().unwrap_or(0.0),
                start_time.elapsed().as_secs()
            );

            // Show top positions by margin
            let mut sorted_positions: Vec<(&String, &PairPosition)> = positions
                .iter()
                .filter(|(_, p)| p.matched_pairs > Decimal::ZERO)
                .collect();
            sorted_positions.sort_by(|a, b| {
                b.1.profit_per_pair()
                    .partial_cmp(&a.1.profit_per_pair())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            for (cid, pos) in sorted_positions.iter().take(5) {
                let margin_pct: f64 = (pos.profit_per_pair() * dec!(100))
                    .to_string()
                    .parse()
                    .unwrap_or(0.0);
                println!(
                    "    {} | {} pairs | cost={:.3} | margin={:.1}%",
                    &pos.question[..pos.question.len().min(50)],
                    pos.matched_pairs,
                    pos.combined_cost_per_pair(),
                    margin_pct
                );
                let _ = cid; // used for debug if needed
            }
        }

        // ── Clean up resolved positions ──
        // After a market resolves and enough time passes, remove from tracking
        // (redemption handles the actual payout)
        let resolved_cids: Vec<String> = positions
            .keys()
            .filter(|cid| {
                market_cache
                    .iter()
                    .find(|m| m.condition_id == **cid)
                    .map(|m| m.is_resolved())
                    .unwrap_or(true) // if not in cache anymore, assume resolved
            })
            .cloned()
            .collect();

        for cid in &resolved_cids {
            if let Some(pos) = positions.get(cid) {
                if pos.matched_pairs > Decimal::ZERO {
                    info!(
                        "[daily] Market resolved: {} | {} pairs @ cost={:.3}",
                        pos.question,
                        pos.matched_pairs,
                        pos.combined_cost_per_pair()
                    );
                }
            }
            // Keep for a while so redeem_sweep picks it up, but stop re-evaluating
        }

        tokio::time::sleep(poll_interval).await;
    }
}
