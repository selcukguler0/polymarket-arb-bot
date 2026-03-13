//! Post-Resolution Pair Completion Strategy
//!
//! After a 5-min BTC Up/Down period resolves, there's a window (5-300s) where
//! winner tokens trade below $1.00 and loser tokens trade above $0.00.
//! Buy both sides → hold matched pairs → redeem at $1.00 per pair.
//!
//! Based on analysis of top-20 Polymarket wallets (84K trades):
//! - 19/20 wallets use this strategy exclusively
//! - Combined costs: $0.63-0.95 depending on speed
//! - Profit per share: $0.03-0.21
//!
//! Run:
//!   cargo run --release --bin post_resolution_bot                      # paper mode
//!   cargo run --release --bin post_resolution_bot -- --live            # live mode
//!   cargo run --release --bin post_resolution_bot -- --live --dry-run  # live book, no orders

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use tracing::{info, warn};

use super::core::{self, Executor, Market, RedeemContext, TradeLogger};

// ── Config ──

#[derive(Debug, Clone)]
pub struct PostResolutionConfig {
    /// Maximum combined cost (Up ask + Down ask) to enter a pair. E.g., 0.95 → 5% margin.
    pub max_combined_cost: Decimal,
    /// Minimum margin after fees to consider a trade worthwhile. E.g., 0.02 → 2%.
    pub min_margin: Decimal,
    /// Shares to buy per side per trade.
    pub order_size: Decimal,
    /// Maximum total matched pairs to hold across all markets simultaneously.
    pub max_total_pairs: Decimal,
    /// How often to poll books (ms).
    pub poll_interval_ms: u64,
    /// How often to scan for new markets (seconds).
    pub market_discovery_secs: u64,
    /// Maximum seconds after resolution to still consider buying.
    /// Books thin out fast — after 300s there's usually nothing left.
    pub max_post_resolution_secs: i64,
    /// Minimum seconds after resolution before buying (avoid racing against
    /// queue-racers who are faster). 0 = try immediately.
    pub min_post_resolution_secs: i64,
    /// How often to trigger redemption sweeps (seconds).
    pub redeem_interval_secs: u64,
    /// Which market durations to scan (in minutes).
    pub allowed_durations: Vec<u32>,
    /// Live mode flag.
    pub live: bool,
    /// Dry-run: live book data but no order submission.
    pub dry_run: bool,
}

impl Default for PostResolutionConfig {
    fn default() -> Self {
        Self {
            max_combined_cost: dec!(0.95),
            min_margin: dec!(0.02),
            order_size: dec!(50),
            max_total_pairs: dec!(500),
            poll_interval_ms: 2000,
            market_discovery_secs: 10,
            max_post_resolution_secs: 300,
            min_post_resolution_secs: 0,
            redeem_interval_secs: 120,
            allowed_durations: vec![5],
            live: false,
            dry_run: false,
        }
    }
}

// ── Position Tracking ──

#[derive(Debug, Clone, Default)]
struct PairPosition {
    up_qty: Decimal,
    up_cost: Decimal,
    down_qty: Decimal,
    down_cost: Decimal,
    matched_pairs: Decimal,
    status: PositionStatus,
}

#[derive(Debug, Clone, Default, PartialEq)]
enum PositionStatus {
    #[default]
    Open,
    BoughtUp,
    BoughtDown,
    BoughtBoth,
    Redeemed,
}

impl PairPosition {
    fn avg_up_cost(&self) -> Decimal {
        if self.up_qty.is_zero() {
            Decimal::ZERO
        } else {
            self.up_cost / self.up_qty
        }
    }

    fn avg_down_cost(&self) -> Decimal {
        if self.down_qty.is_zero() {
            Decimal::ZERO
        } else {
            self.down_cost / self.down_qty
        }
    }

    fn combined_cost_per_pair(&self) -> Decimal {
        self.avg_up_cost() + self.avg_down_cost()
    }

    fn profit_per_pair(&self) -> Decimal {
        Decimal::ONE - self.combined_cost_per_pair()
    }

    fn complete_pairs(&self) -> Decimal {
        self.up_qty.min(self.down_qty)
    }

    fn total_invested(&self) -> Decimal {
        self.up_cost + self.down_cost
    }
}

// ── Dashboard State ──

#[derive(Debug, Clone, Default, Serialize)]
pub struct Dashboard {
    pub mode: String,
    pub uptime_secs: u64,
    pub markets_tracked: usize,
    pub total_pairs_bought: u32,
    pub total_pairs_redeemed: u32,
    pub session_pnl: f64,
    pub avg_combined_cost: f64,
    pub avg_margin: f64,
    pub last_trade_at: String,
    pub last_redeem_at: String,
    pub active_positions: usize,
}

pub type SharedDashboard = Arc<RwLock<Dashboard>>;

// ── Main Strategy Loop ──

pub async fn run(
    config: PostResolutionConfig,
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
    let mut processed_markets: HashMap<String, Instant> = HashMap::new(); // condition_id → when we finished with it
    let mut last_discovery = Instant::now() - Duration::from_secs(60);
    let mut last_redeem = Instant::now() - Duration::from_secs(config.redeem_interval_secs);
    let mut last_heartbeat = Instant::now() - Duration::from_secs(30);
    let mut last_status = Instant::now();
    let mut total_pairs_bought = 0u32;
    let mut total_pnl = 0.0_f64;
    let mut total_invested = Decimal::ZERO;
    let mut fetch_latencies: Vec<u128> = Vec::new(); // track all book fetch latencies

    // CLOB market cache
    let clob_start_cursor = core::estimate_clob_start_cursor();
    info!("[post-res] Initial CLOB scan...");
    let mut clob_market_cache: Vec<Market>;
    {
        let cursor = clob_start_cursor.clone();
        let durations = config.allowed_durations.clone();
        let (markets, _) =
            tokio::task::spawn_blocking(move || core::scan_clob_markets(&cursor, &durations, true))
                .await
                .context("Initial CLOB scan failed")?;
        info!("[post-res] Found {} markets in initial scan", markets.len());
        clob_market_cache = markets;
    }
    let mut last_clob_refresh = Instant::now();

    // Trade log
    let log_dir = "strategies/post_resolution/logs";
    std::fs::create_dir_all(log_dir).ok();
    let log_path = format!(
        "{}/trades_{}.csv",
        log_dir,
        Utc::now().format("%Y%m%d_%H%M%S")
    );
    let mut trade_log = TradeLogger::new(
        &log_path,
        "timestamp,condition_id,action,side,qty,price,combined_cost,margin,total_pairs,pnl",
    )?;

    // Book snapshot log — captures EVERY resolved market book state for latency analysis
    let snapshot_path = format!(
        "{}/book_snapshots_{}.csv",
        log_dir,
        Utc::now().format("%Y%m%d_%H%M%S")
    );
    let mut snapshot_log = TradeLogger::new(
        &snapshot_path,
        "timestamp,condition_id,secs_post_res,book_fetch_ms,up_ask,up_ask_size,down_ask,down_ask_size,combined_ask,margin,tradeable",
    )?;

    info!("[post-res] Trade log: {}", log_path);
    info!("[post-res] Snapshot log: {}", snapshot_path);

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║      POST-RESOLUTION PAIR COMPLETION STRATEGY              ║");
    println!(
        "║  max_cost={} min_margin={} size={} max_pairs={}    ║",
        config.max_combined_cost, config.min_margin, config.order_size, config.max_total_pairs
    );
    println!(
        "║  window: {}s - {}s after resolution                     ║",
        config.min_post_resolution_secs, config.max_post_resolution_secs
    );
    println!(
        "║  mode: {}{}                                          ║",
        if config.live { "LIVE" } else { "PAPER" },
        if config.dry_run { " (dry-run)" } else { "" }
    );
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    loop {
        // ── Market Discovery ──
        if last_discovery.elapsed() > Duration::from_secs(config.market_discovery_secs) {
            last_discovery = Instant::now();

            // Periodically refresh CLOB cache
            if last_clob_refresh.elapsed() > Duration::from_secs(60) {
                last_clob_refresh = Instant::now();
                let cursor = clob_start_cursor.clone();
                let durations = config.allowed_durations.clone();
                match tokio::task::spawn_blocking(move || {
                    core::scan_clob_markets(&cursor, &durations, true)
                })
                .await
                {
                    Ok((new_markets, _)) if !new_markets.is_empty() => {
                        info!(
                            "[post-res] CLOB cache refreshed: {} markets",
                            new_markets.len()
                        );
                        clob_market_cache = new_markets;
                    }
                    Ok(_) => {}
                    Err(e) => warn!("[post-res] CLOB refresh failed: {e}"),
                }
            }

            // Find resolved markets within our window
            let resolved_markets: Vec<&Market> = clob_market_cache
                .iter()
                .filter(|m| {
                    let secs_since = m.secs_since_end();
                    m.is_resolved()
                        && secs_since >= config.min_post_resolution_secs
                        && secs_since <= config.max_post_resolution_secs
                        && !processed_markets.contains_key(&m.condition_id)
                })
                .collect();

            if !resolved_markets.is_empty() {
                info!(
                    "[post-res] {} resolved markets in window",
                    resolved_markets.len()
                );
            }

            // Check total position limit
            let current_total_pairs: Decimal = positions.values().map(|p| p.complete_pairs()).sum();

            for market in &resolved_markets {
                if current_total_pairs >= config.max_total_pairs {
                    info!(
                        "[post-res] Max total pairs ({}) reached, skipping",
                        config.max_total_pairs
                    );
                    break;
                }

                // Already have a position in this market?
                if positions.contains_key(&market.condition_id) {
                    continue;
                }

                let secs_since = market.secs_since_end();

                // Fetch books for both sides — measure latency
                let fetch_start = Instant::now();
                let (up_book, down_book) = tokio::task::spawn_blocking({
                    let market = (*market).clone();
                    move || core::fetch_market_books(&market)
                })
                .await
                .context("Book fetch task failed")?;
                let fetch_ms = fetch_start.elapsed().as_millis();

                fetch_latencies.push(fetch_ms);

                // Log snapshot regardless of tradability
                let up_ask = up_book
                    .as_ref()
                    .map(|b| b.best_ask)
                    .unwrap_or(Decimal::ZERO);
                let up_size = up_book
                    .as_ref()
                    .map(|b| b.ask_size)
                    .unwrap_or(Decimal::ZERO);
                let down_ask = down_book
                    .as_ref()
                    .map(|b| b.best_ask)
                    .unwrap_or(Decimal::ZERO);
                let down_size = down_book
                    .as_ref()
                    .map(|b| b.ask_size)
                    .unwrap_or(Decimal::ZERO);
                let snap_combined = up_ask + down_ask;
                let snap_margin = if up_ask > Decimal::ZERO && down_ask > Decimal::ZERO {
                    Decimal::ONE - snap_combined
                } else {
                    Decimal::ZERO
                };
                let tradeable = up_ask > Decimal::ZERO
                    && down_ask > Decimal::ZERO
                    && snap_combined <= config.max_combined_cost
                    && snap_margin >= config.min_margin;

                let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
                snapshot_log.write_line(&format!(
                    "{},{},{},{},{},{},{},{},{},{},{}",
                    ts,
                    market.condition_id,
                    secs_since,
                    fetch_ms,
                    up_ask,
                    up_size,
                    down_ask,
                    down_size,
                    snap_combined,
                    snap_margin,
                    tradeable
                ));

                let up_book = match up_book {
                    Some(b) if b.best_ask > Decimal::ZERO => b,
                    _ => {
                        info!(
                            "[post-res] No Up asks for {} ({}s post-res, fetch={}ms)",
                            &market.condition_id[..12.min(market.condition_id.len())],
                            secs_since,
                            fetch_ms
                        );
                        continue;
                    }
                };
                let down_book = match down_book {
                    Some(b) if b.best_ask > Decimal::ZERO => b,
                    _ => {
                        info!(
                            "[post-res] No Down asks for {} ({}s post-res, fetch={}ms)",
                            &market.condition_id[..12.min(market.condition_id.len())],
                            secs_since,
                            fetch_ms
                        );
                        continue;
                    }
                };

                let combined_ask = up_book.best_ask + down_book.best_ask;
                let margin = Decimal::ONE - combined_ask;
                let available_size = up_book
                    .ask_size
                    .min(down_book.ask_size)
                    .min(config.order_size);

                // Minimum 5 shares to be worth the effort
                if available_size < dec!(5) {
                    continue;
                }

                info!(
                    "[post-res] {} | {}s post-res | fetch={}ms | Up ask={} ({} avail) | Down ask={} ({} avail) | combined={} | margin={:.1}%",
                    &market.question,
                    secs_since, fetch_ms,
                    up_book.best_ask, up_book.ask_size,
                    down_book.best_ask, down_book.ask_size,
                    combined_ask,
                    (margin * dec!(100)).to_string().parse::<f64>().unwrap_or(0.0)
                );

                // Check profitability
                if combined_ask > config.max_combined_cost {
                    info!(
                        "[post-res] Combined cost {} > max {}, skipping",
                        combined_ask, config.max_combined_cost
                    );
                    processed_markets.insert(market.condition_id.clone(), Instant::now());
                    continue;
                }
                if margin < config.min_margin {
                    info!(
                        "[post-res] Margin {} < min {}, skipping",
                        margin, config.min_margin
                    );
                    continue;
                }

                // ── Execute pair buy ──
                println!(
                    "╭─ PAIR BUY: {} | {}s post-resolution",
                    market.question, secs_since
                );
                println!(
                    "│  Up ask={} Down ask={} | combined={} | margin={:.1}%",
                    up_book.best_ask,
                    down_book.best_ask,
                    combined_ask,
                    (margin * dec!(100))
                        .to_string()
                        .parse::<f64>()
                        .unwrap_or(0.0)
                );

                let trade_size = available_size.min(config.order_size);

                if let Some(ref exec) = executor {
                    // Buy Up side
                    let up_result = exec
                        .buy_fok(
                            &market.token_id_up,
                            up_book.best_ask,
                            trade_size,
                            market.tick_size,
                        )
                        .await;

                    match up_result {
                        Ok(order_id) => {
                            info!("[post-res] Up buy OK: order={order_id}");
                            println!(
                                "│  ✓ Up buy: {} shares @ {} (order={})",
                                trade_size,
                                up_book.best_ask,
                                &order_id[..8.min(order_id.len())]
                            );

                            // Buy Down side
                            let down_result = exec
                                .buy_fok(
                                    &market.token_id_down,
                                    down_book.best_ask,
                                    trade_size,
                                    market.tick_size,
                                )
                                .await;

                            match down_result {
                                Ok(order_id_down) => {
                                    info!("[post-res] Down buy OK: order={order_id_down}");
                                    println!(
                                        "│  ✓ Down buy: {} shares @ {} (order={})",
                                        trade_size,
                                        down_book.best_ask,
                                        &order_id_down[..8.min(order_id_down.len())]
                                    );

                                    let pos = PairPosition {
                                        up_qty: trade_size,
                                        up_cost: up_book.best_ask * trade_size,
                                        down_qty: trade_size,
                                        down_cost: down_book.best_ask * trade_size,
                                        matched_pairs: trade_size,
                                        status: PositionStatus::BoughtBoth,
                                    };

                                    let pnl = pos.profit_per_pair() * trade_size;
                                    let pnl_f64: f64 = pnl.to_string().parse().unwrap_or(0.0);
                                    total_pnl += pnl_f64;
                                    total_pairs_bought += 1;
                                    total_invested += pos.total_invested();

                                    println!(
                                        "╰─ PAIRED: {} pairs | cost={}/pair | expected profit=${:.2}",
                                        trade_size,
                                        pos.combined_cost_per_pair(),
                                        pnl_f64
                                    );

                                    // Log trade
                                    let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
                                    trade_log.write_line(&format!(
                                        "{},{},BUY_PAIR,BOTH,{},{},{},{},{},{:.2}",
                                        ts,
                                        market.condition_id,
                                        trade_size,
                                        combined_ask,
                                        pos.combined_cost_per_pair(),
                                        margin,
                                        trade_size,
                                        pnl_f64
                                    ));

                                    positions.insert(market.condition_id.clone(), pos);
                                }
                                Err(e) => {
                                    warn!("[post-res] Down buy FAILED: {e}");
                                    println!("│  ✗ Down buy failed: {e}");
                                    println!("╰─ PARTIAL: Have Up tokens only — will hold for redemption");

                                    // We bought Up but not Down — partial position
                                    let pos = PairPosition {
                                        up_qty: trade_size,
                                        up_cost: up_book.best_ask * trade_size,
                                        down_qty: Decimal::ZERO,
                                        down_cost: Decimal::ZERO,
                                        matched_pairs: Decimal::ZERO,
                                        status: PositionStatus::BoughtUp,
                                    };
                                    positions.insert(market.condition_id.clone(), pos);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("[post-res] Up buy FAILED: {e}");
                            println!("╰─ SKIP: Up buy failed: {e}");
                        }
                    }
                } else {
                    // Paper mode — simulate fill
                    let pos = PairPosition {
                        up_qty: trade_size,
                        up_cost: up_book.best_ask * trade_size,
                        down_qty: trade_size,
                        down_cost: down_book.best_ask * trade_size,
                        matched_pairs: trade_size,
                        status: PositionStatus::BoughtBoth,
                    };

                    let pnl = pos.profit_per_pair() * trade_size;
                    let pnl_f64: f64 = pnl.to_string().parse().unwrap_or(0.0);
                    total_pnl += pnl_f64;
                    total_pairs_bought += 1;
                    total_invested += pos.total_invested();

                    println!(
                        "╰─ [PAPER] PAIRED: {} pairs | cost={}/pair | profit=${:.2} | {}s post-res | fetch={}ms",
                        trade_size,
                        pos.combined_cost_per_pair(),
                        pnl_f64,
                        secs_since,
                        fetch_ms
                    );

                    let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
                    trade_log.write_line(&format!(
                        "{},{},BUY_PAIR,BOTH,{},{},{},{},{},{:.2}",
                        ts,
                        market.condition_id,
                        trade_size,
                        combined_ask,
                        pos.combined_cost_per_pair(),
                        margin,
                        trade_size,
                        pnl_f64
                    ));

                    positions.insert(market.condition_id.clone(), pos);
                }

                // Mark as processed so we don't re-buy
                processed_markets.insert(market.condition_id.clone(), Instant::now());
            }

            // Clean up old processed markets (older than 15 min)
            processed_markets.retain(|_, when| when.elapsed() < Duration::from_secs(900));
        }

        // ── Periodic Redemption Sweep ──
        if last_redeem.elapsed() > Duration::from_secs(config.redeem_interval_secs) {
            if let Some(ref ctx) = redeem_ctx {
                let ctx = Arc::clone(ctx);
                last_redeem = Instant::now();
                tokio::spawn(async move {
                    let (ok, fail) = core::redeem_sweep(&ctx).await;
                    if ok > 0 || fail > 0 {
                        info!("[post-res] Redeem sweep: {ok} ok, {fail} failed");
                    }
                });
            }
        }

        // ── Dashboard Update ──
        {
            let mut d = dashboard.write();
            d.uptime_secs = start_time.elapsed().as_secs();
            d.markets_tracked = clob_market_cache.len();
            d.total_pairs_bought = total_pairs_bought;
            d.session_pnl = total_pnl;
            d.active_positions = positions
                .values()
                .filter(|p| p.status != PositionStatus::Redeemed)
                .count();
            if total_pairs_bought > 0 {
                let avg_cost: f64 = (total_invested / Decimal::from(total_pairs_bought))
                    .to_string()
                    .parse()
                    .unwrap_or(0.0);
                d.avg_combined_cost = avg_cost;
                d.avg_margin = 1.0 - avg_cost;
            }
        }

        // ── Periodic Heartbeat (every 30s) ──
        if last_heartbeat.elapsed() >= Duration::from_secs(30) {
            last_heartbeat = Instant::now();

            // Find next resolving market
            let next_end = clob_market_cache
                .iter()
                .filter(|m| m.end_date > Utc::now())
                .map(|m| m.end_date)
                .min();
            let next_str = match next_end {
                Some(t) => {
                    let secs = (t - Utc::now()).num_seconds();
                    format!("next resolution in {}s", secs)
                }
                None => "no upcoming markets".to_string(),
            };

            let resolved_in_window = clob_market_cache
                .iter()
                .filter(|m| {
                    let s = m.secs_since_end();
                    m.is_resolved() && s >= 0 && s <= config.max_post_resolution_secs
                })
                .count();

            let active = positions
                .values()
                .filter(|p| p.status != PositionStatus::Redeemed)
                .count();

            let latency_str = if fetch_latencies.is_empty() {
                "no fetches yet".to_string()
            } else {
                let n = fetch_latencies.len();
                let avg = fetch_latencies.iter().sum::<u128>() / n as u128;
                let min = fetch_latencies.iter().min().copied().unwrap_or(0);
                let max = fetch_latencies.iter().max().copied().unwrap_or(0);
                let mut sorted = fetch_latencies.clone();
                sorted.sort();
                let p50 = sorted[n / 2];
                let p95 = sorted[(n as f64 * 0.95) as usize];
                format!(
                    "avg={}ms min={}ms p50={}ms p95={}ms max={}ms n={}",
                    avg, min, p50, p95, max, n
                )
            };

            println!(
                "  [HEARTBEAT] uptime={}s | {} | resolved_in_window={} | pairs={} active={} pnl=${:.2} | cached={}",
                start_time.elapsed().as_secs(),
                next_str,
                resolved_in_window,
                total_pairs_bought,
                active,
                total_pnl,
                clob_market_cache.len(),
            );
            println!("  [LATENCY]   book_fetch: {}", latency_str);
        }

        // ── Detailed Status (when trades exist) ──
        if last_status.elapsed() >= Duration::from_secs(60) {
            last_status = Instant::now();
            let active = positions
                .values()
                .filter(|p| p.status != PositionStatus::Redeemed)
                .count();
            if total_pairs_bought > 0 || active > 0 {
                println!(
                    "  [STATUS] pairs={} active={} pnl=${:.2} avg_cost={:.3} | uptime={}s",
                    total_pairs_bought,
                    active,
                    total_pnl,
                    dashboard.read().avg_combined_cost,
                    start_time.elapsed().as_secs()
                );
            }
        }

        tokio::time::sleep(poll_interval).await;
    }
}
