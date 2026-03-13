//! Post-Resolution Winner Token Buying Strategy
//!
//! After a market resolves, buys ONLY the winner token at a discount to $1.00.
//! Redeems at $1.00 for guaranteed profit. Zero directional risk — winner is
//! already known. The only risk is not getting filled.
//!
//! Based on corrected 700K-row analysis (2026-03-10):
//! - 0xd84c2b buys winners at $0.991 post-res, 13K trades over 2 days
//! - 0xba2643 same pattern — winner tokens at $0.991
//! - NO top wallet does post-res pair completion (previous analysis had timing bug)
//!
//! Run:
//!   cargo run --release --bin post_resolution_maker_bot                      # paper mode
//!   cargo run --release --bin post_resolution_maker_bot -- --live            # live mode
//!   cargo run --release --bin post_resolution_maker_bot -- --live --dry-run  # live books, log only

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, info, warn};

use super::core::{self, BookSnapshot, Executor, Market, RedeemContext, TradeLogger};

// ── Config ──

#[derive(Debug, Clone)]
pub struct PostResMakerConfig {
    /// Bid prices for winner token, from widest discount to narrowest.
    /// E.g., [0.95, 0.96, 0.97, 0.98, 0.99] → $0.01-0.05 profit per share.
    pub bid_levels: Vec<Decimal>,
    /// Shares to bid at each price level.
    pub shares_per_level: Decimal,
    /// Maximum total markets to have active bids in simultaneously.
    pub max_active_markets: usize,
    /// How often to poll for fills and new markets (ms).
    pub poll_interval_ms: u64,
    /// Minimum seconds after resolution before placing bids.
    pub min_post_res_secs: i64,
    /// Maximum seconds after resolution to consider entering a market.
    pub max_post_res_secs: i64,
    /// Seconds after placing bids to wait before cancelling unfilled orders.
    pub bid_timeout_secs: u64,
    /// How often to scan for new resolved markets (seconds).
    pub discovery_interval_secs: u64,
    /// How often to trigger redemption sweeps (seconds).
    pub redeem_interval_secs: u64,
    /// Which market durations to scan (in minutes).
    pub allowed_durations: Vec<u32>,
    /// Which coins to trade. Empty = all.
    pub coins: Vec<String>,
    /// Live mode flag.
    pub live: bool,
    /// Dry-run: live book data but no order submission, no simulated fills.
    pub dry_run: bool,
}

impl Default for PostResMakerConfig {
    fn default() -> Self {
        Self {
            bid_levels: vec![dec!(0.95), dec!(0.96), dec!(0.97), dec!(0.98), dec!(0.99)],
            shares_per_level: dec!(100),
            max_active_markets: 50,
            poll_interval_ms: 3000,
            min_post_res_secs: 3,
            max_post_res_secs: 300,
            bid_timeout_secs: 120,
            discovery_interval_secs: 5,
            redeem_interval_secs: 120,
            allowed_durations: vec![5],
            coins: vec![],
            live: false,
            dry_run: false,
        }
    }
}

// ── Position Tracking ──

#[derive(Debug, Clone)]
struct LevelPosition {
    /// Price of the bid at this level.
    price: Decimal,
    /// GTC order ID (None if paper mode).
    order_id: Option<String>,
    /// Shares filled at this level.
    filled: Decimal,
    /// Cost of filled shares.
    cost: Decimal,
}

#[derive(Debug, Clone)]
struct MarketPosition {
    /// The market we're trading.
    market: Market,
    /// Which side won (true = Up won).
    up_won: bool,
    /// Token ID of the winner.
    winner_token_id: String,
    /// Bid levels for the winner token.
    levels: Vec<LevelPosition>,
    /// When we placed bids.
    bids_placed_at: Instant,
    /// Whether we've cancelled unfilled bids.
    bids_cancelled: bool,
    /// Current phase.
    phase: MarketPhase,
}

#[derive(Debug, Clone, PartialEq)]
enum MarketPhase {
    /// Bids placed, waiting for fills.
    Bidding,
    /// Bid timeout expired, cancelling unfilled.
    Cancelling,
    /// Done — filled or cancelled.
    Complete,
}

impl MarketPosition {
    fn total_filled(&self) -> Decimal {
        self.levels.iter().map(|l| l.filled).sum()
    }

    fn total_cost(&self) -> Decimal {
        self.levels.iter().map(|l| l.cost).sum()
    }

    fn avg_fill_price(&self) -> Decimal {
        let filled = self.total_filled();
        if filled > Decimal::ZERO {
            self.total_cost() / filled
        } else {
            Decimal::ZERO
        }
    }

    /// Profit = shares * ($1.00 - avg_price).
    fn total_profit(&self) -> Decimal {
        let filled = self.total_filled();
        if filled > Decimal::ZERO {
            filled * (Decimal::ONE - self.avg_fill_price())
        } else {
            Decimal::ZERO
        }
    }

    fn is_bid_timed_out(&self, timeout_secs: u64) -> bool {
        self.bids_placed_at.elapsed() > Duration::from_secs(timeout_secs)
    }

    fn unfilled_order_ids(&self) -> Vec<&str> {
        self.levels
            .iter()
            .filter(|l| l.order_id.is_some() && l.filled < Decimal::ONE)
            .filter_map(|l| l.order_id.as_deref())
            .filter(|id| !id.starts_with("paper-"))
            .collect()
    }
}

/// Determine which side won based on book state.
/// Post-resolution, winner side has best_bid near $1.00, loser side near $0.00.
fn determine_winner(up_book: &BookSnapshot, down_book: &BookSnapshot) -> bool {
    up_book.best_bid > down_book.best_bid
}

// ── Main Strategy Loop ──

pub async fn run(
    config: PostResMakerConfig,
    executor: Option<Executor>,
    redeem_ctx: Option<Arc<RedeemContext>>,
) -> Result<()> {
    let start_time = Instant::now();
    let poll_interval = Duration::from_millis(config.poll_interval_ms);

    // State
    let mut positions: HashMap<String, MarketPosition> = HashMap::new();
    let mut completed_markets: HashMap<String, Instant> = HashMap::new();
    let mut last_discovery = Instant::now() - Duration::from_secs(60);
    let mut last_redeem = Instant::now() - Duration::from_secs(config.redeem_interval_secs);
    let mut last_heartbeat = Instant::now() - Duration::from_secs(30);
    let mut total_filled = Decimal::ZERO;
    let mut total_profit = Decimal::ZERO;
    let mut markets_entered = 0u64;
    let mut markets_with_fills = 0u64;

    // CLOB market cache
    let clob_start_cursor = core::estimate_clob_start_cursor();
    info!("[winner] Initial CLOB scan...");
    let mut clob_market_cache: Vec<Market>;
    {
        let cursor = clob_start_cursor.clone();
        let durations = config.allowed_durations.clone();
        let (markets, _) =
            tokio::task::spawn_blocking(move || core::scan_clob_markets(&cursor, &durations, true))
                .await
                .context("Initial CLOB scan failed")?;
        info!("[winner] Found {} markets in initial scan", markets.len());
        clob_market_cache = markets;
    }
    let mut last_clob_refresh = Instant::now();

    // Trade log
    let log_dir = "strategies/post_resolution_maker/logs";
    std::fs::create_dir_all(log_dir).ok();
    let log_path = format!(
        "{}/winner_trades_{}.csv",
        log_dir,
        Utc::now().format("%Y%m%d_%H%M%S")
    );
    let mut trade_log = TradeLogger::new(
        &log_path,
        "timestamp,condition_id,action,winner,price,qty,filled,cost,profit,best_bid,best_ask,ask_size",
    )?;

    let levels_str: Vec<String> = config.bid_levels.iter().map(|l| l.to_string()).collect();
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║      POST-RESOLUTION WINNER TOKEN BUYER                    ║");
    println!(
        "║  levels=[{}] x {} shares                     ║",
        levels_str.join(","),
        config.shares_per_level,
    );
    println!(
        "║  window: {}s - {}s | timeout={}s                       ║",
        config.min_post_res_secs, config.max_post_res_secs, config.bid_timeout_secs,
    );
    println!(
        "║  mode: {}{}                                          ║",
        if config.live { "LIVE" } else { "PAPER" },
        if config.dry_run {
            " (dry-run: log only)"
        } else {
            ""
        }
    );
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    loop {
        // ── Market Discovery ──
        if last_discovery.elapsed() > Duration::from_secs(config.discovery_interval_secs) {
            last_discovery = Instant::now();

            // Refresh CLOB cache periodically
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
                        debug!(
                            "[winner] CLOB cache refreshed: {} markets",
                            new_markets.len()
                        );
                        clob_market_cache = new_markets;
                    }
                    Ok(_) => {}
                    Err(e) => warn!("[winner] CLOB refresh failed: {e}"),
                }
            }

            // Find resolved markets in our window
            let active_count = positions
                .values()
                .filter(|p| p.phase != MarketPhase::Complete)
                .count();

            if active_count < config.max_active_markets {
                let resolved_markets: Vec<Market> = clob_market_cache
                    .iter()
                    .filter(|m| {
                        let secs = m.secs_since_end();
                        m.is_resolved()
                            && secs >= config.min_post_res_secs
                            && secs <= config.max_post_res_secs
                            && !positions.contains_key(&m.condition_id)
                            && !completed_markets.contains_key(&m.condition_id)
                    })
                    .cloned()
                    .collect();

                for market in resolved_markets {
                    if active_count + positions.len() >= config.max_active_markets {
                        break;
                    }

                    // Coin filter
                    if !config.coins.is_empty() {
                        let q_lower = market.question.to_lowercase();
                        if !config
                            .coins
                            .iter()
                            .any(|c| q_lower.contains(&c.to_lowercase()))
                        {
                            continue;
                        }
                    }

                    let secs_since = market.secs_since_end();

                    // Fetch books for both sides
                    let (up_book, down_book) = tokio::task::spawn_blocking({
                        let m = market.clone();
                        move || core::fetch_market_books(&m)
                    })
                    .await
                    .context("Book fetch failed")?;

                    let up_book = match up_book {
                        Some(b) => b,
                        None => {
                            debug!("[winner] No Up book for {}", &market.condition_id[..12]);
                            continue;
                        }
                    };
                    let down_book = match down_book {
                        Some(b) => b,
                        None => {
                            debug!("[winner] No Down book for {}", &market.condition_id[..12]);
                            continue;
                        }
                    };

                    // Determine winner from book state
                    let up_won = determine_winner(&up_book, &down_book);
                    let (winner_book, winner_token_id, winner_label) = if up_won {
                        (&up_book, market.token_id_up.clone(), "Up")
                    } else {
                        (&down_book, market.token_id_down.clone(), "Down")
                    };

                    // Log book state for all modes
                    let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
                    trade_log.write_line(&format!(
                        "{},{},BOOK_SNAPSHOT,{},,,,,{},{},{}",
                        ts,
                        market.condition_id,
                        winner_label,
                        winner_book.best_bid,
                        winner_book.best_ask,
                        winner_book.ask_size,
                    ));

                    info!(
                        "[winner] {} | {}s post-res | winner={} | bid={} ask={} ask_sz={}",
                        &market.question[..market.question.len().min(60)],
                        secs_since,
                        winner_label,
                        winner_book.best_bid,
                        winner_book.best_ask,
                        winner_book.ask_size,
                    );

                    // Dry-run: just log, don't place orders or track positions
                    if config.dry_run {
                        for level_price in &config.bid_levels {
                            let would_fill = winner_book.best_ask > Decimal::ZERO
                                && winner_book.best_ask <= *level_price;
                            info!(
                                "[winner] [DRY-RUN] Would bid {} @ {} | fill={} (ask={})",
                                config.shares_per_level,
                                level_price,
                                if would_fill { "YES" } else { "NO" },
                                winner_book.best_ask,
                            );
                            trade_log.write_line(&format!(
                                "{},{},DRY_BID,{},{},{},{},{},{},{},{}",
                                ts,
                                market.condition_id,
                                winner_label,
                                level_price,
                                config.shares_per_level,
                                if would_fill {
                                    config.shares_per_level
                                } else {
                                    Decimal::ZERO
                                },
                                Decimal::ZERO,
                                Decimal::ZERO,
                                winner_book.best_bid,
                                winner_book.best_ask,
                            ));
                        }
                        completed_markets.insert(market.condition_id.clone(), Instant::now());
                        continue;
                    }

                    // Build bid levels
                    let mut levels: Vec<LevelPosition> = config
                        .bid_levels
                        .iter()
                        .map(|&price| LevelPosition {
                            price,
                            order_id: None,
                            filled: Decimal::ZERO,
                            cost: Decimal::ZERO,
                        })
                        .collect();

                    if let Some(ref exec) = executor {
                        // Live mode: batch place all GTC bids in single HTTP call
                        let batch: Vec<(
                            String,
                            polymarket_client_sdk::clob::types::Side,
                            Decimal,
                            Decimal,
                            Decimal,
                        )> = config
                            .bid_levels
                            .iter()
                            .map(|&price| {
                                (
                                    winner_token_id.clone(),
                                    polymarket_client_sdk::clob::types::Side::Buy,
                                    price,
                                    config.shares_per_level,
                                    market.tick_size,
                                )
                            })
                            .collect();

                        match exec.place_batch_gtc(&batch).await {
                            Ok(results) => {
                                for (idx, order_id) in results {
                                    if let Some(level) = levels.get_mut(idx) {
                                        info!(
                                            "[winner] Bid placed: {} @ {} ({})",
                                            config.shares_per_level,
                                            level.price,
                                            &order_id[..8.min(order_id.len())]
                                        );
                                        level.order_id = Some(order_id);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("[winner] Batch bid placement failed: {e}");
                            }
                        }
                    } else {
                        // Paper mode: create paper order IDs
                        let ts_ms = Utc::now().timestamp_millis();
                        for level in &mut levels {
                            level.order_id = Some(format!("paper-{}-{}", level.price, ts_ms));
                        }
                    }

                    // Check at least one bid was placed
                    if levels.iter().all(|l| l.order_id.is_none()) {
                        warn!("[winner] All bids failed, skipping market");
                        completed_markets.insert(market.condition_id.clone(), Instant::now());
                        continue;
                    }

                    let level_strs: Vec<String> = levels
                        .iter()
                        .filter(|l| l.order_id.is_some())
                        .map(|l| l.price.to_string())
                        .collect();
                    println!(
                        "╭─ BIDS: {} | {}s post-res | winner={}",
                        &market.question[..market.question.len().min(50)],
                        secs_since,
                        winner_label,
                    );
                    println!(
                        "│  levels=[{}] x {} shares | ask={}",
                        level_strs.join(","),
                        config.shares_per_level,
                        winner_book.best_ask,
                    );
                    println!(
                        "╰─ Waiting for fills (timeout={}s)...",
                        config.bid_timeout_secs
                    );

                    for level in &levels {
                        if level.order_id.is_some() {
                            trade_log.write_line(&format!(
                                "{},{},BID_PLACED,{},{},{},0,0,0,{},{},{}",
                                ts,
                                market.condition_id,
                                winner_label,
                                level.price,
                                config.shares_per_level,
                                winner_book.best_bid,
                                winner_book.best_ask,
                                winner_book.ask_size,
                            ));
                        }
                    }

                    markets_entered += 1;
                    positions.insert(
                        market.condition_id.clone(),
                        MarketPosition {
                            market,
                            up_won,
                            winner_token_id,
                            levels,
                            bids_placed_at: Instant::now(),
                            bids_cancelled: false,
                            phase: MarketPhase::Bidding,
                        },
                    );
                }
            }
        }

        // ── Poll for Fills & Manage Positions ──
        let cids: Vec<String> = positions.keys().cloned().collect();
        for cid in &cids {
            let pos = match positions.get_mut(cid) {
                Some(p) => p,
                None => continue,
            };

            if pos.phase == MarketPhase::Complete {
                continue;
            }

            match pos.phase {
                MarketPhase::Bidding => {
                    if let Some(ref exec) = executor {
                        // Live mode: check real open orders
                        match exec.get_open_orders(&pos.winner_token_id).await {
                            Ok(orders) => {
                                for level in &mut pos.levels {
                                    let bid_id = match &level.order_id {
                                        Some(id) => id.clone(),
                                        None => continue,
                                    };
                                    if let Some(order) = orders.iter().find(|o| o.id == bid_id) {
                                        let newly_filled = order.size_matched;
                                        if newly_filled > level.filled {
                                            let delta = newly_filled - level.filled;
                                            level.cost += delta * order.price;
                                            level.filled = newly_filled;
                                            info!(
                                                "[winner] Fill @ {}: +{} (total={})",
                                                level.price, delta, level.filled,
                                            );
                                        }
                                    } else if level.filled == Decimal::ZERO {
                                        // Order disappeared with zero fills — likely cancelled
                                        // by exchange (resolved market) or insufficient balance.
                                        // Do NOT assume fill; mark order as gone.
                                        info!(
                                            "[winner] Order gone @ {} — no fill detected, marking cancelled",
                                            level.price,
                                        );
                                        level.order_id = None;
                                    }
                                }
                            }
                            Err(e) => {
                                debug!("[winner] Open orders query failed: {e}");
                            }
                        }
                    } else {
                        // Paper mode: check real book to decide fills
                        let book = tokio::task::spawn_blocking({
                            let token_id = pos.winner_token_id.clone();
                            move || core::fetch_book(&token_id)
                        })
                        .await
                        .ok()
                        .flatten();

                        if let Some(book) = book {
                            for level in &mut pos.levels {
                                if level.order_id.is_none() || level.filled > Decimal::ZERO {
                                    continue;
                                }
                                // Only fill if real ask exists and is <= our bid
                                if book.best_ask > Decimal::ZERO && book.best_ask <= level.price {
                                    // Fill at the ask price (realistic — we'd cross the spread)
                                    let fill_price = book.best_ask;
                                    let fill_qty = config.shares_per_level.min(book.ask_size);
                                    if fill_qty > Decimal::ZERO {
                                        level.filled = fill_qty;
                                        level.cost = fill_qty * fill_price;
                                        info!(
                                            "[winner] [PAPER] Fill @ {} (ask={}): {} shares",
                                            level.price, fill_price, fill_qty,
                                        );
                                    }
                                }
                            }
                        }
                    }

                    // Log fills
                    let filled = pos.total_filled();
                    if filled > Decimal::ZERO {
                        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
                        let profit = pos.total_profit();
                        let avg = pos.avg_fill_price();
                        trade_log.write_line(&format!(
                            "{},{},FILL_UPDATE,{},{},{},{},{},{},0,0",
                            ts,
                            cid,
                            if pos.up_won { "Up" } else { "Down" },
                            avg,
                            config.shares_per_level,
                            filled,
                            pos.total_cost(),
                            profit,
                        ));
                    }

                    // Check timeout
                    if pos.is_bid_timed_out(config.bid_timeout_secs) {
                        info!(
                            "[winner] Bid timeout for {} — cancelling unfilled",
                            &cid[..12]
                        );
                        pos.phase = MarketPhase::Cancelling;
                    }
                }

                MarketPhase::Cancelling => {
                    if !pos.bids_cancelled {
                        let to_cancel = pos.unfilled_order_ids();
                        let to_cancel_owned: Vec<String> =
                            to_cancel.iter().map(|s| s.to_string()).collect();

                        if !to_cancel_owned.is_empty() {
                            if let Some(ref exec) = executor {
                                let refs: Vec<&str> =
                                    to_cancel_owned.iter().map(|s| s.as_str()).collect();
                                match exec.cancel_orders(&refs).await {
                                    Ok(cancelled) => {
                                        info!(
                                            "[winner] Cancelled {} orders for {}",
                                            cancelled.len(),
                                            &cid[..12]
                                        );
                                    }
                                    Err(e) => {
                                        warn!("[winner] Cancel failed for {}: {e}", &cid[..12]);
                                    }
                                }
                            }
                        }

                        pos.bids_cancelled = true;
                    }

                    // Record results
                    let filled = pos.total_filled();
                    let profit = pos.total_profit();
                    if filled > Decimal::ZERO {
                        total_filled += filled;
                        total_profit += profit;
                        markets_with_fills += 1;
                        let profit_f64: f64 = profit.to_string().parse().unwrap_or(0.0);
                        println!(
                            "  + FILLED: {} | {} shares @ avg {} | profit=${:.4}",
                            &pos.market.question[..pos.market.question.len().min(40)],
                            filled,
                            pos.avg_fill_price(),
                            profit_f64,
                        );
                        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
                        trade_log.write_line(&format!(
                            "{},{},COMPLETE,{},{},{},{},{},{},0,0",
                            ts,
                            cid,
                            if pos.up_won { "Up" } else { "Down" },
                            pos.avg_fill_price(),
                            filled,
                            filled,
                            pos.total_cost(),
                            profit,
                        ));
                    } else {
                        debug!("[winner] No fills for {} — expired", &cid[..12]);
                        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
                        trade_log.write_line(&format!(
                            "{},{},EXPIRED,{},0,0,0,0,0,0,0",
                            ts,
                            cid,
                            if pos.up_won { "Up" } else { "Down" },
                        ));
                    }

                    pos.phase = MarketPhase::Complete;
                }

                MarketPhase::Complete => {}
            }
        }

        // Move completed positions to done list
        let completed: Vec<String> = positions
            .iter()
            .filter(|(_, p)| p.phase == MarketPhase::Complete)
            .map(|(k, _)| k.clone())
            .collect();
        for cid in completed {
            positions.remove(&cid);
            completed_markets.insert(cid, Instant::now());
        }

        // Clean up old completed (older than 15 min)
        completed_markets.retain(|_, when| when.elapsed() < Duration::from_secs(900));

        // ── Periodic Redemption Sweep ──
        if last_redeem.elapsed() > Duration::from_secs(config.redeem_interval_secs) {
            if let Some(ref ctx) = redeem_ctx {
                let ctx = Arc::clone(ctx);
                last_redeem = Instant::now();
                tokio::spawn(async move {
                    let (ok, fail) = core::redeem_sweep(&ctx).await;
                    if ok > 0 || fail > 0 {
                        info!("[winner] Redeem sweep: {ok} ok, {fail} failed");
                    }
                });
            }
        }

        // ── Heartbeat ──
        if last_heartbeat.elapsed() >= Duration::from_secs(30) {
            last_heartbeat = Instant::now();

            let active = positions
                .values()
                .filter(|p| p.phase != MarketPhase::Complete)
                .count();

            let bidding = positions
                .values()
                .filter(|p| p.phase == MarketPhase::Bidding)
                .count();

            let next_end = clob_market_cache
                .iter()
                .filter(|m| m.end_date > Utc::now())
                .map(|m| m.end_date)
                .min();
            let next_str = match next_end {
                Some(t) => format!("next res in {}s", (t - Utc::now()).num_seconds()),
                None => "no upcoming".to_string(),
            };

            let profit_f64: f64 = total_profit.to_string().parse().unwrap_or(0.0);
            let fill_rate = if markets_entered > 0 {
                (markets_with_fills as f64 / markets_entered as f64) * 100.0
            } else {
                0.0
            };

            println!(
                "  [HB] {}s | {} | active={} bidding={} | entered={} filled={} ({:.0}%) | shares={} profit=${:.4} | cache={}",
                start_time.elapsed().as_secs(),
                next_str,
                active,
                bidding,
                markets_entered,
                markets_with_fills,
                fill_rate,
                total_filled,
                profit_f64,
                clob_market_cache.len(),
            );
        }

        tokio::time::sleep(poll_interval).await;
    }
}
