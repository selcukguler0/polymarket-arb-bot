//! Deep Discount Maker Strategy — 0xd0d605-style two-sided market making.
//!
//! Places wide grid bids on BOTH Up and Down across $0.05-$0.50, lets orders
//! rest for the entire period, sells excess inventory when imbalanced, and
//! holds remaining shares to resolution. Paired shares = guaranteed profit
//! ($1.00 - combined_cost). Excess = directional bet.
//!
//! Key differences from orchestrator_v2:
//! - No FV model, no Binance price feed, no cancel/replace cycle
//! - Fixed grid placed once at period open, left resting
//! - Accepts directional risk (target ~53% pairing, not 100%)
//! - Sells excess inventory mid-period (loss-cutting DCA)
//!
//! Run:
//!   cargo run --release --bin deep_discount_maker_bot                      # paper mode
//!   cargo run --release --bin deep_discount_maker_bot -- --live            # live mode
//!   cargo run --release --bin deep_discount_maker_bot -- --live --dry-run  # live books, log only

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, info, warn};

use polymarket_client_sdk::clob::types::Side;

use super::core::{self, Executor, Market, RedeemContext, TradeLogger};

// ── Config ──

#[derive(Debug, Clone)]
pub struct DeepDiscountConfig {
    // Price grid
    pub grid_min_price: Decimal,
    pub grid_max_price: Decimal,
    pub grid_step: Decimal,
    pub shares_per_level: Decimal,

    // Timing
    pub entry_delay_secs: u64,
    pub stop_new_orders_secs: u64,
    pub cancel_all_secs: u64,

    // Inventory management
    pub sell_enabled: bool,
    pub sell_imbalance_threshold: Decimal,
    pub sell_cooldown_secs: u64,
    pub sell_max_loss_per_share: Decimal,

    // Markets
    pub allowed_durations: Vec<u32>,
    pub coins: Vec<String>,

    // Risk
    pub max_position_per_side: Decimal,
    pub max_total_spend: Decimal,

    // Mode
    pub live: bool,
    pub dry_run: bool,

    // Polling
    pub poll_interval_ms: u64,
    pub discovery_interval_secs: u64,
    pub redeem_interval_secs: u64,
}

impl Default for DeepDiscountConfig {
    fn default() -> Self {
        Self {
            grid_min_price: dec!(0.05),
            grid_max_price: dec!(0.50),
            grid_step: dec!(0.05),
            shares_per_level: dec!(15),
            entry_delay_secs: 5,
            stop_new_orders_secs: 60,
            cancel_all_secs: 30,
            sell_enabled: true,
            sell_imbalance_threshold: dec!(50),
            sell_cooldown_secs: 5,
            sell_max_loss_per_share: dec!(0.30),
            allowed_durations: vec![5],
            coins: vec!["BTC".to_string()],
            max_position_per_side: dec!(300),
            max_total_spend: dec!(200),
            live: false,
            dry_run: false,
            poll_interval_ms: 2000,
            discovery_interval_secs: 5,
            redeem_interval_secs: 120,
        }
    }
}

impl DeepDiscountConfig {
    /// Generate the bid price levels from grid parameters.
    pub fn grid_levels(&self) -> Vec<Decimal> {
        let mut levels = Vec::new();
        let mut price = self.grid_min_price;
        while price <= self.grid_max_price {
            levels.push(price);
            price += self.grid_step;
        }
        levels
    }
}

// ── Position Tracking ──

#[derive(Debug, Clone)]
struct SideInventory {
    /// Total shares filled on this side.
    shares: Decimal,
    /// Total cost (USDC spent) on this side.
    cost: Decimal,
    /// Shares sold mid-period (inventory management).
    shares_sold: Decimal,
    /// Revenue from mid-period sells.
    sell_revenue: Decimal,
    /// GTC order IDs placed on this side (for cancellation).
    order_ids: Vec<String>,
}

impl Default for SideInventory {
    fn default() -> Self {
        Self {
            shares: Decimal::ZERO,
            cost: Decimal::ZERO,
            shares_sold: Decimal::ZERO,
            sell_revenue: Decimal::ZERO,
            order_ids: Vec::new(),
        }
    }
}

impl SideInventory {
    /// Net shares held (bought - sold).
    fn net_shares(&self) -> Decimal {
        self.shares - self.shares_sold
    }

    /// Average cost per share (for bought shares, not sold).
    fn avg_cost(&self) -> Decimal {
        if self.shares > Decimal::ZERO {
            self.cost / self.shares
        } else {
            Decimal::ZERO
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum MarketPhase {
    /// Waiting for entry_delay after period start.
    WaitingEntry,
    /// Grid bids placed, accumulating fills.
    Accumulating,
    /// Near period end, cancelling resting orders.
    Cancelling,
    /// Holding to resolution.
    HoldingToExpiry,
    /// Period ended, awaiting redemption.
    Complete,
}

#[derive(Debug, Clone)]
struct MarketPosition {
    market: Market,
    up: SideInventory,
    down: SideInventory,
    phase: MarketPhase,
    grid_placed_at: Option<Instant>,
    last_sell_at: Option<Instant>,
    orders_cancelled: bool,
}

impl MarketPosition {
    fn new(market: Market) -> Self {
        Self {
            market,
            up: SideInventory::default(),
            down: SideInventory::default(),
            phase: MarketPhase::WaitingEntry,
            grid_placed_at: None,
            last_sell_at: None,
            orders_cancelled: false,
        }
    }

    /// Imbalance: positive = more Up, negative = more Down.
    fn imbalance(&self) -> Decimal {
        self.up.net_shares() - self.down.net_shares()
    }

    /// Paired shares = min of net shares on each side.
    fn paired_shares(&self) -> Decimal {
        self.up.net_shares().min(self.down.net_shares())
    }

    /// Combined cost for paired shares.
    fn paired_cost(&self) -> Decimal {
        let paired = self.paired_shares();
        if paired <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        // Weighted average: (up_avg + down_avg) * paired
        (self.up.avg_cost() + self.down.avg_cost()) * paired
    }

    /// Locked profit from paired shares.
    fn locked_profit(&self) -> Decimal {
        let paired = self.paired_shares();
        if paired <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        // Each pair redeems for $1.00
        paired - self.paired_cost()
    }

    /// Total USDC spent (buys - sell revenue).
    fn net_spent(&self) -> Decimal {
        (self.up.cost + self.down.cost) - (self.up.sell_revenue + self.down.sell_revenue)
    }

    /// Seconds remaining in this market.
    fn secs_remaining(&self) -> i64 {
        (self.market.end_date - Utc::now()).num_seconds()
    }

    /// Whether we can sell (cooldown elapsed).
    fn can_sell(&self, cooldown_secs: u64) -> bool {
        match self.last_sell_at {
            None => true,
            Some(t) => t.elapsed() > Duration::from_secs(cooldown_secs),
        }
    }
}

// ── Main Strategy Loop ──

pub async fn run(
    config: DeepDiscountConfig,
    executor: Option<Executor>,
    redeem_ctx: Option<Arc<RedeemContext>>,
) -> Result<()> {
    let start_time = Instant::now();
    let poll_interval = Duration::from_millis(config.poll_interval_ms);
    let grid_levels = config.grid_levels();

    // State
    let mut positions: HashMap<String, MarketPosition> = HashMap::new();
    let mut completed_markets: HashMap<String, Instant> = HashMap::new();
    let mut last_discovery = Instant::now() - Duration::from_secs(60);
    let mut last_redeem = Instant::now() - Duration::from_secs(config.redeem_interval_secs);
    let mut last_heartbeat = Instant::now() - Duration::from_secs(30);

    // Running totals
    let mut total_markets_entered = 0u64;
    let mut total_paired_profit = Decimal::ZERO;
    let mut total_sell_pnl = Decimal::ZERO;
    let mut total_up_shares = Decimal::ZERO;
    let mut total_down_shares = Decimal::ZERO;

    // CLOB market cache
    let clob_start_cursor = core::estimate_clob_start_cursor();
    info!("[ddm] Initial CLOB scan...");
    let mut clob_market_cache: Vec<Market>;
    {
        let cursor = clob_start_cursor.clone();
        let durations = config.allowed_durations.clone();
        let (markets, _) = tokio::task::spawn_blocking(move || {
            core::scan_clob_markets(&cursor, &durations, false)
        })
        .await
        .context("Initial CLOB scan failed")?;
        info!("[ddm] Found {} markets in initial scan", markets.len());
        clob_market_cache = markets;
    }
    let mut last_clob_refresh = Instant::now();

    // Trade log
    let log_dir = "strategies/deep_discount_maker/logs";
    std::fs::create_dir_all(log_dir).ok();
    let log_path = format!(
        "{}/ddm_trades_{}.csv",
        log_dir,
        Utc::now().format("%Y%m%d_%H%M%S")
    );
    let mut trade_log = TradeLogger::new(
        &log_path,
        "timestamp,condition_id,action,side,price,size,up_shares,down_shares,imbalance,paired,locked_profit,net_spent",
    )?;

    let levels_str: Vec<String> = grid_levels.iter().map(|l| l.to_string()).collect();
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║      DEEP DISCOUNT MAKER (0xd0d605 style)                  ║");
    println!(
        "║  grid=[{}] x {} shares/level/side          ║",
        levels_str.join(","),
        config.shares_per_level,
    );
    println!(
        "║  sell_imbalance={} | max_spend=${} | max_pos={}         ║",
        config.sell_imbalance_threshold, config.max_total_spend, config.max_position_per_side,
    );
    println!(
        "║  mode: {}{}                                          ║",
        if config.live { "LIVE" } else { "PAPER" },
        if config.dry_run { " (dry-run)" } else { "" },
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
                    core::scan_clob_markets(&cursor, &durations, false)
                })
                .await
                {
                    Ok((new_markets, _)) if !new_markets.is_empty() => {
                        debug!("[ddm] CLOB cache refreshed: {} markets", new_markets.len());
                        clob_market_cache = new_markets;
                    }
                    Ok(_) => {}
                    Err(e) => warn!("[ddm] CLOB refresh failed: {e}"),
                }
            }

            // Find active markets we can enter
            let now = Utc::now();
            for market in &clob_market_cache {
                if !market.is_active() {
                    continue;
                }
                if positions.contains_key(&market.condition_id) {
                    continue;
                }
                if completed_markets.contains_key(&market.condition_id) {
                    continue;
                }

                // Coin filter: match both short name ("BTC") and full name ("bitcoin")
                if !config.coins.is_empty() {
                    let q_lower = market.question.to_lowercase();
                    let coin_names: &[(&str, &str)] = &[
                        ("btc", "bitcoin"),
                        ("eth", "ethereum"),
                        ("sol", "solana"),
                        ("xrp", "xrp"),
                    ];
                    let matches = config.coins.iter().any(|c| {
                        let c_lower = c.to_lowercase();
                        if q_lower.contains(&c_lower) {
                            return true;
                        }
                        // Also check full name
                        coin_names
                            .iter()
                            .any(|(short, full)| c_lower == *short && q_lower.contains(full))
                    });
                    if !matches {
                        continue;
                    }
                }

                let secs_since_start = (now - market.start_date).num_seconds();

                // Only enter within entry window
                if secs_since_start < config.entry_delay_secs as i64 {
                    // Will enter on next poll
                    if !positions.contains_key(&market.condition_id) {
                        let pos = MarketPosition::new(market.clone());
                        info!(
                            "[ddm] Queued: {} | starts in {}s",
                            &market.question[..market.question.len().min(60)],
                            config.entry_delay_secs as i64 - secs_since_start,
                        );
                        positions.insert(market.condition_id.clone(), pos);
                    }
                    continue;
                }

                // Don't enter too late
                let secs_remaining = (market.end_date - now).num_seconds();
                if secs_remaining < config.stop_new_orders_secs as i64 + 30 {
                    continue;
                }

                // Enter: create position in WaitingEntry
                let pos = MarketPosition::new(market.clone());
                info!(
                    "[ddm] Entering: {} | {}s into period, {}s remaining",
                    &market.question[..market.question.len().min(60)],
                    secs_since_start,
                    secs_remaining,
                );
                positions.insert(market.condition_id.clone(), pos);
            }
        }

        // ── Process Each Position ──
        let cids: Vec<String> = positions.keys().cloned().collect();
        for cid in &cids {
            let pos = match positions.get_mut(cid) {
                Some(p) => p,
                None => continue,
            };

            match pos.phase.clone() {
                MarketPhase::WaitingEntry => {
                    let secs_since_start = (Utc::now() - pos.market.start_date).num_seconds();
                    if secs_since_start >= config.entry_delay_secs as i64 && pos.market.is_active()
                    {
                        // Place grid bids on both sides
                        let placed =
                            place_grid_bids(pos, &config, &grid_levels, &executor, &mut trade_log)
                                .await;

                        if placed > 0 {
                            pos.phase = MarketPhase::Accumulating;
                            pos.grid_placed_at = Some(Instant::now());
                            total_markets_entered += 1;
                            println!(
                                "╭─ GRID: {} | {} orders placed ({} levels x 2 sides)",
                                &pos.market.question[..pos.market.question.len().min(50)],
                                placed,
                                grid_levels.len(),
                            );
                            println!(
                                "╰─ {}s remaining | grid=[{}-{}]",
                                pos.secs_remaining(),
                                config.grid_min_price,
                                config.grid_max_price,
                            );
                        } else {
                            warn!("[ddm] No orders placed for {}, skipping", &cid[..12]);
                            pos.phase = MarketPhase::Complete;
                        }
                    } else if !pos.market.is_active() {
                        // Market already ended before we could enter
                        pos.phase = MarketPhase::Complete;
                    }
                }

                MarketPhase::Accumulating => {
                    let secs_remaining = pos.secs_remaining();

                    // Check fills
                    check_fills(pos, &executor, &config).await;

                    // Inventory management: sell excess
                    if config.sell_enabled
                        && pos.imbalance().abs() > config.sell_imbalance_threshold
                        && pos.can_sell(config.sell_cooldown_secs)
                        && secs_remaining > config.stop_new_orders_secs as i64
                    {
                        sell_excess(pos, &config, &executor, &mut trade_log).await;
                    }

                    // Transition to cancelling near end
                    if secs_remaining <= config.cancel_all_secs as i64 {
                        pos.phase = MarketPhase::Cancelling;
                    } else if secs_remaining <= config.stop_new_orders_secs as i64 {
                        // No new orders but keep existing
                        // (grid is already placed, nothing to do)
                    }

                    // Market ended unexpectedly
                    if !pos.market.is_active() && pos.secs_remaining() < -5 {
                        pos.phase = MarketPhase::Cancelling;
                    }
                }

                MarketPhase::Cancelling => {
                    if !pos.orders_cancelled {
                        cancel_all_orders(pos, &executor).await;
                        pos.orders_cancelled = true;

                        // Final fill check
                        check_fills(pos, &executor, &config).await;

                        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
                        trade_log.write_line(&format!(
                            "{},{},CANCEL_ALL,,,,{},{},{},{},{:.4},{:.4}",
                            ts,
                            cid,
                            pos.up.net_shares(),
                            pos.down.net_shares(),
                            pos.imbalance(),
                            pos.paired_shares(),
                            pos.locked_profit(),
                            pos.net_spent(),
                        ));
                    }

                    pos.phase = MarketPhase::HoldingToExpiry;
                }

                MarketPhase::HoldingToExpiry => {
                    if pos.market.is_resolved() {
                        // Record final stats
                        let paired = pos.paired_shares();
                        let locked = pos.locked_profit();
                        let sell_pnl = (pos.up.sell_revenue + pos.down.sell_revenue)
                            - (pos.up.shares_sold * pos.up.avg_cost()
                                + pos.down.shares_sold * pos.down.avg_cost());

                        total_paired_profit += locked;
                        total_sell_pnl += sell_pnl;
                        total_up_shares += pos.up.net_shares();
                        total_down_shares += pos.down.net_shares();

                        let excess_up = pos.up.net_shares() - paired;
                        let excess_down = pos.down.net_shares() - paired;

                        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
                        trade_log.write_line(&format!(
                            "{},{},RESOLVED,,,,{},{},{},{},{:.4},{:.4}",
                            ts,
                            cid,
                            pos.up.net_shares(),
                            pos.down.net_shares(),
                            pos.imbalance(),
                            paired,
                            locked,
                            pos.net_spent(),
                        ));

                        let locked_f64: f64 = locked.to_string().parse().unwrap_or(0.0);
                        println!(
                            "  + RESOLVED: {} | paired={} locked=${:.4} | excess: up={} down={}",
                            &pos.market.question[..pos.market.question.len().min(40)],
                            paired,
                            locked_f64,
                            excess_up,
                            excess_down,
                        );

                        pos.phase = MarketPhase::Complete;
                    }
                }

                MarketPhase::Complete => {}
            }
        }

        // Move completed positions
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
                        info!("[ddm] Redeem sweep: {ok} ok, {fail} failed");
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

            let accumulating = positions
                .values()
                .filter(|p| p.phase == MarketPhase::Accumulating)
                .count();

            let total_imbalance: Decimal = positions.values().map(|p| p.imbalance().abs()).sum();

            let paired_f64: f64 = total_paired_profit.to_string().parse().unwrap_or(0.0);
            let sell_f64: f64 = total_sell_pnl.to_string().parse().unwrap_or(0.0);

            println!(
                "  [HB] {}s | active={} accum={} | entered={} | paired_profit=${:.4} sell_pnl=${:.4} | imbalance={} | cache={}",
                start_time.elapsed().as_secs(),
                active,
                accumulating,
                total_markets_entered,
                paired_f64,
                sell_f64,
                total_imbalance,
                clob_market_cache.len(),
            );
        }

        tokio::time::sleep(poll_interval).await;
    }
}

// ── Grid Placement ──

/// Place GTC bids on both Up and Down at all grid levels.
/// Returns count of orders successfully placed.
async fn place_grid_bids(
    pos: &mut MarketPosition,
    config: &DeepDiscountConfig,
    grid_levels: &[Decimal],
    executor: &Option<Executor>,
    trade_log: &mut TradeLogger,
) -> usize {
    let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
    let tick_size = pos.market.tick_size;

    // Build order specs: all levels for both sides
    let mut order_specs: Vec<(String, Side, Decimal, Decimal, Decimal)> = Vec::new();

    for &price in grid_levels {
        // Up side bid
        order_specs.push((
            pos.market.token_id_up.clone(),
            Side::Buy,
            price,
            config.shares_per_level,
            tick_size,
        ));
        // Down side bid
        order_specs.push((
            pos.market.token_id_down.clone(),
            Side::Buy,
            price,
            config.shares_per_level,
            tick_size,
        ));
    }

    if let Some(ref exec) = executor {
        // Live/dry-run: batch place all orders
        match exec.place_batch_gtc(&order_specs).await {
            Ok(results) => {
                for (idx, order_id) in &results {
                    let (_, side, price, size, _) = &order_specs[*idx];
                    let is_up = *side == Side::Buy && order_specs[*idx].0 == pos.market.token_id_up;

                    if is_up {
                        pos.up.order_ids.push(order_id.clone());
                    } else {
                        pos.down.order_ids.push(order_id.clone());
                    }

                    let side_label = if is_up { "Up" } else { "Down" };
                    trade_log.write_line(&format!(
                        "{},{},BID_PLACED,{},{},{},{},{},{},{},{:.4},{:.4}",
                        ts,
                        pos.market.condition_id,
                        side_label,
                        price,
                        size,
                        pos.up.net_shares(),
                        pos.down.net_shares(),
                        pos.imbalance(),
                        pos.paired_shares(),
                        pos.locked_profit(),
                        pos.net_spent(),
                    ));
                }

                info!(
                    "[ddm] Placed {} orders for {}",
                    results.len(),
                    &pos.market.condition_id[..12]
                );
                results.len()
            }
            Err(e) => {
                warn!("[ddm] Batch GTC failed: {e}");
                0
            }
        }
    } else {
        // Paper mode: create paper order IDs
        let ts_ms = Utc::now().timestamp_millis();
        for (i, (token_id, _, price, _, _)) in order_specs.iter().enumerate() {
            let order_id = format!("paper-{ts_ms}-{i}");
            let is_up = *token_id == pos.market.token_id_up;

            if is_up {
                pos.up.order_ids.push(order_id);
            } else {
                pos.down.order_ids.push(order_id);
            }

            let side_label = if is_up { "Up" } else { "Down" };
            trade_log.write_line(&format!(
                "{},{},BID_PLACED,{},{},{},{},{},{},{},{:.4},{:.4}",
                ts,
                pos.market.condition_id,
                side_label,
                price,
                config.shares_per_level,
                pos.up.net_shares(),
                pos.down.net_shares(),
                pos.imbalance(),
                pos.paired_shares(),
                pos.locked_profit(),
                pos.net_spent(),
            ));
        }

        info!(
            "[ddm] [PAPER] Placed {} paper orders for {}",
            order_specs.len(),
            &pos.market.condition_id[..12]
        );
        order_specs.len()
    }
}

// ── Fill Checking ──

/// Check for fills on both Up and Down tokens.
async fn check_fills(
    pos: &mut MarketPosition,
    executor: &Option<Executor>,
    config: &DeepDiscountConfig,
) {
    if let Some(ref exec) = executor {
        // Live mode: check open orders for both tokens
        for (token_id, inv) in [
            (pos.market.token_id_up.clone(), &mut pos.up),
            (pos.market.token_id_down.clone(), &mut pos.down),
        ] {
            if inv.order_ids.is_empty() {
                continue;
            }

            match exec.get_open_orders(&token_id).await {
                Ok(orders) => {
                    // Track total matched across all our orders
                    let mut total_matched = Decimal::ZERO;
                    let mut total_cost = Decimal::ZERO;

                    for oid in &inv.order_ids {
                        if oid.starts_with("dry-run") {
                            continue;
                        }
                        if let Some(order) = orders.iter().find(|o| o.id == *oid) {
                            total_matched += order.size_matched;
                            total_cost += order.size_matched * order.price;
                        } else {
                            // Order disappeared — might be fully filled
                            // We can't know the exact fill price, estimate from grid
                            // This is conservative; real fills tracked via order updates
                        }
                    }

                    // Count orders that disappeared (no longer in open orders)
                    let disappeared: usize = inv
                        .order_ids
                        .iter()
                        .filter(|oid| {
                            !oid.starts_with("dry-run") && !orders.iter().any(|o| o.id == **oid)
                        })
                        .count();

                    if total_matched > inv.shares || disappeared > 0 {
                        let delta = total_matched - inv.shares;
                        if delta > Decimal::ZERO {
                            info!(
                                "[ddm] Fill: {} +{} shares (total={})",
                                if token_id == pos.market.token_id_up {
                                    "Up"
                                } else {
                                    "Down"
                                },
                                delta,
                                total_matched,
                            );
                        }
                        inv.shares = total_matched;
                        inv.cost = total_cost;
                    }
                }
                Err(e) => {
                    debug!("[ddm] Open orders query failed: {e}");
                }
            }
        }
    } else {
        // Paper mode: simulate fills based on real book state
        for (token_id, inv, side_label) in [
            (pos.market.token_id_up.clone(), &mut pos.up, "Up"),
            (pos.market.token_id_down.clone(), &mut pos.down, "Down"),
        ] {
            let book = tokio::task::spawn_blocking({
                let tid = token_id.clone();
                move || core::fetch_book(&tid)
            })
            .await
            .ok()
            .flatten();

            if let Some(book) = book {
                // In paper mode, check if any of our grid bids would have been lifted.
                // Simulated: if ask is <= any of our bid prices, we get filled at our bid.
                // This is conservative (real fills happen at the bid, not the ask).
                if book.best_ask > Decimal::ZERO {
                    let grid_levels = {
                        let mut levels = Vec::new();
                        let mut p = config.grid_min_price;
                        while p <= config.grid_max_price {
                            levels.push(p);
                            p += config.grid_step;
                        }
                        levels
                    };

                    for &bid_price in &grid_levels {
                        if book.best_ask <= bid_price && inv.shares < config.max_position_per_side {
                            // Simulate a fill at the ask price
                            let fill_size = config
                                .shares_per_level
                                .min(book.ask_size)
                                .min(config.max_position_per_side - inv.shares);
                            if fill_size > Decimal::ZERO {
                                inv.shares += fill_size;
                                inv.cost += fill_size * bid_price; // filled at our bid
                                info!(
                                    "[ddm] [PAPER] {} fill: {} shares @ {} (ask was {})",
                                    side_label, fill_size, bid_price, book.best_ask,
                                );
                            }
                            break; // Only fill at the best matching level per tick
                        }
                    }
                }
            }
        }
    }
}

// ── Inventory Management (Selling) ──

/// Sell excess shares on the heavier side when imbalance exceeds threshold.
async fn sell_excess(
    pos: &mut MarketPosition,
    config: &DeepDiscountConfig,
    executor: &Option<Executor>,
    trade_log: &mut TradeLogger,
) {
    let imb = pos.imbalance();
    if imb.abs() <= config.sell_imbalance_threshold {
        return;
    }

    // Determine which side to sell (the heavier one)
    let (sell_token_id, sell_inv, side_label) = if imb > Decimal::ZERO {
        // More Up than Down — sell Up
        (pos.market.token_id_up.clone(), &mut pos.up, "Up")
    } else {
        // More Down than Up — sell Down
        (pos.market.token_id_down.clone(), &mut pos.down, "Down")
    };

    let excess = imb.abs() - config.sell_imbalance_threshold;
    let sell_size = excess.min(sell_inv.net_shares());
    if sell_size <= Decimal::ZERO {
        return;
    }

    // Get current book to find best bid for FOK sell
    let book = tokio::task::spawn_blocking({
        let tid = sell_token_id.clone();
        move || core::fetch_book(&tid)
    })
    .await
    .ok()
    .flatten();

    let best_bid = match book {
        Some(ref b) if b.best_bid > Decimal::ZERO => b.best_bid,
        _ => {
            debug!("[ddm] No book for sell on {side_label}");
            return;
        }
    };

    // Check max loss guard
    let avg_cost = sell_inv.avg_cost();
    if avg_cost > Decimal::ZERO {
        let loss_per_share = avg_cost - best_bid;
        if loss_per_share > config.sell_max_loss_per_share {
            debug!(
                "[ddm] Sell {side_label} blocked: loss/share={loss_per_share} > max={}",
                config.sell_max_loss_per_share
            );
            return;
        }
    }

    let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");

    if let Some(ref exec) = executor {
        match exec
            .sell_fok(&sell_token_id, best_bid, sell_size, pos.market.tick_size)
            .await
        {
            Ok(_oid) => {
                sell_inv.shares_sold += sell_size;
                sell_inv.sell_revenue += sell_size * best_bid;
                pos.last_sell_at = Some(Instant::now());

                info!(
                    "[ddm] SELL {} {} shares @ {} (avg_cost={}, loss/share={})",
                    side_label,
                    sell_size,
                    best_bid,
                    avg_cost,
                    avg_cost - best_bid,
                );

                trade_log.write_line(&format!(
                    "{},{},SELL,{},{},{},{},{},{},{},{:.4},{:.4}",
                    ts,
                    pos.market.condition_id,
                    side_label,
                    best_bid,
                    sell_size,
                    pos.up.net_shares(),
                    pos.down.net_shares(),
                    pos.imbalance(),
                    pos.paired_shares(),
                    pos.locked_profit(),
                    pos.net_spent(),
                ));
            }
            Err(e) => {
                warn!("[ddm] Sell {side_label} failed: {e}");
            }
        }
    } else {
        // Paper mode: simulate sell
        sell_inv.shares_sold += sell_size;
        sell_inv.sell_revenue += sell_size * best_bid;
        pos.last_sell_at = Some(Instant::now());

        info!(
            "[ddm] [PAPER] SELL {} {} shares @ {} (avg_cost={}, loss/share={})",
            side_label,
            sell_size,
            best_bid,
            avg_cost,
            avg_cost - best_bid,
        );

        trade_log.write_line(&format!(
            "{},{},PAPER_SELL,{},{},{},{},{},{},{},{:.4},{:.4}",
            ts,
            pos.market.condition_id,
            side_label,
            best_bid,
            sell_size,
            pos.up.net_shares(),
            pos.down.net_shares(),
            pos.imbalance(),
            pos.paired_shares(),
            pos.locked_profit(),
            pos.net_spent(),
        ));
    }
}

// ── Order Cancellation ──

/// Cancel all resting orders for this market (both sides).
async fn cancel_all_orders(pos: &mut MarketPosition, executor: &Option<Executor>) {
    let all_ids: Vec<String> = pos
        .up
        .order_ids
        .iter()
        .chain(pos.down.order_ids.iter())
        .filter(|id| !id.starts_with("paper-") && !id.starts_with("dry-run"))
        .cloned()
        .collect();

    if all_ids.is_empty() {
        debug!("[ddm] No live orders to cancel");
        return;
    }

    if let Some(ref exec) = executor {
        let refs: Vec<&str> = all_ids.iter().map(|s| s.as_str()).collect();
        match exec.cancel_orders(&refs).await {
            Ok(cancelled) => {
                info!(
                    "[ddm] Cancelled {} orders for {}",
                    cancelled.len(),
                    &pos.market.condition_id[..12]
                );
            }
            Err(e) => {
                warn!(
                    "[ddm] Cancel failed for {}: {e}",
                    &pos.market.condition_id[..12]
                );
            }
        }
    }
}
