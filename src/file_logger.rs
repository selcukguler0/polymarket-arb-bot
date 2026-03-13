//! Per-period CSV file logger for orders, prices, and enhanced analytics.
//!
//! Creates separate log files under `logs/<period_name>/`:
//! - `orders.csv` — every placed order (buy/sell/fok), even if not filled
//! - `prices.csv` — binance BTC price + FV up/down on every quote tick
//! - `fills.csv` — every filled order
//! - `order_events.csv` — full order lifecycle (PLACED, CANCELLED, FILLED, EXPIRED)
//! - `decisions.csv` — why the bot did or didn't take action at each decision point
//! - `book_snapshots.csv` — top-5 orderbook depth every 5 seconds
//! - `latency.csv` — API call latency tracking
//! - `period_result.csv` — single-row summary when period resolves
//!
//! Session-level (outside period folders):
//! - `logs/session_summary.csv` — one row per period, appended across the session

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use chrono::Utc;
use rust_decimal::Decimal;
use tracing::warn;

use crate::types::Outcome;

/// Manages per-period log files.
pub struct PeriodLogger {
    base_dir: PathBuf,
    run_id: String,
    /// Cached file handles keyed by period name.
    order_files: HashMap<String, File>,
    /// Placement snapshots for `orders.csv` lifecycle finalization.
    /// Keyed by period -> order_id.
    order_snapshots: HashMap<String, HashMap<String, OrderSnapshot>>,
    price_files: HashMap<String, File>,
    fill_files: HashMap<String, File>,
    order_event_files: HashMap<String, File>,
    decision_files: HashMap<String, File>,
    book_snapshot_files: HashMap<String, File>,
    latency_files: HashMap<String, File>,
    /// Session-level summary file (not per-period).
    session_summary_file: Option<File>,
}

#[derive(Debug, Clone)]
struct OrderSnapshot {
    placed_ts: String,
    order_type: String,
    outcome: Outcome,
    price: Decimal,
    size: Decimal,
    binance_btc: f64,
    btc_open: f64,
    fv_up: f64,
    fv_down: f64,
    sigma: f64,
    remaining_secs: f64,
    condition_id: String,
    mode: String,
}

impl PeriodLogger {
    pub fn new<P: AsRef<Path>>(base_dir: P, run_id: &str) -> Self {
        let base = base_dir.as_ref().to_path_buf();
        if let Err(e) = fs::create_dir_all(&base) {
            warn!("Failed to create logs base dir: {e}");
        }

        // Initialize session summary file (append mode, session-level)
        let summary_path = base.join("session_summary.csv");
        let is_new = !summary_path.exists();
        let session_summary_file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&summary_path)
        {
            Ok(mut f) => {
                if is_new {
                    let _ = writeln!(
                        f,
                        "run_id,session_start,period_name,condition_id,btc_open,btc_close,result,orders_placed,orders_filled,orders_cancelled,orders_expired,fill_rate,total_up_shares,total_down_shares,complete_pairs,locked_profit,gross_cost,period_pnl,cumulative_session_pnl,max_excess,quote_levels_yes,quote_levels_no,pair_completion_attempts,pair_completion_successes,suppression_reason_counts,cancel_all_count,settlement_mode,avg_fill_edge,avg_latency_ms,mode,deep_grid_fills_up,deep_grid_fills_down,deep_grid_fill_shares,avg_deep_fill_price"
                    );
                }
                Some(f)
            }
            Err(e) => {
                warn!("Failed to open session summary: {e}");
                None
            }
        };

        Self {
            base_dir: base,
            run_id: run_id.to_string(),
            order_files: HashMap::new(),
            order_snapshots: HashMap::new(),
            price_files: HashMap::new(),
            fill_files: HashMap::new(),
            order_event_files: HashMap::new(),
            decision_files: HashMap::new(),
            book_snapshot_files: HashMap::new(),
            latency_files: HashMap::new(),
            session_summary_file,
        }
    }

    /// Extract a filesystem-safe period name from a market question.
    /// e.g. "Bitcoin Up or Down - February 16, 7:00AM-7:15AM ET" → "2026-02-16_7-00AM_7-15AM"
    pub fn period_name(question: &str) -> String {
        // Extract the time part after " - "
        let time_part = if let Some(idx) = question.find(" - ") {
            &question[idx + 3..]
        } else {
            question
        };

        // Make filesystem-safe: replace colons, spaces, commas
        let safe: String = time_part
            .replace(':', "-")
            .replace(' ', "_")
            .replace(',', "");

        let today = Utc::now().format("%Y-%m-%d").to_string();
        format!("{today}_{safe}")
    }

    // ═══════════════════════════════════════════════════════════════════
    // File ensure helpers (lazy creation with headers)
    // ═══════════════════════════════════════════════════════════════════

    fn ensure_order_file(&mut self, period: &str) -> Option<&mut File> {
        if !self.order_files.contains_key(period) {
            let dir = self.base_dir.join(period);
            if let Err(e) = fs::create_dir_all(&dir) {
                warn!("Failed to create period log dir {}: {e}", dir.display());
                return None;
            }
            let path = dir.join("orders.csv");
            let is_new = !path.exists();
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(mut f) => {
                    if is_new {
                        let _ = writeln!(
                            f,
                            "timestamp,order_type,outcome,price,size,binance_btc,btc_open,fv_up,fv_down,sigma,remaining_secs,condition_id,order_id,mode,cancel_timestamp,cancel_reason,fill_timestamp,fill_price,fill_size,final_status"
                        );
                    }
                    self.order_files.insert(period.to_string(), f);
                }
                Err(e) => {
                    warn!("Failed to open order log {}: {e}", path.display());
                    return None;
                }
            }
        }
        self.order_files.get_mut(period)
    }

    fn ensure_price_file(&mut self, period: &str) -> Option<&mut File> {
        if !self.price_files.contains_key(period) {
            let dir = self.base_dir.join(period);
            if let Err(e) = fs::create_dir_all(&dir) {
                warn!("Failed to create period log dir {}: {e}", dir.display());
                return None;
            }
            let path = dir.join("prices.csv");
            let is_new = !path.exists();
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(mut f) => {
                    if is_new {
                        let _ = writeln!(
                            f,
                            "timestamp,binance_btc,btc_open,fv_up,fv_down,sigma,remaining_secs,best_bid_up,best_ask_up,best_bid_down,best_ask_down,combined_bid,yes_qty,no_qty,pairs,locked_profit,sigma_source,realized_vol_1m,realized_vol_5m,btc_price_1m_ago,spread_up,spread_down,mid_up,mid_down,raw_fv_up"
                        );
                    }
                    self.price_files.insert(period.to_string(), f);
                }
                Err(e) => {
                    warn!("Failed to open price log {}: {e}", path.display());
                    return None;
                }
            }
        }
        self.price_files.get_mut(period)
    }

    fn ensure_fill_file(&mut self, period: &str) -> Option<&mut File> {
        if !self.fill_files.contains_key(period) {
            let dir = self.base_dir.join(period);
            if let Err(e) = fs::create_dir_all(&dir) {
                warn!("Failed to create period log dir {}: {e}", dir.display());
                return None;
            }
            let path = dir.join("fills.csv");
            let is_new = !path.exists();
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(mut f) => {
                    if is_new {
                        let _ = writeln!(
                            f,
                            "timestamp,side,outcome,price,size,binance_btc,btc_open,fv_up,fv_down,sigma,remaining_secs,condition_id,order_id,mode"
                        );
                    }
                    self.fill_files.insert(period.to_string(), f);
                }
                Err(e) => {
                    warn!("Failed to open fill log {}: {e}", path.display());
                    return None;
                }
            }
        }
        self.fill_files.get_mut(period)
    }

    fn ensure_order_event_file(&mut self, period: &str) -> Option<&mut File> {
        if !self.order_event_files.contains_key(period) {
            let dir = self.base_dir.join(period);
            if let Err(e) = fs::create_dir_all(&dir) {
                warn!("Failed to create period log dir {}: {e}", dir.display());
                return None;
            }
            let path = dir.join("order_events.csv");
            let is_new = !path.exists();
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(mut f) => {
                    if is_new {
                        let _ = writeln!(
                            f,
                            "timestamp,order_id,event_type,outcome,price,original_size,remaining_size,reason"
                        );
                    }
                    self.order_event_files.insert(period.to_string(), f);
                }
                Err(e) => {
                    warn!("Failed to open order events log {}: {e}", path.display());
                    return None;
                }
            }
        }
        self.order_event_files.get_mut(period)
    }

    fn ensure_decision_file(&mut self, period: &str) -> Option<&mut File> {
        if !self.decision_files.contains_key(period) {
            let dir = self.base_dir.join(period);
            if let Err(e) = fs::create_dir_all(&dir) {
                warn!("Failed to create period log dir {}: {e}", dir.display());
                return None;
            }
            let path = dir.join("decisions.csv");
            let is_new = !path.exists();
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(mut f) => {
                    if is_new {
                        let _ = writeln!(
                            f,
                            "timestamp,decision,action,outcome,price,size,reason,fv_up,fv_down,sigma,btc_price,btc_open,remaining_secs,best_bid_up,best_ask_up,best_bid_down,best_ask_down,pos_up,pos_down,pairs,budget_used,budget_limit"
                        );
                    }
                    self.decision_files.insert(period.to_string(), f);
                }
                Err(e) => {
                    warn!("Failed to open decisions log {}: {e}", path.display());
                    return None;
                }
            }
        }
        self.decision_files.get_mut(period)
    }

    fn ensure_book_snapshot_file(&mut self, period: &str) -> Option<&mut File> {
        if !self.book_snapshot_files.contains_key(period) {
            let dir = self.base_dir.join(period);
            if let Err(e) = fs::create_dir_all(&dir) {
                warn!("Failed to create period log dir {}: {e}", dir.display());
                return None;
            }
            let path = dir.join("book_snapshots.csv");
            let is_new = !path.exists();
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(mut f) => {
                    if is_new {
                        let _ = writeln!(
                            f,
                            "timestamp,side,level_1_price,level_1_size,level_2_price,level_2_size,level_3_price,level_3_size,level_4_price,level_4_size,level_5_price,level_5_size"
                        );
                    }
                    self.book_snapshot_files.insert(period.to_string(), f);
                }
                Err(e) => {
                    warn!("Failed to open book snapshots log {}: {e}", path.display());
                    return None;
                }
            }
        }
        self.book_snapshot_files.get_mut(period)
    }

    fn ensure_latency_file(&mut self, period: &str) -> Option<&mut File> {
        if !self.latency_files.contains_key(period) {
            let dir = self.base_dir.join(period);
            if let Err(e) = fs::create_dir_all(&dir) {
                warn!("Failed to create period log dir {}: {e}", dir.display());
                return None;
            }
            let path = dir.join("latency.csv");
            let is_new = !path.exists();
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(mut f) => {
                    if is_new {
                        let _ = writeln!(f, "timestamp,operation,latency_ms,success,error_msg");
                    }
                    self.latency_files.insert(period.to_string(), f);
                }
                Err(e) => {
                    warn!("Failed to open latency log {}: {e}", path.display());
                    return None;
                }
            }
        }
        self.latency_files.get_mut(period)
    }

    // ═══════════════════════════════════════════════════════════════════
    // Existing log methods (enhanced with new columns)
    // ═══════════════════════════════════════════════════════════════════

    /// Log an order placement (buy, sell, or FOK).
    /// A PENDING row is written immediately; terminal lifecycle events append a
    /// finalized row with cancel/fill columns populated.
    #[allow(clippy::too_many_arguments)]
    pub fn log_order(
        &mut self,
        period: &str,
        order_type: &str, // "buy", "sell", "buy_fok"
        outcome: Outcome,
        price: Decimal,
        size: Decimal,
        binance_btc: f64,
        btc_open: f64,
        fv_up: f64,
        fv_down: f64,
        sigma: f64,
        remaining_secs: f64,
        condition_id: &str,
        order_id: &str,
        mode: &str, // "paper" or "live"
    ) {
        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let outcome_str = match outcome {
            Outcome::Yes => "UP",
            Outcome::No => "DOWN",
        };
        let snapshot = OrderSnapshot {
            placed_ts: ts.to_string(),
            order_type: order_type.to_string(),
            outcome,
            price,
            size,
            binance_btc,
            btc_open,
            fv_up,
            fv_down,
            sigma,
            remaining_secs,
            condition_id: condition_id.to_string(),
            mode: mode.to_string(),
        };
        self.order_snapshots
            .entry(period.to_string())
            .or_default()
            .insert(order_id.to_string(), snapshot);

        // Original columns + 6 new empty columns for lifecycle tracking
        let line = format!(
            "{ts},{order_type},{outcome_str},{price},{size},{binance_btc:.2},{btc_open:.2},{fv_up:.6},{fv_down:.6},{sigma:.10},{remaining_secs:.1},{condition_id},{order_id},{mode},,,,,,PENDING"
        );
        if let Some(f) = self.ensure_order_file(period) {
            let _ = writeln!(f, "{line}");
        }
    }

    /// Log price snapshot on each quote refresh tick.
    /// Enhanced with: sigma_source, realized_vol_1m, realized_vol_5m, btc_price_1m_ago,
    /// spread_up, spread_down, mid_up, mid_down
    #[allow(clippy::too_many_arguments)]
    pub fn log_prices(
        &mut self,
        period: &str,
        binance_btc: f64,
        btc_open: f64,
        fv_up: f64,
        fv_down: f64,
        sigma: f64,
        remaining_secs: f64,
        best_bid_up: Decimal,
        best_ask_up: Decimal,
        best_bid_down: Decimal,
        best_ask_down: Decimal,
        yes_qty: Decimal,
        no_qty: Decimal,
        pairs: Decimal,
        locked_profit: Decimal,
        sigma_source: &str,
        realized_vol_1m: f64,
        realized_vol_5m: f64,
        btc_price_1m_ago: f64,
        raw_fv_up: f64,
    ) {
        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let combined_bid = best_bid_up + best_bid_down;
        let spread_up = best_ask_up - best_bid_up;
        let spread_down = best_ask_down - best_bid_down;
        let mid_up = if best_bid_up > Decimal::ZERO && best_ask_up > Decimal::ZERO {
            (best_bid_up + best_ask_up) / Decimal::TWO
        } else {
            Decimal::ZERO
        };
        let mid_down = if best_bid_down > Decimal::ZERO && best_ask_down > Decimal::ZERO {
            (best_bid_down + best_ask_down) / Decimal::TWO
        } else {
            Decimal::ZERO
        };
        let line = format!(
            "{ts},{binance_btc:.2},{btc_open:.2},{fv_up:.6},{fv_down:.6},{sigma:.10},{remaining_secs:.1},{best_bid_up},{best_ask_up},{best_bid_down},{best_ask_down},{combined_bid},{yes_qty},{no_qty},{pairs},{locked_profit},{sigma_source},{realized_vol_1m:.10},{realized_vol_5m:.10},{btc_price_1m_ago:.2},{spread_up},{spread_down},{mid_up},{mid_down},{raw_fv_up:.6}"
        );
        if let Some(f) = self.ensure_price_file(period) {
            let _ = writeln!(f, "{line}");
        }
    }

    /// Log a filled order (buy or sell).
    #[allow(clippy::too_many_arguments)]
    pub fn log_fill(
        &mut self,
        period: &str,
        side: &str, // "buy" or "sell"
        outcome: Outcome,
        price: Decimal,
        size: Decimal,
        binance_btc: f64,
        btc_open: f64,
        fv_up: f64,
        fv_down: f64,
        sigma: f64,
        remaining_secs: f64,
        condition_id: &str,
        order_id: &str,
        mode: &str, // "paper" or "live"
    ) {
        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let outcome_str = match outcome {
            Outcome::Yes => "UP",
            Outcome::No => "DOWN",
        };
        let line = format!(
            "{ts},{side},{outcome_str},{price},{size},{binance_btc:.2},{btc_open:.2},{fv_up:.6},{fv_down:.6},{sigma:.10},{remaining_secs:.1},{condition_id},{order_id},{mode}"
        );
        if let Some(f) = self.ensure_fill_file(period) {
            let _ = writeln!(f, "{line}");
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // New log methods
    // ═══════════════════════════════════════════════════════════════════

    /// Log an order lifecycle event (PLACED, CANCELLED, FILLED, EXPIRED, REPLACED).
    #[allow(clippy::too_many_arguments)]
    pub fn log_order_event(
        &mut self,
        period: &str,
        order_id: &str,
        event_type: &str, // "PLACED", "CANCELLED", "FILLED", "EXPIRED", "REPLACED", "PARTIAL_FILL"
        outcome: Outcome,
        price: Decimal,
        original_size: Decimal,
        remaining_size: Decimal,
        reason: &str,
    ) {
        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let outcome_str = match outcome {
            Outcome::Yes => "UP",
            Outcome::No => "DOWN",
        };
        let line = format!(
            "{ts},{order_id},{event_type},{outcome_str},{price},{original_size},{remaining_size},{reason}"
        );
        if let Some(f) = self.ensure_order_event_file(period) {
            let _ = writeln!(f, "{line}");
        }

        let event_type_upper = event_type.to_ascii_uppercase();
        match event_type_upper.as_str() {
            "CANCELLED" => {
                self.log_order_terminal(period, order_id, "CANCELLED", Some(reason), None, None);
            }
            "EXPIRED" => {
                self.log_order_terminal(period, order_id, "EXPIRED", Some(reason), None, None);
            }
            "FILLED" => {
                self.log_order_terminal(
                    period,
                    order_id,
                    "FILLED",
                    None,
                    Some(price),
                    Some(original_size),
                );
            }
            _ => {}
        }
    }

    /// Log a decision point: why the bot did or didn't take action.
    #[allow(clippy::too_many_arguments)]
    pub fn log_decision(
        &mut self,
        period: &str,
        decision: &str, // "DCA_ROUND", "PRICE_SELECT", "CANCEL", "EXIT"
        action: &str, // "PLACE_ORDER", "SKIP", "CANCEL_ALL", "CANCEL_STALE", "SELL_*", "BID_UP", "BID_DOWN"
        outcome: &str, // "UP", "DOWN", "BOTH", ""
        price: Decimal,
        size: Decimal,
        reason: &str,
        fv_up: f64,
        fv_down: f64,
        sigma: f64,
        btc_price: f64,
        btc_open: f64,
        remaining_secs: f64,
        best_bid_up: Decimal,
        best_ask_up: Decimal,
        best_bid_down: Decimal,
        best_ask_down: Decimal,
        pos_up: Decimal,
        pos_down: Decimal,
        pairs: Decimal,
        budget_used: Decimal,
        budget_limit: Decimal,
    ) {
        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let line = format!(
            "{ts},{decision},{action},{outcome},{price},{size},{reason},{fv_up:.6},{fv_down:.6},{sigma:.10},{btc_price:.2},{btc_open:.2},{remaining_secs:.1},{best_bid_up},{best_ask_up},{best_bid_down},{best_ask_down},{pos_up},{pos_down},{pairs},{budget_used},{budget_limit}"
        );
        if let Some(f) = self.ensure_decision_file(period) {
            let _ = writeln!(f, "{line}");
        }
    }

    /// Log a top-5 orderbook snapshot for one side.
    /// Call 4 times per snapshot: UP_BID, UP_ASK, DOWN_BID, DOWN_ASK.
    pub fn log_book_snapshot(
        &mut self,
        period: &str,
        side: &str,                    // "UP_BID", "UP_ASK", "DOWN_BID", "DOWN_ASK"
        levels: &[(Decimal, Decimal)], // up to 5 (price, size) pairs
    ) {
        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let mut parts = Vec::with_capacity(12);
        parts.push(ts.to_string());
        parts.push(side.to_string());
        for i in 0..5 {
            if let Some((price, size)) = levels.get(i) {
                parts.push(price.to_string());
                parts.push(size.to_string());
            } else {
                parts.push(String::new());
                parts.push(String::new());
            }
        }
        let line = parts.join(",");
        if let Some(f) = self.ensure_book_snapshot_file(period) {
            let _ = writeln!(f, "{line}");
        }
    }

    /// Log API call latency.
    pub fn log_latency(
        &mut self,
        period: &str,
        operation: &str, // "place_order", "cancel_order", "check_balance", "fetch_book_rest"
        latency_ms: u128,
        success: bool,
        error_msg: Option<&str>,
    ) {
        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let err = csv_escape_field(error_msg.unwrap_or(""));
        let line = format!("{ts},{operation},{latency_ms},{success},{err}");
        if let Some(f) = self.ensure_latency_file(period) {
            let _ = writeln!(f, "{line}");
        }
    }

    /// Write period result (single row) at the end of a market period.
    #[allow(clippy::too_many_arguments)]
    pub fn log_period_result(
        &mut self,
        period: &str,
        condition_id: &str,
        btc_open: f64,
        btc_close: f64,
        result: &str, // "UP" or "DOWN"
        final_pos_up: Decimal,
        final_pos_down: Decimal,
        complete_pairs: Decimal,
        locked_profit: Decimal,
        sell_realized_pnl: Decimal,
        merge_realized_pnl: Decimal,
        total_merged_pairs: Decimal,
        period_pnl: Decimal,
        cumulative_pnl: Decimal,
        deep_grid_fills_up: u32,
        deep_grid_fills_down: u32,
        deep_grid_fill_shares: Decimal,
        avg_deep_fill_price: f64,
    ) {
        let dir = self.base_dir.join(period);
        if let Err(e) = fs::create_dir_all(&dir) {
            warn!("Failed to create period log dir {}: {e}", dir.display());
            return;
        }
        let path = dir.join("period_result.csv");
        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        match OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
        {
            Ok(mut f) => {
                let _ = writeln!(
                    f,
                    "run_id,period_name,condition_id,btc_open,btc_close,result,resolved_at,final_pos_up,final_pos_down,complete_pairs,locked_profit,sell_realized_pnl,merge_realized_pnl,total_merged_pairs,period_pnl,cumulative_pnl,deep_grid_fills_up,deep_grid_fills_down,deep_grid_fill_shares,avg_deep_fill_price,resolution_source"
                );
                let _ = writeln!(
                    f,
                    "{},{period},{condition_id},{btc_open:.2},{btc_close:.2},{result},{ts},{final_pos_up},{final_pos_down},{complete_pairs},{locked_profit},{sell_realized_pnl},{merge_realized_pnl},{total_merged_pairs},{period_pnl},{cumulative_pnl},{deep_grid_fills_up},{deep_grid_fills_down},{deep_grid_fill_shares},{avg_deep_fill_price:.6},binance_approximation",
                    self.run_id
                );
            }
            Err(e) => {
                warn!("Failed to write period result {}: {e}", path.display());
            }
        }
    }

    /// Append a row to the session-level summary CSV.
    #[allow(clippy::too_many_arguments)]
    pub fn log_session_summary(
        &mut self,
        session_start: &str,
        period_name: &str,
        condition_id: &str,
        btc_open: f64,
        btc_close: f64,
        result: &str,
        orders_placed: u32,
        orders_filled: u32,
        orders_cancelled: u32,
        orders_expired: u32,
        total_up_shares: Decimal,
        total_down_shares: Decimal,
        complete_pairs: Decimal,
        locked_profit: Decimal,
        gross_cost: Decimal,
        period_pnl: Decimal,
        cumulative_session_pnl: Decimal,
        max_excess: Decimal,
        quote_levels_yes: u32,
        quote_levels_no: u32,
        pair_completion_attempts: u32,
        pair_completion_successes: u32,
        suppression_reason_counts: &str,
        cancel_all_count: u32,
        settlement_mode: &str,
        avg_fill_edge: f64,
        avg_latency_ms: f64,
        mode: &str,
        deep_grid_fills_up: u32,
        deep_grid_fills_down: u32,
        deep_grid_fill_shares: Decimal,
        avg_deep_fill_price: f64,
    ) {
        let fill_rate = if orders_placed > 0 {
            orders_filled as f64 / orders_placed as f64
        } else {
            0.0
        };
        let suppression_reason_counts = csv_escape_field(suppression_reason_counts);
        let settlement_mode = csv_escape_field(settlement_mode);
        let line = format!(
            "{},{session_start},{period_name},{condition_id},{btc_open:.2},{btc_close:.2},{result},{orders_placed},{orders_filled},{orders_cancelled},{orders_expired},{fill_rate:.4},{total_up_shares},{total_down_shares},{complete_pairs},{locked_profit},{gross_cost},{period_pnl},{cumulative_session_pnl},{max_excess},{quote_levels_yes},{quote_levels_no},{pair_completion_attempts},{pair_completion_successes},{suppression_reason_counts},{cancel_all_count},{settlement_mode},{avg_fill_edge:.6},{avg_latency_ms:.1},{mode},{deep_grid_fills_up},{deep_grid_fills_down},{deep_grid_fill_shares},{avg_deep_fill_price:.6}",
            self.run_id
        );
        if let Some(f) = &mut self.session_summary_file {
            let _ = writeln!(f, "{line}");
        }
    }

    /// Flush all open file handles to ensure data is written to disk.
    pub fn flush_all(&mut self) {
        for f in self.order_files.values_mut() {
            let _ = f.flush();
        }
        for f in self.price_files.values_mut() {
            let _ = f.flush();
        }
        for f in self.fill_files.values_mut() {
            let _ = f.flush();
        }
        for f in self.order_event_files.values_mut() {
            let _ = f.flush();
        }
        for f in self.decision_files.values_mut() {
            let _ = f.flush();
        }
        for f in self.book_snapshot_files.values_mut() {
            let _ = f.flush();
        }
        for f in self.latency_files.values_mut() {
            let _ = f.flush();
        }
        if let Some(f) = &mut self.session_summary_file {
            let _ = f.flush();
        }
    }

    /// Close files for a period (e.g., when market resolves).
    pub fn close_period(&mut self, period: &str) {
        self.order_files.remove(period);
        self.order_snapshots.remove(period);
        self.price_files.remove(period);
        self.fill_files.remove(period);
        self.order_event_files.remove(period);
        self.decision_files.remove(period);
        self.book_snapshot_files.remove(period);
        self.latency_files.remove(period);
    }

    fn log_order_terminal(
        &mut self,
        period: &str,
        order_id: &str,
        final_status: &str,
        cancel_reason: Option<&str>,
        fill_price: Option<Decimal>,
        fill_size: Option<Decimal>,
    ) {
        let terminal_ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        let snapshot = self
            .order_snapshots
            .get_mut(period)
            .and_then(|orders| orders.remove(order_id));

        let Some(snapshot) = snapshot else {
            warn!(
                period,
                order_id,
                final_status,
                "[logger] Missing order snapshot for terminal lifecycle event"
            );
            return;
        };

        let outcome_str = match snapshot.outcome {
            Outcome::Yes => "UP",
            Outcome::No => "DOWN",
        };
        let cancel_ts = if final_status == "CANCELLED" || final_status == "EXPIRED" {
            terminal_ts.as_str()
        } else {
            ""
        };
        let fill_ts = if final_status == "FILLED" {
            terminal_ts.as_str()
        } else {
            ""
        };
        let cancel_reason = csv_escape_field(cancel_reason.unwrap_or(""));
        let fill_price_str = fill_price.map(|p| p.to_string()).unwrap_or_default();
        let fill_size_str = fill_size.map(|s| s.to_string()).unwrap_or_default();
        let line = format!(
            "{},{},{},{},{},{:.2},{:.2},{:.6},{:.6},{:.10},{:.1},{},{},{},{},{},{},{},{},{}",
            snapshot.placed_ts,
            snapshot.order_type,
            outcome_str,
            snapshot.price,
            snapshot.size,
            snapshot.binance_btc,
            snapshot.btc_open,
            snapshot.fv_up,
            snapshot.fv_down,
            snapshot.sigma,
            snapshot.remaining_secs,
            snapshot.condition_id,
            order_id,
            snapshot.mode,
            cancel_ts,
            cancel_reason,
            fill_ts,
            fill_price_str,
            fill_size_str,
            final_status
        );
        if let Some(f) = self.ensure_order_file(period) {
            let _ = writeln!(f, "{line}");
        }
    }
}

fn csv_escape_field(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}
