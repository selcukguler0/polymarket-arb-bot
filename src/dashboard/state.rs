use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;

use crate::persistence::db::{EquityCurvePoint, PeriodResultRow};

/// Maximum number of data points to keep in history buffers.
const MAX_HISTORY: usize = 300;
const MAX_ORDERS: usize = 100;

/// Shared dashboard state updated by the orchestrator, rendered by the web UI.
#[derive(Debug, Clone, Serialize)]
pub struct DashboardState {
    // ── Asset Tag (e.g. "BTC" or "ETH") ──
    pub asset: String,

    // ── Top Status Bar ──
    pub btc_price: f64,
    pub btc_open: f64,
    pub total_pnl: Decimal,
    pub today_pnl: Decimal,
    pub win_rate: f64,
    pub total_periods: u32,
    pub total_fills: u32,
    pub wins: u32,
    pub losses: u32,
    pub open_positions: u32,

    // ── Active Market ──
    pub active_market_question: String,
    pub active_market_end: Option<DateTime<Utc>>,
    pub active_condition_id: String,

    // ── Fair Values ──
    pub fv_up: f64,
    pub fv_down: f64,
    pub sigma: f64,

    // ── Bids (our top ladder prices) ──
    pub bid_yes: Decimal,
    pub bid_no: Decimal,
    pub combined_bid: Decimal,

    // ── Actual Polymarket orderbook prices ──
    pub market_best_bid_up: Decimal,
    pub market_best_ask_up: Decimal,
    pub market_best_bid_down: Decimal,
    pub market_best_ask_down: Decimal,

    // ── Charts ──
    pub btc_price_history: VecDeque<f64>,
    pub pnl_history: VecDeque<f64>,
    pub equity_history: VecDeque<f64>,

    // ── Order Feed ──
    pub order_feed: VecDeque<OrderFeedEntry>,

    // ── Positions ──
    pub positions: Vec<PositionEntry>,

    // ── Resting Orders (current bids, updated every tick) ──
    pub resting_bids: Vec<RestingBid>,

    // ── Running combined cost (avg_yes + avg_no) ──
    pub running_combined_cost: Decimal,

    // ── Current Period Summary ──
    pub period_summary: PeriodSummary,

    // ── Period History (from DB, sent to frontend) ──
    pub period_history: Vec<PeriodResultRow>,

    // ── Equity Curve from DB ──
    pub equity_curve_db: Vec<EquityCurvePoint>,

    // ── Execution Pipeline ──
    pub pipeline: PipelineState,

    // ── Stats ──
    pub avg_per_trade: Decimal,
    pub sharpe: f64,
    pub max_drawdown: Decimal,
    pub kelly_fraction: f64,
    pub dd_limit: Decimal,

    // ── Detailed Order Log (for CSV export, not sent via SSE) ──
    #[serde(skip)]
    pub detailed_order_log: Vec<DetailedOrderEntry>,

    // ── Track current period condition_id for period summary scoping ──
    #[serde(skip)]
    pub current_period_condition_id: Option<String>,

    // ── System ──
    pub vol_per_sec: f64,
    pub markets_discovered: u32,
    pub active_market_count: u32,
    pub uptime_secs: u64,
    pub last_update: DateTime<Utc>,
    pub bot_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderFeedEntry {
    pub time: DateTime<Utc>,
    pub market: String,
    pub side: String,
    pub outcome: String,
    pub price: Decimal,
    pub size: Decimal,
    pub status: OrderStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum OrderStatus {
    Pending,
    Filled,
    Cancelled,
    Rejected,
}

impl std::fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderStatus::Pending => write!(f, "PENDING"),
            OrderStatus::Filled => write!(f, "FILLED"),
            OrderStatus::Cancelled => write!(f, "CANCELLED"),
            OrderStatus::Rejected => write!(f, "REJECTED"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionEntry {
    pub market_label: String,
    pub outcome: String,
    pub entry_price: Decimal,
    pub size: Decimal,
    pub pnl: Decimal,
    pub resolved: bool,
    pub winner: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RestingBid {
    pub outcome: String,
    pub price: Decimal,
    pub size: Decimal,
}

/// Per-outcome summary for the current active market period.
#[derive(Debug, Clone, Serialize, Default)]
pub struct PeriodSummaryEntry {
    pub outcome: String,
    pub qty: Decimal,
    pub avg_price: Decimal,
    pub cost: Decimal,
    pub current_price: Decimal,
    pub value: Decimal,
    pub return_pnl: Decimal,
    pub return_pct: f64,
}

/// Aggregated period summary (UP + DOWN + totals).
#[derive(Debug, Clone, Serialize, Default)]
pub struct PeriodSummary {
    pub up: PeriodSummaryEntry,
    pub down: PeriodSummaryEntry,
    pub total_cost: Decimal,
    pub total_value: Decimal,
    pub total_return: Decimal,
    pub complete_pairs: Decimal,
    pub locked_profit: Decimal,
}

/// Detailed order entry for CSV export / post-session analysis.
/// Contains all context at the moment the fill occurred.
#[derive(Debug, Clone, Serialize)]
pub struct DetailedOrderEntry {
    pub time: DateTime<Utc>,
    pub market: String,
    pub condition_id: String,
    pub side: String,
    pub outcome: String,
    pub fill_price: Decimal,
    pub size: Decimal,
    pub status: String,
    // Binance context at fill time
    pub btc_price_at_fill: f64,
    pub btc_open: f64,
    // Fair values at fill time
    pub fv_up: f64,
    pub fv_down: f64,
    pub sigma: f64,
    // Current bids at fill time
    pub bid_yes: Decimal,
    pub bid_no: Decimal,
    pub combined_bid: Decimal,
    // CLOB best prices at fill time
    pub best_ask_yes: Decimal,
    pub best_ask_no: Decimal,
    pub best_bid_yes: Decimal,
    pub best_bid_no: Decimal,
    // Position state at fill time
    pub pos_yes_qty: Decimal,
    pub pos_no_qty: Decimal,
    pub complete_pairs: Decimal,
    pub locked_profit: Decimal,
    // Timing
    pub secs_remaining: f64,
    pub total_pnl: Decimal,
    pub today_pnl: Decimal,
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineState {
    pub cex_feed_ok: bool,
    pub pm_odds_ok: bool,
    pub edge_found: bool,
    pub kelly_ok: bool,
    pub exec_ok: bool,
    pub last_edge: f64,
    pub last_kelly: f64,
}

impl Default for DashboardState {
    fn default() -> Self {
        Self {
            asset: String::new(),
            btc_price: 0.0,
            btc_open: 0.0,
            total_pnl: Decimal::ZERO,
            today_pnl: Decimal::ZERO,
            win_rate: 0.0,
            total_periods: 0,
            total_fills: 0,
            wins: 0,
            losses: 0,
            open_positions: 0,
            active_market_question: String::new(),
            active_market_end: None,
            active_condition_id: String::new(),
            fv_up: 0.5,
            fv_down: 0.5,
            sigma: 0.0,
            bid_yes: Decimal::ZERO,
            bid_no: Decimal::ZERO,
            combined_bid: Decimal::ZERO,
            market_best_bid_up: Decimal::ZERO,
            market_best_ask_up: Decimal::ZERO,
            market_best_bid_down: Decimal::ZERO,
            market_best_ask_down: Decimal::ZERO,
            btc_price_history: VecDeque::with_capacity(MAX_HISTORY),
            pnl_history: VecDeque::with_capacity(MAX_HISTORY),
            equity_history: VecDeque::with_capacity(MAX_HISTORY),
            order_feed: VecDeque::with_capacity(MAX_ORDERS),
            positions: Vec::new(),
            resting_bids: Vec::new(),
            running_combined_cost: Decimal::ZERO,
            period_summary: PeriodSummary::default(),
            period_history: Vec::new(),
            equity_curve_db: Vec::new(),
            pipeline: PipelineState {
                cex_feed_ok: false,
                pm_odds_ok: false,
                edge_found: false,
                kelly_ok: false,
                exec_ok: false,
                last_edge: 0.0,
                last_kelly: 0.0,
            },
            detailed_order_log: Vec::new(),
            current_period_condition_id: None,
            avg_per_trade: Decimal::ZERO,
            sharpe: 0.0,
            max_drawdown: Decimal::ZERO,
            kelly_fraction: 0.0,
            dd_limit: dec!(5.0),
            vol_per_sec: 0.0,
            markets_discovered: 0,
            active_market_count: 0,
            uptime_secs: 0,
            last_update: Utc::now(),
            bot_status: "running".to_string(),
        }
    }
}

impl DashboardState {
    pub fn push_btc_price(&mut self, price: f64) {
        if self.btc_price_history.len() >= MAX_HISTORY {
            self.btc_price_history.pop_front();
        }
        self.btc_price_history.push_back(price);
        self.btc_price = price;
    }

    pub fn push_pnl(&mut self, pnl: f64) {
        if self.pnl_history.len() >= MAX_HISTORY {
            self.pnl_history.pop_front();
        }
        self.pnl_history.push_back(pnl);
    }

    pub fn push_equity(&mut self, eq: f64) {
        if self.equity_history.len() >= MAX_HISTORY {
            self.equity_history.pop_front();
        }
        self.equity_history.push_back(eq);
    }

    pub fn push_order(&mut self, entry: OrderFeedEntry) {
        if self.order_feed.len() >= MAX_ORDERS {
            self.order_feed.pop_front();
        }
        self.order_feed.push_back(entry);
    }

    pub fn push_detailed_order(&mut self, entry: DetailedOrderEntry) {
        const MAX_DETAILED_LOG: usize = 10_000;
        if self.detailed_order_log.len() >= MAX_DETAILED_LOG {
            // Drain oldest 20% to avoid frequent shifts
            let drain_count = MAX_DETAILED_LOG / 5;
            self.detailed_order_log.drain(..drain_count);
        }
        self.detailed_order_log.push(entry);
    }

    pub fn record_trade_result(&mut self, pnl: Decimal) {
        self.total_periods += 1;
        self.total_pnl += pnl;
        self.today_pnl += pnl;
        if pnl > Decimal::ZERO {
            self.wins += 1;
        } else {
            self.losses += 1;
        }
        let total_wl = self.wins + self.losses;
        self.win_rate = if total_wl > 0 {
            self.wins as f64 / total_wl as f64
        } else {
            0.0
        };
        self.avg_per_trade = if self.total_periods > 0 {
            self.total_pnl / Decimal::from(self.total_periods)
        } else {
            Decimal::ZERO
        };

        let peak = self.pnl_history.iter().copied().fold(0.0_f64, f64::max);
        let current = self.total_pnl.to_f64().unwrap_or(0.0);
        let dd = peak - current;
        let dd_dec = Decimal::from_f64_retain(dd).unwrap_or(Decimal::ZERO);
        if dd_dec > self.max_drawdown {
            self.max_drawdown = dd_dec;
        }
    }

    pub fn secs_remaining(&self) -> i64 {
        match self.active_market_end {
            Some(end) => (end - Utc::now()).num_seconds().max(0),
            None => 0,
        }
    }

    pub fn countdown_str(&self) -> String {
        let secs = self.secs_remaining();
        format!("{:02}:{:02}", secs / 60, secs % 60)
    }
}
