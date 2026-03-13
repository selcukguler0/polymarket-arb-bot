use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

// ── Asset Enum ──

/// Which asset the bot is trading (BTC, ETH, SOL, XRP Up/Down markets).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Asset {
    BTC,
    ETH,
    SOL,
    XRP,
}

impl Asset {
    /// Binance stream symbol (lowercase, e.g. "btcusdt").
    pub fn binance_symbol(&self) -> &'static str {
        match self {
            Asset::BTC => "btcusdt",
            Asset::ETH => "ethusdt",
            Asset::SOL => "solusdt",
            Asset::XRP => "xrpusdt",
        }
    }

    /// Gamma API tag slug for market discovery.
    pub fn gamma_tag(&self) -> &'static str {
        match self {
            Asset::BTC => "bitcoin",
            Asset::ETH => "ethereum",
            Asset::SOL => "solana",
            Asset::XRP => "xrp",
        }
    }

    /// Prefix used in Polymarket market question titles.
    pub fn market_prefix(&self) -> &'static str {
        match self {
            Asset::BTC => "Bitcoin",
            Asset::ETH => "Ethereum",
            Asset::SOL => "Solana",
            Asset::XRP => "XRP",
        }
    }

    /// Short display name.
    pub fn display_name(&self) -> &'static str {
        match self {
            Asset::BTC => "BTC",
            Asset::ETH => "ETH",
            Asset::SOL => "SOL",
            Asset::XRP => "XRP",
        }
    }
}

impl fmt::Display for Asset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

/// Unique identifier for a market condition
pub type ConditionId = String;

/// Token ID (string representation of U256)
pub type TokenId = String;

/// Order ID from the CLOB
pub type OrderId = String;

/// Shared concurrent orderbook map: token_id → latest snapshot
pub type SharedOrderBooks = Arc<RwLock<HashMap<TokenId, OrderBookSnapshot>>>;

/// Shared tick_size updates from WS: condition_id → new tick_size.
/// WS task writes on tick_size_change events, main loop reads + applies.
pub type SharedTickSizes = Arc<RwLock<HashMap<ConditionId, Decimal>>>;

/// Notify handle: WS tasks signal this after writing new orderbook data,
/// waking the orchestrator's main loop to immediately run a quote cycle.
pub type BookNotify = Arc<tokio::sync::Notify>;

/// Which outcome of a binary market
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Yes,
    No,
}

impl Outcome {
    pub fn opposite(self) -> Self {
        match self {
            Outcome::Yes => Outcome::No,
            Outcome::No => Outcome::Yes,
        }
    }
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Outcome::Yes => write!(f, "yes"),
            Outcome::No => write!(f, "no"),
        }
    }
}

/// A tracked market with both token IDs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackedMarket {
    pub condition_id: ConditionId,
    pub token_id_yes: TokenId,
    pub token_id_no: TokenId,
    pub question: String,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: DateTime<Utc>,
    pub tick_size: Decimal,
    pub neg_risk: bool,
}

impl TrackedMarket {
    pub fn token_id(&self, outcome: Outcome) -> &str {
        match outcome {
            Outcome::Yes => &self.token_id_yes,
            Outcome::No => &self.token_id_no,
        }
    }

    pub fn outcome_for_token(&self, token_id: &str) -> Option<Outcome> {
        if token_id == self.token_id_yes {
            Some(Outcome::Yes)
        } else if token_id == self.token_id_no {
            Some(Outcome::No)
        } else {
            None
        }
    }

    /// Market duration in seconds if `start_date` is available and sane.
    pub fn duration_secs(&self) -> Option<i64> {
        self.start_date
            .map(|start| (self.end_date - start).num_seconds())
            .filter(|secs| *secs > 0)
    }

    /// Runtime fallback used by v2 logic while preserving compatibility with
    /// older persisted/in-memory markets that may not carry `start_date`.
    pub fn effective_start_date_15m_fallback(&self) -> DateTime<Utc> {
        self.start_date
            .unwrap_or(self.end_date - chrono::Duration::minutes(15))
    }

    /// Runtime fallback used by v2 15m math until all timing assumptions are
    /// fully metadata-driven.
    pub fn effective_duration_secs_15m_fallback(&self) -> i64 {
        self.duration_secs().unwrap_or(900)
    }

    pub fn effective_duration_minutes_15m_fallback(&self) -> u32 {
        (self.effective_duration_secs_15m_fallback() / 60).max(1) as u32
    }
}

/// Snapshot of one side of an orderbook
#[derive(Debug, Clone, Default)]
pub struct OrderBookSnapshot {
    pub asset_id: TokenId,
    pub bids: BTreeMap<Decimal, Decimal>, // price → size, sorted descending
    pub asks: BTreeMap<Decimal, Decimal>, // price → size, sorted ascending
    pub timestamp: DateTime<Utc>,
}

impl OrderBookSnapshot {
    pub fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.bids.iter().next_back().map(|(&p, &s)| (p, s))
    }

    pub fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.asks.iter().next().map(|(&p, &s)| (p, s))
    }

    pub fn midpoint(&self) -> Option<Decimal> {
        let (bid, _) = self.best_bid()?;
        let (ask, _) = self.best_ask()?;
        Some((bid + ask) / Decimal::TWO)
    }

    /// Total visible size at or better than target_price on the bid side
    pub fn bid_depth_at_or_above(&self, target_price: Decimal) -> Decimal {
        self.bids.range(target_price..).map(|(_, &size)| size).sum()
    }

    pub fn bid_depth_top_n(&self, levels: usize) -> Decimal {
        self.bids
            .iter()
            .rev()
            .take(levels)
            .map(|(_, &size)| size)
            .sum()
    }

    pub fn ask_depth_top_n(&self, levels: usize) -> Decimal {
        self.asks.iter().take(levels).map(|(_, &size)| size).sum()
    }

    pub fn best_level_imbalance(&self) -> Decimal {
        let bid = self
            .best_bid()
            .map(|(_, size)| size)
            .unwrap_or(Decimal::ZERO);
        let ask = self
            .best_ask()
            .map(|(_, size)| size)
            .unwrap_or(Decimal::ZERO);
        imbalance_ratio(bid, ask)
    }

    pub fn top_n_imbalance(&self, levels: usize) -> Decimal {
        imbalance_ratio(self.bid_depth_top_n(levels), self.ask_depth_top_n(levels))
    }
}

fn imbalance_ratio(bid_depth: Decimal, ask_depth: Decimal) -> Decimal {
    let denom = bid_depth + ask_depth;
    if denom <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        (bid_depth - ask_depth) / denom
    }
}

/// Lifecycle phase of a 15-minute market
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketPhase {
    /// >10 minutes remaining
    Early,
    /// 5-10 minutes remaining
    Middle,
    /// safety_margin to 5 minutes remaining
    Late,
    /// <safety_margin remaining — CANCEL EVERYTHING
    Closing,
    /// Market has ended
    Resolved,
}

/// Position in a single market
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Position {
    pub condition_id: ConditionId,
    pub yes_qty: Decimal,
    pub no_qty: Decimal,
    pub total_yes_spent: Decimal,
    pub total_no_spent: Decimal,
}

impl Position {
    pub fn total_qty(&self) -> Decimal {
        self.yes_qty + self.no_qty
    }

    /// Ratio of the heavier side to total. Returns 0.5 if balanced, up to 1.0 if fully one-sided.
    pub fn imbalance_ratio(&self) -> Decimal {
        let total = self.total_qty();
        if total.is_zero() {
            return Decimal::ZERO;
        }
        let heavy = self.yes_qty.max(self.no_qty);
        heavy / total
    }

    /// Which outcome we hold more of
    pub fn heavy_side(&self) -> Option<Outcome> {
        if self.yes_qty > self.no_qty {
            Some(Outcome::Yes)
        } else if self.no_qty > self.yes_qty {
            Some(Outcome::No)
        } else {
            None
        }
    }

    /// Cost-weighted imbalance: positive = heavier YES spending, negative = heavier NO.
    /// Used by imbalance guards so cheap deep-grid fills don't trigger the same
    /// restrictions as expensive regular-ladder fills.
    pub fn cost_imbalance(&self) -> Decimal {
        self.total_yes_spent - self.total_no_spent
    }

    /// Number of complete YES+NO pairs
    pub fn complete_pairs(&self) -> Decimal {
        self.yes_qty.min(self.no_qty)
    }

    /// Average cost per YES share, or ZERO if no YES shares held.
    pub fn avg_yes_cost(&self) -> Decimal {
        if self.yes_qty.is_zero() {
            Decimal::ZERO
        } else {
            self.total_yes_spent / self.yes_qty
        }
    }

    /// Average cost per NO share, or ZERO if no NO shares held.
    pub fn avg_no_cost(&self) -> Decimal {
        if self.no_qty.is_zero() {
            Decimal::ZERO
        } else {
            self.total_no_spent / self.no_qty
        }
    }

    /// Average combined cost per pair (avg_yes_cost + avg_no_cost).
    pub fn avg_combined_cost(&self) -> Decimal {
        self.avg_yes_cost() + self.avg_no_cost()
    }

    /// The side with fewer shares (the "light" side), or None if balanced.
    pub fn light_side(&self) -> Option<Outcome> {
        if self.no_qty < self.yes_qty {
            Some(Outcome::No)
        } else if self.yes_qty < self.no_qty {
            Some(Outcome::Yes)
        } else {
            None
        }
    }

    /// Quantity that is below the minimum order size and cannot be sold via API.
    /// Returns total dust across both sides.
    pub fn dust_qty(&self, min_order_shares: Decimal) -> Decimal {
        let yes_dust = if self.yes_qty > Decimal::ZERO && self.yes_qty < min_order_shares {
            self.yes_qty
        } else {
            Decimal::ZERO
        };
        let no_dust = if self.no_qty > Decimal::ZERO && self.no_qty < min_order_shares {
            self.no_qty
        } else {
            Decimal::ZERO
        };
        yes_dust + no_dust
    }

    /// Estimated USDC value locked in dust positions (uses avg cost).
    pub fn dust_value(&self, min_order_shares: Decimal) -> Decimal {
        let yes_dust_val = if self.yes_qty > Decimal::ZERO && self.yes_qty < min_order_shares {
            self.avg_yes_cost() * self.yes_qty
        } else {
            Decimal::ZERO
        };
        let no_dust_val = if self.no_qty > Decimal::ZERO && self.no_qty < min_order_shares {
            self.avg_no_cost() * self.no_qty
        } else {
            Decimal::ZERO
        };
        yes_dust_val + no_dust_val
    }

    /// Locked profit from complete pairs: pairs × (1.0 - avg_yes_cost - avg_no_cost)
    pub fn locked_profit(&self) -> Decimal {
        let pairs = self.complete_pairs();
        if pairs.is_zero() {
            return Decimal::ZERO;
        }
        pairs * (Decimal::ONE - self.avg_yes_cost() - self.avg_no_cost())
    }
}

/// A fill event received from the WebSocket
#[derive(Debug, Clone)]
pub struct FillEvent {
    /// Unique trade ID from the WS — used for deduplication across market subscriptions.
    pub trade_id: String,
    pub order_id: OrderId,
    pub condition_id: ConditionId,
    pub outcome: Outcome,
    pub price: Decimal,
    pub size: Decimal,
    pub side: FillSide,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillSide {
    Buy,
    Sell,
}

/// Action the fill handler decides to take
#[derive(Debug, Clone)]
pub enum FillAction {
    /// No action needed, position is balanced
    None,
    /// Skew quotes (adjust prices but don't cancel)
    SkewQuotes,
    /// Cancel orders on the overweight side
    CancelOverweightSide {
        outcome: Outcome,
        order_ids: Vec<OrderId>,
    },
    /// Emergency: cancel all + sell excess
    EmergencySell {
        outcome: Outcome,
        excess_qty: Decimal,
        order_ids_to_cancel: Vec<OrderId>,
    },
}

/// Portfolio summary for monitoring
#[derive(Debug, Clone, Serialize)]
pub struct PortfolioSummary {
    pub active_markets: usize,
    pub total_exposure: Decimal,
    pub daily_pnl: Decimal,
    pub positions: Vec<Position>,
}

/// Messages sent to the alerting system
#[derive(Debug, Clone)]
pub enum AlertMessage {
    Emergency(String),
    Risk(String),
    System(String),
    Daily(String),
}

impl AlertMessage {
    pub fn category(&self) -> &'static str {
        match self {
            AlertMessage::Emergency(_) => "EMERGENCY",
            AlertMessage::Risk(_) => "RISK",
            AlertMessage::System(_) => "SYSTEM",
            AlertMessage::Daily(_) => "DAILY",
        }
    }

    pub fn text(&self) -> &str {
        match self {
            AlertMessage::Emergency(s)
            | AlertMessage::Risk(s)
            | AlertMessage::System(s)
            | AlertMessage::Daily(s) => s,
        }
    }
}

// ── Order Lifecycle State Machine ──

/// Tracks the complete lifecycle of an order for audit trail and debugging.
#[derive(Debug, Clone, Serialize)]
pub struct OrderLifecycle {
    pub order_id: OrderId,
    pub condition_id: ConditionId,
    pub outcome: Outcome,
    pub price: Decimal,
    pub size: Decimal,
    pub created_at: DateTime<Utc>,
    pub state: OrderState,
}

/// State of an order in its lifecycle.
#[derive(Debug, Clone, Serialize)]
pub enum OrderState {
    Resting,
    Filled {
        filled_at: DateTime<Utc>,
        fill_price: Decimal,
    },
    Cancelled {
        cancelled_at: DateTime<Utc>,
        reason: String,
    },
}

impl OrderLifecycle {
    pub fn new(
        order_id: OrderId,
        condition_id: ConditionId,
        outcome: Outcome,
        price: Decimal,
        size: Decimal,
    ) -> Self {
        Self {
            order_id,
            condition_id,
            outcome,
            price,
            size,
            created_at: Utc::now(),
            state: OrderState::Resting,
        }
    }

    pub fn mark_filled(&mut self, fill_price: Decimal) {
        if matches!(self.state, OrderState::Resting) {
            self.state = OrderState::Filled {
                filled_at: Utc::now(),
                fill_price,
            };
        }
    }

    pub fn mark_cancelled(&mut self, reason: &str) {
        if matches!(self.state, OrderState::Resting) {
            self.state = OrderState::Cancelled {
                cancelled_at: Utc::now(),
                reason: reason.to_string(),
            };
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            OrderState::Filled { .. } | OrderState::Cancelled { .. }
        )
    }
}

// ── Time Manager ──

pub struct TimeManager {
    resolution_safety_margin_secs: u64,
}

impl TimeManager {
    pub fn new(resolution_safety_margin_secs: u64) -> Self {
        Self {
            resolution_safety_margin_secs,
        }
    }

    /// Determine the current phase of a market based on its end_date.
    /// Uses percentage-based thresholds that auto-scale with market duration.
    /// `duration_secs`: total market duration (e.g. 900 for 15-min, 300 for 5-min).
    /// If not provided, falls back to 900s (15-min) for backward compatibility.
    pub fn phase(&self, end_date: DateTime<Utc>) -> MarketPhase {
        self.phase_for_duration(end_date, 900)
    }

    /// Phase determination with explicit duration (auto-scales thresholds).
    pub fn phase_for_duration(&self, end_date: DateTime<Utc>, duration_secs: i64) -> MarketPhase {
        let now = Utc::now();
        let remaining = end_date - now;
        let remaining_secs = remaining.num_seconds();

        if remaining_secs <= 0 {
            MarketPhase::Resolved
        } else if remaining_secs <= self.resolution_safety_margin_secs as i64 {
            MarketPhase::Closing
        } else {
            // Percentage-based thresholds: Late = last 33%, Middle = 33-67%, Early = first 33%
            // resolution_safety_margin_secs is always absolute (not percentage-based)
            let effective_duration = duration_secs - self.resolution_safety_margin_secs as i64;
            if effective_duration <= 0 {
                // Duration too short for meaningful phases
                MarketPhase::Late
            } else {
                let remaining_after_safety =
                    remaining_secs - self.resolution_safety_margin_secs as i64;
                let remaining_pct = remaining_after_safety as f64 / effective_duration as f64;
                if remaining_pct <= 0.33 {
                    MarketPhase::Late
                } else if remaining_pct <= 0.67 {
                    MarketPhase::Middle
                } else {
                    MarketPhase::Early
                }
            }
        }
    }

    /// Seconds until resolution (negative if already resolved)
    pub fn seconds_remaining(&self, end_date: DateTime<Utc>) -> i64 {
        (end_date - Utc::now()).num_seconds()
    }

    /// Whether we should cancel all orders for this market right now
    pub fn should_cancel_all(&self, end_date: DateTime<Utc>) -> bool {
        matches!(
            self.phase(end_date),
            MarketPhase::Closing | MarketPhase::Resolved
        )
    }

    /// Predict the next 15-minute market window start time
    pub fn next_market_start(&self, current_end: DateTime<Utc>) -> DateTime<Utc> {
        current_end
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use rust_decimal_macros::dec;

    #[test]
    fn test_phase_transitions() {
        let tm = TimeManager::new(120);
        let now = Utc::now();

        assert_eq!(tm.phase(now - Duration::seconds(10)), MarketPhase::Resolved);
        assert_eq!(tm.phase(now + Duration::seconds(60)), MarketPhase::Closing);
        assert_eq!(tm.phase(now + Duration::seconds(200)), MarketPhase::Late);
        assert_eq!(tm.phase(now + Duration::seconds(400)), MarketPhase::Middle);
        assert_eq!(tm.phase(now + Duration::seconds(700)), MarketPhase::Early);
    }

    #[test]
    fn test_tracked_market_duration_and_fallback_helpers() {
        let end = Utc::now();
        let start = end - Duration::seconds(900);

        let with_start = TrackedMarket {
            condition_id: "c1".into(),
            token_id_yes: "y".into(),
            token_id_no: "n".into(),
            question: "BTC test".into(),
            start_date: Some(start),
            end_date: end,
            tick_size: dec!(0.01),
            neg_risk: false,
        };
        assert_eq!(with_start.duration_secs(), Some(900));
        assert_eq!(with_start.effective_duration_secs_15m_fallback(), 900);
        assert_eq!(with_start.effective_start_date_15m_fallback(), start);

        let no_start = TrackedMarket {
            start_date: None,
            ..with_start
        };
        assert_eq!(no_start.duration_secs(), None);
        assert_eq!(no_start.effective_duration_secs_15m_fallback(), 900);
        assert_eq!(
            no_start.effective_start_date_15m_fallback(),
            end - Duration::minutes(15)
        );
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    proptest! {
        #[test]
        fn complete_pairs_le_min(yes in 0u64..10_000, no in 0u64..10_000) {
            let pos = Position {
                yes_qty: Decimal::from(yes),
                no_qty: Decimal::from(no),
                ..Default::default()
            };
            let pairs = pos.complete_pairs();
            prop_assert!(pairs <= pos.yes_qty);
            prop_assert!(pairs <= pos.no_qty);
        }

        #[test]
        fn imbalance_ratio_range(yes in 0u64..10_000, no in 0u64..10_000) {
            let pos = Position {
                yes_qty: Decimal::from(yes),
                no_qty: Decimal::from(no),
                ..Default::default()
            };
            let ratio = pos.imbalance_ratio();
            prop_assert!(ratio >= Decimal::ZERO);
            prop_assert!(ratio <= Decimal::ONE);
        }

        #[test]
        fn locked_profit_sign(
            yes_qty in 1u64..1_000,
            no_qty in 1u64..1_000,
            // avg costs that sum below 1.0 → positive locked profit
            yes_cost_cents in 10u64..49,
            no_cost_cents in 10u64..49,
        ) {
            let yes_cost = Decimal::from(yes_cost_cents) / dec!(100);
            let no_cost = Decimal::from(no_cost_cents) / dec!(100);
            let pos = Position {
                yes_qty: Decimal::from(yes_qty),
                no_qty: Decimal::from(no_qty),
                total_yes_spent: yes_cost * Decimal::from(yes_qty),
                total_no_spent: no_cost * Decimal::from(no_qty),
                ..Default::default()
            };
            // avg_combined < 1.0 guaranteed since max = 0.49+0.49 = 0.98
            let profit = pos.locked_profit();
            let pairs = pos.complete_pairs();
            if pairs > Decimal::ZERO {
                prop_assert!(profit > Decimal::ZERO,
                    "Expected positive profit when avg_combined={} < 1.0, got {}",
                    pos.avg_combined_cost(), profit);
            }
        }

        #[test]
        fn dust_qty_bounded(
            yes in 0u64..100,
            no in 0u64..100,
        ) {
            let pos = Position {
                yes_qty: Decimal::from(yes),
                no_qty: Decimal::from(no),
                ..Default::default()
            };
            let dust = pos.dust_qty(dec!(5));
            prop_assert!(dust <= pos.yes_qty + pos.no_qty);
        }
    }
}
