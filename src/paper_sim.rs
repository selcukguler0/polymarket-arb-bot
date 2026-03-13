//! Paper trading fill simulation engine.
//!
//! Simulates realistic order lifecycle:
//! 1. Orders are placed as limit orders with a price.
//! 2. Fills are checked BEFORE orders are cancelled/replaced each tick.
//! 3. Fill conditions:
//!    a) Orderbook match: our bid >= real CLOB best_ask, AND order has been
//!       resting >= QUEUE_MIN_REST_SECS (models FIFO queue position).
//!       Fresh orders (<1s) that cross the book are PostOnly-rejected instead.
//!    b) FV-crossing: fair value crossed through our bid level since last tick
//!       (in real markets, as FV drops past our bid, sellers hit our order)
//!    c) Proximity fill: bid is within 2c of best_ask AND has been resting
//!       for >5 seconds. Models natural market flow at ~5% rate per tick.
//! 4. Fills flow through the existing FillHandler/InventoryManager pipeline.
//! 5. PostOnly rejections are returned separately for orchestrator cleanup.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::types::*;

/// A simulated resting limit order.
#[derive(Debug, Clone)]
pub struct PaperOrder {
    pub order_id: OrderId,
    pub condition_id: ConditionId,
    pub outcome: Outcome,
    pub price: Decimal,
    pub size: Decimal,
    pub side: PaperSide,
    pub placed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaperSide {
    Buy,
    Sell,
}

/// Result of checking if paper orders should fill.
#[derive(Debug, Clone)]
pub struct PaperFill {
    pub order: PaperOrder,
    pub fill_price: Decimal,
    pub fill_time: DateTime<Utc>,
}

/// Combined result from a fill check: fills that executed + PostOnly rejections.
#[derive(Debug)]
pub struct PaperFillResult {
    pub fills: Vec<PaperFill>,
    /// Orders that were PostOnly-rejected (crossed the book while too fresh).
    /// The orchestrator must remove these from its resting_orders map.
    pub postonly_rejections: Vec<PaperOrder>,
}

/// Paper trading simulation engine.
pub struct PaperSimulator {
    /// All resting paper orders, keyed by order_id.
    orders: HashMap<OrderId, PaperOrder>,
    /// Counter for generating unique order IDs.
    next_id: u64,
    /// Track last known fair values per market to detect FV crosses.
    last_fv: HashMap<ConditionId, (f64, f64)>, // (fv_yes, fv_no)
    /// Simple deterministic counter for proximity fill decisions.
    /// Fills every Nth check when proximity conditions are met.
    tick_counter: u64,
}

/// How often a proximity-eligible order fills (1 in N ticks).
/// At 500ms ticks, N=20 means ~one fill every 10 seconds on average.
/// Calibrated against live fill rate of ~8% (was 3, producing ~35% paper fill rate).
const PROXIMITY_FILL_EVERY_N: u64 = 20;

/// Maximum distance (in cents) from best_ask/best_bid for proximity fills.
/// Set to 2c — in real CLOB, bids more than 2c from the ask rarely fill
/// without an active price-cross event.
const PROXIMITY_MAX_DISTANCE: Decimal = dec!(0.02);

/// Minimum resting time before proximity fill is eligible (seconds).
/// Models queue position: other makers ahead in the queue fill first.
/// 5 seconds means most orders cancelled by fv_stale_shift never reach eligibility.
const PROXIMITY_MIN_REST_SECS: i64 = 5;

/// Minimum resting time for book-match fills (bid >= ask) in seconds.
/// Models FIFO queue position — even when your price matches the ask,
/// orders ahead of you in the queue fill first.
const QUEUE_MIN_REST_SECS: i64 = 3;

/// Orders resting less than this (seconds) that cross the book are
/// treated as PostOnly rejections (the order would have been rejected
/// at submission in live mode).
const POSTONLY_FRESH_SECS: i64 = 1;

impl PaperSimulator {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            next_id: 1,
            last_fv: HashMap::new(),
            tick_counter: 0,
        }
    }

    /// Place a simulated buy limit order. Returns the synthetic order ID.
    pub fn place_buy(
        &mut self,
        condition_id: &str,
        outcome: Outcome,
        price: Decimal,
        size: Decimal,
    ) -> OrderId {
        self.place_buy_at(condition_id, outcome, price, size, Utc::now())
    }

    /// Place a buy order with explicit timestamp (for backtesting).
    pub fn place_buy_at(
        &mut self,
        condition_id: &str,
        outcome: Outcome,
        price: Decimal,
        size: Decimal,
        now: DateTime<Utc>,
    ) -> OrderId {
        let order_id = format!("paper_{:06}", self.next_id);
        self.next_id += 1;

        let order = PaperOrder {
            order_id: order_id.clone(),
            condition_id: condition_id.to_string(),
            outcome,
            price,
            size,
            side: PaperSide::Buy,
            placed_at: now,
        };
        self.orders.insert(order_id.clone(), order);
        order_id
    }

    /// Place a simulated sell limit order. Returns the synthetic order ID.
    pub fn place_sell(
        &mut self,
        condition_id: &str,
        outcome: Outcome,
        price: Decimal,
        size: Decimal,
    ) -> OrderId {
        self.place_sell_at(condition_id, outcome, price, size, Utc::now())
    }

    /// Place a sell order with explicit timestamp (for backtesting).
    pub fn place_sell_at(
        &mut self,
        condition_id: &str,
        outcome: Outcome,
        price: Decimal,
        size: Decimal,
        now: DateTime<Utc>,
    ) -> OrderId {
        let order_id = format!("paper_{:06}", self.next_id);
        self.next_id += 1;

        let order = PaperOrder {
            order_id: order_id.clone(),
            condition_id: condition_id.to_string(),
            outcome,
            price,
            size,
            side: PaperSide::Sell,
            placed_at: now,
        };
        self.orders.insert(order_id.clone(), order);
        order_id
    }

    /// Cancel a paper order.
    pub fn cancel(&mut self, order_id: &str) -> Option<PaperOrder> {
        self.orders.remove(order_id)
    }

    /// Cancel all orders for a market.
    pub fn cancel_market(&mut self, condition_id: &str) -> Vec<PaperOrder> {
        let to_remove: Vec<OrderId> = self
            .orders
            .iter()
            .filter(|(_, o)| o.condition_id == condition_id)
            .map(|(id, _)| id.clone())
            .collect();
        to_remove
            .into_iter()
            .filter_map(|id| self.orders.remove(&id))
            .collect()
    }

    /// Cancel all orders.
    pub fn cancel_all(&mut self) -> Vec<PaperOrder> {
        self.orders.drain().map(|(_, o)| o).collect()
    }

    /// Count resting buy shares for a condition_id / outcome.
    /// Used by pair completion to avoid placing duplicate orders.
    pub fn resting_buy_shares(&self, condition_id: &str, outcome: Outcome) -> Decimal {
        self.orders
            .values()
            .filter(|o| {
                o.condition_id == condition_id && o.outcome == outcome && o.side == PaperSide::Buy
            })
            .map(|o| o.size)
            .sum()
    }

    /// Check for fills using real CLOB orderbook data AND fair-value crossing.
    ///
    /// This must be called BEFORE cancelling/replacing orders each tick.
    ///
    /// Fill conditions for BUY orders:
    ///   1. Orderbook match: our bid >= real best_ask, resting >= QUEUE_MIN_REST_SECS
    ///      (models FIFO queue position). Fresh orders that cross get PostOnly-rejected.
    ///   2. FV-crossing: fair value dropped through our bid level since last tick
    ///      (as FV drops past our bid, a rational seller would hit our order)
    ///   3. Proximity fill: bid is within 2c of best_ask, has been resting >5s,
    ///      and the deterministic counter says it's time (~5% rate). Models natural flow.
    ///
    /// Fill conditions for SELL orders:
    ///   1. FV rose through our ask level since last tick
    ///   2. Proximity fill: ask is within 2c of best_bid, resting >5s.
    ///
    /// **Burst fill protection**: Tracks running position within the tick. If a
    /// buy fill would push imbalance beyond `max_imbalance_per_tick`, it is skipped
    /// (order stays resting). This prevents unrealistic all-15-levels-at-once fills.
    pub fn check_fills_with_book(
        &mut self,
        condition_id: &str,
        yes_best_ask: Option<Decimal>,
        no_best_ask: Option<Decimal>,
        yes_best_bid: Option<Decimal>,
        no_best_bid: Option<Decimal>,
        fv_yes: f64,
        fv_no: f64,
    ) -> PaperFillResult {
        self.check_fills_with_book_limited(
            condition_id,
            yes_best_ask,
            no_best_ask,
            yes_best_bid,
            no_best_bid,
            fv_yes,
            fv_no,
            None,
            Decimal::ZERO,
            Decimal::ZERO,
        )
    }

    /// Like `check_fills_with_book` but with optional imbalance limit for burst protection.
    ///
    /// - `max_imbalance`: if Some, skip buy fills that would push cost imbalance past this (USDC).
    /// - `current_yes_cost`/`current_no_cost`: current position costs (total_yes_spent/total_no_spent).
    ///
    /// Returns `PaperFillResult` containing both fills and PostOnly rejections.
    pub fn check_fills_with_book_limited(
        &mut self,
        condition_id: &str,
        yes_best_ask: Option<Decimal>,
        no_best_ask: Option<Decimal>,
        yes_best_bid: Option<Decimal>,
        no_best_bid: Option<Decimal>,
        fv_yes: f64,
        fv_no: f64,
        max_imbalance: Option<Decimal>,
        current_yes_cost: Decimal,
        current_no_cost: Decimal,
    ) -> PaperFillResult {
        self.check_fills_with_book_at(
            condition_id,
            yes_best_ask,
            no_best_ask,
            yes_best_bid,
            no_best_bid,
            fv_yes,
            fv_no,
            max_imbalance,
            current_yes_cost,
            current_no_cost,
            Utc::now(),
        )
    }

    /// Fill check with explicit timestamp (for backtesting).
    pub fn check_fills_with_book_at(
        &mut self,
        condition_id: &str,
        yes_best_ask: Option<Decimal>,
        no_best_ask: Option<Decimal>,
        yes_best_bid: Option<Decimal>,
        no_best_bid: Option<Decimal>,
        fv_yes: f64,
        fv_no: f64,
        max_imbalance: Option<Decimal>,
        current_yes_cost: Decimal,
        current_no_cost: Decimal,
        now: DateTime<Utc>,
    ) -> PaperFillResult {
        self.tick_counter += 1;
        let proximity_eligible = self.tick_counter % PROXIMITY_FILL_EVERY_N == 0;

        let prev_fv = self.last_fv.get(condition_id).copied();
        self.last_fv
            .insert(condition_id.to_string(), (fv_yes, fv_no));

        let prev_fv_yes = prev_fv.map(|(y, _)| y);
        let prev_fv_no = prev_fv.map(|(_, n)| n);

        // Collect fillable candidates and PostOnly rejections
        let mut candidates: Vec<(OrderId, PaperOrder, Decimal)> = Vec::new();
        let mut postonly_rejections: Vec<PaperOrder> = Vec::new();

        for (id, order) in &self.orders {
            if order.condition_id != condition_id {
                continue;
            }

            let price_f64 = order.price.to_f64().unwrap_or(0.0);
            let resting_secs = (now - order.placed_at).num_seconds();

            // Check for PostOnly rejection: fresh order that crosses the book.
            // In live mode, PostOnly orders that would immediately execute are
            // rejected by the exchange. We model this for orders < POSTONLY_FRESH_SECS.
            let crosses_book = match (order.side, order.outcome) {
                (PaperSide::Buy, Outcome::Yes) => {
                    yes_best_ask.map(|ask| order.price >= ask).unwrap_or(false)
                }
                (PaperSide::Buy, Outcome::No) => {
                    no_best_ask.map(|ask| order.price >= ask).unwrap_or(false)
                }
                (PaperSide::Sell, Outcome::Yes) => {
                    yes_best_bid.map(|bid| bid >= order.price).unwrap_or(false)
                }
                (PaperSide::Sell, Outcome::No) => {
                    no_best_bid.map(|bid| bid >= order.price).unwrap_or(false)
                }
            };

            if crosses_book && resting_secs < POSTONLY_FRESH_SECS {
                // PostOnly rejection: order is too fresh and crosses the book
                postonly_rejections.push(order.clone());
                continue;
            }

            let should_fill = match (order.side, order.outcome) {
                (PaperSide::Buy, Outcome::Yes) => {
                    // Book match with queue position delay
                    let book_match = resting_secs >= QUEUE_MIN_REST_SECS
                        && yes_best_ask.map(|ask| order.price >= ask).unwrap_or(false);
                    // FV-crossing: no queue delay (market event crosses through your price)
                    let fv_cross = prev_fv_yes
                        .map(|prev| prev > price_f64 && fv_yes <= price_f64)
                        .unwrap_or(false);
                    // Proximity: bid within 2c of ask, resting >5s, ~5% rate
                    let proximity = proximity_eligible
                        && resting_secs >= PROXIMITY_MIN_REST_SECS
                        && yes_best_ask
                            .map(|ask| {
                                ask > order.price && ask - order.price <= PROXIMITY_MAX_DISTANCE
                            })
                            .unwrap_or(false);
                    book_match || fv_cross || proximity
                }
                (PaperSide::Buy, Outcome::No) => {
                    let book_match = resting_secs >= QUEUE_MIN_REST_SECS
                        && no_best_ask.map(|ask| order.price >= ask).unwrap_or(false);
                    let fv_cross = prev_fv_no
                        .map(|prev| prev > price_f64 && fv_no <= price_f64)
                        .unwrap_or(false);
                    let proximity = proximity_eligible
                        && resting_secs >= PROXIMITY_MIN_REST_SECS
                        && no_best_ask
                            .map(|ask| {
                                ask > order.price && ask - order.price <= PROXIMITY_MAX_DISTANCE
                            })
                            .unwrap_or(false);
                    book_match || fv_cross || proximity
                }
                (PaperSide::Sell, Outcome::Yes) => {
                    let book_match = resting_secs >= QUEUE_MIN_REST_SECS
                        && yes_best_bid.map(|bid| bid >= order.price).unwrap_or(false);
                    let fv_cross = prev_fv_yes
                        .map(|prev| prev < price_f64 && fv_yes >= price_f64)
                        .unwrap_or(false);
                    let proximity = proximity_eligible
                        && resting_secs >= PROXIMITY_MIN_REST_SECS
                        && yes_best_bid
                            .map(|bid| {
                                order.price > bid && order.price - bid <= PROXIMITY_MAX_DISTANCE
                            })
                            .unwrap_or(false);
                    book_match || fv_cross || proximity
                }
                (PaperSide::Sell, Outcome::No) => {
                    let book_match = resting_secs >= QUEUE_MIN_REST_SECS
                        && no_best_bid.map(|bid| bid >= order.price).unwrap_or(false);
                    let fv_cross = prev_fv_no
                        .map(|prev| prev < price_f64 && fv_no >= price_f64)
                        .unwrap_or(false);
                    let proximity = proximity_eligible
                        && resting_secs >= PROXIMITY_MIN_REST_SECS
                        && no_best_bid
                            .map(|bid| {
                                order.price > bid && order.price - bid <= PROXIMITY_MAX_DISTANCE
                            })
                            .unwrap_or(false);
                    book_match || fv_cross || proximity
                }
            };

            if should_fill {
                candidates.push((id.clone(), order.clone(), order.price));
            }
        }

        // Remove PostOnly-rejected orders from the internal map
        for rejected in &postonly_rejections {
            self.orders.remove(&rejected.order_id);
        }

        // Sort by price descending so best-priced orders fill first
        candidates.sort_by(|a, b| b.2.cmp(&a.2));

        // Apply burst fill protection: track running cost (USDC-weighted imbalance)
        let mut running_yes_cost = current_yes_cost;
        let mut running_no_cost = current_no_cost;
        let mut fills = Vec::new();
        let mut filled_ids = Vec::new();

        for (id, order, _price) in candidates {
            // Check cost-based imbalance guard for BUY orders
            if let Some(max_imb) = max_imbalance {
                if order.side == PaperSide::Buy {
                    let order_cost = order.price * order.size;
                    let (new_yes_cost, new_no_cost) = match order.outcome {
                        Outcome::Yes => (running_yes_cost + order_cost, running_no_cost),
                        Outcome::No => (running_yes_cost, running_no_cost + order_cost),
                    };
                    let would_imbalance = (new_yes_cost - new_no_cost).abs();
                    if would_imbalance > max_imb {
                        // Skip this fill — cost imbalance guard
                        continue;
                    }
                }
            }

            // Update running cost
            let order_cost = order.price * order.size;
            match (order.side, order.outcome) {
                (PaperSide::Buy, Outcome::Yes) => running_yes_cost += order_cost,
                (PaperSide::Buy, Outcome::No) => running_no_cost += order_cost,
                (PaperSide::Sell, Outcome::Yes) => {
                    running_yes_cost -= order_cost.min(running_yes_cost)
                }
                (PaperSide::Sell, Outcome::No) => {
                    running_no_cost -= order_cost.min(running_no_cost)
                }
            }

            filled_ids.push(id);
            fills.push(PaperFill {
                order,
                fill_price: _price,
                fill_time: now,
            });
        }

        for id in filled_ids {
            self.orders.remove(&id);
        }

        PaperFillResult {
            fills,
            postonly_rejections,
        }
    }

    /// Get all resting orders for a market.
    pub fn orders_for_market(&self, condition_id: &str) -> Vec<&PaperOrder> {
        self.orders
            .values()
            .filter(|o| o.condition_id == condition_id)
            .collect()
    }

    /// Count orders on a specific side/outcome.
    pub fn order_count(&self, condition_id: &str, outcome: Outcome) -> usize {
        self.orders
            .values()
            .filter(|o| {
                o.condition_id == condition_id && o.outcome == outcome && o.side == PaperSide::Buy
            })
            .count()
    }

    /// Total resting orders.
    pub fn total_orders(&self) -> usize {
        self.orders.len()
    }
}
