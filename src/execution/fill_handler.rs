use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{info, warn};

use crate::config::ValidatedConfig;
use crate::persistence::Database;
use crate::risk::InventoryManager;
use crate::types::*;

/// Pre-indexed order state for a single market, enabling fast lookup on fill.
#[derive(Debug, Default, Clone)]
pub struct MarketOrderState {
    /// Order IDs grouped by outcome
    pub yes_order_ids: Vec<OrderId>,
    pub no_order_ids: Vec<OrderId>,
}

impl MarketOrderState {
    pub fn order_ids_for(&self, outcome: Outcome) -> &[OrderId] {
        match outcome {
            Outcome::Yes => &self.yes_order_ids,
            Outcome::No => &self.no_order_ids,
        }
    }

    pub fn add_order(&mut self, order_id: OrderId, outcome: Outcome) {
        match outcome {
            Outcome::Yes => self.yes_order_ids.push(order_id),
            Outcome::No => self.no_order_ids.push(order_id),
        }
    }

    pub fn remove_order(&mut self, order_id: &str) {
        self.yes_order_ids.retain(|id| id != order_id);
        self.no_order_ids.retain(|id| id != order_id);
    }

    pub fn clear(&mut self) {
        self.yes_order_ids.clear();
        self.no_order_ids.clear();
    }

    pub fn all_order_ids(&self) -> Vec<OrderId> {
        let mut all = self.yes_order_ids.clone();
        all.extend(self.no_order_ids.iter().cloned());
        all
    }
}

pub struct FillHandler {
    max_imbalance_ratio: Decimal,
    pause_imbalance_ratio: Decimal,
    emergency_imbalance_ratio: Decimal,
    /// Pre-indexed order state per market
    order_states: HashMap<ConditionId, MarketOrderState>,
    /// Dedup: trade IDs already processed (prevents phantom fills from duplicate WS subscriptions).
    /// Uses VecDeque for insertion order (LRU eviction) + HashSet for O(1) lookup.
    seen_trade_ids: HashSet<String>,
    seen_trade_ids_order: VecDeque<String>,
    inventory: Arc<InventoryManager>,
    db: Arc<Database>,
}

impl FillHandler {
    pub fn new(
        config: &ValidatedConfig,
        inventory: Arc<InventoryManager>,
        db: Arc<Database>,
    ) -> Self {
        Self {
            max_imbalance_ratio: config.max_imbalance_ratio,
            pause_imbalance_ratio: config.pause_imbalance_ratio,
            emergency_imbalance_ratio: config.emergency_imbalance_ratio,
            order_states: HashMap::new(),
            seen_trade_ids: HashSet::new(),
            seen_trade_ids_order: VecDeque::new(),
            inventory,
            db,
        }
    }

    /// Register a new resting order for fast lookup on fill.
    pub fn register_order(&mut self, condition_id: &str, order_id: OrderId, outcome: Outcome) {
        self.order_states
            .entry(condition_id.to_string())
            .or_default()
            .add_order(order_id, outcome);
    }

    /// Remove a cancelled/filled order from tracking.
    pub fn unregister_order(&mut self, condition_id: &str, order_id: &str) {
        if let Some(state) = self.order_states.get_mut(condition_id) {
            state.remove_order(order_id);
        }
    }

    /// Clear all orders for a market (used on cancel-all).
    pub fn clear_market_orders(&mut self, condition_id: &str) {
        if let Some(state) = self.order_states.get_mut(condition_id) {
            state.clear();
        }
    }

    /// Replace all tracked orders for a market with new ones.
    pub fn replace_orders(&mut self, condition_id: &str, orders: Vec<(OrderId, Outcome)>) {
        let state = self
            .order_states
            .entry(condition_id.to_string())
            .or_default();
        state.clear();
        for (id, outcome) in orders {
            state.add_order(id, outcome);
        }
    }

    /// Process a fill event and determine what action to take.
    /// Returns (FillAction, sell_realized_pnl) — sell_realized_pnl is non-zero only for sell fills.
    pub async fn handle_fill(&mut self, fill: &FillEvent) -> (FillAction, Decimal) {
        // Update position in inventory manager (buy adds, sell reduces)
        let sell_realized_pnl = match fill.side {
            FillSide::Buy => {
                self.inventory
                    .record_fill(&fill.condition_id, fill.outcome, fill.price, fill.size);
                Decimal::ZERO
            }
            FillSide::Sell => {
                self.inventory
                    .record_sell(&fill.condition_id, fill.outcome, fill.price, fill.size)
            }
        };

        // BUG #9 fix: Persist fill and position in a single DB transaction.
        // If either write fails, neither is committed, keeping DB consistent on crash.
        let side_str = match fill.side {
            FillSide::Buy => "buy",
            FillSide::Sell => "sell",
        };
        let position = self.inventory.get_position(&fill.condition_id);
        if let Some(pos) = &position {
            if let Err(e) = self
                .db
                .write_fill_and_position(
                    &fill.order_id,
                    &fill.condition_id,
                    fill.outcome,
                    fill.price,
                    fill.size,
                    side_str,
                    pos,
                )
                .await
            {
                warn!("Failed to persist fill+position: {e}");
            }
        }

        // Compute imbalance
        let imbalance = self.inventory.get_imbalance(&fill.condition_id);

        // Absolute excess: the ratio-based check fires on ANY one-sided position
        // (e.g., 0 UP + 3 DOWN = ratio 1.0 > 0.80 → emergency sell on 3 shares).
        // Only escalate to cancel/emergency when the absolute excess is meaningful
        // (at least one full order size worth of imbalance).
        let abs_excess = position
            .as_ref()
            .map(|p| (p.yes_qty - p.no_qty).abs())
            .unwrap_or(Decimal::ZERO);
        const MIN_EXCESS_FOR_EMERGENCY: Decimal = dec!(20);

        let order_state = self
            .order_states
            .get(&fill.condition_id)
            .cloned()
            .unwrap_or_default();

        // Decision tree
        if imbalance <= self.max_imbalance_ratio {
            // Balanced enough
            if let Some(pos) = &position {
                let pairs = pos.complete_pairs();
                let locked = pos.locked_profit();
                if pairs > Decimal::ZERO {
                    info!(
                        condition_id = %fill.condition_id,
                        %pairs,
                        locked_profit = %locked,
                        "Complete pairs formed"
                    );
                }
            }
            (FillAction::None, sell_realized_pnl)
        } else if imbalance <= self.pause_imbalance_ratio {
            // Moderate imbalance: skew quotes
            info!(
                condition_id = %fill.condition_id,
                %imbalance,
                "Moderate imbalance, skewing quotes"
            );
            (FillAction::SkewQuotes, sell_realized_pnl)
        } else if imbalance <= self.emergency_imbalance_ratio
            || abs_excess < MIN_EXCESS_FOR_EMERGENCY
        {
            // Severe ratio but tolerable absolute excess: cancel overweight side only.
            // The abs_excess guard prevents escalating to emergency sell when the
            // position is small (e.g., first few fills from zero are always 100% one-sided).
            let heavy = position
                .as_ref()
                .and_then(|p| p.heavy_side())
                .unwrap_or(fill.outcome);

            let ids_to_cancel = order_state.order_ids_for(heavy).to_vec();

            warn!(
                condition_id = %fill.condition_id,
                %imbalance,
                %abs_excess,
                ?heavy,
                cancel_count = ids_to_cancel.len(),
                "Severe imbalance, cancelling overweight side"
            );

            (
                FillAction::CancelOverweightSide {
                    outcome: heavy,
                    order_ids: ids_to_cancel,
                },
                sell_realized_pnl,
            )
        } else {
            // Emergency: cancel all + sell excess (only when abs_excess >= MIN_EXCESS_FOR_EMERGENCY)
            let heavy = position
                .as_ref()
                .and_then(|p| p.heavy_side())
                .unwrap_or(fill.outcome);

            let excess = position
                .as_ref()
                .map(|p| match heavy {
                    Outcome::Yes => p.yes_qty - p.no_qty,
                    Outcome::No => p.no_qty - p.yes_qty,
                })
                .unwrap_or_default();

            let all_ids = order_state.all_order_ids();

            warn!(
                condition_id = %fill.condition_id,
                %imbalance,
                ?heavy,
                %excess,
                "EMERGENCY: selling excess inventory"
            );

            (
                FillAction::EmergencySell {
                    outcome: heavy,
                    excess_qty: excess,
                    order_ids_to_cancel: all_ids,
                },
                sell_realized_pnl,
            )
        }
    }

    /// Remove all tracking for a resolved market.
    pub fn remove_market(&mut self, condition_id: &str) {
        self.order_states.remove(condition_id);
    }

    /// FIX 16: Get the current order state for a market (for max_orders_per_side checks).
    pub fn order_state(&self, condition_id: &str) -> Option<&MarketOrderState> {
        self.order_states.get(condition_id)
    }

    /// Returns true if this trade_id was already processed (duplicate). Inserts it if new.
    /// Uses LRU eviction: when exceeding 50k entries, drains the oldest 25k instead of
    /// clearing everything (which could allow recently-seen duplicates to re-enter).
    pub fn is_duplicate_trade(&mut self, trade_id: &str) -> bool {
        if self.seen_trade_ids.contains(trade_id) {
            return true;
        }
        // Evict oldest half when capacity exceeded
        const MAX_DEDUP: usize = 50_000;
        const EVICT_COUNT: usize = MAX_DEDUP / 2;
        if self.seen_trade_ids.len() >= MAX_DEDUP {
            for _ in 0..EVICT_COUNT {
                if let Some(old) = self.seen_trade_ids_order.pop_front() {
                    self.seen_trade_ids.remove(&old);
                }
            }
        }
        let id = trade_id.to_string();
        self.seen_trade_ids.insert(id.clone());
        self.seen_trade_ids_order.push_back(id);
        false
    }

    /// Returns true if the given order_id is registered under any market.
    pub fn has_registered_order(&self, order_id: &str) -> bool {
        self.order_states.values().any(|state| {
            state.yes_order_ids.iter().any(|id| id == order_id)
                || state.no_order_ids.iter().any(|id| id == order_id)
        })
    }
}
