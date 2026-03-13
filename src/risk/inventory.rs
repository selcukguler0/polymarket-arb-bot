use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use chrono::{NaiveDate, Utc};
use parking_lot::RwLock;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, info, warn};

use crate::config::ValidatedConfig;
use crate::types::*;

pub struct InventoryManager {
    state: RwLock<InventoryState>,
    /// Max position per market — behind RwLock so canary graduation can update through Arc.
    max_position_per_market: RwLock<Decimal>,
    max_total_exposure: Decimal,
    max_imbalance_ratio: Decimal,
    daily_loss_limit: Decimal,
    /// FIX 21: Minimum USDC balance required to place orders
    min_usdc_balance: Decimal,
    /// FIX 21: Callback to check cached USDC balance
    usdc_balance_fn: Option<Arc<dyn Fn() -> Decimal + Send + Sync>>,
    /// Token registry: maps token_id → (condition_id, outcome) for SDK-level sell guard.
    token_registry: RwLock<HashMap<String, (String, Outcome)>>,
    /// Shared aggregate daily P&L counter (cents). Updated atomically on every sell/resolution.
    /// Read by EmergencyHandler for global daily loss limit enforcement.
    aggregate_pnl_cents: Option<Arc<AtomicI64>>,
}

struct InventoryState {
    positions: HashMap<ConditionId, Position>,
    daily_pnl: Decimal,
    daily_date: NaiveDate,
}

impl InventoryManager {
    pub fn new(config: &ValidatedConfig) -> Self {
        Self {
            state: RwLock::new(InventoryState {
                positions: HashMap::new(),
                daily_pnl: Decimal::ZERO,
                daily_date: Utc::now().date_naive(),
            }),
            max_position_per_market: RwLock::new(config.max_position_per_market),
            max_total_exposure: config.max_total_exposure,
            max_imbalance_ratio: config.max_imbalance_ratio,
            daily_loss_limit: config.daily_loss_limit,
            min_usdc_balance: dec!(1.0), // Must have at least $1 USDC
            usdc_balance_fn: None,
            token_registry: RwLock::new(HashMap::new()),
            aggregate_pnl_cents: None,
        }
    }

    /// Attach the shared aggregate P&L counter (called from main.rs for per-asset inventories).
    pub fn set_aggregate_pnl(&mut self, counter: Arc<AtomicI64>) {
        self.aggregate_pnl_cents = Some(counter);
    }

    /// Update the shared aggregate daily P&L counter with a resolution P&L amount.
    /// Called from the Resolved phase where the true period P&L is computed from
    /// closing_position, since record_resolution() can't see positions already
    /// freed during the Closing phase.
    pub fn update_aggregate_pnl(&self, net_pnl: Decimal) {
        if let Some(ref counter) = self.aggregate_pnl_cents {
            let delta_cents = (net_pnl * Decimal::from(100)).to_i64().unwrap_or(0);
            if delta_cents != 0 {
                counter.fetch_add(delta_cents, Ordering::Relaxed);
            }
        }
    }

    /// Override max_position_per_market (used for per-asset budget overrides and canary graduation).
    /// Takes &self so it can be called through Arc.
    pub fn set_max_position_per_market(&self, limit: Decimal) {
        *self.max_position_per_market.write() = limit;
    }

    /// Override max_total_exposure (used for per-asset exposure overrides).
    pub fn set_max_total_exposure(&mut self, limit: Decimal) {
        self.max_total_exposure = limit;
    }

    /// Set the USDC balance check callback (FIX 21).
    pub fn set_usdc_balance_fn(&mut self, f: Arc<dyn Fn() -> Decimal + Send + Sync>) {
        self.usdc_balance_fn = Some(f);
    }

    /// Load positions from database on startup (reconciliation)
    pub fn load_positions(&self, positions: Vec<Position>) {
        let mut state = self.state.write();
        state.positions.clear();
        for pos in positions {
            state.positions.insert(pos.condition_id.clone(), pos);
        }
    }

    /// Set daily P&L from database on startup
    pub fn set_daily_pnl(&self, pnl: Decimal) {
        let mut state = self.state.write();
        state.daily_pnl = pnl;
    }

    /// Check whether a new order is allowed by all risk limits.
    /// FIX 16: max_orders_per_side enforcement is done by the caller (orchestrator).
    /// FIX 21: USDC balance check added.
    /// FIX 22: Daily P&L reset race condition fixed by upgrading lock.
    pub fn can_place_order(
        &self,
        condition_id: &str,
        outcome: Outcome,
        price: Decimal,
        size: Decimal,
    ) -> bool {
        // FIX 22: Use a write lock so we can atomically reset daily P&L if needed,
        // avoiding the race condition where read() sees stale date but can't reset.
        let mut state = self.state.write();
        self.check_daily_reset_write(&mut state);

        // Daily loss limit
        if state.daily_pnl < -self.daily_loss_limit {
            return false;
        }

        // FIX 21: Check USDC balance via cached callback
        if let Some(ref balance_fn) = self.usdc_balance_fn {
            let usdc = balance_fn();
            if usdc < self.min_usdc_balance {
                return false;
            }
        }

        // Total exposure check
        let total_exposure: Decimal = state
            .positions
            .values()
            .map(|p| p.total_yes_spent + p.total_no_spent)
            .sum();
        let order_cost = price * size;
        if total_exposure + order_cost > self.max_total_exposure {
            return false;
        }

        // Per-market exposure check
        let market_exposure = state
            .positions
            .get(condition_id)
            .map(|p| p.total_yes_spent + p.total_no_spent)
            .unwrap_or_default();
        if market_exposure + order_cost > *self.max_position_per_market.read() {
            return false;
        }

        // Would this worsen an already-bad imbalance?
        if let Some(pos) = state.positions.get(condition_id) {
            let current_imbalance = pos.imbalance_ratio();
            if current_imbalance > self.max_imbalance_ratio {
                // Only allow if this would rebalance
                if let Some(heavy) = pos.heavy_side() {
                    if heavy == outcome {
                        return false; // Adding to the already heavy side
                    }
                }
            }
        }

        true
    }

    /// Record a buy fill and update position.
    pub fn record_fill(&self, condition_id: &str, outcome: Outcome, price: Decimal, size: Decimal) {
        let mut state = self.state.write();
        self.check_daily_reset_write(&mut state);

        let pos = state
            .positions
            .entry(condition_id.to_string())
            .or_insert_with(|| Position {
                condition_id: condition_id.to_string(),
                ..Default::default()
            });

        match outcome {
            Outcome::Yes => {
                pos.yes_qty += size;
                pos.total_yes_spent += price * size;
            }
            Outcome::No => {
                pos.no_qty += size;
                pos.total_no_spent += price * size;
            }
        }
    }

    /// Record a sell fill — reduces position qty and proportionally reduces cost basis.
    /// Returns the realized P&L from this sell (positive = profit, negative = loss).
    pub fn record_sell(
        &self,
        condition_id: &str,
        outcome: Outcome,
        price: Decimal,
        size: Decimal,
    ) -> Decimal {
        let mut state = self.state.write();
        self.check_daily_reset_write(&mut state);

        let pos = match state.positions.get_mut(condition_id) {
            Some(p) => p,
            None => return Decimal::ZERO,
        };

        let realized_pnl = match outcome {
            Outcome::Yes => {
                let sold = size.min(pos.yes_qty);
                if sold.is_zero() {
                    return Decimal::ZERO;
                }
                let avg_cost = pos.total_yes_spent / pos.yes_qty;
                pos.yes_qty -= sold;
                pos.total_yes_spent -= avg_cost * sold;
                // Prevent Decimal precision residuals when selling all shares
                if pos.yes_qty.is_zero() {
                    pos.total_yes_spent = Decimal::ZERO;
                }
                (price - avg_cost) * sold
            }
            Outcome::No => {
                let sold = size.min(pos.no_qty);
                if sold.is_zero() {
                    return Decimal::ZERO;
                }
                let avg_cost = pos.total_no_spent / pos.no_qty;
                pos.no_qty -= sold;
                pos.total_no_spent -= avg_cost * sold;
                // Prevent Decimal precision residuals when selling all shares
                if pos.no_qty.is_zero() {
                    pos.total_no_spent = Decimal::ZERO;
                }
                (price - avg_cost) * sold
            }
        };

        state.daily_pnl += realized_pnl;
        // Update shared aggregate P&L (for global daily loss limit)
        if let Some(ref counter) = self.aggregate_pnl_cents {
            let delta_cents = (realized_pnl * Decimal::from(100)).to_i64().unwrap_or(0);
            counter.fetch_add(delta_cents, Ordering::Relaxed);
        }
        realized_pnl
    }

    /// Record market resolution and calculate P&L.
    /// Returns (gross_pnl, net_pnl, winning_outcome)
    pub fn record_resolution(
        &self,
        condition_id: &str,
        winning_outcome: Option<Outcome>,
    ) -> (Decimal, Decimal) {
        let mut state = self.state.write();
        self.check_daily_reset_write(&mut state);

        let pos = match state.positions.remove(condition_id) {
            Some(p) => p,
            None => return (Decimal::ZERO, Decimal::ZERO),
        };

        let total_cost = pos.total_yes_spent + pos.total_no_spent;

        let winning_payout = match winning_outcome {
            Some(Outcome::Yes) => pos.yes_qty * Decimal::ONE,
            Some(Outcome::No) => pos.no_qty * Decimal::ONE,
            None => Decimal::ZERO, // voided market, assume total loss
        };

        let gross_pnl = winning_payout - total_cost;
        // Polymarket charges ~2% fee on winning redemptions (not on fills)
        let fee = if gross_pnl > Decimal::ZERO {
            gross_pnl * dec!(0.02)
        } else {
            Decimal::ZERO
        };
        let net_pnl = gross_pnl - fee;

        state.daily_pnl += net_pnl;
        // Update shared aggregate P&L (for global daily loss limit)
        if let Some(ref counter) = self.aggregate_pnl_cents {
            let delta_cents = (net_pnl * Decimal::from(100)).to_i64().unwrap_or(0);
            counter.fetch_add(delta_cents, Ordering::Relaxed);
        }

        info!(
            condition_id,
            %gross_pnl,
            %net_pnl,
            ?winning_outcome,
            yes_qty = %pos.yes_qty,
            no_qty = %pos.no_qty,
            "Market resolved"
        );

        (gross_pnl, net_pnl)
    }

    /// Record an on-chain merge: reduce both yes_qty and no_qty by `amount`,
    /// and reduce cost basis proportionally.
    pub fn record_merge(&self, condition_id: &str, amount: Decimal) {
        let mut state = self.state.write();
        if let Some(pos) = state.positions.get_mut(condition_id) {
            let yes_before = pos.yes_qty;
            let no_before = pos.no_qty;

            pos.yes_qty = (pos.yes_qty - amount).max(Decimal::ZERO);
            pos.no_qty = (pos.no_qty - amount).max(Decimal::ZERO);

            // Reduce cost basis proportionally (zero out on full merge to avoid Decimal residuals)
            if yes_before > Decimal::ZERO {
                if pos.yes_qty.is_zero() {
                    pos.total_yes_spent = Decimal::ZERO;
                } else {
                    let yes_ratio = pos.yes_qty / yes_before;
                    pos.total_yes_spent *= yes_ratio;
                }
            }
            if no_before > Decimal::ZERO {
                if pos.no_qty.is_zero() {
                    pos.total_no_spent = Decimal::ZERO;
                } else {
                    let no_ratio = pos.no_qty / no_before;
                    pos.total_no_spent *= no_ratio;
                }
            }

            info!(
                condition_id,
                merged = %amount,
                yes_remaining = %pos.yes_qty,
                no_remaining = %pos.no_qty,
                "Recorded merge in inventory"
            );
        }
    }

    /// Force-update position quantities from an external source (e.g., Data API reconciliation).
    /// For existing positions, scales cost basis proportionally to quantity changes.
    /// For discovered positions, estimates cost basis at $0.50/share (conservative midpoint for binary markets).
    pub fn force_update_position(&self, condition_id: &str, yes_qty: Decimal, no_qty: Decimal) {
        let mut state = self.state.write();
        if let Some(pos) = state.positions.get_mut(condition_id) {
            // Scale cost basis proportionally: if qty doubled, cost basis doubles.
            // If qty halved (some filled externally), cost basis halves.
            // This preserves average cost per share accuracy.
            if pos.yes_qty > Decimal::ZERO && yes_qty > Decimal::ZERO {
                pos.total_yes_spent = pos.total_yes_spent * yes_qty / pos.yes_qty;
            } else if yes_qty.is_zero() {
                pos.total_yes_spent = Decimal::ZERO;
            }
            // else: yes_qty went from 0 to positive — estimate below
            if pos.no_qty > Decimal::ZERO && no_qty > Decimal::ZERO {
                pos.total_no_spent = pos.total_no_spent * no_qty / pos.no_qty;
            } else if no_qty.is_zero() {
                pos.total_no_spent = Decimal::ZERO;
            }
            // Handle case where we now have shares we didn't before (external fill)
            if pos.yes_qty.is_zero() && yes_qty > Decimal::ZERO {
                pos.total_yes_spent = yes_qty * dec!(0.50);
            }
            if pos.no_qty.is_zero() && no_qty > Decimal::ZERO {
                pos.total_no_spent = no_qty * dec!(0.50);
            }
            pos.yes_qty = yes_qty;
            pos.no_qty = no_qty;
        } else if yes_qty > Decimal::ZERO || no_qty > Decimal::ZERO {
            // Position exists on exchange but not locally — estimate cost at $0.50/share.
            // This is conservative for remaining_capacity: slightly overestimates cost,
            // which is safer than zero (which would show infinite capacity).
            state.positions.insert(
                condition_id.to_string(),
                Position {
                    condition_id: condition_id.to_string(),
                    yes_qty,
                    no_qty,
                    total_yes_spent: yes_qty * dec!(0.50),
                    total_no_spent: no_qty * dec!(0.50),
                },
            );
        }
    }

    /// Get imbalance ratio for a market.
    pub fn get_imbalance(&self, condition_id: &str) -> Decimal {
        let state = self.state.read();
        state
            .positions
            .get(condition_id)
            .map(|p| p.imbalance_ratio())
            .unwrap_or(Decimal::ZERO)
    }

    /// Get a copy of the current position for a market.
    pub fn get_position(&self, condition_id: &str) -> Option<Position> {
        let state = self.state.read();
        state.positions.get(condition_id).cloned()
    }

    /// Remove a position (e.g., after market resolution) so it no longer counts toward exposure.
    pub fn remove_position(&self, condition_id: &str) -> Option<Position> {
        let mut state = self.state.write();
        state.positions.remove(condition_id)
    }

    // ── Token registry for SDK-level sell guard ──

    /// Register token_id → (condition_id, outcome) mapping for sell position validation.
    /// Called when a market is discovered and its tokens are known.
    pub fn register_tokens(&self, condition_id: &str, yes_token: &str, no_token: &str) {
        let mut reg = self.token_registry.write();
        reg.insert(
            yes_token.to_string(),
            (condition_id.to_string(), Outcome::Yes),
        );
        reg.insert(
            no_token.to_string(),
            (condition_id.to_string(), Outcome::No),
        );
        debug!(
            condition_id = %condition_id,
            yes_token = %yes_token,
            no_token = %no_token,
            registry_size = reg.len(),
            "Token registry: registered tokens"
        );
    }

    /// Unregister tokens when a market is removed.
    pub fn unregister_tokens(&self, yes_token: &str, no_token: &str) {
        let mut reg = self.token_registry.write();
        reg.remove(yes_token);
        reg.remove(no_token);
    }

    /// Available quantity to sell for a given condition_id + outcome.
    /// Returns ZERO if no position exists.
    pub fn available_to_sell(&self, condition_id: &str, outcome: Outcome) -> Decimal {
        let state = self.state.read();
        match state.positions.get(condition_id) {
            Some(pos) => match outcome {
                Outcome::Yes => pos.yes_qty,
                Outcome::No => pos.no_qty,
            },
            None => Decimal::ZERO,
        }
    }

    /// Available quantity to sell for a given token_id (uses token registry).
    /// Returns ZERO if the token is unknown or no position exists.
    /// This is the canonical method used by the SDK-level sell guard.
    pub fn available_to_sell_by_token(&self, token_id: &str) -> Decimal {
        let reg = self.token_registry.read();
        let (cid, outcome) = match reg.get(token_id) {
            Some((c, o)) => (c.clone(), *o),
            None => {
                warn!(
                    token_id = %token_id,
                    registry_size = reg.len(),
                    registry_keys = %reg.keys().take(4).cloned().collect::<Vec<_>>().join(", "),
                    "Token NOT FOUND in registry — sell guard returning 0"
                );
                return Decimal::ZERO;
            }
        };
        drop(reg); // release registry lock before taking position lock
        self.available_to_sell(&cid, outcome)
    }

    /// Get remaining capacity for a market (how much more USDC can be spent).
    pub fn remaining_capacity(&self, condition_id: &str) -> Decimal {
        let state = self.state.read();
        let market_spent = state
            .positions
            .get(condition_id)
            .map(|p| p.total_yes_spent + p.total_no_spent)
            .unwrap_or_default();
        let max_pos = *self.max_position_per_market.read();
        let market_remaining = max_pos - market_spent;

        let total_spent: Decimal = state
            .positions
            .values()
            .map(|p| p.total_yes_spent + p.total_no_spent)
            .sum();
        let total_remaining = self.max_total_exposure - total_spent;

        let result = market_remaining.min(total_remaining).max(Decimal::ZERO);
        if result.is_zero() {
            tracing::warn!(
                condition_id,
                %market_spent,
                %market_remaining,
                %total_spent,
                %total_remaining,
                max_position = %max_pos,
                max_exposure = %self.max_total_exposure,
                active_positions = state.positions.len(),
                "remaining_capacity = 0 diagnostic"
            );
            for (cid, pos) in &state.positions {
                let spent = pos.total_yes_spent + pos.total_no_spent;
                if !spent.is_zero() {
                    tracing::warn!(
                        position_cid = %cid,
                        yes_spent = %pos.total_yes_spent,
                        no_spent = %pos.total_no_spent,
                        yes_qty = %pos.yes_qty,
                        no_qty = %pos.no_qty,
                        "  position consuming capacity"
                    );
                }
            }
        }
        result
    }

    /// Get daily P&L (local per-asset).
    pub fn daily_pnl(&self) -> Decimal {
        let state = self.state.read();
        state.daily_pnl
    }

    /// Get the shared aggregate daily P&L in cents (across all assets).
    /// Returns None if no aggregate counter is attached.
    pub fn aggregate_daily_pnl_cents(&self) -> Option<i64> {
        self.aggregate_pnl_cents
            .as_ref()
            .map(|c| c.load(Ordering::Relaxed))
    }

    /// Whether daily loss limit has been breached.
    pub fn daily_loss_breached(&self) -> bool {
        let state = self.state.read();
        state.daily_pnl < -self.daily_loss_limit
    }

    /// Get portfolio summary for monitoring.
    pub fn portfolio_summary(&self) -> PortfolioSummary {
        let state = self.state.read();
        let total_exposure: Decimal = state
            .positions
            .values()
            .map(|p| p.total_yes_spent + p.total_no_spent)
            .sum();
        PortfolioSummary {
            active_markets: state.positions.len(),
            total_exposure,
            daily_pnl: state.daily_pnl,
            positions: state.positions.values().cloned().collect(),
        }
    }

    /// Reset daily P&L if we've crossed midnight UTC (write).
    /// FIX 22: This is now the only reset path — can_place_order upgrades to write lock.
    fn check_daily_reset_write(&self, state: &mut InventoryState) {
        let today = Utc::now().date_naive();
        if state.daily_date != today {
            info!(
                previous_pnl = %state.daily_pnl,
                previous_date = %state.daily_date,
                "Daily P&L reset"
            );
            state.daily_pnl = Decimal::ZERO;
            state.daily_date = today;
        }
    }
}
