//! Backtest harness: replays historical market data through PaperSimulator
//! using a profile derived from a bot config.
//!
//! Usage:
//!   cargo run --bin backtest -- [--config path/to/config.toml] [--asset BTC] [--summary-json out.json] [--quiet] [data_dir]
//!
//! Default data directory: "700 periods/BTC"

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::{env, fs};

use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use toml::Value;

use polymarket_arb::paper_sim::{PaperSide, PaperSimulator};
use polymarket_arb::types::Outcome;

const TICK_SIZE: Decimal = dec!(0.01);
const LADDER_TICK_SPACING: u32 = 1;
const MIN_ORDER_SHARES: Decimal = dec!(5);
const MIN_ROWS: usize = 20;

struct CliArgs {
    config_path: Option<PathBuf>,
    asset: String,
    data_dir: PathBuf,
    summary_json: Option<PathBuf>,
    quiet: bool,
}

#[derive(Clone, Debug, Serialize)]
struct ReplayProfile {
    name: String,
    config_path: Option<String>,
    allowed_durations: Vec<u32>,
    resolution_safety_margin_secs: u32,
    target_combined: Decimal,
    max_combined_avg_cost: Decimal,
    light_side_max_combined: Decimal,
    base_order_shares: Decimal,
    ladder_levels: u32,
    ladder_levels_5m: Option<u32>,
    ladder_levels_15m: Option<u32>,
    ladder_levels_60m: Option<u32>,
    buy_level_activation_limit_5m: Option<u32>,
    ladder_size_decay: f64,
    postonly_buffer_ticks: u32,
    min_bid_fv_ratio: f64,
    min_bid_floor: Decimal,
    fv_stale_cents: f64,
    max_share_imbalance: Decimal,
    one_sided_threshold: Decimal,
    skew_per_share: f64,
    skew_activation_threshold: Decimal,
    shares_per_skew_tick: Decimal,
    max_skew_ticks: u32,
    fv_dead_threshold: f64,
    sellback_edge: Decimal,
    sell_level_size: Decimal,
    sell_levels: u32,
    sell_buy_cooldown_secs: u64,
    sellback_grace_period_secs: u64,
    sellback_max_loss_cents: Decimal,
    period_gross_buy_cap_usdc: Decimal,
    early_phase_pct: f64,
    early_phase_gross_buy_cap_usdc: Decimal,
    pair_ratio_eval_min_total_shares: Decimal,
    period_min_pair_ratio_for_heavy_add: f64,
    rebalance_budget_override: bool,
    rebalance_max_extra_budget: Decimal,
    rebalance_size_multiplier: u32,
    single_order_notional_cap_usdc: Decimal,
    pair_fee_buffer: Decimal,
    very_late_phase_secs: u64,
    trading_window_start_pct: f64,
    trading_window_end_pct: f64,
    wind_down_allow_pair_completion: bool,
    pair_completion_retry_secs: u64,
    pair_completion_max_attempts: u32,
    merge_at_closing: bool,
    continuous_merge_enabled: bool,
    merge_interval_secs: u64,
    merge_min_pairs: u32,
    merge_reserve_pairs: u32,
    merge_min_profit_per_pair: Decimal,
    exit_soft_excess: Decimal,
    exit_hard_excess: Decimal,
    exit_taker_after_secs: f64,
    exit_sell_chunk: Decimal,
    exit_force_remaining_secs: f64,
}

impl Default for ReplayProfile {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            config_path: None,
            allowed_durations: vec![5, 15, 60],
            resolution_safety_margin_secs: 120,
            target_combined: dec!(0.97),
            max_combined_avg_cost: dec!(0.97),
            light_side_max_combined: dec!(0.99),
            base_order_shares: dec!(15),
            ladder_levels: 10,
            ladder_levels_5m: Some(5),
            ladder_levels_15m: Some(10),
            ladder_levels_60m: Some(15),
            buy_level_activation_limit_5m: None,
            ladder_size_decay: 0.10,
            postonly_buffer_ticks: 3,
            min_bid_fv_ratio: 0.5,
            min_bid_floor: dec!(0.02),
            fv_stale_cents: 0.08,
            max_share_imbalance: dec!(150),
            one_sided_threshold: dec!(50),
            skew_per_share: 0.008,
            skew_activation_threshold: dec!(15),
            shares_per_skew_tick: dec!(5),
            max_skew_ticks: 15,
            fv_dead_threshold: 0.10,
            sellback_edge: dec!(0.01),
            sell_level_size: dec!(10),
            sell_levels: 3,
            sell_buy_cooldown_secs: 10,
            sellback_grace_period_secs: 15,
            sellback_max_loss_cents: dec!(0.02),
            period_gross_buy_cap_usdc: dec!(80),
            early_phase_pct: 0.10,
            early_phase_gross_buy_cap_usdc: dec!(30),
            pair_ratio_eval_min_total_shares: dec!(60),
            period_min_pair_ratio_for_heavy_add: 0.35,
            rebalance_budget_override: true,
            rebalance_max_extra_budget: dec!(25),
            rebalance_size_multiplier: 1,
            single_order_notional_cap_usdc: dec!(12.5),
            pair_fee_buffer: dec!(0.03),
            very_late_phase_secs: 300,
            trading_window_start_pct: 0.35,
            trading_window_end_pct: 0.60,
            wind_down_allow_pair_completion: true,
            pair_completion_retry_secs: 5,
            pair_completion_max_attempts: 8,
            merge_at_closing: false,
            continuous_merge_enabled: false,
            merge_interval_secs: 30,
            merge_min_pairs: 10,
            merge_reserve_pairs: 0,
            merge_min_profit_per_pair: Decimal::ZERO,
            exit_soft_excess: dec!(15),
            exit_hard_excess: dec!(30),
            exit_taker_after_secs: 12.0,
            exit_sell_chunk: dec!(15),
            exit_force_remaining_secs: 15.0,
        }
    }
}

impl ReplayProfile {
    fn from_config(config_path: &Path, asset: &str) -> Result<Self, String> {
        let mut profile = Self::default();
        profile.name = config_path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "profile".to_string());
        profile.config_path = Some(config_path.display().to_string());

        let content = fs::read_to_string(config_path)
            .map_err(|e| format!("read config {}: {e}", config_path.display()))?;
        let doc: Value = toml::from_str(&content)
            .map_err(|e| format!("parse config {}: {e}", config_path.display()))?;

        if let Some(v2) = doc.get("v2").and_then(Value::as_table) {
            apply_profile_table(&mut profile, v2);
        }
        if let Some(risk) = doc.get("risk").and_then(Value::as_table) {
            apply_profile_table(&mut profile, risk);
        }
        if let Some(asset_table) = doc.get(&asset.to_lowercase()).and_then(Value::as_table) {
            apply_profile_table(&mut profile, asset_table);
        }

        Ok(profile)
    }

    fn ladder_levels_for_duration(&self, duration_mins: u32) -> u32 {
        if duration_mins >= 60 {
            self.ladder_levels_60m.unwrap_or(self.ladder_levels)
        } else if duration_mins > 7 {
            self.ladder_levels_15m.unwrap_or(self.ladder_levels)
        } else {
            self.ladder_levels_5m.unwrap_or(self.ladder_levels)
        }
    }

    fn allows_duration(&self, duration_mins: u32) -> bool {
        self.allowed_durations.is_empty() || self.allowed_durations.contains(&duration_mins)
    }

    fn activation_limit_for_duration(&self, duration_mins: u32) -> Option<usize> {
        if duration_mins <= 7 {
            self.buy_level_activation_limit_5m.map(|v| v as usize)
        } else {
            None
        }
    }
}

fn apply_profile_table(profile: &mut ReplayProfile, table: &toml::map::Map<String, Value>) {
    if let Some(v) = table.get("allowed_durations").and_then(value_as_vec_u32) {
        profile.allowed_durations = v;
    }
    if let Some(v) = table
        .get("resolution_safety_margin_secs")
        .and_then(value_as_u32)
    {
        profile.resolution_safety_margin_secs = v;
    }
    if let Some(v) = table.get("target_combined").and_then(value_as_decimal) {
        profile.target_combined = v;
    }
    if let Some(v) = table
        .get("max_combined_avg_cost")
        .and_then(value_as_decimal)
    {
        profile.max_combined_avg_cost = v;
    }
    if let Some(v) = table
        .get("light_side_max_combined")
        .and_then(value_as_decimal)
    {
        profile.light_side_max_combined = v;
    }
    if let Some(v) = table.get("base_order_shares").and_then(value_as_decimal) {
        profile.base_order_shares = v;
    }
    if let Some(v) = table.get("ladder_levels").and_then(value_as_u32) {
        profile.ladder_levels = v;
    }
    if let Some(v) = table.get("ladder_levels_5m").and_then(value_as_u32) {
        profile.ladder_levels_5m = Some(v);
    }
    if let Some(v) = table.get("ladder_levels_15m").and_then(value_as_u32) {
        profile.ladder_levels_15m = Some(v);
    }
    if let Some(v) = table.get("ladder_levels_60m").and_then(value_as_u32) {
        profile.ladder_levels_60m = Some(v);
    }
    if let Some(v) = table
        .get("buy_level_activation_limit_5m")
        .and_then(value_as_u32)
    {
        profile.buy_level_activation_limit_5m = Some(v);
    }
    if let Some(v) = table.get("ladder_size_decay").and_then(value_as_f64) {
        profile.ladder_size_decay = v;
    }
    if let Some(v) = table
        .get("postonly_regen_buffer_ticks")
        .and_then(value_as_u32)
    {
        profile.postonly_buffer_ticks = v;
    }
    if let Some(v) = table.get("min_bid_fv_ratio").and_then(value_as_f64) {
        profile.min_bid_fv_ratio = v;
    }
    if let Some(v) = table
        .get("min_bid_absolute_floor")
        .and_then(value_as_decimal)
    {
        profile.min_bid_floor = v;
    }
    if let Some(v) = table.get("fv_stale_cancel_cents").and_then(value_as_f64) {
        profile.fv_stale_cents = v;
    }
    if let Some(v) = table.get("max_share_imbalance").and_then(value_as_decimal) {
        profile.max_share_imbalance = v;
    }
    if let Some(v) = table.get("one_sided_threshold").and_then(value_as_decimal) {
        profile.one_sided_threshold = v;
    }
    if let Some(v) = table.get("imbalance_skew_per_share").and_then(value_as_f64) {
        profile.skew_per_share = v;
    }
    if let Some(v) = table
        .get("skew_activation_threshold")
        .and_then(value_as_decimal)
    {
        profile.skew_activation_threshold = v;
    }
    if let Some(v) = table.get("shares_per_skew_tick").and_then(value_as_decimal) {
        profile.shares_per_skew_tick = v;
    }
    if let Some(v) = table.get("max_skew_ticks").and_then(value_as_u32) {
        profile.max_skew_ticks = v;
    }
    if let Some(v) = table.get("fv_dead_threshold").and_then(value_as_f64) {
        profile.fv_dead_threshold = v;
    }
    if let Some(v) = table.get("sellback_edge").and_then(value_as_decimal) {
        profile.sellback_edge = v;
    }
    if let Some(v) = table.get("sell_level_size").and_then(value_as_decimal) {
        profile.sell_level_size = v;
    }
    if let Some(v) = table.get("sell_levels").and_then(value_as_u32) {
        profile.sell_levels = v;
    }
    if let Some(v) = table.get("sell_buy_cooldown_secs").and_then(value_as_u32) {
        profile.sell_buy_cooldown_secs = v as u64;
    }
    if let Some(v) = table
        .get("sellback_grace_period_secs")
        .and_then(value_as_u32)
    {
        profile.sellback_grace_period_secs = v as u64;
    }
    if let Some(v) = table
        .get("sellback_max_loss_cents")
        .and_then(value_as_decimal)
    {
        profile.sellback_max_loss_cents = v;
    }
    if let Some(v) = table
        .get("period_gross_buy_cap_usdc")
        .and_then(value_as_decimal)
    {
        profile.period_gross_buy_cap_usdc = v;
    }
    if let Some(v) = table.get("early_phase_pct").and_then(value_as_f64) {
        profile.early_phase_pct = v;
    }
    if let Some(v) = table
        .get("early_phase_gross_buy_cap_usdc")
        .and_then(value_as_decimal)
    {
        profile.early_phase_gross_buy_cap_usdc = v;
    }
    if let Some(v) = table
        .get("pair_ratio_eval_min_total_shares")
        .and_then(value_as_decimal)
    {
        profile.pair_ratio_eval_min_total_shares = v;
    }
    if let Some(v) = table
        .get("period_min_pair_ratio_for_heavy_add")
        .and_then(value_as_f64)
    {
        profile.period_min_pair_ratio_for_heavy_add = v;
    }
    if let Some(v) = table
        .get("rebalance_budget_override")
        .and_then(value_as_bool)
    {
        profile.rebalance_budget_override = v;
    }
    if let Some(v) = table
        .get("rebalance_max_extra_budget")
        .and_then(value_as_decimal)
    {
        profile.rebalance_max_extra_budget = v;
    }
    if let Some(v) = table
        .get("rebalance_size_multiplier")
        .and_then(value_as_u32)
    {
        profile.rebalance_size_multiplier = v;
    }
    if let Some(v) = table
        .get("single_order_notional_cap_usdc")
        .and_then(value_as_decimal)
    {
        profile.single_order_notional_cap_usdc = v;
    }
    if let Some(v) = table.get("pair_fee_buffer").and_then(value_as_decimal) {
        profile.pair_fee_buffer = v;
    }
    if let Some(v) = table.get("very_late_phase_secs").and_then(value_as_u32) {
        profile.very_late_phase_secs = v as u64;
    }
    if let Some(v) = table.get("trading_window_start_pct").and_then(value_as_f64) {
        profile.trading_window_start_pct = v;
    }
    if let Some(v) = table.get("trading_window_end_pct").and_then(value_as_f64) {
        profile.trading_window_end_pct = v;
    }
    if let Some(v) = table
        .get("wind_down_allow_pair_completion")
        .and_then(value_as_bool)
    {
        profile.wind_down_allow_pair_completion = v;
    }
    if let Some(v) = table
        .get("pair_completion_retry_secs")
        .and_then(value_as_u32)
    {
        profile.pair_completion_retry_secs = v as u64;
    }
    if let Some(v) = table
        .get("pair_completion_max_attempts")
        .and_then(value_as_u32)
    {
        profile.pair_completion_max_attempts = v;
    }
    if let Some(v) = table.get("merge_at_closing").and_then(value_as_bool) {
        profile.merge_at_closing = v;
    }
    if let Some(v) = table
        .get("continuous_merge_enabled")
        .and_then(value_as_bool)
    {
        profile.continuous_merge_enabled = v;
    }
    if let Some(v) = table.get("merge_interval_secs").and_then(value_as_u32) {
        profile.merge_interval_secs = v as u64;
    }
    if let Some(v) = table.get("merge_min_pairs").and_then(value_as_u32) {
        profile.merge_min_pairs = v;
    }
    if let Some(v) = table.get("merge_reserve_pairs").and_then(value_as_u32) {
        profile.merge_reserve_pairs = v;
    }
    if let Some(v) = table
        .get("merge_min_profit_per_pair")
        .and_then(value_as_decimal)
    {
        profile.merge_min_profit_per_pair = v;
    }
    if let Some(v) = table.get("exit_soft_excess").and_then(value_as_decimal) {
        profile.exit_soft_excess = v;
    }
    if let Some(v) = table.get("exit_hard_excess").and_then(value_as_decimal) {
        profile.exit_hard_excess = v;
    }
    if let Some(v) = table.get("exit_taker_after_secs").and_then(value_as_f64) {
        profile.exit_taker_after_secs = v;
    }
    if let Some(v) = table.get("sell_level_size").and_then(value_as_decimal) {
        profile.exit_sell_chunk = v;
    }
    if let Some(v) = table
        .get("exit_force_taker_remaining_secs")
        .and_then(value_as_f64)
    {
        profile.exit_force_remaining_secs = v;
    }
}

fn value_as_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(s) => Decimal::from_str(s).ok(),
        Value::Integer(v) => Some(Decimal::from(*v)),
        Value::Float(v) => Decimal::from_f64(*v),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::String(s) => s.parse().ok(),
        Value::Integer(v) => Some(*v as f64),
        Value::Float(v) => Some(*v),
        _ => None,
    }
}

fn value_as_u32(value: &Value) -> Option<u32> {
    match value {
        Value::String(s) => s.parse().ok(),
        Value::Integer(v) => (*v).try_into().ok(),
        _ => None,
    }
}

fn value_as_vec_u32(value: &Value) -> Option<Vec<u32>> {
    let arr = value.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        out.push(value_as_u32(entry)?);
    }
    Some(out)
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(v) => Some(*v),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

struct TickData {
    timestamp: DateTime<Utc>,
    fv_up: f64,
    fv_down: f64,
    remaining_secs: f64,
    best_bid_up: Decimal,
    best_ask_up: Decimal,
    best_bid_down: Decimal,
    best_ask_down: Decimal,
}

struct PeriodResult {
    period_name: String,
    result: String,
}

#[derive(Clone)]
struct RestingOrderSnapshot {
    order_id: String,
    outcome: Outcome,
    price: Decimal,
    side: PaperSide,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplayPhase {
    Early,
    Middle,
    Late,
    Closing,
    Resolved,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExitMode {
    None,
    Maker,
    Taker,
}

#[derive(Clone)]
enum ExitPlan {
    Skip,
    Maker {
        heavy_outcome: Outcome,
        levels: Vec<LadderLevel>,
    },
    Taker {
        heavy_outcome: Outcome,
        size: Decimal,
        price: Decimal,
    },
}

#[derive(Clone)]
struct LadderLevel {
    outcome: Outcome,
    price: Decimal,
    size: Decimal,
}

#[derive(Default)]
struct Position {
    yes_qty: Decimal,
    no_qty: Decimal,
    total_yes_spent: Decimal,
    total_no_spent: Decimal,
}

impl Position {
    fn avg_yes_cost(&self) -> Decimal {
        if self.yes_qty > Decimal::ZERO {
            self.total_yes_spent / self.yes_qty
        } else {
            Decimal::ZERO
        }
    }

    fn avg_no_cost(&self) -> Decimal {
        if self.no_qty > Decimal::ZERO {
            self.total_no_spent / self.no_qty
        } else {
            Decimal::ZERO
        }
    }

    fn complete_pairs(&self) -> Decimal {
        self.yes_qty.min(self.no_qty)
    }

    fn total_qty(&self) -> Decimal {
        self.yes_qty + self.no_qty
    }

    fn avg_combined_cost(&self) -> Decimal {
        self.avg_yes_cost() + self.avg_no_cost()
    }

    fn heavy_side(&self) -> Option<Outcome> {
        if self.yes_qty > self.no_qty {
            Some(Outcome::Yes)
        } else if self.no_qty > self.yes_qty {
            Some(Outcome::No)
        } else {
            None
        }
    }

    fn light_side(&self) -> Option<Outcome> {
        if self.yes_qty < self.no_qty {
            Some(Outcome::Yes)
        } else if self.no_qty < self.yes_qty {
            Some(Outcome::No)
        } else {
            None
        }
    }

    fn record_merge(&mut self, pairs: Decimal) -> Decimal {
        let pairs = pairs.min(self.complete_pairs()).max(Decimal::ZERO);
        if pairs <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        let avg_yes = self.avg_yes_cost();
        let avg_no = self.avg_no_cost();
        let avg_combined = avg_yes + avg_no;

        self.yes_qty -= pairs;
        self.no_qty -= pairs;
        self.total_yes_spent = (self.total_yes_spent - avg_yes * pairs).max(Decimal::ZERO);
        self.total_no_spent = (self.total_no_spent - avg_no * pairs).max(Decimal::ZERO);

        pairs * (Decimal::ONE - avg_combined)
    }
}

#[derive(Serialize)]
struct PeriodStats {
    period_name: String,
    duration_mins: u32,
    result: String,
    traded: bool,
    skipped_due_to_duration: bool,
    ticks: usize,
    orders_placed: u64,
    fills: u64,
    sells: u64,
    sell_orders_placed: u64,
    sell_orders_filled: u64,
    sell_orders_cancelled: u64,
    maker_exit_orders: u64,
    taker_exit_orders: u64,
    merge_count: u64,
    postonly_rejections: u64,
    cost_guard_filtered: u64,
    activation_limited_levels: u64,
    yes_qty: Decimal,
    no_qty: Decimal,
    pairs: Decimal,
    max_excess: Decimal,
    end_excess: Decimal,
    locked_profit: Decimal,
    merge_pnl: Decimal,
    merged_pairs: Decimal,
    sell_pnl: Decimal,
    spec_pnl: Decimal,
    total_pnl: Decimal,
}

#[derive(Serialize)]
struct SummaryMetrics {
    profile_name: String,
    config_path: Option<String>,
    data_dir: String,
    periods_processed: usize,
    periods_traded: usize,
    periods_no_trade_due_to_duration: usize,
    periods_skipped: u32,
    total_pnl: String,
    avg_pnl_per_traded_period: String,
    avg_pnl_per_processed_period: String,
    lower_tail_p10_avg: String,
    win_rate_traded: f64,
    fill_rate: f64,
    total_locked: String,
    total_sell_pnl: String,
    total_merge_pnl: String,
    total_spec_pnl: String,
    sell_orders_placed: u64,
    sell_orders_filled: u64,
    sell_orders_cancelled: u64,
    maker_exit_orders: u64,
    taker_exit_orders: u64,
    merge_count: u64,
    total_merged_pairs: String,
    end_excess_gt_zero_count: usize,
    end_excess_gt_zero_frac: f64,
    sharpe_traded: f64,
    negative_locked_profit_count: usize,
    negative_locked_profit_frac: f64,
    catastrophic_excess_count: usize,
    catastrophic_excess_frac: f64,
    min_period_pnl: String,
    max_period_pnl: String,
    durations_processed: BTreeMap<String, usize>,
}

fn round_down_to_tick(price: Decimal, tick_size: Decimal) -> Decimal {
    if tick_size.is_zero() {
        return price;
    }
    (price / tick_size).floor() * tick_size
}

fn quantize_order_size(size: Decimal) -> Decimal {
    if size <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let scale = Decimal::from(100u64);
    (size * scale).floor() / scale
}

fn cap_buy_size_for_notional(size: Decimal, price: Decimal, notional_cap: Decimal) -> Decimal {
    if size <= Decimal::ZERO || price <= Decimal::ZERO || notional_cap <= Decimal::ZERO {
        return size;
    }
    let max_size = quantize_order_size((notional_cap / price).floor());
    size.min(max_size)
}

fn elapsed_pct_from_remaining(remaining_secs: f64, total_secs: f64) -> f64 {
    if total_secs <= 0.0 {
        return 0.0;
    }
    (1.0 - remaining_secs / total_secs).clamp(0.0, 1.0)
}

fn phase_from_remaining(
    remaining_secs: f64,
    total_secs: f64,
    resolution_safety_margin_secs: u32,
) -> ReplayPhase {
    if remaining_secs <= 0.0 {
        return ReplayPhase::Resolved;
    }
    if remaining_secs <= resolution_safety_margin_secs as f64 {
        return ReplayPhase::Closing;
    }
    let effective_duration = total_secs - resolution_safety_margin_secs as f64;
    if effective_duration <= 0.0 {
        return ReplayPhase::Late;
    }
    let remaining_after_safety = remaining_secs - resolution_safety_margin_secs as f64;
    let remaining_pct = remaining_after_safety / effective_duration;
    if remaining_pct <= 0.33 {
        ReplayPhase::Late
    } else if remaining_pct <= 0.67 {
        ReplayPhase::Middle
    } else {
        ReplayPhase::Early
    }
}

fn dynamic_loss_cap(remaining_secs: f64) -> Decimal {
    if remaining_secs > 420.0 {
        dec!(0.03)
    } else if remaining_secs > 300.0 {
        dec!(0.06)
    } else if remaining_secs > 180.0 {
        dec!(0.10)
    } else if remaining_secs > 120.0 {
        dec!(0.15)
    } else {
        dec!(1.00)
    }
}

const SOFT_EXIT_START_REMAINING_SECS: f64 = 600.0;

fn select_exit_mode(
    abs_excess: Decimal,
    remaining_secs: f64,
    breaker_secs: f64,
    profile: &ReplayProfile,
) -> ExitMode {
    if abs_excess <= Decimal::ZERO {
        return ExitMode::None;
    }
    let force_taker_late = remaining_secs <= profile.exit_force_remaining_secs
        && abs_excess >= profile.exit_soft_excess;
    let force_taker_breaker =
        abs_excess >= profile.exit_hard_excess && breaker_secs >= profile.exit_taker_after_secs;
    if force_taker_late || force_taker_breaker {
        return ExitMode::Taker;
    }
    if abs_excess >= profile.exit_soft_excess && remaining_secs <= SOFT_EXIT_START_REMAINING_SECS {
        return ExitMode::Maker;
    }
    ExitMode::None
}

#[allow(clippy::too_many_arguments)]
fn compute_excess_exit_plan(
    position: &Position,
    fv_up: f64,
    fv_down: f64,
    yes_best_bid: Option<Decimal>,
    no_best_bid: Option<Decimal>,
    yes_best_ask: Option<Decimal>,
    no_best_ask: Option<Decimal>,
    tick_size: Decimal,
    profile: &ReplayProfile,
    remaining_secs: f64,
    breaker_secs: f64,
    in_grace_period: bool,
    heavy_side_has_buys: bool,
) -> ExitPlan {
    let excess = position.yes_qty - position.no_qty;
    let abs_excess = excess.abs();
    let heavy_outcome = if excess > Decimal::ZERO {
        Some(Outcome::Yes)
    } else if excess < Decimal::ZERO {
        Some(Outcome::No)
    } else {
        None
    };
    if abs_excess <= Decimal::ZERO || heavy_outcome.is_none() {
        return ExitPlan::Skip;
    }
    if in_grace_period {
        return ExitPlan::Skip;
    }
    if heavy_side_has_buys && abs_excess < profile.exit_soft_excess {
        return ExitPlan::Skip;
    }

    let mode = select_exit_mode(abs_excess, remaining_secs, breaker_secs, profile);
    if mode == ExitMode::None {
        return ExitPlan::Skip;
    }

    let (heavy_outcome, avg_heavy, best_bid, best_ask, fv_heavy) = if excess > Decimal::ZERO {
        let avg = if position.yes_qty > Decimal::ZERO {
            position.total_yes_spent / position.yes_qty
        } else {
            Decimal::ZERO
        };
        (
            Outcome::Yes,
            avg,
            yes_best_bid,
            yes_best_ask,
            Decimal::from_f64(fv_up).unwrap_or(Decimal::ZERO),
        )
    } else {
        let avg = if position.no_qty > Decimal::ZERO {
            position.total_no_spent / position.no_qty
        } else {
            Decimal::ZERO
        };
        (
            Outcome::No,
            avg,
            no_best_bid,
            no_best_ask,
            Decimal::from_f64(fv_down).unwrap_or(Decimal::ZERO),
        )
    };

    let best_bid = match best_bid {
        Some(bid) if bid > Decimal::ZERO => bid,
        _ => return ExitPlan::Skip,
    };

    let mut max_loss = dynamic_loss_cap(remaining_secs);
    if remaining_secs > 300.0 && profile.sellback_max_loss_cents < max_loss {
        max_loss = profile.sellback_max_loss_cents;
    }
    let loss_floor = avg_heavy - max_loss;

    if mode == ExitMode::Taker {
        if best_bid < loss_floor {
            return ExitPlan::Skip;
        }
        let size = quantize_order_size(abs_excess.min(profile.sell_level_size));
        if size < MIN_ORDER_SHARES {
            return ExitPlan::Skip;
        }
        return ExitPlan::Taker {
            heavy_outcome,
            size,
            price: best_bid,
        };
    }

    let default_anchor = best_bid + TICK_SIZE;
    let maker_anchor = match best_ask {
        Some(ask) if ask > Decimal::ZERO => {
            if ask > best_bid + tick_size {
                ask - tick_size
            } else {
                ask
            }
        }
        _ => default_anchor,
    };
    let fv_target = (fv_heavy - profile.sellback_edge).max(avg_heavy + profile.sellback_edge);
    let mut base_price = maker_anchor.min(fv_target);
    if base_price < loss_floor {
        if best_bid < loss_floor {
            return ExitPlan::Skip;
        }
        base_price = loss_floor;
    }
    let mut base_price = round_down_to_tick(base_price, tick_size);
    if base_price < loss_floor {
        base_price += tick_size;
    }
    if base_price <= Decimal::ZERO {
        return ExitPlan::Skip;
    }
    if base_price <= best_bid {
        base_price = if let Some(ask) = best_ask {
            if ask > best_bid {
                round_down_to_tick(ask, tick_size)
            } else {
                best_bid + tick_size
            }
        } else {
            best_bid + tick_size
        };
    }
    if base_price < loss_floor {
        return ExitPlan::Skip;
    }

    let mut levels = Vec::with_capacity(profile.sell_levels as usize);
    let mut remaining_to_sell = abs_excess;
    for idx in 0..profile.sell_levels {
        let price = base_price - tick_size * Decimal::from(idx);
        if price <= Decimal::ZERO || price < loss_floor || remaining_to_sell <= Decimal::ZERO {
            break;
        }
        let size = quantize_order_size(profile.sell_level_size.min(remaining_to_sell));
        if size < MIN_ORDER_SHARES {
            break;
        }
        remaining_to_sell -= size;
        levels.push(LadderLevel {
            outcome: heavy_outcome,
            price,
            size,
        });
    }
    if levels.is_empty() {
        return ExitPlan::Skip;
    }

    ExitPlan::Maker {
        heavy_outcome,
        levels,
    }
}

fn position_pair_ratio(position: &Position) -> f64 {
    let total = position.total_qty();
    if total <= Decimal::ZERO {
        return 1.0;
    }
    let pairs = position.complete_pairs();
    let num = (pairs * dec!(2)).to_f64().unwrap_or(0.0);
    let den = total.to_f64().unwrap_or(0.0);
    if den <= 0.0 {
        1.0
    } else {
        (num / den).clamp(0.0, 1.0)
    }
}

fn resting_buy_notional(sim: &PaperSimulator, condition_id: &str) -> Decimal {
    sim.orders_for_market(condition_id)
        .into_iter()
        .filter(|order| order.side == PaperSide::Buy)
        .map(|order| order.price * order.size)
        .sum()
}

fn snapshot_orders(sim: &PaperSimulator, condition_id: &str) -> Vec<RestingOrderSnapshot> {
    sim.orders_for_market(condition_id)
        .into_iter()
        .map(|order| RestingOrderSnapshot {
            order_id: order.order_id.clone(),
            outcome: order.outcome,
            price: order.price,
            side: order.side,
        })
        .collect()
}

fn rebalance_extra_capacity(profile: &ReplayProfile, position: &Position) -> Decimal {
    if !profile.rebalance_budget_override {
        return Decimal::ZERO;
    }
    let excess = (position.yes_qty - position.no_qty).abs();
    if excess <= Decimal::ZERO || position.avg_combined_cost() >= dec!(0.98) {
        return Decimal::ZERO;
    }
    let heavy_avg = match position.heavy_side() {
        Some(Outcome::Yes) => position.avg_yes_cost(),
        Some(Outcome::No) => position.avg_no_cost(),
        None => Decimal::ZERO,
    };
    (excess * heavy_avg).min(profile.rebalance_max_extra_budget)
}

fn effective_buy_commitment_cap(
    profile: &ReplayProfile,
    position: &Position,
    elapsed_pct: f64,
) -> Decimal {
    let mut cap = if profile.period_gross_buy_cap_usdc > Decimal::ZERO {
        profile.period_gross_buy_cap_usdc
    } else {
        dec!(1000000)
    };
    if elapsed_pct < profile.early_phase_pct
        && profile.early_phase_gross_buy_cap_usdc > Decimal::ZERO
    {
        cap = cap.min(profile.early_phase_gross_buy_cap_usdc);
    }
    cap + rebalance_extra_capacity(profile, position)
}

fn interleave_ladders(yes_ladder: &[LadderLevel], no_ladder: &[LadderLevel]) -> Vec<LadderLevel> {
    let mut interleaved = Vec::with_capacity(yes_ladder.len() + no_ladder.len());
    let max_len = yes_ladder.len().max(no_ladder.len());
    for idx in 0..max_len {
        if let Some(level) = yes_ladder.get(idx) {
            interleaved.push(level.clone());
        }
        if let Some(level) = no_ladder.get(idx) {
            interleaved.push(level.clone());
        }
    }
    interleaved
}

fn apply_rebalance_size_multiplier(
    profile: &ReplayProfile,
    yes_ladder: &mut Vec<LadderLevel>,
    no_ladder: &mut Vec<LadderLevel>,
    position: &Position,
) {
    if profile.rebalance_size_multiplier <= 1 {
        return;
    }
    let excess = position.yes_qty - position.no_qty;
    let abs_excess = excess.abs();
    if abs_excess < profile.exit_soft_excess {
        return;
    }

    let multiplier = Decimal::from(profile.rebalance_size_multiplier);
    let light_ladder = if excess > Decimal::ZERO {
        no_ladder
    } else {
        yes_ladder
    };
    let mut remaining = abs_excess;
    for level in light_ladder.iter_mut() {
        if remaining <= Decimal::ZERO {
            level.size = Decimal::ZERO;
            continue;
        }
        let boosted = quantize_order_size((level.size * multiplier).max(MIN_ORDER_SHARES));
        let capped = boosted.min(remaining);
        level.size = capped;
        remaining -= capped;
    }
    light_ladder.retain(|level| level.size > Decimal::ZERO);
}

fn compute_pair_completion(
    position: &Position,
    yes_best_ask: Option<Decimal>,
    no_best_ask: Option<Decimal>,
    max_per_cycle: Decimal,
    fee_buffer_fallback: Decimal,
) -> Option<(Outcome, Decimal, Decimal)> {
    let excess = position.yes_qty - position.no_qty;
    let abs_excess = excess.abs();
    if abs_excess < Decimal::ONE {
        return None;
    }

    let (heavy_side_avg, light_ask, light_outcome) = if excess > Decimal::ZERO {
        let avg_yes = if position.yes_qty > Decimal::ZERO {
            position.total_yes_spent / position.yes_qty
        } else {
            return None;
        };
        let ask = no_best_ask?;
        (avg_yes, ask, Outcome::No)
    } else {
        let avg_no = if position.no_qty > Decimal::ZERO {
            position.total_no_spent / position.no_qty
        } else {
            return None;
        };
        let ask = yes_best_ask?;
        (avg_no, ask, Outcome::Yes)
    };

    let effective_pair_cost = heavy_side_avg + light_ask + fee_buffer_fallback;
    if effective_pair_cost >= Decimal::ONE {
        return None;
    }

    let shares = quantize_order_size(abs_excess.min(max_per_cycle));
    if shares < MIN_ORDER_SHARES || shares * light_ask < Decimal::ONE {
        return None;
    }

    Some((light_outcome, shares, light_ask))
}

fn apply_fill_to_position(
    position: &mut Position,
    side: PaperSide,
    outcome: Outcome,
    fill_price: Decimal,
    size: Decimal,
) -> Decimal {
    match (side, outcome) {
        (PaperSide::Buy, Outcome::Yes) => {
            position.yes_qty += size;
            position.total_yes_spent += fill_price * size;
            Decimal::ZERO
        }
        (PaperSide::Buy, Outcome::No) => {
            position.no_qty += size;
            position.total_no_spent += fill_price * size;
            Decimal::ZERO
        }
        (PaperSide::Sell, Outcome::Yes) => {
            let sell_qty = size.min(position.yes_qty);
            if sell_qty <= Decimal::ZERO {
                return Decimal::ZERO;
            }
            let avg_cost = position.avg_yes_cost();
            position.total_yes_spent -= avg_cost * sell_qty;
            position.yes_qty -= sell_qty;
            (fill_price - avg_cost) * sell_qty
        }
        (PaperSide::Sell, Outcome::No) => {
            let sell_qty = size.min(position.no_qty);
            if sell_qty <= Decimal::ZERO {
                return Decimal::ZERO;
            }
            let avg_cost = position.avg_no_cost();
            position.total_no_spent -= avg_cost * sell_qty;
            position.no_qty -= sell_qty;
            (fill_price - avg_cost) * sell_qty
        }
    }
}

fn ladder_size_at_level(profile: &ReplayProfile, level: u32) -> Decimal {
    let factor = (1.0 - (level as f64) * profile.ladder_size_decay).max(0.2);
    let raw = profile.base_order_shares * Decimal::from_f64(factor).unwrap_or(Decimal::ONE);
    quantize_order_size(raw.max(MIN_ORDER_SHARES))
}

fn compute_bid_ladder(
    profile: &ReplayProfile,
    fv_up: f64,
    fv_down: f64,
    position: &Position,
    ladder_levels: u32,
) -> (Vec<LadderLevel>, Vec<LadderLevel>) {
    let yes_dead = fv_up < profile.fv_dead_threshold;
    let no_dead = fv_down < profile.fv_dead_threshold;

    if yes_dead && no_dead {
        return (Vec::new(), Vec::new());
    }

    let mut center_yes_f64 = profile.target_combined.to_f64().unwrap_or(0.97) * fv_up;
    let mut center_no_f64 = profile.target_combined.to_f64().unwrap_or(0.97) * fv_down;

    let excess = position.yes_qty - position.no_qty;
    if excess.abs() > Decimal::ZERO {
        let skew = excess.to_f64().unwrap_or(0.0) * profile.skew_per_share;
        center_yes_f64 -= skew;
        center_no_f64 += skew;
    }

    let min_bid_yes =
        (fv_up * profile.min_bid_fv_ratio).max(profile.min_bid_floor.to_f64().unwrap_or(0.02));
    let min_bid_no =
        (fv_down * profile.min_bid_fv_ratio).max(profile.min_bid_floor.to_f64().unwrap_or(0.02));

    center_yes_f64 = center_yes_f64.max(min_bid_yes);
    center_no_f64 = center_no_f64.max(min_bid_no);

    let center_yes = round_down_to_tick(
        Decimal::from_f64(center_yes_f64).unwrap_or(dec!(0.04)),
        TICK_SIZE,
    );
    let center_no = round_down_to_tick(
        Decimal::from_f64(center_no_f64).unwrap_or(dec!(0.04)),
        TICK_SIZE,
    );

    let step = TICK_SIZE * Decimal::from(LADDER_TICK_SPACING);
    let min_bid_yes_dec = Decimal::from_f64(min_bid_yes).unwrap_or(profile.min_bid_floor);
    let min_bid_no_dec = Decimal::from_f64(min_bid_no).unwrap_or(profile.min_bid_floor);

    let yes_ladder = if yes_dead && position.no_qty == Decimal::ZERO {
        Vec::new()
    } else if yes_dead {
        vec![LadderLevel {
            outcome: Outcome::Yes,
            price: min_bid_yes_dec,
            size: profile.base_order_shares,
        }]
    } else {
        let mut ladder = Vec::with_capacity(ladder_levels as usize);
        for i in 0..ladder_levels {
            let price = center_yes - step * Decimal::from(i);
            if price < min_bid_yes_dec || price <= Decimal::ZERO {
                break;
            }
            ladder.push(LadderLevel {
                outcome: Outcome::Yes,
                price,
                size: ladder_size_at_level(profile, i),
            });
        }
        ladder
    };

    let no_ladder = if no_dead && position.yes_qty == Decimal::ZERO {
        Vec::new()
    } else if no_dead {
        vec![LadderLevel {
            outcome: Outcome::No,
            price: min_bid_no_dec,
            size: profile.base_order_shares,
        }]
    } else {
        let mut ladder = Vec::with_capacity(ladder_levels as usize);
        for i in 0..ladder_levels {
            let price = center_no - step * Decimal::from(i);
            if price < min_bid_no_dec || price <= Decimal::ZERO {
                break;
            }
            ladder.push(LadderLevel {
                outcome: Outcome::No,
                price,
                size: ladder_size_at_level(profile, i),
            });
        }
        ladder
    };

    (yes_ladder, no_ladder)
}

fn apply_ask_anchoring(
    profile: &ReplayProfile,
    yes_ladder: &mut Vec<LadderLevel>,
    no_ladder: &mut Vec<LadderLevel>,
    best_ask_up: Option<Decimal>,
    best_ask_down: Option<Decimal>,
    fv_up: f64,
    fv_down: f64,
    ladder_levels: u32,
) {
    if let Some(yes_ask) = best_ask_up {
        let buffer = TICK_SIZE * Decimal::from(profile.postonly_buffer_ticks);
        let max_bid = round_down_to_tick((yes_ask - buffer).max(Decimal::ZERO), TICK_SIZE);
        let yes_top = yes_ladder.first().map(|l| l.price).unwrap_or(Decimal::ZERO);
        let needs_reanchor = !yes_ladder.is_empty() && (yes_top >= yes_ask || yes_top < max_bid);
        if needs_reanchor {
            let step = TICK_SIZE * Decimal::from(LADDER_TICK_SPACING);
            let min_bid = Decimal::from_f64(
                (fv_up * profile.min_bid_fv_ratio)
                    .max(profile.min_bid_floor.to_f64().unwrap_or(0.02)),
            )
            .unwrap_or(profile.min_bid_floor);
            yes_ladder.clear();
            for i in 0..ladder_levels {
                let price = max_bid - step * Decimal::from(i);
                if price < min_bid || price <= Decimal::ZERO {
                    break;
                }
                yes_ladder.push(LadderLevel {
                    outcome: Outcome::Yes,
                    price,
                    size: ladder_size_at_level(profile, i),
                });
            }
        }
    }
    if let Some(no_ask) = best_ask_down {
        let buffer = TICK_SIZE * Decimal::from(profile.postonly_buffer_ticks);
        let max_bid = round_down_to_tick((no_ask - buffer).max(Decimal::ZERO), TICK_SIZE);
        let no_top = no_ladder.first().map(|l| l.price).unwrap_or(Decimal::ZERO);
        let needs_reanchor = !no_ladder.is_empty() && (no_top >= no_ask || no_top < max_bid);
        if needs_reanchor {
            let step = TICK_SIZE * Decimal::from(LADDER_TICK_SPACING);
            let min_bid = Decimal::from_f64(
                (fv_down * profile.min_bid_fv_ratio)
                    .max(profile.min_bid_floor.to_f64().unwrap_or(0.02)),
            )
            .unwrap_or(profile.min_bid_floor);
            no_ladder.clear();
            for i in 0..ladder_levels {
                let price = max_bid - step * Decimal::from(i);
                if price < min_bid || price <= Decimal::ZERO {
                    break;
                }
                no_ladder.push(LadderLevel {
                    outcome: Outcome::No,
                    price,
                    size: ladder_size_at_level(profile, i),
                });
            }
        }
    }
}

fn apply_post_anchor_skew(
    profile: &ReplayProfile,
    yes_ladder: &mut Vec<LadderLevel>,
    no_ladder: &mut Vec<LadderLevel>,
    position: &Position,
) {
    let excess = position.yes_qty - position.no_qty;
    let abs_excess = excess.abs();

    if abs_excess <= profile.skew_activation_threshold {
        return;
    }

    let skew_shares = abs_excess - profile.skew_activation_threshold;
    let raw_ticks = if profile.shares_per_skew_tick > Decimal::ZERO {
        (skew_shares / profile.shares_per_skew_tick)
            .floor()
            .to_u32()
            .unwrap_or(0)
    } else {
        0
    };
    let skew_ticks = raw_ticks.min(profile.max_skew_ticks);
    if skew_ticks == 0 {
        return;
    }

    let shift = TICK_SIZE * Decimal::from(skew_ticks);
    let heavy_ladder = if excess > Decimal::ZERO {
        yes_ladder
    } else {
        no_ladder
    };
    for level in heavy_ladder.iter_mut() {
        level.price -= shift;
    }
    heavy_ladder.retain(|l| l.price >= profile.min_bid_floor && l.price > Decimal::ZERO);
}

fn parse_prices_csv(path: &Path) -> Result<Vec<TickData>, String> {
    let content = fs::read_to_string(path).map_err(|e| format!("read prices.csv: {e}"))?;
    let mut ticks = Vec::new();
    for (i, line) in content.lines().enumerate() {
        if i == 0 {
            continue;
        }
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() < 11 {
            continue;
        }
        let timestamp = DateTime::parse_from_rfc3339(cols[0])
            .map_err(|e| format!("row {i}: bad timestamp: {e}"))?
            .with_timezone(&Utc);
        let fv_up: f64 = cols[3].parse().unwrap_or(0.5);
        let fv_down: f64 = cols[4].parse().unwrap_or(0.5);
        let remaining_secs: f64 = cols[6].parse().unwrap_or(300.0);
        let best_bid_up = Decimal::from_str(cols[7]).unwrap_or(Decimal::ZERO);
        let best_ask_up = Decimal::from_str(cols[8]).unwrap_or(Decimal::ZERO);
        let best_bid_down = Decimal::from_str(cols[9]).unwrap_or(Decimal::ZERO);
        let best_ask_down = Decimal::from_str(cols[10]).unwrap_or(Decimal::ZERO);

        ticks.push(TickData {
            timestamp,
            fv_up,
            fv_down,
            remaining_secs,
            best_bid_up,
            best_ask_up,
            best_bid_down,
            best_ask_down,
        });
    }
    Ok(ticks)
}

fn parse_period_result(path: &Path) -> Result<PeriodResult, String> {
    let content = fs::read_to_string(path).map_err(|e| format!("read period_result.csv: {e}"))?;
    let lines: Vec<&str> = content.lines().collect();
    if lines.len() < 2 {
        return Err("period_result.csv has no data row".into());
    }

    let header: Vec<&str> = lines[0].split(',').collect();
    let cols: Vec<&str> = lines[1].split(',').collect();
    let period_idx = header.iter().position(|c| *c == "period_name").unwrap_or(0);
    let result_idx = header.iter().position(|c| *c == "result").unwrap_or(4);

    if cols.len() <= result_idx || cols.len() <= period_idx {
        return Err("period_result.csv row missing required columns".into());
    }

    Ok(PeriodResult {
        period_name: cols[period_idx].to_string(),
        result: cols[result_idx].to_string(),
    })
}

fn detect_duration_mins(first_remaining: f64) -> u32 {
    if first_remaining > 1800.0 {
        60
    } else if first_remaining > 500.0 {
        15
    } else {
        5
    }
}

fn combined_cost_limit(profile: &ReplayProfile, position: &Position, outcome: Outcome) -> Decimal {
    match outcome {
        Outcome::Yes if position.yes_qty < position.no_qty => profile.light_side_max_combined,
        Outcome::No if position.no_qty < position.yes_qty => profile.light_side_max_combined,
        _ => profile.max_combined_avg_cost,
    }
}

fn simulate_period(period_dir: &Path, profile: &ReplayProfile) -> Result<PeriodStats, String> {
    let prices_path = period_dir.join("prices.csv");
    let result_path = period_dir.join("period_result.csv");

    let period_result = parse_period_result(&result_path)?;
    let ticks = parse_prices_csv(&prices_path)?;
    if ticks.len() < MIN_ROWS {
        return Err(format!("only {} rows (need {})", ticks.len(), MIN_ROWS));
    }

    let duration_mins = detect_duration_mins(ticks[0].remaining_secs);
    if !profile.allows_duration(duration_mins) {
        return Ok(PeriodStats {
            period_name: period_result.period_name,
            duration_mins,
            result: period_result.result,
            traded: false,
            skipped_due_to_duration: true,
            ticks: ticks.len(),
            orders_placed: 0,
            fills: 0,
            sells: 0,
            sell_orders_placed: 0,
            sell_orders_filled: 0,
            sell_orders_cancelled: 0,
            maker_exit_orders: 0,
            taker_exit_orders: 0,
            merge_count: 0,
            postonly_rejections: 0,
            cost_guard_filtered: 0,
            activation_limited_levels: 0,
            yes_qty: Decimal::ZERO,
            no_qty: Decimal::ZERO,
            pairs: Decimal::ZERO,
            max_excess: Decimal::ZERO,
            end_excess: Decimal::ZERO,
            locked_profit: Decimal::ZERO,
            merge_pnl: Decimal::ZERO,
            merged_pairs: Decimal::ZERO,
            sell_pnl: Decimal::ZERO,
            spec_pnl: Decimal::ZERO,
            total_pnl: Decimal::ZERO,
        });
    }

    let ladder_levels = profile.ladder_levels_for_duration(duration_mins);
    let activation_limit = profile.activation_limit_for_duration(duration_mins);
    let total_market_secs = ticks[0].remaining_secs.max(1.0);

    let mut sim = PaperSimulator::new();
    let mut position = Position::default();
    let condition_id = "bt";

    let mut last_fv_up: Option<f64> = None;
    let mut last_fv_down: Option<f64> = None;
    let mut total_orders = 0u64;
    let mut total_fills = 0u64;
    let mut total_sells = 0u64;
    let mut sell_orders_placed = 0u64;
    let mut sell_orders_filled = 0u64;
    let mut sell_orders_cancelled = 0u64;
    let mut maker_exit_orders = 0u64;
    let mut taker_exit_orders = 0u64;
    let mut merge_count = 0u64;
    let mut total_rejections = 0u64;
    let mut total_cost_guard_filtered = 0u64;
    let mut activation_limited_levels = 0u64;
    let mut merge_pnl = Decimal::ZERO;
    let mut merged_pairs = Decimal::ZERO;
    let mut sell_pnl = Decimal::ZERO;
    let mut max_excess = Decimal::ZERO;
    let mut gross_buy_filled_usdc = Decimal::ZERO;
    let mut merge_cost_basis_released = Decimal::ZERO;
    let mut pair_completion_attempts = 0u32;
    let mut last_pair_completion_at: Option<DateTime<Utc>> = None;
    let mut first_order_placed_at: Option<DateTime<Utc>> = None;
    let mut exit_buy_block: Option<Outcome> = None;
    let mut last_sell_time_yes: Option<DateTime<Utc>> = None;
    let mut last_sell_time_no: Option<DateTime<Utc>> = None;
    let mut hard_excess_started_at: Option<DateTime<Utc>> = None;
    let mut last_merge_time: Option<DateTime<Utc>> = None;

    for tick in &ticks {
        let now = tick.timestamp;
        let ask_up = (tick.best_ask_up > Decimal::ZERO).then_some(tick.best_ask_up);
        let ask_down = (tick.best_ask_down > Decimal::ZERO).then_some(tick.best_ask_down);
        let bid_up = (tick.best_bid_up > Decimal::ZERO).then_some(tick.best_bid_up);
        let bid_down = (tick.best_bid_down > Decimal::ZERO).then_some(tick.best_bid_down);

        let fill_result = sim.check_fills_with_book_at(
            condition_id,
            ask_up,
            ask_down,
            bid_up,
            bid_down,
            tick.fv_up,
            tick.fv_down,
            Some(profile.max_share_imbalance),
            position.total_yes_spent,
            position.total_no_spent,
            now,
        );

        for fill in &fill_result.fills {
            total_fills += 1;
            let realized = apply_fill_to_position(
                &mut position,
                fill.order.side,
                fill.order.outcome,
                fill.fill_price,
                fill.order.size,
            );
            if fill.order.side == PaperSide::Buy {
                gross_buy_filled_usdc += fill.fill_price * fill.order.size;
            } else {
                sell_pnl += realized;
                total_sells += 1;
                sell_orders_filled += 1;
                match fill.order.outcome {
                    Outcome::Yes => last_sell_time_yes = Some(now),
                    Outcome::No => last_sell_time_no = Some(now),
                }
            }
        }
        total_rejections += fill_result.postonly_rejections.len() as u64;
        max_excess = max_excess.max((position.yes_qty - position.no_qty).abs());
        max_excess = max_excess.max((position.yes_qty - position.no_qty).abs());

        let current_abs_excess = (position.yes_qty - position.no_qty).abs();
        if current_abs_excess >= profile.exit_hard_excess {
            if hard_excess_started_at.is_none() {
                hard_excess_started_at = Some(now);
            }
        } else {
            hard_excess_started_at = None;
        }

        let phase = phase_from_remaining(
            tick.remaining_secs,
            total_market_secs,
            profile.resolution_safety_margin_secs,
        );

        let fv_shifted = match (last_fv_up, last_fv_down) {
            (Some(last_up), Some(last_down)) => {
                (tick.fv_up - last_up).abs() > profile.fv_stale_cents
                    || (tick.fv_down - last_down).abs() > profile.fv_stale_cents
            }
            _ => true,
        };
        let elapsed_pct = elapsed_pct_from_remaining(tick.remaining_secs, total_market_secs);

        if fv_shifted {
            if profile.continuous_merge_enabled {
                let should_merge = last_merge_time
                    .map(|last| (now - last).num_seconds() >= profile.merge_interval_secs as i64)
                    .unwrap_or(true);
                if should_merge {
                    let complete_pairs = position.complete_pairs();
                    let min_pairs = Decimal::from(profile.merge_min_pairs);
                    let reserve = Decimal::from(profile.merge_reserve_pairs);
                    if complete_pairs >= min_pairs {
                        let profit_per_pair = Decimal::ONE - position.avg_combined_cost();
                        if profit_per_pair >= profile.merge_min_profit_per_pair {
                            let mergeable = (complete_pairs - reserve).max(Decimal::ZERO);
                            if mergeable > Decimal::ZERO {
                                let released_cost_basis = mergeable * position.avg_combined_cost();
                                let resting_sells: Vec<String> =
                                    snapshot_orders(&sim, condition_id)
                                        .into_iter()
                                        .filter(|order| order.side == PaperSide::Sell)
                                        .map(|order| order.order_id)
                                        .collect();
                                for order_id in resting_sells {
                                    if sim.cancel(&order_id).is_some() {
                                        sell_orders_cancelled += 1;
                                    }
                                }
                                let realized = position.record_merge(mergeable);
                                merge_pnl += realized;
                                merge_cost_basis_released += released_cost_basis;
                                merged_pairs += mergeable;
                                merge_count += 1;
                                last_merge_time = Some(now);
                                if exit_buy_block.is_some()
                                    && (position.yes_qty - position.no_qty).abs()
                                        < profile.exit_soft_excess
                                {
                                    exit_buy_block = None;
                                }
                            }
                        }
                    }
                }
            }

            let resting_orders = snapshot_orders(&sim, condition_id);
            let buy_order_ids: Vec<String> = resting_orders
                .iter()
                .filter(|order| order.side == PaperSide::Buy)
                .map(|order| order.order_id.clone())
                .collect();
            for order_id in buy_order_ids {
                let _ = sim.cancel(&order_id);
            }

            let (mut yes_ladder, mut no_ladder) =
                compute_bid_ladder(profile, tick.fv_up, tick.fv_down, &position, ladder_levels);

            apply_ask_anchoring(
                profile,
                &mut yes_ladder,
                &mut no_ladder,
                ask_up,
                ask_down,
                tick.fv_up,
                tick.fv_down,
                ladder_levels,
            );
            apply_post_anchor_skew(profile, &mut yes_ladder, &mut no_ladder, &position);

            if elapsed_pct < profile.trading_window_start_pct {
                yes_ladder.clear();
                no_ladder.clear();
            } else if elapsed_pct > profile.trading_window_end_pct {
                if profile.wind_down_allow_pair_completion {
                    match position.light_side() {
                        Some(Outcome::Yes) => no_ladder.clear(),
                        Some(Outcome::No) => yes_ladder.clear(),
                        None => {
                            yes_ladder.clear();
                            no_ladder.clear();
                        }
                    }
                } else {
                    yes_ladder.clear();
                    no_ladder.clear();
                }
            }

            let yes_before = yes_ladder.len();
            let no_before = no_ladder.len();

            if let Some(no_best) = no_ladder.first().map(|l| l.price) {
                let max_yes = combined_cost_limit(profile, &position, Outcome::Yes) - no_best;
                yes_ladder.retain(|l| l.price <= max_yes);
            }
            if let Some(yes_best) = yes_ladder.first().map(|l| l.price) {
                let max_no = combined_cost_limit(profile, &position, Outcome::No) - yes_best;
                no_ladder.retain(|l| l.price <= max_no);
            }
            if position.no_qty > Decimal::ZERO {
                let max_yes =
                    combined_cost_limit(profile, &position, Outcome::Yes) - position.avg_no_cost();
                yes_ladder.retain(|l| l.price <= max_yes);
            }
            if position.yes_qty > Decimal::ZERO {
                let max_no =
                    combined_cost_limit(profile, &position, Outcome::No) - position.avg_yes_cost();
                no_ladder.retain(|l| l.price <= max_no);
            }

            total_cost_guard_filtered +=
                (yes_before - yes_ladder.len() + no_before - no_ladder.len()) as u64;

            if position.complete_pairs() == Decimal::ZERO {
                if position.yes_qty >= profile.one_sided_threshold
                    && position.no_qty == Decimal::ZERO
                {
                    yes_ladder.clear();
                } else if position.no_qty >= profile.one_sided_threshold
                    && position.yes_qty == Decimal::ZERO
                {
                    no_ladder.clear();
                }
            }

            if elapsed_pct >= profile.early_phase_pct
                && position.total_qty() >= profile.pair_ratio_eval_min_total_shares
                && position_pair_ratio(&position) < profile.period_min_pair_ratio_for_heavy_add
            {
                match position.heavy_side() {
                    Some(Outcome::Yes) => yes_ladder.clear(),
                    Some(Outcome::No) => no_ladder.clear(),
                    None => {}
                }
            }

            apply_rebalance_size_multiplier(profile, &mut yes_ladder, &mut no_ladder, &position);

            if profile.sell_buy_cooldown_secs > 0 {
                if last_sell_time_yes
                    .map(|last| (now - last).num_seconds() < profile.sell_buy_cooldown_secs as i64)
                    .unwrap_or(false)
                {
                    yes_ladder.clear();
                }
                if last_sell_time_no
                    .map(|last| (now - last).num_seconds() < profile.sell_buy_cooldown_secs as i64)
                    .unwrap_or(false)
                {
                    no_ladder.clear();
                }
            }

            if let Some(blocked) = exit_buy_block {
                let still_heavy = match blocked {
                    Outcome::Yes => position.yes_qty > position.no_qty,
                    Outcome::No => position.no_qty > position.yes_qty,
                };
                if still_heavy {
                    match blocked {
                        Outcome::Yes => yes_ladder.clear(),
                        Outcome::No => no_ladder.clear(),
                    }
                } else {
                    exit_buy_block = None;
                }
            }

            let in_grace_period = first_order_placed_at
                .map(|started| {
                    (now - started).num_seconds() < profile.sellback_grace_period_secs as i64
                })
                .unwrap_or(true);
            let heavy_side_has_buys = if position.yes_qty > position.no_qty {
                !yes_ladder.is_empty()
            } else if position.no_qty > position.yes_qty {
                !no_ladder.is_empty()
            } else {
                false
            };
            let breaker_secs = hard_excess_started_at
                .map(|started| (now - started).num_seconds().max(0) as f64)
                .unwrap_or(0.0);
            let exit_plan = compute_excess_exit_plan(
                &position,
                tick.fv_up,
                tick.fv_down,
                bid_up,
                bid_down,
                ask_up,
                ask_down,
                TICK_SIZE,
                profile,
                tick.remaining_secs,
                breaker_secs,
                in_grace_period,
                heavy_side_has_buys,
            );
            let (sell_ladder, taker_exit) = match exit_plan {
                ExitPlan::Skip => (Vec::new(), None),
                ExitPlan::Maker {
                    heavy_outcome,
                    levels,
                } => {
                    exit_buy_block = Some(heavy_outcome);
                    match heavy_outcome {
                        Outcome::Yes => yes_ladder.clear(),
                        Outcome::No => no_ladder.clear(),
                    }
                    (levels, None)
                }
                ExitPlan::Taker {
                    heavy_outcome,
                    size,
                    price,
                } => {
                    exit_buy_block = Some(heavy_outcome);
                    match heavy_outcome {
                        Outcome::Yes => yes_ladder.clear(),
                        Outcome::No => no_ladder.clear(),
                    }
                    (Vec::new(), Some((heavy_outcome, size, price)))
                }
            };

            let resting_sells: Vec<RestingOrderSnapshot> = resting_orders
                .into_iter()
                .filter(|order| order.side == PaperSide::Sell)
                .collect();
            let sell_target_keys: std::collections::HashSet<(Outcome, Decimal)> = sell_ladder
                .iter()
                .map(|level| (level.outcome, level.price))
                .collect();
            let sells_to_cancel: Vec<String> = resting_sells
                .iter()
                .filter(|order| {
                    taker_exit.is_some()
                        || !sell_target_keys.contains(&(order.outcome, order.price))
                })
                .map(|order| order.order_id.clone())
                .collect();
            for order_id in sells_to_cancel {
                if sim.cancel(&order_id).is_some() {
                    sell_orders_cancelled += 1;
                }
            }
            let existing_sell_keys: std::collections::HashSet<(Outcome, Decimal)> =
                snapshot_orders(&sim, condition_id)
                    .into_iter()
                    .filter(|order| order.side == PaperSide::Sell)
                    .map(|order| (order.outcome, order.price))
                    .collect();

            if let Some(limit) = activation_limit {
                if yes_ladder.len() > limit {
                    activation_limited_levels += (yes_ladder.len() - limit) as u64;
                    yes_ladder.truncate(limit);
                }
                if no_ladder.len() > limit {
                    activation_limited_levels += (no_ladder.len() - limit) as u64;
                    no_ladder.truncate(limit);
                }
            }

            let commitment_cap = effective_buy_commitment_cap(profile, &position, elapsed_pct);
            let mut committed_usdc = (gross_buy_filled_usdc - merge_cost_basis_released)
                .max(Decimal::ZERO)
                + resting_buy_notional(&sim, condition_id);
            if committed_usdc < commitment_cap {
                for level in interleave_ladders(&yes_ladder, &no_ladder) {
                    let remaining_cap = (commitment_cap - committed_usdc).max(Decimal::ZERO);
                    if remaining_cap <= Decimal::ZERO {
                        break;
                    }
                    let budget_capped_size =
                        quantize_order_size((remaining_cap / level.price.max(dec!(0.01))).floor());
                    let size = cap_buy_size_for_notional(
                        level.size.min(budget_capped_size),
                        level.price,
                        profile.single_order_notional_cap_usdc,
                    );
                    if size < MIN_ORDER_SHARES {
                        continue;
                    }
                    sim.place_buy_at(condition_id, level.outcome, level.price, size, now);
                    committed_usdc += size * level.price;
                    total_orders += 1;
                    if first_order_placed_at.is_none() {
                        first_order_placed_at = Some(now);
                    }
                }
            }

            for level in sell_ladder {
                if existing_sell_keys.contains(&(level.outcome, level.price)) {
                    continue;
                }
                sim.place_sell_at(condition_id, level.outcome, level.price, level.size, now);
                total_orders += 1;
                sell_orders_placed += 1;
                maker_exit_orders += 1;
                if first_order_placed_at.is_none() {
                    first_order_placed_at = Some(now);
                }
            }

            if let Some((outcome, size, price)) = taker_exit {
                let available = match outcome {
                    Outcome::Yes => position.yes_qty,
                    Outcome::No => position.no_qty,
                };
                let size = quantize_order_size(size.min(available));
                if size >= MIN_ORDER_SHARES && price > Decimal::ZERO {
                    let realized = apply_fill_to_position(
                        &mut position,
                        PaperSide::Sell,
                        outcome,
                        price,
                        size,
                    );
                    sell_pnl += realized;
                    total_sells += 1;
                    sell_orders_placed += 1;
                    sell_orders_filled += 1;
                    taker_exit_orders += 1;
                    total_orders += 1;
                    match outcome {
                        Outcome::Yes => last_sell_time_yes = Some(now),
                        Outcome::No => last_sell_time_no = Some(now),
                    }
                }
            }

            last_fv_up = Some(tick.fv_up);
            last_fv_down = Some(tick.fv_down);
        }
        let imbalance_now = (position.yes_qty - position.no_qty).abs();
        if imbalance_now < Decimal::ONE {
            pair_completion_attempts = 0;
            last_pair_completion_at = None;
        }

        let can_attempt_pair_completion = (phase == ReplayPhase::Late
            || tick.remaining_secs <= profile.very_late_phase_secs as f64)
            && tick.remaining_secs > profile.resolution_safety_margin_secs as f64
            && pair_completion_attempts < profile.pair_completion_max_attempts
            && last_pair_completion_at
                .map(|last| (now - last).num_seconds() >= profile.pair_completion_retry_secs as i64)
                .unwrap_or(true);

        if can_attempt_pair_completion {
            if let Some((outcome, shares, price)) = compute_pair_completion(
                &position,
                ask_up,
                ask_down,
                dec!(20),
                profile.pair_fee_buffer,
            ) {
                let already_resting = sim.resting_buy_shares(condition_id, outcome);
                let shares = quantize_order_size((shares - already_resting).max(Decimal::ZERO));
                let shares = cap_buy_size_for_notional(
                    shares,
                    price,
                    profile.single_order_notional_cap_usdc,
                );
                let commitment_cap = effective_buy_commitment_cap(profile, &position, elapsed_pct);
                let remaining_cap = (commitment_cap
                    - ((gross_buy_filled_usdc - merge_cost_basis_released).max(Decimal::ZERO)
                        + resting_buy_notional(&sim, condition_id)))
                .max(Decimal::ZERO);
                let budget_capped_size =
                    quantize_order_size((remaining_cap / price.max(dec!(0.01))).floor());
                let shares = shares.min(budget_capped_size);
                if shares >= MIN_ORDER_SHARES {
                    sim.place_buy_at(condition_id, outcome, price, shares, now);
                    total_orders += 1;
                    pair_completion_attempts += 1;
                    last_pair_completion_at = Some(now);
                }
            }
        }
    }

    if profile.merge_at_closing {
        let complete_pairs = position.complete_pairs();
        if complete_pairs > Decimal::ZERO {
            let profit_per_pair = Decimal::ONE - position.avg_combined_cost();
            if profit_per_pair >= profile.merge_min_profit_per_pair {
                let realized = position.record_merge(complete_pairs);
                if realized != Decimal::ZERO {
                    merge_pnl += realized;
                    merged_pairs += complete_pairs;
                    merge_count += 1;
                }
            }
        }
    }

    let pairs = position.yes_qty.min(position.no_qty);
    let locked_profit = if pairs > Decimal::ZERO {
        let avg_pair_cost = position.avg_yes_cost() + position.avg_no_cost();
        pairs * (Decimal::ONE - avg_pair_cost)
    } else {
        Decimal::ZERO
    };

    let excess_yes = position.yes_qty - pairs;
    let excess_no = position.no_qty - pairs;
    let spec_pnl = match period_result.result.as_str() {
        "UP" => {
            excess_yes * (Decimal::ONE - position.avg_yes_cost())
                - excess_no * position.avg_no_cost()
        }
        "DOWN" => {
            excess_no * (Decimal::ONE - position.avg_no_cost())
                - excess_yes * position.avg_yes_cost()
        }
        _ => Decimal::ZERO,
    };
    let total_pnl = locked_profit + merge_pnl + sell_pnl + spec_pnl;

    Ok(PeriodStats {
        period_name: period_result.period_name,
        duration_mins,
        result: period_result.result,
        traded: true,
        skipped_due_to_duration: false,
        ticks: ticks.len(),
        orders_placed: total_orders,
        fills: total_fills,
        sells: total_sells,
        sell_orders_placed,
        sell_orders_filled,
        sell_orders_cancelled,
        maker_exit_orders,
        taker_exit_orders,
        merge_count,
        postonly_rejections: total_rejections,
        cost_guard_filtered: total_cost_guard_filtered,
        activation_limited_levels,
        yes_qty: position.yes_qty,
        no_qty: position.no_qty,
        pairs,
        max_excess,
        end_excess: (position.yes_qty - position.no_qty).abs(),
        locked_profit,
        merge_pnl,
        merged_pairs,
        sell_pnl,
        spec_pnl,
        total_pnl,
    })
}

fn parse_args() -> Result<CliArgs, String> {
    let mut config_path = None;
    let mut asset = "BTC".to_string();
    let mut data_dir = None;
    let mut summary_json = None;
    let mut quiet = false;

    let args: Vec<String> = env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                i += 1;
                let value = args.get(i).ok_or("--config requires a path")?;
                config_path = Some(PathBuf::from(value));
            }
            "--asset" => {
                i += 1;
                let value = args.get(i).ok_or("--asset requires a symbol")?;
                asset = value.to_uppercase();
            }
            "--summary-json" => {
                i += 1;
                let value = args.get(i).ok_or("--summary-json requires a path")?;
                summary_json = Some(PathBuf::from(value));
            }
            "--quiet" => {
                quiet = true;
            }
            other if other.starts_with("--") => {
                return Err(format!("unknown flag: {other}"));
            }
            other => {
                if data_dir.is_some() {
                    return Err(format!("unexpected extra positional argument: {other}"));
                }
                data_dir = Some(PathBuf::from(other));
            }
        }
        i += 1;
    }

    Ok(CliArgs {
        config_path,
        asset,
        data_dir: data_dir.unwrap_or_else(|| PathBuf::from("700 periods/BTC")),
        summary_json,
        quiet,
    })
}

fn build_summary(
    profile: &ReplayProfile,
    data_dir: &Path,
    results: &[PeriodStats],
    skipped: u32,
) -> SummaryMetrics {
    let periods_processed = results.len();
    let traded: Vec<&PeriodStats> = results.iter().filter(|r| r.traded).collect();
    let periods_traded = traded.len();
    let periods_no_trade_due_to_duration =
        results.iter().filter(|r| r.skipped_due_to_duration).count();

    let total_pnl: Decimal = results.iter().map(|r| r.total_pnl).sum();
    let avg_pnl_traded = if periods_traded > 0 {
        traded.iter().map(|r| r.total_pnl).sum::<Decimal>() / Decimal::from(periods_traded as u64)
    } else {
        Decimal::ZERO
    };
    let avg_pnl_all = if periods_processed > 0 {
        total_pnl / Decimal::from(periods_processed as u64)
    } else {
        Decimal::ZERO
    };

    let wins = traded
        .iter()
        .filter(|r| r.total_pnl > Decimal::ZERO)
        .count();
    let win_rate_traded = if periods_traded > 0 {
        wins as f64 / periods_traded as f64 * 100.0
    } else {
        0.0
    };
    let total_fills: u64 = traded.iter().map(|r| r.fills).sum();
    let total_orders: u64 = traded.iter().map(|r| r.orders_placed).sum();
    let fill_rate = if total_orders > 0 {
        total_fills as f64 / total_orders as f64 * 100.0
    } else {
        0.0
    };
    let total_locked: Decimal = traded.iter().map(|r| r.locked_profit).sum();
    let total_merge_pnl: Decimal = traded.iter().map(|r| r.merge_pnl).sum();
    let total_sell_pnl: Decimal = traded.iter().map(|r| r.sell_pnl).sum();
    let total_spec_pnl: Decimal = traded.iter().map(|r| r.spec_pnl).sum();
    let sell_orders_placed: u64 = traded.iter().map(|r| r.sell_orders_placed).sum();
    let sell_orders_filled: u64 = traded.iter().map(|r| r.sell_orders_filled).sum();
    let sell_orders_cancelled: u64 = traded.iter().map(|r| r.sell_orders_cancelled).sum();
    let maker_exit_orders: u64 = traded.iter().map(|r| r.maker_exit_orders).sum();
    let taker_exit_orders: u64 = traded.iter().map(|r| r.taker_exit_orders).sum();
    let merge_count: u64 = traded.iter().map(|r| r.merge_count).sum();
    let total_merged_pairs: Decimal = traded.iter().map(|r| r.merged_pairs).sum();
    let end_excess_gt_zero_count = traded
        .iter()
        .filter(|r| r.end_excess > Decimal::ZERO)
        .count();
    let end_excess_gt_zero_frac = if periods_traded > 0 {
        end_excess_gt_zero_count as f64 / periods_traded as f64
    } else {
        0.0
    };

    let pnl_values: Vec<f64> = traded
        .iter()
        .map(|r| r.total_pnl.to_f64().unwrap_or(0.0))
        .collect();
    let sharpe_traded = if pnl_values.is_empty() {
        0.0
    } else {
        let mean = pnl_values.iter().sum::<f64>() / pnl_values.len() as f64;
        let variance =
            pnl_values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / pnl_values.len() as f64;
        let std_dev = variance.sqrt();
        if std_dev > 0.0 {
            mean / std_dev
        } else {
            0.0
        }
    };

    let mut sorted_traded_pnls: Vec<Decimal> = traded.iter().map(|r| r.total_pnl).collect();
    sorted_traded_pnls.sort();
    let tail_count = sorted_traded_pnls.len().max(1).div_ceil(10);
    let lower_tail_p10_avg = if sorted_traded_pnls.is_empty() {
        Decimal::ZERO
    } else {
        sorted_traded_pnls
            .iter()
            .take(tail_count)
            .copied()
            .sum::<Decimal>()
            / Decimal::from(tail_count as u64)
    };

    let negative_locked_profit_count = traded
        .iter()
        .filter(|r| r.locked_profit < Decimal::ZERO)
        .count();
    let negative_locked_profit_frac = if periods_traded > 0 {
        negative_locked_profit_count as f64 / periods_traded as f64
    } else {
        0.0
    };

    let catastrophic_excess_threshold = profile.base_order_shares * dec!(2);
    let catastrophic_excess_count = traded
        .iter()
        .filter(|r| r.max_excess > catastrophic_excess_threshold && r.total_pnl < Decimal::ZERO)
        .count();
    let catastrophic_excess_frac = if periods_traded > 0 {
        catastrophic_excess_count as f64 / periods_traded as f64
    } else {
        0.0
    };

    let min_period_pnl = results
        .iter()
        .map(|r| r.total_pnl)
        .min()
        .unwrap_or(Decimal::ZERO);
    let max_period_pnl = results
        .iter()
        .map(|r| r.total_pnl)
        .max()
        .unwrap_or(Decimal::ZERO);

    let mut durations_processed = BTreeMap::new();
    for stat in results {
        *durations_processed
            .entry(format!("{}m", stat.duration_mins))
            .or_insert(0usize) += 1;
    }

    SummaryMetrics {
        profile_name: profile.name.clone(),
        config_path: profile.config_path.clone(),
        data_dir: data_dir.display().to_string(),
        periods_processed,
        periods_traded,
        periods_no_trade_due_to_duration,
        periods_skipped: skipped,
        total_pnl: total_pnl.to_string(),
        avg_pnl_per_traded_period: avg_pnl_traded.to_string(),
        avg_pnl_per_processed_period: avg_pnl_all.to_string(),
        lower_tail_p10_avg: lower_tail_p10_avg.to_string(),
        win_rate_traded,
        fill_rate,
        total_locked: total_locked.to_string(),
        total_merge_pnl: total_merge_pnl.to_string(),
        total_sell_pnl: total_sell_pnl.to_string(),
        total_spec_pnl: total_spec_pnl.to_string(),
        sell_orders_placed,
        sell_orders_filled,
        sell_orders_cancelled,
        maker_exit_orders,
        taker_exit_orders,
        merge_count,
        total_merged_pairs: total_merged_pairs.to_string(),
        end_excess_gt_zero_count,
        end_excess_gt_zero_frac,
        sharpe_traded,
        negative_locked_profit_count,
        negative_locked_profit_frac,
        catastrophic_excess_count,
        catastrophic_excess_frac,
        min_period_pnl: min_period_pnl.to_string(),
        max_period_pnl: max_period_pnl.to_string(),
        durations_processed,
    }
}

fn main() {
    let cli = match parse_args() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };

    if !cli.data_dir.exists() {
        eprintln!("Data directory not found: {}", cli.data_dir.display());
        std::process::exit(2);
    }

    let profile = match &cli.config_path {
        Some(path) => match ReplayProfile::from_config(path, &cli.asset) {
            Ok(profile) => profile,
            Err(e) => {
                eprintln!("Failed to load replay profile: {e}");
                std::process::exit(2);
            }
        },
        None => ReplayProfile::default(),
    };

    let mut period_dirs: Vec<PathBuf> = fs::read_dir(&cli.data_dir)
        .expect("Cannot read data directory")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    period_dirs.sort();

    if !cli.quiet {
        println!(
            "Profile: {} ({})",
            profile.name,
            profile
                .config_path
                .as_deref()
                .unwrap_or("hardcoded defaults")
        );
        println!(
            "{:<55} | {:>3} | {:>6} | {:>5} | {:>4} | {:>4} | {:>5} | {:>4} | {:>5} | {:>5} | {:>5} | {:>7} | {:>7} | {:>7}",
            "Period",
            "Dur",
            "Mode",
            "Ticks",
            "Ord",
            "Fill",
            "SFll",
            "Mrg",
            "Pair",
            "ExMx",
            "ExEnd",
            "Locked",
            "Sell",
            "PnL"
        );
        println!("{}", "-".repeat(175));
    }

    let mut results = Vec::new();
    let mut skipped = 0u32;

    for dir in &period_dirs {
        match simulate_period(dir, &profile) {
            Ok(stats) => {
                if !cli.quiet {
                    let mode = if stats.skipped_due_to_duration {
                        "NOOP"
                    } else {
                        "TRADE"
                    };
                    println!(
                        "{:<55} | {:>2}m | {:>6} | {:>5} | {:>4} | {:>4} | {:>5} | {:>4} | {:>5} | {:>5} | {:>5} | {:>7.2} | {:>7.2} | {:>7.2}",
                        stats.period_name,
                        stats.duration_mins,
                        mode,
                        stats.ticks,
                        stats.orders_placed,
                        stats.fills,
                        stats.sell_orders_filled,
                        stats.merge_count,
                        stats.pairs,
                        stats.max_excess,
                        stats.end_excess,
                        stats.locked_profit,
                        stats.sell_pnl,
                        stats.total_pnl,
                    );
                }
                results.push(stats);
            }
            Err(e) => {
                skipped += 1;
                if !cli.quiet {
                    let dir_name = dir
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_default();
                    eprintln!("SKIP {}: {}", dir_name, e);
                }
            }
        }
    }

    if results.is_empty() {
        eprintln!("No periods processed.");
        std::process::exit(1);
    }

    let summary = build_summary(&profile, &cli.data_dir, &results, skipped);

    if !cli.quiet {
        println!("\n{}", "=".repeat(80));
        println!(
            "Periods: {} processed, {} traded, {} no-op due to duration, {} skipped",
            summary.periods_processed,
            summary.periods_traded,
            summary.periods_no_trade_due_to_duration,
            summary.periods_skipped
        );
        println!("Total PnL:      ${}", summary.total_pnl);
        println!("Avg PnL/traded: ${}", summary.avg_pnl_per_traded_period);
        println!("Tail P10 avg:   ${}", summary.lower_tail_p10_avg);
        println!("Win rate:       {:.1}%", summary.win_rate_traded);
        println!("Fill rate:      {:.1}%", summary.fill_rate);
        println!(
            "Sell fills:     {} placed / {} filled / {} cancelled",
            summary.sell_orders_placed, summary.sell_orders_filled, summary.sell_orders_cancelled
        );
        println!(
            "Merges:         {} merges / {} pairs / ${}",
            summary.merge_count, summary.total_merged_pairs, summary.total_merge_pnl
        );
        println!(
            "Exit mix:       {} maker / {} taker",
            summary.maker_exit_orders, summary.taker_exit_orders
        );
        println!(
            "End excess >0:  {:.1}% ({}/{})",
            summary.end_excess_gt_zero_frac * 100.0,
            summary.end_excess_gt_zero_count,
            summary.periods_traded
        );
        println!(
            "Neg locked:     {:.1}% ({}/{})",
            summary.negative_locked_profit_frac * 100.0,
            summary.negative_locked_profit_count,
            summary.periods_traded
        );
        println!(
            "Cat. excess:    {:.1}% ({}/{})",
            summary.catastrophic_excess_frac * 100.0,
            summary.catastrophic_excess_count,
            summary.periods_traded
        );
        println!("Sharpe traded:  {:.3}", summary.sharpe_traded);
    }

    if let Some(path) = cli.summary_json {
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let body = serde_json::to_vec_pretty(&summary).expect("serialize summary");
        fs::write(&path, body).expect("write summary json");
    }
}
