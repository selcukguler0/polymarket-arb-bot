//! Orchestrator — Gabagool ladder strategy.
//!
//! Core features:
//!
//! 1. **Binance WS feed**: Real-time BTC/USDT price for fair-value computation.
//! 2. **Fair-value pricing**: `P(up) = Φ(ln(S/S₀) / (σ√T))` — quotes track
//!    actual probability rather than orderbook midpoints.
//! 3. **Bid ladder**: Multiple resting bid levels per side, with diff-based
//!    cancel/place to avoid cancel-replace churn.
//! 4. **Per-order combined cost guard**: Each level checked against
//!    `level_price + opposite_avg <= max_per_order_combined`.
//! 5. **Soft/hard balance management**: Linearly reduce heavy side above
//!    soft threshold, hard block above max imbalance.
//! 6. **Volatility filter**: Skip calm markets where vol is too low for profitable
//!    pair accumulation.
//! 7. **EV circuit breaker**: Stop buying when expected loss from directional
//!    excess exceeds locked arbitrage profit.
//! 8. **FV dead-zone**: Stop bidding on a side when its fair value < 10%.
//! 9. **Sell-back engine**: Actively sell excess shares to reduce directional risk.
//! 10. **Time-decaying imbalance**: Allowed imbalance shrinks as resolution approaches.

use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::connect_async;
use tracing::{debug, error, info, warn};

use crate::config::{TradingMode, ValidatedConfig};
use crate::dashboard::state::{
    DetailedOrderEntry, OrderFeedEntry, OrderStatus, PeriodSummary, PeriodSummaryEntry,
    PositionEntry, RestingBid,
};
use crate::execution::FillHandler;
use crate::file_logger::PeriodLogger;
use crate::onchain::OnChainManager;
use crate::paper_sim::{PaperFill, PaperSide, PaperSimulator};
use crate::persistence::Database;
use crate::risk::{EmergencyHandler, InventoryManager};
use crate::sdk::SdkClients;
use crate::types::*;
use crate::web::{BotStatus, SharedBotControl, SharedDashboard};

// ═══════════════════════════════════════════════════════════════════════
// Trading Suspension
// ═══════════════════════════════════════════════════════════════════════

/// Why trading is temporarily suspended.
/// Different reasons have different cooldown strategies:
/// - `EngineRestart` (425): CLOB engine restarting, 30-60s cooldown, orders may be wiped.
/// - `RateLimited` (429): Too many requests, exponential backoff.
/// - `CancelOnly` (503): CLOB in maintenance/degraded mode, only cancels allowed.
/// - `Manual`: Operator-initiated pause via dashboard control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingSuspendReason {
    /// 425: CLOB engine restart. All resting orders may have been cancelled.
    /// Wait for engine to come back, then reconcile before placing new orders.
    EngineRestart,
    /// 429: Too many requests. Exponential backoff (2s → 4s → 8s → ... → 30s max).
    RateLimited,
    /// 503: CLOB in cancel-only / maintenance mode. Can cancel but not place orders.
    CancelOnly,
    /// Operator pause via dashboard control.
    Manual,
}

impl std::fmt::Display for TradingSuspendReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TradingSuspendReason::EngineRestart => write!(f, "engine_restart_425"),
            TradingSuspendReason::RateLimited => write!(f, "rate_limited_429"),
            TradingSuspendReason::CancelOnly => write!(f, "cancel_only_503"),
            TradingSuspendReason::Manual => write!(f, "manual"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// V2 Configuration
// ═══════════════════════════════════════════════════════════════════════

/// V2-specific configuration parameters.
/// These supplement the base `ValidatedConfig`.
#[derive(Debug, Clone)]
pub struct V2Config {
    /// Base edge per side: `bid = fv * target_combined / 1.0`.
    /// Controls guaranteed profit: `profit_per_pair = 1.0 - target_combined`.
    pub target_combined: Decimal,

    /// Minimum bid on either side (prevents 0-bids on extreme fair values).
    pub min_bid: Decimal,

    /// Quote skew: reduce heavy-side bid by this much per excess share.
    pub imbalance_skew_per_share: Decimal,

    /// Minimum realized vol (per-second log-return σ) to place quotes.
    /// Below this, markets are too calm for profitable pair accumulation.
    pub min_vol_per_sec: f64,

    /// Rolling window (seconds) for volatility estimation.
    pub vol_window_secs: u64,

    /// V2 quote refresh interval (ms). Faster than v1's 1000ms.
    pub quote_refresh_ms: u64,

    /// Number of price levels per side in the bid ladder.
    pub ladder_levels: u32,

    /// Tick spacing between consecutive ladder levels.
    pub ladder_tick_spacing: u32,

    /// Shares per ladder level (base size at level 0).
    pub level_order_size: Decimal,

    /// Size decay rate per ladder level (0.0-1.0). Each level further from
    /// center gets `(1 - level * decay)` fraction of base size, floored at 20%.
    pub ladder_size_decay: f64,

    /// Per-order combined cost guard: skip level if price + opposite avg >= this.
    pub max_per_order_combined: Decimal,

    /// Cancel resting orders more than this many ticks from the ladder edge.
    pub stale_distance_ticks: u32,

    /// Cancel ALL resting orders on a side when the highest resting bid exceeds
    /// the new ladder top by more than this amount. Reacts to FV shifts faster
    /// than stale_distance_ticks which only ages out individual orders.
    pub fv_stale_cancel_cents: Decimal,

    /// Hard block: stop buying heavy side if |yes_cost - no_cost| > this (USDC).
    /// Cost-weighted so cheap deep-grid fills don't trigger the same as expensive ladder fills.
    pub max_abs_imbalance: Decimal,

    /// Begin linearly reducing heavy-side size when |yes_cost - no_cost| > this (USDC).
    pub soft_imbalance_threshold: Decimal,

    // ── EV Circuit Breaker ──
    /// Enable EV-based circuit breaker that stops buying when directional
    /// risk exceeds locked arbitrage profit.
    pub ev_circuit_breaker_enabled: bool,

    /// Stop buying when |excess_ev| > locked_profit * this ratio.
    pub ev_stop_buying_ratio: Decimal,

    /// Minimum |excess_ev| threshold before the EV breaker can fire.
    /// Prevents the breaker from tripping on tiny FV fluctuations during
    /// the pair-building phase when locked_profit is near zero.
    pub ev_min_excess_threshold: Decimal,

    /// Scale up EV breaker thresholds early in the period (e.g. 3.0 = 3x).
    /// Linearly decays from this multiplier to 1.0 over the early phase.
    pub ev_early_period_multiplier: Decimal,

    /// Fraction of total period considered "early" for EV threshold scaling.
    /// E.g. 0.50 = first half of the period uses scaled-up thresholds.
    pub ev_early_period_end_pct: f64,

    // ── FV Dead-Zone ──
    /// Stop bidding on a side entirely when its fair value drops below this.
    pub fv_dead_threshold: f64,

    /// Absolute floor for dynamic min bid (never bid below this).
    pub min_bid_absolute_floor: Decimal,

    /// Dynamic min bid = max(fv * this ratio, min_bid_absolute_floor).
    pub min_bid_fv_ratio: f64,

    // ── Sell-Back Engine ──
    /// Minimum profit above avg cost to place sell-back orders.
    pub sellback_edge: Decimal,

    /// Minimum excess shares before sell-back activates.
    pub sellback_min_excess: Decimal,

    /// Shares per sell-back level.
    pub sell_level_size: Decimal,

    /// Number of sell price levels.
    pub sell_levels: u32,

    // ── Time-Decaying Imbalance ──
    /// Floor for max_abs_imbalance as it decays toward resolution (USDC).
    pub imbalance_decay_floor_abs: Decimal,

    /// Floor for soft_imbalance_threshold as it decays toward resolution (USDC).
    pub imbalance_decay_floor_soft: Decimal,

    /// Seconds before end_date to enter VeryLate phase.
    pub very_late_phase_secs: u64,

    // ── Anti-Oscillation ──
    /// PostOnly regeneration buffer: place bids this many ticks below ask
    /// instead of 1 tick. Prevents immediate fill on tiny market moves.
    pub postonly_regen_buffer_ticks: u32,

    /// Seconds to suppress buying a side after selling it.
    pub sell_buy_cooldown_secs: u64,

    // ── Sell-Back Grace Period ──
    /// Suppress sell-back entirely for the first N seconds of a market period.
    /// This gives the bot time to accumulate pairs before the sell-back engine
    /// starts trimming excess. Prevents the "stuck at 15 pairs" problem.
    pub sellback_grace_period_secs: u64,

    /// When true, skip sell-back if executing it would reduce locked_profit.
    /// Prevents the churn cycle where buy+sell-back permanently dilutes
    /// the average cost basis.
    pub sellback_protect_locked_profit: bool,

    /// Maximum loss per share the sell-back engine will accept below avg cost.
    /// E.g., 0.02 = sell at avg_cost - 2¢ when market bid is below cost.
    /// This prevents holding losing excess all the way to expiry.
    pub sellback_max_loss_cents: Decimal,

    // ── Exit Escalation Policy ──
    /// Start active exit planning when |cost_excess| >= this (USDC).
    pub exit_soft_excess: Decimal,

    /// Escalate to taker logic sooner when |cost_excess| >= this (USDC).
    pub exit_hard_excess: Decimal,

    /// If EV breaker has persisted for this many seconds (and excess is hard),
    /// escalate from maker to taker exits.
    pub exit_taker_after_secs: u64,

    /// Force taker exits in late phase when remaining seconds <= this and
    /// excess is at least soft threshold.
    pub exit_force_taker_remaining_secs: u64,

    /// Cooldown for EV breaker warning logs (seconds) per market.
    pub ev_log_cooldown_secs: u64,

    // ── Trending Market Detection ──
    /// Rolling window (seconds) to measure BTC trend direction.
    pub trend_window_secs: u64,

    /// Suppress buying the losing side when BTC moves more than this many
    /// dollars in one direction over the trend window.
    pub trend_threshold_dollars: f64,

    /// Duration-aware trend thresholds: override trend_threshold_dollars per market duration.
    /// If set, used instead of trend_threshold_dollars for the respective duration.
    pub trend_threshold_5m: Option<f64>,
    pub trend_threshold_15m: Option<f64>,
    pub trend_threshold_60m: Option<f64>,

    // ── Sigma Blending ──
    /// Blend weight for 1-minute vol: sigma = alpha*rv_1m + (1-alpha)*rv_all.
    /// 0.0 = pure all-window, 1.0 = pure 1-minute. Default 0.4.
    pub sigma_blend_alpha: f64,

    // ── Volatility Circuit Breaker ──
    /// Maximum sigma (per-second log-return vol) before suppressing new orders.
    /// When sigma exceeds this, no new orders are placed but resting orders are
    /// kept alive (they may still fill). Set to 0.0 to disable.
    pub max_sigma: f64,

    /// Hysteresis factor: resume placing orders when sigma drops below
    /// max_sigma * this. Prevents flip-flopping at the boundary.
    pub max_sigma_resume_factor: f64,

    // ── Fee-Aware Pair Completion ──
    /// Fee buffer: accept pairs at combined cost < 1.0 - this.
    pub pair_fee_buffer: Decimal,

    // ── Ladder Churn Reduction ──
    /// Minimum center price movement to trigger cancel/replace cycle.
    pub ladder_reprice_threshold: Decimal,

    // ── One-Sided Guard ──
    /// Threshold for one-sided position guard (was hardcoded to 30).
    pub one_sided_threshold: Decimal,

    // ── Hybrid FV ──
    /// Blend weight for book midpoint in FV: 0.0 = pure BS, 1.0 = pure book.
    pub fv_book_blend_weight: f64,

    // ── Sigma Warm-Up ──
    /// Minimum price samples before trading (prevents noisy early vol estimates).
    pub min_sigma_samples: u32,

    // ── Late-Entry Guard ──
    /// Minimum fraction of period remaining to accept a new market.
    /// 0.75 = skip if less than 75% of the period remains (e.g., >3:45 into 15-min).
    pub min_period_remaining_pct: f64,

    // ── Trading Window ──
    /// Fraction of period elapsed before placing orders (observation phase ends).
    /// 0.35 = observe for first 35% of period, then start trading.
    pub trading_window_start_pct: f64,

    /// Fraction of period elapsed after which no new buys are placed (wind-down).
    /// 0.60 = stop new buys after 60% elapsed. Sell/exit logic still runs.
    pub trading_window_end_pct: f64,

    /// During wind-down, allow pair-completion buys on the light side.
    pub wind_down_allow_pair_completion: bool,

    // ── Market Duration Filter ──
    /// Which market durations (in minutes) to trade. Default: [15].
    pub allowed_durations: Vec<u32>,

    // ── Pair Completion Retry Guard ──
    /// Minimum gap between pair completion attempts (seconds).
    pub pair_completion_retry_secs: u64,

    /// Maximum pair completion attempts per period.
    pub pair_completion_max_attempts: u32,

    // ── Merge at Closing ──
    /// When true (and eoa_mode + live mode), merge complete YES+NO pairs on-chain
    /// during the Closing phase before resolution. Converts pairs back to USDC,
    /// avoiding the 2% redemption fee on winning outcomes.
    pub merge_at_closing: bool,

    // ── Dynamic Rebalancing ──
    /// Allow extra budget for light-side buying when imbalanced (pair completion).
    pub rebalance_budget_override: bool,

    /// Maximum extra USDC budget for rebalance buying.
    pub rebalance_max_extra_budget: Decimal,

    /// Multiply light-side order sizes when imbalanced (1 = no multiplier).
    pub rebalance_size_multiplier: u32,

    // ── Continuous Merge ──
    /// Merge completed pairs mid-period to free USDC for more pair accumulation.
    pub continuous_merge_enabled: bool,

    /// Minimum seconds between merge attempts.
    pub merge_interval_secs: u64,

    /// Minimum complete pairs before merging.
    pub merge_min_pairs: u32,

    /// Reserve this many pairs (don't merge them) for resolution hedge.
    pub merge_reserve_pairs: u32,

    /// Minimum seconds after market discovery before placing orders.
    /// Ensures WS orderbook has connected and delivered meaningful depth.
    pub market_warmup_secs: u64,
    /// Optional override for 5-minute market warmup.
    pub market_warmup_secs_5m: Option<u64>,

    /// Maximum orders per minute before rate-limit circuit breaker trips.
    /// Skips entire quote cycle when exceeded. 0 = disabled.
    pub max_orders_per_minute: u32,

    /// Path to external kill file. If this file exists, bot triggers emergency.
    pub kill_file_path: String,

    /// Asset-local guard enabled: when recent rolling performance degrades,
    /// suppress new bids for this asset for `asset_guard_pause_secs`.
    pub asset_guard_enabled: bool,
    /// Rolling period window size for asset guard evaluation.
    pub asset_guard_window_periods: u32,
    /// Minimum acceptable rolling fill-rate before guard triggers.
    pub asset_guard_min_fill_rate: f64,
    /// Minimum acceptable rolling PnL before guard triggers.
    pub asset_guard_min_rolling_pnl: Decimal,
    /// Suppression duration after guard trigger.
    pub asset_guard_pause_secs: u64,

    /// Cancel-churn breaker enabled: reduce aggressiveness when cancel ratio spikes.
    pub churn_breaker_enabled: bool,
    /// Minimum orders in period before churn breaker can trigger.
    pub churn_breaker_min_orders: u32,
    /// Cancel ratio threshold for churn breaker activation.
    pub churn_breaker_cancel_ratio: f64,
    /// Multiply reprice threshold by this factor while churn breaker is active.
    pub churn_breaker_reprice_multiplier: u32,
    /// Keep at most this many ladder levels per side while churn breaker is active.
    pub churn_breaker_keep_levels: u32,

    /// Cooldown between emergency sell placements per market.
    pub emergency_sell_cooldown_secs: u64,

    /// Light-side combined cost guard threshold.
    /// When imbalanced, the light side (needed to complete pairs) uses this
    /// instead of $1.00. Prevents creating breakeven/losing pairs.
    pub light_side_max_combined: Decimal,

    /// Minimum profit per pair before allowing a merge.
    /// Skip merge if `1.0 - avg_combined_cost < this` to avoid realizing losses.
    pub merge_min_profit_per_pair: Decimal,

    // ── Period-Level Risk Caps (Phase 2) ──
    /// Max buy commitment per market period (gross buy fills + resting buy notional).
    pub period_gross_buy_cap_usdc: Decimal,
    /// Fraction of period treated as "early" for burst-cap enforcement.
    pub early_phase_pct: f64,
    /// Max buy commitment while elapsed_pct < early_phase_pct.
    pub early_phase_gross_buy_cap_usdc: Decimal,
    /// Hard cap on worst-case terminal loss for the period.
    pub period_worst_case_loss_cap_usdc: Decimal,
    /// Per-buy-order notional cap (maker ladders + pair-completion buys).
    pub single_order_notional_cap_usdc: Decimal,
    /// Minimum paired shares before pair-quality hysteresis activates.
    pub period_pair_quality_min_pairs: Decimal,
    /// Activate pair-quality block when avg combined cost >= this.
    pub period_pair_quality_max_combined: Decimal,
    /// Clear pair-quality block when avg combined cost <= this.
    pub period_pair_quality_resume_combined: Decimal,
    /// Minimum total shares before pair-ratio guard activates.
    pub pair_ratio_eval_min_total_shares: Decimal,
    /// After early phase, block heavy-side adds when pair ratio falls below this.
    pub period_min_pair_ratio_for_heavy_add: f64,

    // ── Phase 1: Post-Anchoring Inventory Skew ──
    /// Enable post-anchoring skew: after ask-anchoring, shift heavy-side ladder
    /// deeper to discourage accumulation on the wrong side.
    pub post_anchor_skew_enabled: bool,
    /// Shares of excess before post-anchor skew kicks in.
    pub skew_activation_threshold: Decimal,
    /// Every N excess shares = 1 additional tick of skew.
    pub shares_per_skew_tick: Decimal,
    /// Maximum ticks of skew to apply.
    pub max_skew_ticks: u32,

    // ── Phase 1: Price-Shock Fast-Path ──
    /// Price delta (dollars) to trigger fast-path wakeup for 5-min markets.
    pub price_shock_threshold_5m: f64,
    /// Price delta (dollars) to trigger fast-path wakeup for 15-min markets.
    pub price_shock_threshold_15m: f64,
    /// Price delta (dollars) to trigger fast-path wakeup for 1-hour markets.
    pub price_shock_threshold_60m: f64,
    /// Use cancel_all_orders() for speed on shock cycles (single HTTP call).
    pub price_shock_use_cancel_all: bool,

    // ── Phase 1: Duration-Aware Ladder Levels ──
    /// Ladder levels for 5-min markets (overrides ladder_levels).
    pub ladder_levels_5m: Option<u32>,
    /// Ladder levels for 15-min markets (overrides ladder_levels).
    pub ladder_levels_15m: Option<u32>,
    /// Ladder levels for 1-hour markets (overrides ladder_levels).
    pub ladder_levels_60m: Option<u32>,
    /// Max new/updated buy levels to activate per side per cycle for 5-min markets.
    pub buy_level_activation_limit_5m: Option<u32>,
    /// When enabled, 5-minute market buy ladders sit at best bid / best bid - 1 tick.
    pub best_bid_anchor_5m: bool,
    /// Toggle late-market directional size skew without recompiling.
    pub directional_skew_enabled: bool,
    /// Mild skew applies at or below this remaining-seconds threshold.
    pub directional_skew_mild_start_secs: u64,
    /// Strong skew applies at or below this remaining-seconds threshold.
    pub directional_skew_strong_start_secs: u64,
    /// Terminal skew applies at or below this remaining-seconds threshold.
    pub directional_skew_terminal_start_secs: u64,
    /// Long taker-flow lookback window.
    pub directional_skew_flow_window_secs: u64,
    /// Short taker-flow lookback window.
    pub directional_skew_short_flow_window_secs: u64,
    /// Minimum absolute spot return from market open before skew activates.
    pub directional_skew_spot_ret_threshold_bps: f64,
    /// Minimum absolute signed taker flow over the long window.
    pub directional_skew_flow_threshold_usdc: Decimal,
    /// Minimum per-trade notional for the "large flow" bucket.
    pub directional_skew_large_trade_min_usdc: Decimal,
    /// Minimum absolute signed large-trade flow over the long window.
    pub directional_skew_large_flow_threshold_usdc: Decimal,
    /// Minimum absolute signed taker flow over the short window.
    pub directional_skew_short_flow_threshold_usdc: Decimal,
    /// Terminal-stage imbalance-diff threshold.
    pub directional_skew_terminal_imbalance_diff_threshold: Decimal,
    /// Terminal-stage top-level imbalance threshold.
    pub directional_skew_terminal_best_imbalance_threshold: Decimal,
    /// Mild-stage favored-side size multiplier.
    pub directional_skew_mild_favored_multiplier: Decimal,
    /// Mild-stage unfavored-side size multiplier.
    pub directional_skew_mild_unfavored_multiplier: Decimal,
    /// Strong-stage favored-side size multiplier.
    pub directional_skew_strong_favored_multiplier: Decimal,
    /// Strong-stage unfavored-side size multiplier.
    pub directional_skew_strong_unfavored_multiplier: Decimal,
    /// Terminal-stage favored-side size multiplier.
    pub directional_skew_terminal_favored_multiplier: Decimal,
    /// Terminal-stage unfavored-side size multiplier.
    pub directional_skew_terminal_unfavored_multiplier: Decimal,
    /// In terminal skew, cancel the deepest unfavored bid if it exists.
    pub directional_skew_terminal_cancel_deepest_unfavored: bool,

    // ── VPIN Toxic Flow Detection ──
    /// Enable VPIN-based spread widening, size reduction, and toxic pullback.
    pub vpin_enabled: bool,
    /// Volume per VPIN bucket (shares).
    pub vpin_bucket_volume: f64,
    /// Number of sealed buckets in the rolling VPIN window.
    pub vpin_n_buckets: usize,
    /// VPIN level above which spread is widened (0.0–1.0).
    pub vpin_widen_threshold: f64,
    /// VPIN level above which we pull back to level-0 only.
    pub vpin_pullback_threshold: f64,
    /// Maximum spread multiplier at VPIN = 1.0.
    pub vpin_max_spread_multiplier: f64,

    // ── Avellaneda-Stoikov Volatility-Scaled Skew ──
    /// Enable A-S formula for inventory skew instead of fixed tick-based skew.
    pub as_skew_enabled: bool,
    /// Gamma parameter: risk aversion coefficient for A-S skew.
    pub as_gamma: f64,

    // ── Continuous Liquidity Tapering ──
    /// Enable PM-AMM-inspired sqrt decay of levels and sizes as settlement approaches.
    pub taper_enabled: bool,
    /// Minimum taper factor (floor, prevents going to zero).
    pub taper_min_factor: f64,

    // ── Randomized Skew Noise ──
    /// Enable random perturbation of inventory skew shift to avoid pattern detection.
    pub skew_noise_enabled: bool,
    /// Amplitude of uniform noise multiplier: shift *= (1 + uniform(-amp, +amp)).
    pub skew_noise_amplitude: f64,

    // ── Deep Discount Ladder ──
    /// Tick spacing for deep levels (beyond `deep_ladder_start_level`).
    /// First N levels use `ladder_tick_spacing` (tight, near ask), deeper levels
    /// use this wider spacing to catch panic dumps at deep discounts.
    pub deep_ladder_tick_spacing: u32,
    /// Level index where spacing switches from tight to deep.
    /// Levels 0..start use `ladder_tick_spacing`, levels start+ use `deep_ladder_tick_spacing`.
    pub deep_ladder_start_level: u32,

    // ── Sell Unmatched at Period End ──
    /// Enable selling unmatched (excess) shares at period end before resolution.
    /// Places FOK sell at best bid for the heavy side to convert one-leg risk
    /// into partial recovery instead of holding to expiry.
    pub sell_unmatched_enabled: bool,
    /// Minimum excess shares to trigger unmatched sell at period end.
    pub sell_unmatched_min_excess: Decimal,
    /// Maximum loss per share (below avg cost) to accept on unmatched sell.
    /// E.g. 0.20 = sell at avg_cost - 20¢ if that's where the bid is.
    pub sell_unmatched_max_loss: Decimal,

    // ── Static Deep Grid ──
    /// Enable static deep grid: fixed-price resting bids at $0.01-$0.15 on both sides.
    /// Independent of ask-anchored ladder, NOT cancelled on FV moves.
    pub deep_static_grid_enabled: bool,
    /// Fixed price levels for the static deep grid (e.g., [0.01, 0.02, ..., 0.15]).
    pub deep_static_levels: Vec<Decimal>,
    /// Share size per deep grid level at prices <= $0.05.
    pub deep_static_size_below_05: Decimal,
    /// Share size per deep grid level at prices > $0.05.
    pub deep_static_size_above_05: Decimal,
    /// Combined cost guard for deep grid (relaxed, default $1.00).
    pub deep_static_max_combined: Decimal,

    // ── Cancel Protection for Deep Levels ──
    /// FV-stale cancel exemption: don't cancel resting orders below this price.
    /// Protects deep ask-anchored levels from aggressive FV-shift cancels.
    pub fv_cancel_min_price: Decimal,
    /// Stale distance (ticks) for deep levels — wider than the default stale_distance_ticks.
    pub deep_level_stale_distance: u32,

    // ── 5-Minute Market Feature Flag ──
    /// When true, add 5-min markets to allowed_durations.
    pub enable_5m_markets: bool,
}

impl Default for V2Config {
    fn default() -> Self {
        Self {
            target_combined: dec!(0.93), // 7% profit per pair (was 4% — too thin, adverse selection)
            min_bid: dec!(0.04),         // minimum bid on either side
            imbalance_skew_per_share: dec!(0.005), // 0.5c skew per excess share
            min_vol_per_sec: 0.000_01,   // ~1 bps/sec minimum
            vol_window_secs: 120,        // 2-minute vol window
            quote_refresh_ms: 500,       // 500ms refresh
            ladder_levels: 5,            // 5 price levels per side (was 15 — too aggressive)
            ladder_tick_spacing: 1,      // 1 tick between levels
            level_order_size: dec!(15),  // 15 shares per level (gabagool sizing)
            ladder_size_decay: 0.10,     // 10% decay per level (level 0 = 100%, level 1 = 90%, ...)
            max_per_order_combined: dec!(0.99), // per-order combined cost guard ($0.01 gross profit per pair, no maker/taker fees)
            stale_distance_ticks: 5,            // cancel if >5 ticks from ladder
            fv_stale_cancel_cents: dec!(0.08), // cancel all on side if top resting > ladder top + 8c
            max_abs_imbalance: dec!(15),       // hard block (USDC cost imbalance)
            soft_imbalance_threshold: dec!(8), // begin size reduction (USDC)
            // EV circuit breaker
            ev_circuit_breaker_enabled: true,
            ev_stop_buying_ratio: dec!(1), // stop when |excess_ev| > locked
            ev_min_excess_threshold: dec!(3), // ignore EV breaker below $3 (startup noise)
            ev_early_period_multiplier: dec!(3), // 3x thresholds early in period
            ev_early_period_end_pct: 0.50, // first 50% of period is "early"
            // FV dead-zone
            fv_dead_threshold: 0.10,            // 10% FV threshold
            min_bid_absolute_floor: dec!(0.02), // never bid below 2c
            min_bid_fv_ratio: 0.5,              // min_bid = fv * 0.5
            // Sell-back
            sellback_edge: dec!(0.01),    // 1c above avg cost
            sellback_min_excess: dec!(5), // activate at 5 excess
            sell_level_size: dec!(10),    // 10 shares per sell level
            sell_levels: 3,               // 3 price levels
            // Exit escalation policy (USDC cost excess thresholds)
            exit_soft_excess: dec!(25),
            exit_hard_excess: dec!(40),
            exit_taker_after_secs: 20,
            exit_force_taker_remaining_secs: 240,
            ev_log_cooldown_secs: 5,
            // Time-decaying imbalance (USDC floors)
            imbalance_decay_floor_abs: dec!(5), // floor near expiry (USDC)
            imbalance_decay_floor_soft: dec!(2), // floor near expiry (USDC)
            very_late_phase_secs: 300,          // 5 min before end (was 180)
            // Anti-oscillation
            postonly_regen_buffer_ticks: 1, // 1 tick below ask (top of queue)
            sell_buy_cooldown_secs: 10,     // 10s cooldown after selling
            // Sell-back grace period
            sellback_grace_period_secs: 300, // suppress sell-back for first 5 min
            sellback_protect_locked_profit: true, // skip sell-back if it reduces LP
            sellback_max_loss_cents: dec!(0.02), // accept up to 2c/share loss on sell-back
            // Trending market detection
            trend_window_secs: 30,         // 30-second rolling window
            trend_threshold_dollars: 75.0, // suppress losing side if BTC moves $75+
            trend_threshold_5m: None,      // Phase 1: use trend_threshold_dollars as fallback
            trend_threshold_15m: None,     // Phase 1: use trend_threshold_dollars as fallback
            trend_threshold_60m: None,     // 1-hour: use trend_threshold_dollars as fallback
            // Sigma blending
            sigma_blend_alpha: 0.4, // 40% 1-min vol + 60% full-window vol
            // Volatility circuit breaker
            max_sigma: 0.00012,           // suppress new orders above this vol
            max_sigma_resume_factor: 0.8, // resume at max_sigma * 0.8
            // Fee-aware pair completion
            pair_fee_buffer: dec!(0.03), // 3c fee buffer
            // Ladder churn reduction
            ladder_reprice_threshold: dec!(0.03), // 3c minimum center movement
            // One-sided guard (USDC cost threshold)
            one_sided_threshold: dec!(10), // $10 USDC cost on one side with zero on other
            // Hybrid FV
            fv_book_blend_weight: 0.0, // disabled by default (pure BS)
            // Sigma warm-up
            min_sigma_samples: 20, // 20 samples minimum
            // Late-entry guard
            min_period_remaining_pct: 0.75, // skip if <75% of period remaining
            // Trading window
            trading_window_start_pct: 0.35, // observe for first 35%, then trade
            trading_window_end_pct: 0.60,   // stop new buys after 60% elapsed
            wind_down_allow_pair_completion: true, // allow pair-completion during wind-down
            // Market duration filter
            allowed_durations: vec![15], // only 15-min markets by default
            // Pair-completion retry guard
            pair_completion_retry_secs: 5,
            pair_completion_max_attempts: 8,
            // Merge at closing
            merge_at_closing: false,
            // Dynamic rebalancing
            rebalance_budget_override: false,
            rebalance_max_extra_budget: dec!(25),
            rebalance_size_multiplier: 1,
            // Continuous merge
            continuous_merge_enabled: false,
            merge_interval_secs: 30,
            merge_min_pairs: 10,
            merge_reserve_pairs: 0,
            market_warmup_secs: 3,
            market_warmup_secs_5m: None,
            max_orders_per_minute: 60,
            kill_file_path: "/tmp/gabagool_kill".to_string(),
            asset_guard_enabled: true,
            asset_guard_window_periods: 6,
            asset_guard_min_fill_rate: 0.45,
            asset_guard_min_rolling_pnl: Decimal::ZERO,
            asset_guard_pause_secs: 900,
            churn_breaker_enabled: true,
            churn_breaker_min_orders: 25,
            churn_breaker_cancel_ratio: 0.55,
            churn_breaker_reprice_multiplier: 3,
            churn_breaker_keep_levels: 2,
            emergency_sell_cooldown_secs: 8,
            light_side_max_combined: dec!(0.99), // light side: max combined cost $0.99 → 1c/pair minimum profit
            merge_min_profit_per_pair: Decimal::ZERO, // break-even floor: never merge at a loss
            period_gross_buy_cap_usdc: dec!(80),
            early_phase_pct: 0.20,
            early_phase_gross_buy_cap_usdc: dec!(30),
            period_worst_case_loss_cap_usdc: dec!(12),
            single_order_notional_cap_usdc: dec!(12.5),
            period_pair_quality_min_pairs: dec!(20),
            period_pair_quality_max_combined: dec!(1.00),
            period_pair_quality_resume_combined: dec!(0.98),
            pair_ratio_eval_min_total_shares: dec!(60),
            period_min_pair_ratio_for_heavy_add: 0.35,
            // Phase 1: Post-anchoring skew
            post_anchor_skew_enabled: false,
            skew_activation_threshold: dec!(15),
            shares_per_skew_tick: dec!(5),
            max_skew_ticks: 15,
            // Phase 1: Price-shock fast-path
            price_shock_threshold_5m: 15.0,
            price_shock_threshold_15m: 40.0,
            price_shock_threshold_60m: 100.0,
            price_shock_use_cancel_all: true,
            // Phase 1: Duration-aware ladder levels
            ladder_levels_5m: None,
            ladder_levels_15m: None,
            ladder_levels_60m: None,
            buy_level_activation_limit_5m: None,
            best_bid_anchor_5m: false,
            directional_skew_enabled: false,
            directional_skew_mild_start_secs: 60,
            directional_skew_strong_start_secs: 30,
            directional_skew_terminal_start_secs: 15,
            directional_skew_flow_window_secs: 60,
            directional_skew_short_flow_window_secs: 15,
            directional_skew_spot_ret_threshold_bps: 6.0,
            directional_skew_flow_threshold_usdc: dec!(4000),
            directional_skew_large_trade_min_usdc: dec!(50),
            directional_skew_large_flow_threshold_usdc: dec!(3000),
            directional_skew_short_flow_threshold_usdc: dec!(1100),
            directional_skew_terminal_imbalance_diff_threshold: dec!(1.5),
            directional_skew_terminal_best_imbalance_threshold: dec!(0.75),
            directional_skew_mild_favored_multiplier: dec!(1.25),
            directional_skew_mild_unfavored_multiplier: dec!(0.75),
            directional_skew_strong_favored_multiplier: dec!(1.50),
            directional_skew_strong_unfavored_multiplier: dec!(0.50),
            directional_skew_terminal_favored_multiplier: dec!(1.75),
            directional_skew_terminal_unfavored_multiplier: dec!(0.25),
            directional_skew_terminal_cancel_deepest_unfavored: true,
            // VPIN toxic flow detection (disabled by default)
            vpin_enabled: false,
            vpin_bucket_volume: 100.0,
            vpin_n_buckets: 50,
            vpin_widen_threshold: 0.50,
            vpin_pullback_threshold: 0.70,
            vpin_max_spread_multiplier: 3.0,
            // Avellaneda-Stoikov skew (disabled by default)
            as_skew_enabled: false,
            as_gamma: 0.1,
            // Continuous liquidity tapering (disabled by default)
            taper_enabled: false,
            taper_min_factor: 0.10,
            // Randomized skew noise (disabled by default)
            skew_noise_enabled: false,
            skew_noise_amplitude: 0.3,
            // Deep discount ladder
            deep_ladder_tick_spacing: 3, // 3 ticks (3¢) between deep levels
            deep_ladder_start_level: 3,  // first 3 levels tight, then wide
            // Sell unmatched
            sell_unmatched_enabled: false, // safe default: disabled
            sell_unmatched_min_excess: dec!(5),
            sell_unmatched_max_loss: dec!(0.20), // accept up to 20¢/share loss
            // Static deep grid
            deep_static_grid_enabled: false, // safe default: disabled
            deep_static_levels: vec![
                dec!(0.01),
                dec!(0.02),
                dec!(0.03),
                dec!(0.04),
                dec!(0.05),
                dec!(0.06),
                dec!(0.08),
                dec!(0.10),
                dec!(0.12),
                dec!(0.15),
            ],
            deep_static_size_below_05: dec!(10),
            deep_static_size_above_05: dec!(5),
            deep_static_max_combined: dec!(1.00),
            // Cancel protection for deep levels
            fv_cancel_min_price: dec!(0.15),
            deep_level_stale_distance: 15,
            // 5m market flag
            enable_5m_markets: false,
        }
    }
}

impl V2Config {
    /// Create V2Config from the raw TOML [v2] section, falling back to defaults.
    pub fn from_raw(raw: &crate::config::V2RawConfig) -> Self {
        let def = Self::default();
        Self {
            target_combined: raw
                .target_combined
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.target_combined),
            min_bid: raw
                .min_bid
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.min_bid),
            imbalance_skew_per_share: raw
                .imbalance_skew_per_share
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.imbalance_skew_per_share),
            min_vol_per_sec: raw
                .min_vol_per_sec
                .as_deref()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(def.min_vol_per_sec),
            vol_window_secs: raw.vol_window_secs.unwrap_or(def.vol_window_secs),
            quote_refresh_ms: raw.quote_refresh_ms.unwrap_or(def.quote_refresh_ms),
            level_order_size: raw
                .base_order_shares
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.level_order_size),
            ladder_levels: raw.ladder_levels.unwrap_or(def.ladder_levels),
            ladder_size_decay: raw
                .ladder_size_decay
                .as_deref()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(def.ladder_size_decay),
            max_per_order_combined: raw
                .max_combined_avg_cost
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.max_per_order_combined),
            // EV circuit breaker
            ev_circuit_breaker_enabled: raw
                .ev_circuit_breaker_enabled
                .unwrap_or(def.ev_circuit_breaker_enabled),
            ev_stop_buying_ratio: raw
                .ev_stop_buying_ratio
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.ev_stop_buying_ratio),
            ev_min_excess_threshold: raw
                .ev_min_excess_threshold
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.ev_min_excess_threshold),
            ev_early_period_multiplier: raw
                .ev_early_period_multiplier
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.ev_early_period_multiplier),
            ev_early_period_end_pct: raw
                .ev_early_period_end_pct
                .unwrap_or(def.ev_early_period_end_pct),
            // FV dead-zone
            fv_dead_threshold: raw
                .fv_dead_threshold
                .as_deref()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(def.fv_dead_threshold),
            min_bid_absolute_floor: raw
                .min_bid_absolute_floor
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.min_bid_absolute_floor),
            min_bid_fv_ratio: raw
                .min_bid_fv_ratio
                .as_deref()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(def.min_bid_fv_ratio),
            // Sell-back
            sellback_edge: raw
                .sellback_edge
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.sellback_edge),
            sellback_min_excess: raw
                .sellback_min_excess
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.sellback_min_excess),
            sell_level_size: raw
                .sell_level_size
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.sell_level_size),
            sell_levels: raw.sell_levels.unwrap_or(def.sell_levels),
            // Time-decaying imbalance
            imbalance_decay_floor_abs: raw
                .imbalance_decay_floor_abs
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.imbalance_decay_floor_abs),
            imbalance_decay_floor_soft: raw
                .imbalance_decay_floor_soft
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.imbalance_decay_floor_soft),
            very_late_phase_secs: raw.very_late_phase_secs.unwrap_or(def.very_late_phase_secs),
            // Anti-oscillation
            postonly_regen_buffer_ticks: raw
                .postonly_regen_buffer_ticks
                .unwrap_or(def.postonly_regen_buffer_ticks),
            sell_buy_cooldown_secs: raw
                .sell_buy_cooldown_secs
                .unwrap_or(def.sell_buy_cooldown_secs),
            // Sell-back grace period
            sellback_grace_period_secs: raw
                .sellback_grace_period_secs
                .unwrap_or(def.sellback_grace_period_secs),
            sellback_protect_locked_profit: raw
                .sellback_protect_locked_profit
                .unwrap_or(def.sellback_protect_locked_profit),
            sellback_max_loss_cents: raw
                .sellback_max_loss_cents
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.sellback_max_loss_cents),
            // Exit escalation policy
            exit_soft_excess: raw
                .exit_soft_excess
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.exit_soft_excess),
            exit_hard_excess: raw
                .exit_hard_excess
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.exit_hard_excess),
            exit_taker_after_secs: raw
                .exit_taker_after_secs
                .unwrap_or(def.exit_taker_after_secs),
            exit_force_taker_remaining_secs: raw
                .exit_force_taker_remaining_secs
                .unwrap_or(def.exit_force_taker_remaining_secs),
            ev_log_cooldown_secs: raw.ev_log_cooldown_secs.unwrap_or(def.ev_log_cooldown_secs),
            // Trending market detection
            trend_window_secs: raw.trend_window_secs.unwrap_or(def.trend_window_secs),
            trend_threshold_dollars: raw
                .trend_threshold_dollars
                .unwrap_or(def.trend_threshold_dollars),
            trend_threshold_5m: raw.trend_threshold_5m.or(def.trend_threshold_5m),
            trend_threshold_15m: raw.trend_threshold_15m.or(def.trend_threshold_15m),
            trend_threshold_60m: raw.trend_threshold_60m.or(def.trend_threshold_60m),
            // FV-stale cancel
            fv_stale_cancel_cents: raw
                .fv_stale_cancel_cents
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.fv_stale_cancel_cents),
            // Sigma blending
            sigma_blend_alpha: raw
                .sigma_blend_alpha
                .as_deref()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(def.sigma_blend_alpha),
            // Volatility circuit breaker
            max_sigma: raw.max_sigma.unwrap_or(def.max_sigma),
            max_sigma_resume_factor: raw
                .max_sigma_resume_factor
                .unwrap_or(def.max_sigma_resume_factor),
            // Fee-aware pair completion
            pair_fee_buffer: raw
                .pair_fee_buffer
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.pair_fee_buffer),
            // Ladder churn reduction
            ladder_reprice_threshold: raw
                .ladder_reprice_threshold
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.ladder_reprice_threshold),
            // One-sided guard
            one_sided_threshold: raw
                .one_sided_threshold
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.one_sided_threshold),
            // Hybrid FV
            fv_book_blend_weight: raw
                .fv_book_blend_weight
                .as_deref()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(def.fv_book_blend_weight),
            // Sigma warm-up
            min_sigma_samples: raw.min_sigma_samples.unwrap_or(def.min_sigma_samples),
            // Late-entry guard
            min_period_remaining_pct: raw
                .min_period_remaining_pct
                .unwrap_or(def.min_period_remaining_pct),
            // Trading window
            trading_window_start_pct: raw
                .trading_window_start_pct
                .unwrap_or(def.trading_window_start_pct),
            trading_window_end_pct: raw
                .trading_window_end_pct
                .unwrap_or(def.trading_window_end_pct),
            wind_down_allow_pair_completion: raw
                .wind_down_allow_pair_completion
                .unwrap_or(def.wind_down_allow_pair_completion),
            // Market duration filter
            allowed_durations: raw
                .allowed_durations
                .clone()
                .unwrap_or(def.allowed_durations.clone()),
            // Pair-completion retry guard
            pair_completion_retry_secs: raw
                .pair_completion_retry_secs
                .unwrap_or(def.pair_completion_retry_secs),
            pair_completion_max_attempts: raw
                .pair_completion_max_attempts
                .unwrap_or(def.pair_completion_max_attempts),
            // Imbalance limits (USDC cost-weighted, map from config max_share_imbalance)
            max_abs_imbalance: raw
                .max_share_imbalance
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.max_abs_imbalance),
            soft_imbalance_threshold: raw
                .soft_imbalance_threshold
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.soft_imbalance_threshold),
            // Merge at closing
            merge_at_closing: raw.merge_at_closing.unwrap_or(def.merge_at_closing),
            // Dynamic rebalancing
            rebalance_budget_override: raw
                .rebalance_budget_override
                .unwrap_or(def.rebalance_budget_override),
            rebalance_max_extra_budget: raw
                .rebalance_max_extra_budget
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.rebalance_max_extra_budget),
            rebalance_size_multiplier: raw
                .rebalance_size_multiplier
                .unwrap_or(def.rebalance_size_multiplier),
            // Continuous merge
            continuous_merge_enabled: raw
                .continuous_merge_enabled
                .unwrap_or(def.continuous_merge_enabled),
            merge_interval_secs: raw.merge_interval_secs.unwrap_or(def.merge_interval_secs),
            merge_min_pairs: raw.merge_min_pairs.unwrap_or(def.merge_min_pairs),
            merge_reserve_pairs: raw.merge_reserve_pairs.unwrap_or(def.merge_reserve_pairs),
            market_warmup_secs: raw.market_warmup_secs.unwrap_or(def.market_warmup_secs),
            market_warmup_secs_5m: raw.market_warmup_secs_5m.or(def.market_warmup_secs_5m),
            max_orders_per_minute: raw
                .max_orders_per_minute
                .unwrap_or(def.max_orders_per_minute),
            kill_file_path: raw
                .kill_file_path
                .clone()
                .unwrap_or(def.kill_file_path.clone()),
            asset_guard_enabled: raw.asset_guard_enabled.unwrap_or(def.asset_guard_enabled),
            asset_guard_window_periods: raw
                .asset_guard_window_periods
                .unwrap_or(def.asset_guard_window_periods),
            asset_guard_min_fill_rate: raw
                .asset_guard_min_fill_rate
                .unwrap_or(def.asset_guard_min_fill_rate),
            asset_guard_min_rolling_pnl: raw
                .asset_guard_min_rolling_pnl
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.asset_guard_min_rolling_pnl),
            asset_guard_pause_secs: raw
                .asset_guard_pause_secs
                .unwrap_or(def.asset_guard_pause_secs),
            churn_breaker_enabled: raw
                .churn_breaker_enabled
                .unwrap_or(def.churn_breaker_enabled),
            churn_breaker_min_orders: raw
                .churn_breaker_min_orders
                .unwrap_or(def.churn_breaker_min_orders),
            churn_breaker_cancel_ratio: raw
                .churn_breaker_cancel_ratio
                .unwrap_or(def.churn_breaker_cancel_ratio),
            churn_breaker_reprice_multiplier: raw
                .churn_breaker_reprice_multiplier
                .unwrap_or(def.churn_breaker_reprice_multiplier),
            churn_breaker_keep_levels: raw
                .churn_breaker_keep_levels
                .unwrap_or(def.churn_breaker_keep_levels),
            emergency_sell_cooldown_secs: raw
                .emergency_sell_cooldown_secs
                .unwrap_or(def.emergency_sell_cooldown_secs),
            light_side_max_combined: raw
                .light_side_max_combined
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.light_side_max_combined),
            merge_min_profit_per_pair: raw
                .merge_min_profit_per_pair
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.merge_min_profit_per_pair),
            period_gross_buy_cap_usdc: raw
                .period_gross_buy_cap_usdc
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.period_gross_buy_cap_usdc),
            early_phase_pct: raw.early_phase_pct.unwrap_or(def.early_phase_pct),
            early_phase_gross_buy_cap_usdc: raw
                .early_phase_gross_buy_cap_usdc
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.early_phase_gross_buy_cap_usdc),
            period_worst_case_loss_cap_usdc: raw
                .period_worst_case_loss_cap_usdc
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.period_worst_case_loss_cap_usdc),
            single_order_notional_cap_usdc: raw
                .single_order_notional_cap_usdc
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.single_order_notional_cap_usdc),
            period_pair_quality_min_pairs: raw
                .period_pair_quality_min_pairs
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.period_pair_quality_min_pairs),
            period_pair_quality_max_combined: raw
                .period_pair_quality_max_combined
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.period_pair_quality_max_combined),
            period_pair_quality_resume_combined: raw
                .period_pair_quality_resume_combined
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.period_pair_quality_resume_combined),
            pair_ratio_eval_min_total_shares: raw
                .pair_ratio_eval_min_total_shares
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.pair_ratio_eval_min_total_shares),
            period_min_pair_ratio_for_heavy_add: raw
                .period_min_pair_ratio_for_heavy_add
                .unwrap_or(def.period_min_pair_ratio_for_heavy_add),
            // Phase 1: Post-anchoring skew
            post_anchor_skew_enabled: raw
                .post_anchor_skew_enabled
                .unwrap_or(def.post_anchor_skew_enabled),
            skew_activation_threshold: raw
                .skew_activation_threshold
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.skew_activation_threshold),
            shares_per_skew_tick: raw
                .shares_per_skew_tick
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.shares_per_skew_tick),
            max_skew_ticks: raw.max_skew_ticks.unwrap_or(def.max_skew_ticks),
            // Phase 1: Price-shock fast-path
            price_shock_threshold_5m: raw
                .price_shock_threshold_5m
                .unwrap_or(def.price_shock_threshold_5m),
            price_shock_threshold_15m: raw
                .price_shock_threshold_15m
                .unwrap_or(def.price_shock_threshold_15m),
            price_shock_threshold_60m: raw
                .price_shock_threshold_60m
                .unwrap_or(def.price_shock_threshold_60m),
            price_shock_use_cancel_all: raw
                .price_shock_use_cancel_all
                .unwrap_or(def.price_shock_use_cancel_all),
            // Phase 1: Duration-aware ladder levels
            ladder_levels_5m: raw.ladder_levels_5m.or(def.ladder_levels_5m),
            ladder_levels_15m: raw.ladder_levels_15m.or(def.ladder_levels_15m),
            ladder_levels_60m: raw.ladder_levels_60m.or(def.ladder_levels_60m),
            buy_level_activation_limit_5m: raw
                .buy_level_activation_limit_5m
                .or(def.buy_level_activation_limit_5m),
            best_bid_anchor_5m: raw.best_bid_anchor_5m.unwrap_or(def.best_bid_anchor_5m),
            directional_skew_enabled: raw
                .directional_skew_enabled
                .unwrap_or(def.directional_skew_enabled),
            directional_skew_mild_start_secs: raw
                .directional_skew_mild_start_secs
                .unwrap_or(def.directional_skew_mild_start_secs),
            directional_skew_strong_start_secs: raw
                .directional_skew_strong_start_secs
                .unwrap_or(def.directional_skew_strong_start_secs),
            directional_skew_terminal_start_secs: raw
                .directional_skew_terminal_start_secs
                .unwrap_or(def.directional_skew_terminal_start_secs),
            directional_skew_flow_window_secs: raw
                .directional_skew_flow_window_secs
                .unwrap_or(def.directional_skew_flow_window_secs),
            directional_skew_short_flow_window_secs: raw
                .directional_skew_short_flow_window_secs
                .unwrap_or(def.directional_skew_short_flow_window_secs),
            directional_skew_spot_ret_threshold_bps: raw
                .directional_skew_spot_ret_threshold_bps
                .unwrap_or(def.directional_skew_spot_ret_threshold_bps),
            directional_skew_flow_threshold_usdc: raw
                .directional_skew_flow_threshold_usdc
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.directional_skew_flow_threshold_usdc),
            directional_skew_large_trade_min_usdc: raw
                .directional_skew_large_trade_min_usdc
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.directional_skew_large_trade_min_usdc),
            directional_skew_large_flow_threshold_usdc: raw
                .directional_skew_large_flow_threshold_usdc
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.directional_skew_large_flow_threshold_usdc),
            directional_skew_short_flow_threshold_usdc: raw
                .directional_skew_short_flow_threshold_usdc
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.directional_skew_short_flow_threshold_usdc),
            directional_skew_terminal_imbalance_diff_threshold: raw
                .directional_skew_terminal_imbalance_diff_threshold
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.directional_skew_terminal_imbalance_diff_threshold),
            directional_skew_terminal_best_imbalance_threshold: raw
                .directional_skew_terminal_best_imbalance_threshold
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.directional_skew_terminal_best_imbalance_threshold),
            directional_skew_mild_favored_multiplier: raw
                .directional_skew_mild_favored_multiplier
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.directional_skew_mild_favored_multiplier),
            directional_skew_mild_unfavored_multiplier: raw
                .directional_skew_mild_unfavored_multiplier
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.directional_skew_mild_unfavored_multiplier),
            directional_skew_strong_favored_multiplier: raw
                .directional_skew_strong_favored_multiplier
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.directional_skew_strong_favored_multiplier),
            directional_skew_strong_unfavored_multiplier: raw
                .directional_skew_strong_unfavored_multiplier
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.directional_skew_strong_unfavored_multiplier),
            directional_skew_terminal_favored_multiplier: raw
                .directional_skew_terminal_favored_multiplier
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.directional_skew_terminal_favored_multiplier),
            directional_skew_terminal_unfavored_multiplier: raw
                .directional_skew_terminal_unfavored_multiplier
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.directional_skew_terminal_unfavored_multiplier),
            directional_skew_terminal_cancel_deepest_unfavored: raw
                .directional_skew_terminal_cancel_deepest_unfavored
                .unwrap_or(def.directional_skew_terminal_cancel_deepest_unfavored),
            // VPIN toxic flow detection
            vpin_enabled: raw.vpin_enabled.unwrap_or(def.vpin_enabled),
            vpin_bucket_volume: raw.vpin_bucket_volume.unwrap_or(def.vpin_bucket_volume),
            vpin_n_buckets: raw.vpin_n_buckets.unwrap_or(def.vpin_n_buckets),
            vpin_widen_threshold: raw.vpin_widen_threshold.unwrap_or(def.vpin_widen_threshold),
            vpin_pullback_threshold: raw
                .vpin_pullback_threshold
                .unwrap_or(def.vpin_pullback_threshold),
            vpin_max_spread_multiplier: raw
                .vpin_max_spread_multiplier
                .unwrap_or(def.vpin_max_spread_multiplier),
            // Avellaneda-Stoikov skew
            as_skew_enabled: raw.as_skew_enabled.unwrap_or(def.as_skew_enabled),
            as_gamma: raw.as_gamma.unwrap_or(def.as_gamma),
            // Continuous liquidity tapering
            taper_enabled: raw.taper_enabled.unwrap_or(def.taper_enabled),
            taper_min_factor: raw.taper_min_factor.unwrap_or(def.taper_min_factor),
            // Randomized skew noise
            skew_noise_enabled: raw.skew_noise_enabled.unwrap_or(def.skew_noise_enabled),
            skew_noise_amplitude: raw.skew_noise_amplitude.unwrap_or(def.skew_noise_amplitude),
            // Deep discount ladder
            deep_ladder_tick_spacing: raw
                .deep_ladder_tick_spacing
                .unwrap_or(def.deep_ladder_tick_spacing),
            deep_ladder_start_level: raw
                .deep_ladder_start_level
                .unwrap_or(def.deep_ladder_start_level),
            // Sell unmatched
            sell_unmatched_enabled: raw
                .sell_unmatched_enabled
                .unwrap_or(def.sell_unmatched_enabled),
            sell_unmatched_min_excess: raw
                .sell_unmatched_min_excess
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.sell_unmatched_min_excess),
            sell_unmatched_max_loss: raw
                .sell_unmatched_max_loss
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.sell_unmatched_max_loss),
            // Static deep grid
            deep_static_grid_enabled: raw
                .deep_static_grid_enabled
                .unwrap_or(def.deep_static_grid_enabled),
            deep_static_levels: raw
                .deep_static_levels
                .as_ref()
                .map(|v| v.iter().filter_map(|f| Decimal::from_f64(*f)).collect())
                .unwrap_or_else(|| def.deep_static_levels.clone()),
            deep_static_size_below_05: raw
                .deep_static_size_below_05
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.deep_static_size_below_05),
            deep_static_size_above_05: raw
                .deep_static_size_above_05
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.deep_static_size_above_05),
            deep_static_max_combined: raw
                .deep_static_max_combined
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.deep_static_max_combined),
            // Cancel protection
            fv_cancel_min_price: raw
                .fv_cancel_min_price
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(def.fv_cancel_min_price),
            deep_level_stale_distance: raw
                .deep_level_stale_distance
                .unwrap_or(def.deep_level_stale_distance),
            // 5m market flag
            enable_5m_markets: raw.enable_5m_markets.unwrap_or(def.enable_5m_markets),
            ..def
        }
        .normalized()
    }

    fn normalized(mut self) -> Self {
        let defaults = V2Config::default();
        self.early_phase_pct = self.early_phase_pct.clamp(0.0, 1.0);
        self.trading_window_start_pct = self.trading_window_start_pct.clamp(0.0, 1.0);
        self.trading_window_end_pct = self.trading_window_end_pct.clamp(0.0, 1.0);
        if self.trading_window_end_pct < self.trading_window_start_pct {
            self.trading_window_end_pct = self.trading_window_start_pct;
        }
        self.period_min_pair_ratio_for_heavy_add =
            self.period_min_pair_ratio_for_heavy_add.clamp(0.0, 1.0);
        if self.period_pair_quality_resume_combined > self.period_pair_quality_max_combined {
            self.period_pair_quality_resume_combined = self.period_pair_quality_max_combined;
        }
        if self.allowed_durations.is_empty() {
            self.allowed_durations = vec![15];
        }
        if self.enable_5m_markets && !self.allowed_durations.contains(&5) {
            self.allowed_durations.push(5);
        }
        if matches!(self.buy_level_activation_limit_5m, Some(0)) {
            self.buy_level_activation_limit_5m = None;
        }
        if self.directional_skew_flow_window_secs == 0 {
            self.directional_skew_flow_window_secs = 60;
        }
        if self.directional_skew_short_flow_window_secs == 0 {
            self.directional_skew_short_flow_window_secs = 15;
        }
        if self.directional_skew_short_flow_window_secs > self.directional_skew_flow_window_secs {
            self.directional_skew_short_flow_window_secs = self.directional_skew_flow_window_secs;
        }
        self.directional_skew_mild_start_secs = self.directional_skew_mild_start_secs.max(1);
        self.directional_skew_strong_start_secs = self
            .directional_skew_strong_start_secs
            .min(self.directional_skew_mild_start_secs)
            .max(1);
        self.directional_skew_terminal_start_secs = self
            .directional_skew_terminal_start_secs
            .min(self.directional_skew_strong_start_secs)
            .max(1);
        self.directional_skew_spot_ret_threshold_bps =
            self.directional_skew_spot_ret_threshold_bps.max(0.0);
        if self.directional_skew_flow_threshold_usdc <= Decimal::ZERO {
            self.directional_skew_flow_threshold_usdc =
                defaults.directional_skew_flow_threshold_usdc;
        }
        if self.directional_skew_large_trade_min_usdc <= Decimal::ZERO {
            self.directional_skew_large_trade_min_usdc =
                defaults.directional_skew_large_trade_min_usdc;
        }
        if self.directional_skew_large_flow_threshold_usdc <= Decimal::ZERO {
            self.directional_skew_large_flow_threshold_usdc =
                defaults.directional_skew_large_flow_threshold_usdc;
        }
        if self.directional_skew_short_flow_threshold_usdc <= Decimal::ZERO {
            self.directional_skew_short_flow_threshold_usdc =
                defaults.directional_skew_short_flow_threshold_usdc;
        }
        if self.directional_skew_terminal_imbalance_diff_threshold <= Decimal::ZERO {
            self.directional_skew_terminal_imbalance_diff_threshold =
                defaults.directional_skew_terminal_imbalance_diff_threshold;
        }
        if self.directional_skew_terminal_best_imbalance_threshold <= Decimal::ZERO {
            self.directional_skew_terminal_best_imbalance_threshold =
                defaults.directional_skew_terminal_best_imbalance_threshold;
        }
        if self.directional_skew_mild_favored_multiplier <= Decimal::ZERO {
            self.directional_skew_mild_favored_multiplier =
                defaults.directional_skew_mild_favored_multiplier;
        }
        if self.directional_skew_mild_unfavored_multiplier <= Decimal::ZERO {
            self.directional_skew_mild_unfavored_multiplier =
                defaults.directional_skew_mild_unfavored_multiplier;
        }
        if self.directional_skew_strong_favored_multiplier <= Decimal::ZERO {
            self.directional_skew_strong_favored_multiplier =
                defaults.directional_skew_strong_favored_multiplier;
        }
        if self.directional_skew_strong_unfavored_multiplier <= Decimal::ZERO {
            self.directional_skew_strong_unfavored_multiplier =
                defaults.directional_skew_strong_unfavored_multiplier;
        }
        if self.directional_skew_terminal_favored_multiplier <= Decimal::ZERO {
            self.directional_skew_terminal_favored_multiplier =
                defaults.directional_skew_terminal_favored_multiplier;
        }
        if self.directional_skew_terminal_unfavored_multiplier <= Decimal::ZERO {
            self.directional_skew_terminal_unfavored_multiplier =
                defaults.directional_skew_terminal_unfavored_multiplier;
        }
        self
    }

    /// Resolve ladder levels for a given market duration (minutes).
    /// Uses duration-aware overrides if set, otherwise falls back to `ladder_levels`.
    pub fn ladder_levels_for_duration(&self, duration_mins: u32) -> u32 {
        match duration_mins {
            0..=7 => self.ladder_levels_5m.unwrap_or(self.ladder_levels),
            8..=30 => self.ladder_levels_15m.unwrap_or(self.ladder_levels),
            _ => self.ladder_levels_60m.unwrap_or(self.ladder_levels),
        }
    }

    pub fn buy_level_activation_limit_for_duration(&self, duration_mins: u32) -> Option<usize> {
        match duration_mins {
            0..=7 => self.buy_level_activation_limit_5m.map(|v| v as usize),
            _ => None,
        }
    }

    /// Duration-aware market warmup: shorter for 5m markets.
    pub fn market_warmup_secs_for_duration(&self, duration_mins: u32) -> u64 {
        if duration_mins <= 7 {
            if let Some(warmup) = self.market_warmup_secs_5m {
                warmup
            } else if self.enable_5m_markets {
                2
            } else {
                self.market_warmup_secs
            }
        } else {
            self.market_warmup_secs
        }
    }

    /// Duration-aware sigma samples: fewer for 5m markets (less time to waste).
    pub fn min_sigma_samples_for_duration(&self, duration_mins: u32) -> u32 {
        if duration_mins <= 7 && self.enable_5m_markets {
            10
        } else {
            self.min_sigma_samples
        }
    }

    /// Size for a static deep grid level based on its price.
    pub fn deep_static_size_at_price(&self, price: Decimal) -> Decimal {
        if price <= dec!(0.05) {
            self.deep_static_size_below_05
        } else {
            self.deep_static_size_above_05
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Binance BTC Price State
// ═══════════════════════════════════════════════════════════════════════

const SAMPLE_INTERVAL_MS: u64 = 500; // sample price every 500ms for vol

/// Shared asset price state updated by the Binance WS task.
pub type SharedAssetPrice = Arc<RwLock<AssetPriceState>>;

#[derive(Debug)]
pub struct AssetPriceState {
    /// Latest BTC/USDT price.
    pub current_price: Option<f64>,
    /// Rolling window of (monotonic_time, price) for volatility computation.
    price_samples: VecDeque<(Instant, f64)>,
    /// Last time a sample was pushed (rate-limiting).
    last_sample_time: Option<Instant>,
    /// How far back to keep samples.
    window_secs: u64,
    /// Monotonic timestamp of the last price update from Binance WS.
    /// Used to detect stale feeds (WS silently dies but last price stays cached).
    last_update: Option<Instant>,
    /// Latest Chainlink oracle price (via RTDS, optional cross-validation).
    pub chainlink_price: Option<Decimal>,
}

/// Maximum age (seconds) of the Binance price feed before trading is paused.
/// In a 15-minute market, even a few seconds of stale BTC data can cause
/// the fair-value model to produce dangerously wrong quotes.
const MAX_BTC_PRICE_AGE_SECS: u64 = 10;

impl AssetPriceState {
    pub fn new(window_secs: u64) -> Self {
        Self {
            current_price: None,
            price_samples: VecDeque::with_capacity(512),
            last_sample_time: None,
            window_secs,
            last_update: None,
            chainlink_price: None,
        }
    }

    /// Returns true if the price feed hasn't been updated within `max_age`.
    pub fn is_price_stale(&self, max_age: Duration) -> bool {
        self.last_update
            .map(|t| t.elapsed() > max_age)
            .unwrap_or(true) // Never updated = stale
    }

    /// Update with a new Binance trade price.
    pub fn update_price(&mut self, price: f64) {
        self.current_price = Some(price);
        self.last_update = Some(Instant::now());

        let now = Instant::now();
        let should_sample = self
            .last_sample_time
            .map(|t| now.duration_since(t) >= Duration::from_millis(SAMPLE_INTERVAL_MS))
            .unwrap_or(true);

        if should_sample {
            self.price_samples.push_back((now, price));
            self.last_sample_time = Some(now);

            // Trim old samples
            let cutoff = now - Duration::from_secs(self.window_secs);
            while self.price_samples.front().is_some_and(|(t, _)| *t < cutoff) {
                self.price_samples.pop_front();
            }
        }
    }

    /// Realized volatility: standard deviation of per-second log returns.
    /// Returns None if insufficient data (<10 samples).
    pub fn realized_vol_per_sec(&self) -> Option<f64> {
        if self.price_samples.len() < 10 {
            return None;
        }

        // Compute log returns between consecutive samples
        let mut log_returns = Vec::with_capacity(self.price_samples.len());
        for i in 1..self.price_samples.len() {
            let (t0, p0) = self.price_samples[i - 1];
            let (t1, p1) = self.price_samples[i];
            if p0 <= 0.0 {
                continue;
            }
            let dt_secs = t1.duration_since(t0).as_secs_f64();
            if dt_secs <= 0.0 {
                continue;
            }
            // Normalize log return to per-second
            let log_ret = (p1 / p0).ln() / dt_secs.sqrt();
            log_returns.push(log_ret);
        }

        if log_returns.len() < 5 {
            return None;
        }

        let n = log_returns.len() as f64;
        let mean = log_returns.iter().sum::<f64>() / n;
        let variance = log_returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);

        Some(variance.sqrt())
    }

    /// Realized volatility over a specific lookback window (in seconds).
    /// Returns None if insufficient data.
    pub fn realized_vol_over(&self, lookback_secs: u64) -> Option<f64> {
        let now = Instant::now();
        let cutoff = now - Duration::from_secs(lookback_secs);
        let samples: Vec<&(Instant, f64)> = self
            .price_samples
            .iter()
            .filter(|(t, _)| *t >= cutoff)
            .collect();
        if samples.len() < 5 {
            return None;
        }
        let mut log_returns = Vec::with_capacity(samples.len());
        for i in 1..samples.len() {
            let (t0, p0) = samples[i - 1];
            let (t1, p1) = samples[i];
            if *p0 <= 0.0 {
                continue;
            }
            let dt_secs = t1.duration_since(*t0).as_secs_f64();
            if dt_secs <= 0.0 {
                continue;
            }
            let log_ret = (p1 / p0).ln() / dt_secs.sqrt();
            log_returns.push(log_ret);
        }
        if log_returns.len() < 3 {
            return None;
        }
        let n = log_returns.len() as f64;
        let mean = log_returns.iter().sum::<f64>() / n;
        let variance = log_returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
        Some(variance.sqrt())
    }

    /// Get the BTC price from approximately `secs_ago` seconds ago.
    /// Returns None if no sample is available that far back.
    pub fn price_at_offset(&self, secs_ago: u64) -> Option<f64> {
        let target = Instant::now() - Duration::from_secs(secs_ago);
        // Find the sample closest to the target time
        self.price_samples
            .iter()
            .min_by_key(|(t, _)| {
                if *t >= target {
                    (*t - target).as_millis() as i128
                } else {
                    (target - *t).as_millis() as i128
                }
            })
            .map(|(_, p)| *p)
    }

    /// Number of price samples currently in the buffer.
    pub fn sample_count(&self) -> usize {
        self.price_samples.len()
    }

    /// Returns ("rolling_Ns", vol) or ("init_default", min_vol) describing the sigma source.
    pub fn sigma_source(&self) -> &'static str {
        if self.price_samples.len() >= 10 {
            "rolling"
        } else {
            "init_default"
        }
    }

    /// Detect trending: returns the price change over the last `window_secs` seconds.
    /// Positive = BTC trending UP, Negative = BTC trending DOWN.
    /// Returns None if insufficient data.
    pub fn price_change_over(&self, window_secs: u64) -> Option<f64> {
        if self.price_samples.len() < 2 {
            return None;
        }
        let now = Instant::now();
        let cutoff = now - Duration::from_secs(window_secs);

        // Find the oldest sample within the window
        let oldest_in_window = self
            .price_samples
            .iter()
            .find(|(t, _)| *t >= cutoff)
            .map(|(_, p)| *p);

        let latest = self.price_samples.back().map(|(_, p)| *p);

        match (oldest_in_window, latest) {
            (Some(old), Some(new)) => Some(new - old),
            _ => None,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Per-Market V2 State
// ═══════════════════════════════════════════════════════════════════════

struct MarketV2State {
    market: TrackedMarket,
    /// BTC price when this market was first tracked.
    btc_open: Option<f64>,
    /// Resting buy ladder orders keyed by (outcome, price).
    resting_orders: HashMap<(Outcome, Decimal), RestingLadderOrder>,
    /// Resting sell-back orders keyed by (outcome, price).
    resting_sells: HashMap<(Outcome, Decimal), RestingLadderOrder>,
    /// Resting static deep grid orders keyed by (outcome, price).
    /// Placed once at market entry, NOT cancelled on FV moves or imbalance.
    resting_deep_grid: HashMap<(Outcome, Decimal), RestingLadderOrder>,
    /// Whether the static deep grid has been placed for this market period.
    deep_grid_placed: bool,
    /// Count of deep grid BUY fills (Up side) for separate telemetry.
    deep_grid_fills_up: u32,
    /// Count of deep grid BUY fills (Down side) for separate telemetry.
    deep_grid_fills_down: u32,
    /// Total shares filled from deep grid (for avg price calculation).
    deep_grid_fill_shares: Decimal,
    /// Total cost of deep grid fills (for avg price and PnL tracking).
    deep_grid_fill_cost: Decimal,
    /// Last time we sold each outcome (for buy cooldown after sells).
    last_sell_time: HashMap<Outcome, Instant>,
    /// When EV breaker started being continuously active for this market.
    ev_breaker_since: Option<Instant>,
    /// Last timestamp an EV-breaker warning was logged (cooldown).
    last_ev_breaker_log: Option<Instant>,
    /// Last pair-completion attempt timestamp (retry throttling).
    last_pair_completion_attempt: Option<Instant>,
    /// Number of pair-completion attempts in this period.
    pair_completion_attempts: u32,
    /// Successful pair-completion order submissions in this period.
    pair_completion_successes: u32,
    /// Cached period name for file logging.
    period_name: String,
    /// Last ladder center prices for churn reduction (skip reprice if stable).
    last_yes_center: Option<Decimal>,
    last_no_center: Option<Decimal>,
    // ── Period-level counters for session summary ──
    orders_placed: u32,
    orders_filled: u32,
    orders_cancelled: u32,
    orders_expired: u32,
    total_up_shares_filled: Decimal,
    total_down_shares_filled: Decimal,
    gross_cost: Decimal,
    /// Cumulative BUY fill notional this period (monotonic; sells do not reduce).
    /// Merge-released cost basis is tracked separately when enforcing the period
    /// buy commitment cap so continuous merge can actually recycle working capital.
    gross_buy_filled_usdc: Decimal,
    /// Size-weighted sum of per-fill edge (fair - fill for buys, fill - fair for sells).
    fill_edge_notional_sum: f64,
    /// Total filled size used as denominator for `fill_edge_notional_sum`.
    fill_edge_size_sum: f64,
    /// Sum of successful order placement latencies in milliseconds.
    latency_success_sum_ms: f64,
    /// Count of successful order placement latency samples.
    latency_success_count: u64,
    /// Cumulative realized P&L from sell-back orders this period.
    sell_realized_pnl: Decimal,
    /// Cumulative realized P&L from merging complete pairs this period.
    merge_realized_pnl: Decimal,
    /// Cumulative cost basis freed by sell-back fills this period.
    /// Subtracted from remaining_capacity to prevent freed USDC from being
    /// recycled into new buys (which would flip the imbalance).
    sell_cost_basis_freed: Decimal,
    /// Cumulative paired cost basis released by merge operations this period.
    /// Subtracted from `gross_buy_filled_usdc` when enforcing the period buy
    /// commitment cap so continuous merge can free working capital mid-period.
    merge_cost_basis_released: Decimal,
    /// Snapshot of the position at Closing phase, so we can free inventory
    /// capacity immediately while retaining position data for PnL at Resolved.
    closing_position: Option<Position>,
    /// Persistent heavy-side buy block: once exit mode fires, block buying the
    /// heavy side until excess is fully resolved (drops to 0).  Prevents the
    /// sell→cooldown→buy→sell churn loop where the cooldown expiration allows
    /// the heavy side to re-accumulate immediately.
    exit_buy_block: Option<Outcome>,
    /// Last time a continuous merge was attempted (or checked).
    last_merge_time: Option<Instant>,
    /// Cumulative pairs merged during this period (continuous + closing).
    cumulative_merged_pairs: Decimal,
    /// Taker fee rate fetched from CLOB API (e.g., 0.02 = 2%).
    /// None if fetch failed — falls back to config pair_fee_buffer.
    taker_fee_rate: Option<Decimal>,
    /// When the fee rate was last fetched (for periodic refresh).
    fee_last_fetched: Option<Instant>,
    /// When the first order was placed for this market (for grace period timing).
    first_order_placed_at: Option<Instant>,
    /// When this market was first discovered (for warmup delay).
    discovered_at: Instant,
    /// Whether the order book has been validated with sufficient depth.
    book_ready: bool,
    /// Freeze trading for this market when reconciliation detects position drift.
    /// This is a safety stop to prevent qty-only corrections from corrupting PnL.
    reconciliation_blocked: bool,
    /// Human-readable reason for the reconciliation block.
    reconciliation_block_reason: Option<String>,
    /// Last timestamp we logged the reconciliation block warning for this market.
    last_reconciliation_block_log: Option<Instant>,
    /// Last emergency sell placement timestamp for per-market cooldown.
    last_emergency_sell_at: Option<Instant>,
    /// Number of emergency sell placements this period (health signal).
    emergency_sell_placements: u32,
    /// Last timestamp we logged cancel-churn breaker activation for this market.
    last_churn_breaker_log: Option<Instant>,
    /// Largest absolute share excess observed in this period.
    max_excess_seen: Decimal,
    /// Deepest post-filter YES quote ladder observed in this period.
    max_quote_levels_yes: u32,
    /// Deepest post-filter NO quote ladder observed in this period.
    max_quote_levels_no: u32,
    /// Per-period counts of suppression reasons that fired at least once on a cycle.
    suppression_reason_counts: HashMap<String, u32>,
    /// Count of cancel-all style events affecting this market in this period.
    cancel_all_count: u32,
    /// Period-level pair-quality hysteresis state (blocks heavy-side adds when average
    /// combined pair cost has degraded above threshold).
    pair_quality_block_active: bool,
    /// Minimum (most negative) worst-case terminal PnL observed this period.
    min_worst_case_pnl_seen: Decimal,
    /// Whether EXPIRED events have been logged for this market's Closing phase.
    /// Prevents duplicate EXPIRED logging on repeated Closing ticks when
    /// unconfirmed cancels keep orders in resting_orders.
    closing_expired_logged: bool,
}

#[derive(Debug, Clone, Copy)]
struct MarketTradeSignal {
    received_at: Instant,
    signed_up_notional: Decimal,
    signed_up_large_notional: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectionalSkewStage {
    Mild,
    Strong,
    Terminal,
}

impl DirectionalSkewStage {
    fn as_str(self) -> &'static str {
        match self {
            DirectionalSkewStage::Mild => "mild",
            DirectionalSkewStage::Strong => "strong",
            DirectionalSkewStage::Terminal => "terminal",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct DirectionalTradeFlowSummary {
    long_flow_up_notional: Decimal,
    short_flow_up_notional: Decimal,
    large_flow_up_notional: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct DirectionalSkewSnapshot {
    spot_ret_from_start_bps: f64,
    long_flow_up_notional: Decimal,
    short_flow_up_notional: Decimal,
    large_flow_up_notional: Decimal,
    up_best_imbalance: Decimal,
    imbalance_diff: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct DirectionalSkewDecision {
    stage: DirectionalSkewStage,
    favored_outcome: Outcome,
    favored_multiplier: Decimal,
    unfavored_multiplier: Decimal,
    cancel_deepest_unfavored: bool,
}

impl DirectionalSkewDecision {
    fn label(self) -> String {
        format!("{}:{}", self.stage.as_str(), self.favored_outcome)
    }
}

#[derive(Debug, Clone)]
struct RestingLadderOrder {
    order_id: OrderId,
    size: Decimal,
    placed_at: Instant,
}

#[derive(Debug, Clone)]
struct LadderLevel {
    outcome: Outcome,
    price: Decimal,
    size: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct PeriodHealthSample {
    pnl: Decimal,
    fill_rate: f64,
}

// ═══════════════════════════════════════════════════════════════════════
// Fair Value Math
// ═══════════════════════════════════════════════════════════════════════

/// Standard normal CDF approximation (Abramowitz & Stegun).
fn normal_cdf(x: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.231_641_9 * x.abs());
    let d = 0.398_942_280_401_432_7; // 1/sqrt(2π)
    let poly = t
        * (0.319_381_530
            + t * (-0.356_563_782
                + t * (1.781_477_937 + t * (-1.821_255_978 + t * 1.330_274_429))));
    let p = d * (-x * x / 2.0).exp() * poly;
    if x >= 0.0 {
        1.0 - p
    } else {
        p
    }
}

/// Compute fair probability that BTC will be UP at resolution.
///
/// Uses a Brownian motion model:
///   P(S_end > S_start) = Φ(ln(S_current / S_open) / (σ * √T_remaining))
///
/// - `btc_open`: BTC price at market window start.
/// - `btc_current`: Current BTC price from Binance.
/// - `sigma_per_sec`: Realized per-second log-return volatility.
/// - `remaining_secs`: Seconds until market resolution.
fn fair_value_up(btc_open: f64, btc_current: f64, sigma_per_sec: f64, remaining_secs: f64) -> f64 {
    if btc_open <= 0.0 || btc_current <= 0.0 || remaining_secs <= 0.0 {
        return 0.5; // no data → 50/50
    }

    let log_return = (btc_current / btc_open).ln();

    // σ * √T — standard deviation of remaining price move
    let remaining_vol = sigma_per_sec * remaining_secs.sqrt();

    if remaining_vol <= 1e-12 {
        // Vol essentially zero — outcome determined by current direction
        return if log_return > 0.0 {
            0.95
        } else if log_return < 0.0 {
            0.05
        } else {
            0.5
        };
    }

    let z = log_return / remaining_vol;
    normal_cdf(z).clamp(0.02, 0.98) // keep within tradeable range
}

// ═══════════════════════════════════════════════════════════════════════
// Binance WebSocket Task
// ═══════════════════════════════════════════════════════════════════════

#[derive(Deserialize)]
struct BinanceTrade {
    p: String,
}

/// Background task: stream asset/USDT trades from Binance, update shared state.
///
/// Uses ping/pong keepalive to detect silent connection death. Binance sends
/// trade messages frequently (~100ms for BTCUSDT), so any silence >30s means
/// the connection is dead. We send WebSocket pings every 15s and force-reconnect
/// if no message (trade, pong, or otherwise) arrives within 30s.
async fn binance_ws_loop(
    price_state: SharedAssetPrice,
    url: String,
    asset_name: String,
    shock_notify: Arc<tokio::sync::Notify>,
    shock_threshold: f64,
) {
    use tokio_tungstenite::tungstenite::Message;

    const PING_INTERVAL: Duration = Duration::from_secs(15);
    const READ_TIMEOUT: Duration = Duration::from_secs(30);
    // Price-shock detection: track price 1 second ago for fast delta detection.
    const SHOCK_WINDOW: Duration = Duration::from_secs(1);

    let mut backoff = Duration::from_secs(2);
    let max_backoff = Duration::from_secs(30);

    loop {
        match connect_async(&url).await {
            Ok((ws, _)) => {
                info!("[binance-{asset_name}] Connected to Binance WS");
                backoff = Duration::from_secs(2);
                let (mut writer, mut reader) = ws.split();

                let mut ping_interval = tokio::time::interval(PING_INTERVAL);
                ping_interval.reset(); // don't fire immediately
                let mut last_msg = Instant::now();
                // Price-shock detection state
                let mut shock_anchor_price: Option<f64> = None;
                let mut shock_anchor_time = Instant::now();

                loop {
                    tokio::select! {
                        msg = reader.next() => {
                            match msg {
                                Some(Ok(m)) => {
                                    last_msg = Instant::now();
                                    // Pong responses are handled automatically by tungstenite,
                                    // but we still update last_msg for any frame type.
                                    if let Ok(text) = m.into_text() {
                                        if let Ok(trade) = serde_json::from_str::<BinanceTrade>(&text) {
                                            if let Ok(price) = trade.p.parse::<f64>() {
                                                price_state.write().update_price(price);

                                                // Price-shock detection: compare to anchor
                                                let now = Instant::now();
                                                if now.duration_since(shock_anchor_time) > SHOCK_WINDOW || shock_anchor_price.is_none() {
                                                    shock_anchor_price = Some(price);
                                                    shock_anchor_time = now;
                                                } else if let Some(anchor) = shock_anchor_price {
                                                    let delta = (price - anchor).abs();
                                                    if delta >= shock_threshold {
                                                        shock_notify.notify_one();
                                                        // Reset anchor after signaling
                                                        shock_anchor_price = Some(price);
                                                        shock_anchor_time = now;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Some(Err(e)) => {
                                    warn!("[binance-{asset_name}] WS read error: {e}");
                                    break;
                                }
                                None => {
                                    warn!("[binance-{asset_name}] WS stream ended");
                                    break;
                                }
                            }
                        }
                        _ = ping_interval.tick() => {
                            // Check for silent death: no message in READ_TIMEOUT
                            if last_msg.elapsed() > READ_TIMEOUT {
                                warn!(
                                    "[binance-{asset_name}] No data for {}s — forcing reconnect",
                                    last_msg.elapsed().as_secs()
                                );
                                // Try graceful close, ignore errors
                                let _ = writer.close().await;
                                break;
                            }
                            // Send ping to keep connection alive and detect dead sockets
                            if let Err(e) = writer.send(Message::Ping(vec![].into())).await {
                                warn!("[binance-{asset_name}] Ping failed: {e} — reconnecting");
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("[binance-{asset_name}] Connection failed: {e}");
            }
        }

        warn!(
            "[binance-{asset_name}] Reconnecting in {}s...",
            backoff.as_secs()
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

/// Background task: stream Chainlink oracle prices via RTDS for cross-validation.
/// Gracefully handles failures — RTDS is optional and doesn't affect bot operation.
async fn rtds_chainlink_loop(price_state: SharedAssetPrice, pair: String) {
    use futures_util::StreamExt;
    use polymarket_client_sdk::rtds;

    let client = rtds::Client::default();
    let mut backoff = Duration::from_secs(3);
    let max_backoff = Duration::from_secs(60);

    loop {
        info!("[rtds-chainlink] Subscribing to {pair} oracle...");

        match client.subscribe_chainlink_prices(Some(pair.clone())) {
            Ok(stream) => {
                info!("[rtds-chainlink] Connected");
                backoff = Duration::from_secs(3);
                tokio::pin!(stream);

                while let Some(result) = stream.next().await {
                    match result {
                        Ok(price) => {
                            price_state.write().chainlink_price = Some(price.value);
                        }
                        Err(e) => {
                            warn!("[rtds-chainlink] Stream error: {e}");
                            break;
                        }
                    }
                }

                warn!("[rtds-chainlink] Stream ended");
            }
            Err(e) => warn!("[rtds-chainlink] Subscribe failed: {e}"),
        }

        warn!(
            backoff_secs = backoff.as_secs(),
            "[rtds-chainlink] Reconnecting in {}s...",
            backoff.as_secs()
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════

fn round_down_to_tick(price: Decimal, tick_size: Decimal) -> Decimal {
    if tick_size.is_zero() {
        return price;
    }
    (price / tick_size).floor() * tick_size
}

const ORDER_SIZE_DECIMALS: u32 = 2;
const MIN_ORDER_SHARES: Decimal = dec!(5); // Polymarket CLOB minimum is 5 shares

/// Truncate order size to exchange-supported precision (2dp).
fn quantize_order_size(size: Decimal) -> Decimal {
    if size <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let scale = Decimal::from(10u64.pow(ORDER_SIZE_DECIMALS));
    (size * scale).floor() / scale
}

/// Compute order size for a given ladder level, applying decay.
///
/// Level 0 = `base_size`, each subsequent level shrinks by `decay_rate`
/// (e.g., 0.10 = 10% smaller per level). Floored at 20% of base_size
/// and never below `MIN_ORDER_SHARES`.
fn ladder_size_at_level(base_size: Decimal, level: u32, decay_rate: f64) -> Decimal {
    let factor = (1.0 - (level as f64) * decay_rate).max(0.2);
    let raw = base_size * Decimal::from_f64(factor).unwrap_or(Decimal::ONE);
    quantize_order_size(raw.max(MIN_ORDER_SHARES))
}

/// Price-aware ladder sizing: cheap price levels get MORE shares to catch panic dumps.
/// price <= $0.05 → 3x base, price <= $0.15 → 2x base, else → 1x base.
/// Also applies level-based decay.
fn ladder_size_at_level_and_price(
    base_size: Decimal,
    level: u32,
    decay_rate: f64,
    price: Decimal,
) -> Decimal {
    let price_multiplier = if price <= dec!(0.05) {
        dec!(3)
    } else if price <= dec!(0.15) {
        dec!(2)
    } else {
        dec!(1)
    };
    let factor = (1.0 - (level as f64) * decay_rate).max(0.2);
    let raw = base_size * Decimal::from_f64(factor).unwrap_or(Decimal::ONE) * price_multiplier;
    quantize_order_size(raw.max(MIN_ORDER_SHARES))
}

#[derive(Debug, Clone, Copy)]
struct TerminalPnlBounds {
    pnl_if_up: Decimal,
    pnl_if_down: Decimal,
    worst_case_pnl: Decimal,
}

fn compute_terminal_pnl_bounds(
    position: &Position,
    sell_realized_pnl: Decimal,
    merge_realized_pnl: Decimal,
) -> TerminalPnlBounds {
    let realized = sell_realized_pnl + merge_realized_pnl;
    let cost_basis = position.total_yes_spent + position.total_no_spent;
    let pnl_if_up = realized + position.yes_qty - cost_basis;
    let pnl_if_down = realized + position.no_qty - cost_basis;
    let worst_case_pnl = pnl_if_up.min(pnl_if_down);
    TerminalPnlBounds {
        pnl_if_up,
        pnl_if_down,
        worst_case_pnl,
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

fn signed_direction_f64(value: f64) -> i8 {
    if value > 0.0 {
        1
    } else if value < 0.0 {
        -1
    } else {
        0
    }
}

fn signed_direction_decimal(value: Decimal) -> i8 {
    if value > Decimal::ZERO {
        1
    } else if value < Decimal::ZERO {
        -1
    } else {
        0
    }
}

fn outcome_for_up_sign(sign: i8) -> Option<Outcome> {
    match sign {
        1 => Some(Outcome::Yes),
        -1 => Some(Outcome::No),
        _ => None,
    }
}

fn spot_return_bps(open_price: f64, current_price: f64) -> f64 {
    if open_price <= 0.0 || current_price <= 0.0 {
        0.0
    } else {
        ((current_price / open_price) - 1.0) * 10_000.0
    }
}

fn evaluate_directional_skew(
    v2: &V2Config,
    remaining_secs: f64,
    snapshot: DirectionalSkewSnapshot,
) -> Option<DirectionalSkewDecision> {
    let spot_sign = signed_direction_f64(snapshot.spot_ret_from_start_bps);
    if spot_sign == 0
        || snapshot.spot_ret_from_start_bps.abs() < v2.directional_skew_spot_ret_threshold_bps
    {
        return None;
    }

    if snapshot.long_flow_up_notional.abs() < v2.directional_skew_flow_threshold_usdc
        || signed_direction_decimal(snapshot.long_flow_up_notional) != spot_sign
    {
        return None;
    }

    let favored_outcome = outcome_for_up_sign(spot_sign)?;
    let large_flow_aligned = snapshot.large_flow_up_notional.abs()
        >= v2.directional_skew_large_flow_threshold_usdc
        && signed_direction_decimal(snapshot.large_flow_up_notional) == spot_sign;
    let short_flow_aligned = snapshot.short_flow_up_notional.abs()
        >= v2.directional_skew_short_flow_threshold_usdc
        && signed_direction_decimal(snapshot.short_flow_up_notional) == spot_sign;
    let imbalance_diff_aligned = snapshot.imbalance_diff.abs()
        >= v2.directional_skew_terminal_imbalance_diff_threshold
        && signed_direction_decimal(snapshot.imbalance_diff) == spot_sign;
    let up_best_aligned = snapshot.up_best_imbalance.abs()
        >= v2.directional_skew_terminal_best_imbalance_threshold
        && signed_direction_decimal(snapshot.up_best_imbalance) == spot_sign;

    if remaining_secs <= v2.directional_skew_terminal_start_secs as f64
        && (large_flow_aligned || short_flow_aligned)
        && (imbalance_diff_aligned || up_best_aligned)
    {
        return Some(DirectionalSkewDecision {
            stage: DirectionalSkewStage::Terminal,
            favored_outcome,
            favored_multiplier: v2.directional_skew_terminal_favored_multiplier,
            unfavored_multiplier: v2.directional_skew_terminal_unfavored_multiplier,
            cancel_deepest_unfavored: v2.directional_skew_terminal_cancel_deepest_unfavored,
        });
    }

    if remaining_secs <= v2.directional_skew_strong_start_secs as f64
        && (large_flow_aligned || short_flow_aligned)
    {
        return Some(DirectionalSkewDecision {
            stage: DirectionalSkewStage::Strong,
            favored_outcome,
            favored_multiplier: v2.directional_skew_strong_favored_multiplier,
            unfavored_multiplier: v2.directional_skew_strong_unfavored_multiplier,
            cancel_deepest_unfavored: false,
        });
    }

    if remaining_secs <= v2.directional_skew_mild_start_secs as f64 {
        return Some(DirectionalSkewDecision {
            stage: DirectionalSkewStage::Mild,
            favored_outcome,
            favored_multiplier: v2.directional_skew_mild_favored_multiplier,
            unfavored_multiplier: v2.directional_skew_mild_unfavored_multiplier,
            cancel_deepest_unfavored: false,
        });
    }

    None
}

fn scale_ladder_for_directional_skew(
    ladder: &mut Vec<LadderLevel>,
    multiplier: Decimal,
    keep_top_level: bool,
) {
    for (idx, level) in ladder.iter_mut().enumerate() {
        let scaled = quantize_order_size(level.size * multiplier);
        level.size = if idx == 0 && keep_top_level {
            scaled.max(MIN_ORDER_SHARES)
        } else {
            scaled
        };
    }
    ladder.retain(|level| level.size >= MIN_ORDER_SHARES);
}

fn apply_directional_skew_to_ladders(
    yes_ladder: &mut Vec<LadderLevel>,
    no_ladder: &mut Vec<LadderLevel>,
    decision: DirectionalSkewDecision,
) {
    let (favored_ladder, unfavored_ladder) = match decision.favored_outcome {
        Outcome::Yes => (yes_ladder, no_ladder),
        Outcome::No => (no_ladder, yes_ladder),
    };

    scale_ladder_for_directional_skew(favored_ladder, decision.favored_multiplier, false);

    if decision.cancel_deepest_unfavored && unfavored_ladder.len() > 1 {
        unfavored_ladder.pop();
    }

    scale_ladder_for_directional_skew(
        unfavored_ladder,
        decision.unfavored_multiplier,
        decision.unfavored_multiplier < Decimal::ONE,
    );
}

fn resting_buy_notional(
    resting_orders: &HashMap<(Outcome, Decimal), RestingLadderOrder>,
) -> Decimal {
    resting_orders
        .iter()
        .map(|((_, price), order)| *price * order.size)
        .sum()
}

fn cap_buy_size_for_notional(size: Decimal, price: Decimal, notional_cap: Decimal) -> Decimal {
    if size <= Decimal::ZERO || price <= Decimal::ZERO || notional_cap <= Decimal::ZERO {
        return size;
    }
    let max_size = quantize_order_size((notional_cap / price).floor());
    size.min(max_size)
}

fn market_total_secs_f64(market: &TrackedMarket) -> f64 {
    market.effective_duration_secs_15m_fallback().max(1) as f64
}

/// PM-AMM-inspired liquidity taper: `sqrt((T-t)/T)` decay.
/// Returns a factor in [min_factor, 1.0] that reduces levels and sizes
/// as the period approaches settlement.
///   60s remaining / 300s total → 0.45
///   15s remaining / 300s total → 0.22
///    5s remaining / 300s total → 0.13
fn liquidity_taper_factor(remaining_secs: f64, total_secs: f64, min_factor: f64) -> f64 {
    if total_secs <= 0.0 || remaining_secs <= 0.0 {
        return min_factor;
    }
    (remaining_secs / total_secs)
        .clamp(0.0, 1.0)
        .sqrt()
        .max(min_factor)
}

fn elapsed_pct_from_remaining(remaining_secs: f64, total_secs: f64) -> f64 {
    if total_secs <= 0.0 {
        0.0
    } else {
        (1.0 - remaining_secs / total_secs).clamp(0.0, 1.0)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// EV Circuit Breaker (pure, testable)
// ═══════════════════════════════════════════════════════════════════════

/// Expected value breakdown for the current position.
#[derive(Debug, Clone)]
struct PositionEV {
    /// Guaranteed profit from complete pairs.
    locked_profit: Decimal,
    /// Expected value of excess directional shares (negative = losing money).
    excess_ev: Decimal,
    /// Net EV = locked_profit + excess_ev.
    net_ev: Decimal,
    /// Number of excess shares on the heavy side.
    excess_shares: Decimal,
}

/// Compute expected PnL from current position assuming resolution at current fair values.
///
/// - `locked_profit`: guaranteed from complete pairs + already-realized merge profit
/// - `excess_ev`: expected value of unmatched directional shares
///   = excess_qty * P(heavy_side_wins) * $1 - excess_qty * avg_heavy_cost
/// - `net_ev`: locked_profit + excess_ev
fn compute_position_ev(position: &Position, fv_up: f64, merge_realized_pnl: Decimal) -> PositionEV {
    // Include already-realized merge PnL so the breaker sees total value created
    // this period, not just current unrealized pairs (which drop to 0 after merge).
    let locked_profit = position.locked_profit() + merge_realized_pnl;
    let pairs = position.complete_pairs();

    let excess_yes = position.yes_qty - pairs;
    let excess_no = position.no_qty - pairs;

    let avg_yes = if position.yes_qty > Decimal::ZERO {
        position.total_yes_spent / position.yes_qty
    } else {
        Decimal::ZERO
    };
    let avg_no = if position.no_qty > Decimal::ZERO {
        position.total_no_spent / position.no_qty
    } else {
        Decimal::ZERO
    };

    let fv_up_dec = Decimal::from_f64(fv_up).unwrap_or(dec!(0.5));
    let fv_down_dec = Decimal::ONE - fv_up_dec;

    // EV of excess YES = qty * P(YES wins) * $1.00 - qty * avg_cost
    let excess_yes_ev = excess_yes * fv_up_dec - excess_yes * avg_yes;
    // EV of excess NO = qty * P(NO wins) * $1.00 - qty * avg_cost
    let excess_no_ev = excess_no * fv_down_dec - excess_no * avg_no;

    let excess_ev = excess_yes_ev + excess_no_ev;
    let excess_shares = if excess_yes > excess_no {
        excess_yes
    } else {
        excess_no
    };

    PositionEV {
        locked_profit,
        excess_ev,
        net_ev: locked_profit + excess_ev,
        excess_shares,
    }
}

/// Determine sell aggressiveness based on EV.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SellAggressiveness {
    /// Sell above cost (patient — excess is profitable or small)
    AboveCost,
    /// Sell at best bid (moderate urgency)
    AtBestBid,
    /// Sell below best bid (urgent — crossing the spread)
    CrossSpread,
}

fn sell_aggressiveness(ev: &PositionEV) -> SellAggressiveness {
    if ev.excess_ev < Decimal::ZERO {
        if ev.locked_profit > Decimal::ZERO && ev.excess_ev.abs() > ev.locked_profit * dec!(0.5) {
            SellAggressiveness::CrossSpread
        } else {
            SellAggressiveness::AtBestBid
        }
    } else {
        SellAggressiveness::AboveCost
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitMode {
    None,
    Maker,
    Taker,
}

#[derive(Debug, Clone)]
enum ExitPlan {
    Skip {
        reason: &'static str,
        abs_excess: Decimal,
        heavy_outcome: Option<Outcome>,
    },
    Maker {
        reason: &'static str,
        abs_excess: Decimal,
        heavy_outcome: Outcome,
        levels: Vec<LadderLevel>,
    },
    Taker {
        reason: &'static str,
        abs_excess: Decimal,
        heavy_outcome: Outcome,
        size: Decimal,
        price: Decimal,
    },
}

/// Dynamic max-loss cap by remaining time.
///
/// Wider caps are allowed as expiry approaches because carrying excess to
/// resolution has much larger downside than taking a controlled loss.
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
        // Desperation: in the final 2 minutes, sell at any positive price.
        // Holding worthless shares to expiry loses the full position value;
        // selling at even 1¢ recovers something.
        dec!(1.00)
    }
}

const SOFT_EXIT_START_REMAINING_SECS: f64 = 600.0;

fn select_exit_mode(
    abs_excess: Decimal,
    remaining_secs: f64,
    breaker_secs: f64,
    v2: &V2Config,
) -> ExitMode {
    if abs_excess <= Decimal::ZERO {
        return ExitMode::None;
    }
    let force_taker_late = remaining_secs <= v2.exit_force_taker_remaining_secs as f64
        && abs_excess >= v2.exit_soft_excess;
    let force_taker_breaker =
        abs_excess >= v2.exit_hard_excess && breaker_secs >= v2.exit_taker_after_secs as f64;
    if force_taker_late || force_taker_breaker {
        return ExitMode::Taker;
    }
    if abs_excess >= v2.exit_soft_excess && remaining_secs <= SOFT_EXIT_START_REMAINING_SECS {
        return ExitMode::Maker;
    }
    ExitMode::None
}

fn exit_skip_reason_for_mode_none(abs_excess: Decimal, v2: &V2Config) -> &'static str {
    if abs_excess < v2.exit_soft_excess {
        "below_soft_excess"
    } else {
        "soft_window_not_open"
    }
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
    v2: &V2Config,
    remaining_secs: f64,
    breaker_secs: f64,
    in_grace_period: bool,
    heavy_side_has_buys: bool,
) -> ExitPlan {
    // Share-based excess for sell sizing and direction detection
    let excess = position.yes_qty - position.no_qty;
    // Cost-based excess for threshold comparisons (USDC)
    let cost_excess = position.cost_imbalance();
    let abs_cost_excess = cost_excess.abs();
    let abs_excess = excess.abs(); // still needed for sell quantity
    let heavy_outcome = if excess > Decimal::ZERO {
        Some(Outcome::Yes)
    } else if excess < Decimal::ZERO {
        Some(Outcome::No)
    } else {
        None
    };
    if abs_excess <= Decimal::ZERO || heavy_outcome.is_none() {
        return ExitPlan::Skip {
            reason: "no_excess",
            abs_excess: abs_cost_excess,
            heavy_outcome,
        };
    }

    if in_grace_period {
        return ExitPlan::Skip {
            reason: "grace_period",
            abs_excess: abs_cost_excess,
            heavy_outcome,
        };
    }

    // Anti-oscillation stays active only below soft exit threshold (cost-based).
    // For larger excess, risk reduction takes priority over churn suppression.
    if heavy_side_has_buys && abs_cost_excess < v2.exit_soft_excess {
        return ExitPlan::Skip {
            reason: "cooldown_block",
            abs_excess: abs_cost_excess,
            heavy_outcome,
        };
    }

    let mode = select_exit_mode(abs_cost_excess, remaining_secs, breaker_secs, v2);
    if mode == ExitMode::None {
        return ExitPlan::Skip {
            reason: exit_skip_reason_for_mode_none(abs_excess, v2),
            abs_excess,
            heavy_outcome,
        };
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
        Some(b) if b > Decimal::ZERO => b,
        _ => {
            return ExitPlan::Skip {
                reason: "no_bid_liquidity",
                abs_excess,
                heavy_outcome: Some(heavy_outcome),
            }
        }
    };

    let mut max_loss = dynamic_loss_cap(remaining_secs);
    if remaining_secs > 300.0 && v2.sellback_max_loss_cents < max_loss {
        max_loss = v2.sellback_max_loss_cents;
    }
    let loss_floor = avg_heavy - max_loss;

    if mode == ExitMode::Taker {
        if best_bid < loss_floor {
            return ExitPlan::Skip {
                reason: "bid_below_loss_floor",
                abs_excess,
                heavy_outcome: Some(heavy_outcome),
            };
        }
        let size = quantize_order_size(abs_excess.min(v2.sell_level_size));
        if size < MIN_ORDER_SHARES {
            return ExitPlan::Skip {
                reason: "below_min_order_size",
                abs_excess,
                heavy_outcome: Some(heavy_outcome),
            };
        }
        return ExitPlan::Taker {
            reason: "taker_mode",
            abs_excess,
            heavy_outcome,
            size,
            price: best_bid,
        };
    }

    let default_anchor = best_bid + tick_size;
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
    let fv_target = (fv_heavy - v2.sellback_edge).max(avg_heavy + v2.sellback_edge);
    let mut base_price = maker_anchor.min(fv_target);
    if base_price < loss_floor {
        if best_bid < loss_floor {
            return ExitPlan::Skip {
                reason: "bid_below_loss_floor",
                abs_excess,
                heavy_outcome: Some(heavy_outcome),
            };
        }
        base_price = loss_floor;
    }
    let mut base_price = round_down_to_tick(base_price, tick_size);
    if base_price < loss_floor {
        base_price += tick_size;
    }
    if base_price <= Decimal::ZERO {
        return ExitPlan::Skip {
            reason: "invalid_exit_price",
            abs_excess,
            heavy_outcome: Some(heavy_outcome),
        };
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
        return ExitPlan::Skip {
            reason: "bid_below_loss_floor",
            abs_excess,
            heavy_outcome: Some(heavy_outcome),
        };
    }

    let mut levels = Vec::with_capacity(v2.sell_levels as usize);
    let mut remaining_to_sell = abs_excess;
    for i in 0..v2.sell_levels {
        let price = base_price - tick_size * Decimal::from(i);
        if price <= Decimal::ZERO || price < loss_floor || remaining_to_sell <= Decimal::ZERO {
            break;
        }
        let size = quantize_order_size(v2.sell_level_size.min(remaining_to_sell));
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
        return ExitPlan::Skip {
            reason: "maker_no_levels",
            abs_excess,
            heavy_outcome: Some(heavy_outcome),
        };
    }

    ExitPlan::Maker {
        reason: "maker_mode",
        abs_excess,
        heavy_outcome,
        levels,
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Sell-Back Engine (pure, testable)
// ═══════════════════════════════════════════════════════════════════════

/// Compute sell ladder for excess position reduction.
///
/// Returns sell levels for the heavy side only. Sell qty is capped at excess
/// so we never sell below the balanced position.
fn compute_sell_ladder(
    position: &Position,
    fv_up: f64,
    fv_down: f64,
    yes_best_bid: Option<Decimal>,
    no_best_bid: Option<Decimal>,
    tick_size: Decimal,
    v2: &V2Config,
    aggressiveness: SellAggressiveness,
) -> Vec<LadderLevel> {
    let excess = position.yes_qty - position.no_qty; // positive = heavy YES
    let abs_excess = excess.abs();

    if abs_excess < v2.sellback_min_excess {
        return Vec::new();
    }

    let (heavy_outcome, avg_heavy, best_bid, fv_heavy) = if excess > Decimal::ZERO {
        let avg = if position.yes_qty > Decimal::ZERO {
            position.total_yes_spent / position.yes_qty
        } else {
            Decimal::ZERO
        };
        let fv = Decimal::from_f64(fv_up).unwrap_or(Decimal::ZERO);
        (Outcome::Yes, avg, yes_best_bid, fv)
    } else {
        let avg = if position.no_qty > Decimal::ZERO {
            position.total_no_spent / position.no_qty
        } else {
            Decimal::ZERO
        };
        let fv = Decimal::from_f64(fv_down).unwrap_or(Decimal::ZERO);
        (Outcome::No, avg, no_best_bid, fv)
    };

    let best_bid = match best_bid {
        Some(b) if b > Decimal::ZERO => b,
        _ => return Vec::new(), // no liquidity to sell into
    };

    // Determine base sell price based on aggressiveness.
    // In patient/moderate modes, use fair value (not cost basis) as the
    // sell anchor. This captures value when FV has moved in our favor
    // instead of selling at the stale purchase price.
    //
    // CRITICAL: All modes allow selling down to avg_cost - max_loss_cents.
    // For excess shares, accepting a small known loss (2¢/share) is far better
    // than holding to expiry where you lose the entire cost basis (~50¢/share)
    // if the market resolves against you.
    let loss_floor = avg_heavy - v2.sellback_max_loss_cents;
    let base_price = match aggressiveness {
        SellAggressiveness::AboveCost => {
            // Sell at fair value minus a small discount, floored at cost+edge.
            // Falls back: cost+edge → cost → cost-max_loss.
            let fv_sell = fv_heavy - v2.sellback_edge;
            let cost_plus_edge = avg_heavy + v2.sellback_edge;
            let target = fv_sell.max(cost_plus_edge);
            if target <= best_bid {
                target // ideal: sell above cost
            } else if avg_heavy <= best_bid {
                best_bid // fallback: sell at breakeven via best bid
            } else if loss_floor <= best_bid {
                best_bid // last resort: accept small loss via best bid
            } else {
                return Vec::new(); // bid too far below cost — hold
            }
        }
        SellAggressiveness::AtBestBid => {
            // Sell at best bid, floored at cost - max_loss
            if best_bid < loss_floor {
                return Vec::new();
            }
            best_bid
        }
        SellAggressiveness::CrossSpread => {
            // Urgent: sell at best bid, floored at cost - max_loss
            if best_bid < loss_floor {
                return Vec::new();
            }
            best_bid
        }
    };

    // Round down to tick
    let base_price = round_down_to_tick(base_price, tick_size);
    if base_price <= Decimal::ZERO {
        return Vec::new();
    }

    let mut levels = Vec::with_capacity(v2.sell_levels as usize);
    let mut remaining_to_sell = abs_excess;

    for i in 0..v2.sell_levels {
        let price = base_price - tick_size * Decimal::from(i);
        if price <= Decimal::ZERO || remaining_to_sell <= Decimal::ZERO {
            break;
        }
        let size = quantize_order_size(v2.sell_level_size.min(remaining_to_sell));
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

    levels
}

// ═══════════════════════════════════════════════════════════════════════
// Time-Decaying Imbalance Limits (pure, testable)
// ═══════════════════════════════════════════════════════════════════════

/// Compute time-adjusted imbalance limits that decay linearly toward
/// resolution. Returns (max_abs_imbalance, soft_threshold).
fn time_adjusted_imbalance_limits(
    remaining_secs: f64,
    total_market_secs: f64,
    v2: &V2Config,
) -> (Decimal, Decimal) {
    let time_frac = (remaining_secs / total_market_secs).clamp(0.0, 1.0);
    let time_dec = Decimal::from_f64(time_frac).unwrap_or(Decimal::ONE);

    let max_abs = (v2.max_abs_imbalance * time_dec).max(v2.imbalance_decay_floor_abs);
    let soft = (v2.soft_imbalance_threshold * time_dec).max(v2.imbalance_decay_floor_soft);

    (max_abs, soft)
}

// ═══════════════════════════════════════════════════════════════════════
// Late-Phase Pair Completion (pure, testable)
// ═══════════════════════════════════════════════════════════════════════

/// Determine how many light-side shares to buy at the ask for pair completion.
///
/// In Late/VeryLate phases, if we hold excess shares on one side, we can
/// lock in guaranteed profit by crossing the spread to buy the light side —
/// as long as `avg_cost_heavy + ask_light < $1.00`.
///
/// Returns `(outcome_to_buy, shares, price)` or None if no profitable completion exists.
fn compute_pair_completion(
    position: &Position,
    yes_best_ask: Option<Decimal>,
    no_best_ask: Option<Decimal>,
    max_per_cycle: Decimal,
    taker_fee_rate: Option<Decimal>,
    fee_buffer_fallback: Decimal,
) -> Option<(Outcome, Decimal, Decimal)> {
    let excess = position.yes_qty - position.no_qty;
    let abs_excess = excess.abs();

    if abs_excess < Decimal::ONE {
        return None;
    }

    let (heavy_side_avg, light_ask, light_outcome) = if excess > Decimal::ZERO {
        // Heavy YES — need to buy NO to complete pairs
        let avg_yes = if position.yes_qty > Decimal::ZERO {
            position.total_yes_spent / position.yes_qty
        } else {
            return None;
        };
        let ask = no_best_ask?;
        (avg_yes, ask, Outcome::No)
    } else {
        // Heavy NO — need to buy YES to complete pairs
        let avg_no = if position.no_qty > Decimal::ZERO {
            position.total_no_spent / position.no_qty
        } else {
            return None;
        };
        let ask = yes_best_ask?;
        (avg_no, ask, Outcome::Yes)
    };

    // Compute actual taker fee for the light-side buy: fee(p) = p * (1-p) * rate
    // Falls back to flat fee_buffer_fallback from config if no dynamic rate available
    let taker_fee = match taker_fee_rate {
        Some(rate) => light_ask * (Decimal::ONE - light_ask) * rate,
        None => fee_buffer_fallback,
    };
    let effective_pair_cost = heavy_side_avg + light_ask + taker_fee;
    if effective_pair_cost >= Decimal::ONE {
        return None; // Not profitable to cross the spread after fees
    }

    // Buy at most `max_per_cycle` shares, capped at excess and quantized to 2dp.
    let shares = quantize_order_size(abs_excess.min(max_per_cycle));
    if shares < MIN_ORDER_SHARES {
        return None;
    }

    // Defend against "invalid amount for a marketable BUY order" rejections.
    if shares * light_ask < Decimal::ONE {
        return None;
    }

    Some((light_outcome, shares, light_ask))
}

// ═══════════════════════════════════════════════════════════════════════
// Core Ladder Functions (pure, testable)
// ═══════════════════════════════════════════════════════════════════════

/// Compute bid ladders for both YES and NO sides.
///
/// Centers each ladder at `target_combined * fv`, applies position skew,
/// then generates `ladder_levels` descending price levels spaced by `tick_size * tick_spacing`.
///
/// Compute cumulative price offset from level 0 for a given level index.
/// Uses tight spacing (`ladder_tick_spacing`) for levels 0..`deep_start`,
/// then wider spacing (`deep_tick_spacing`) for levels `deep_start`+.
/// This creates a non-uniform ladder: tight near the ask for fillability,
/// deep levels spread wide to catch panic dumps at discount.
fn ladder_price_offset(
    i: u32,
    tick_size: Decimal,
    tight_spacing: u32,
    deep_spacing: u32,
    deep_start: u32,
) -> Decimal {
    if i < deep_start {
        tick_size * Decimal::from(tight_spacing) * Decimal::from(i)
    } else {
        let near = tick_size * Decimal::from(tight_spacing) * Decimal::from(deep_start);
        let deep = tick_size * Decimal::from(deep_spacing) * Decimal::from(i - deep_start);
        near + deep
    }
}

/// **FV dead-zone**: If a side's fair value is below `fv_dead_threshold`, its
/// ladder is cleared entirely (no point buying tokens almost certain to be worthless).
///
/// **Dynamic min bid**: Instead of static `min_bid`, uses `max(fv * min_bid_fv_ratio, min_bid_absolute_floor)`
/// so we never bid more than ~2x fair value.
fn compute_bid_ladder(
    fv_up: f64,
    fv_down: f64,
    tick_size: Decimal,
    position: &Position,
    v2: &V2Config,
    levels_override: Option<u32>,
) -> (Vec<LadderLevel>, Vec<LadderLevel>) {
    // ── FV dead-zone: don't bid on near-worthless tokens ──
    // Exception: when the OTHER side has positions, the "dead" side is needed
    // for pair completion. Generate a floor bid — PostOnly ask-anchoring will
    // move it near the market, and the combined cost guard will reject if unprofitable.
    let yes_dead = fv_up < v2.fv_dead_threshold;
    let no_dead = fv_down < v2.fv_dead_threshold;

    if yes_dead && no_dead {
        return (Vec::new(), Vec::new());
    }

    let tc_f64 = v2
        .target_combined
        .to_string()
        .parse::<f64>()
        .unwrap_or(0.96);

    // Center prices (proportional to fair value, scaled by target_combined)
    let mut center_yes_f64 = tc_f64 * fv_up;
    let mut center_no_f64 = tc_f64 * fv_down;

    // Position skew: heavy YES → lower center_yes, raise center_no
    let excess = position.yes_qty - position.no_qty;
    if excess.abs() > Decimal::ZERO {
        let excess_f64 = excess.to_f64().unwrap_or(0.0);
        let skew_per = v2
            .imbalance_skew_per_share
            .to_string()
            .parse::<f64>()
            .unwrap_or(0.005);
        let skew = excess_f64 * skew_per;
        center_yes_f64 -= skew;
        center_no_f64 += skew;
    }

    // ── Dynamic min bid: fv-aware floor instead of static min_bid ──
    let min_bid_abs_f64 = v2
        .min_bid_absolute_floor
        .to_string()
        .parse::<f64>()
        .unwrap_or(0.02);
    let min_bid_yes = (fv_up * v2.min_bid_fv_ratio).max(min_bid_abs_f64);
    let min_bid_no = (fv_down * v2.min_bid_fv_ratio).max(min_bid_abs_f64);

    center_yes_f64 = center_yes_f64.max(min_bid_yes);
    center_no_f64 = center_no_f64.max(min_bid_no);

    let center_yes = round_down_to_tick(
        Decimal::from_f64(center_yes_f64).unwrap_or(dec!(0.04)),
        tick_size,
    );
    let center_no = round_down_to_tick(
        Decimal::from_f64(center_no_f64).unwrap_or(dec!(0.04)),
        tick_size,
    );

    let levels = levels_override.unwrap_or(v2.ladder_levels);
    let base_size = v2.level_order_size;
    let decay = v2.ladder_size_decay;
    let tight_sp = v2.ladder_tick_spacing;
    let deep_sp = v2.deep_ladder_tick_spacing;
    let deep_start = v2.deep_ladder_start_level;

    let min_bid_yes_dec = Decimal::from_f64(min_bid_yes).unwrap_or(v2.min_bid_absolute_floor);
    let min_bid_no_dec = Decimal::from_f64(min_bid_no).unwrap_or(v2.min_bid_absolute_floor);

    // When a side is FV-dead but the opposite side has positions, generate a
    // floor bid so PostOnly ask-anchoring can move it near the market ask.
    // This enables pair completion at extreme skew (e.g. UP@0.80 + DOWN@0.18 = 0.98).
    let yes_ladder = if yes_dead && position.no_qty == Decimal::ZERO {
        // Both sides empty, truly dead — no point bidding
        Vec::new()
    } else if yes_dead {
        // YES is FV-dead but we have NO positions → need YES for pairs
        // Place a floor bid; PostOnly will anchor it near the ask
        vec![LadderLevel {
            outcome: Outcome::Yes,
            price: min_bid_yes_dec,
            size: base_size,
        }]
    } else {
        let mut ladder = Vec::with_capacity(levels as usize);
        for i in 0..levels {
            let offset = ladder_price_offset(i, tick_size, tight_sp, deep_sp, deep_start);
            let price = center_yes - offset;
            if price < min_bid_yes_dec || price <= Decimal::ZERO {
                break;
            }
            ladder.push(LadderLevel {
                outcome: Outcome::Yes,
                price,
                size: ladder_size_at_level_and_price(base_size, i, decay, price),
            });
        }
        ladder
    };

    let no_ladder = if no_dead && position.yes_qty == Decimal::ZERO {
        // Both sides empty, truly dead — no point bidding
        Vec::new()
    } else if no_dead {
        // NO is FV-dead but we have YES positions → need NO for pairs
        // Place a floor bid; PostOnly will anchor it near the ask
        vec![LadderLevel {
            outcome: Outcome::No,
            price: min_bid_no_dec,
            size: base_size,
        }]
    } else {
        let mut ladder = Vec::with_capacity(levels as usize);
        for i in 0..levels {
            let offset = ladder_price_offset(i, tick_size, tight_sp, deep_sp, deep_start);
            let price = center_no - offset;
            if price < min_bid_no_dec || price <= Decimal::ZERO {
                break;
            }
            ladder.push(LadderLevel {
                outcome: Outcome::No,
                price,
                size: ladder_size_at_level_and_price(base_size, i, decay, price),
            });
        }
        ladder
    };

    (yes_ladder, no_ladder)
}

/// Compute static deep grid levels for both sides.
///
/// Returns `(yes_levels, no_levels)` — fixed-price bids that are placed once
/// and NOT cancelled on FV moves. Each price appears on both Up and Down.
fn compute_static_deep_grid(
    v2: &V2Config,
    tick_size: Decimal,
) -> (Vec<LadderLevel>, Vec<LadderLevel>) {
    let mut yes_levels = Vec::new();
    let mut no_levels = Vec::new();
    for &price in &v2.deep_static_levels {
        let rounded = round_down_to_tick(price, tick_size);
        if rounded <= Decimal::ZERO {
            continue;
        }
        let size = v2.deep_static_size_at_price(rounded);
        yes_levels.push(LadderLevel {
            outcome: Outcome::Yes,
            price: rounded,
            size,
        });
        no_levels.push(LadderLevel {
            outcome: Outcome::No,
            price: rounded,
            size,
        });
    }
    (yes_levels, no_levels)
}

/// Filter ladder levels by per-order combined cost guard.
///
/// For each YES level: retain only if `level.price + avg_no_cost <= threshold`.
/// Symmetric for NO levels.
///
/// When the opposite side has no position yet, the cost is estimated from the
/// top of the opposite ladder (worst-case pairing cost). This prevents placing
/// orders at period start where both sides' ask-anchored prices combine > 1.00.
///
/// When position is imbalanced, the LIGHT side (which we need to complete pairs)
/// uses a relaxed threshold of $1.00 - this ensures we can always bid on the side
/// we need, accepting any profitable pair rather than demanding a fat spread.
/// When BOTH sides are zero (fresh position), strict max_combined applies to both.
fn apply_combined_cost_guard(
    yes_ladder: &mut Vec<LadderLevel>,
    no_ladder: &mut Vec<LadderLevel>,
    position: &Position,
    max_combined: Decimal,
    light_side_max: Decimal,
    ev_recovery_mode: bool,
) {
    let both_zero = position.yes_qty == Decimal::ZERO && position.no_qty == Decimal::ZERO;

    // Use real avg cost when position exists; estimate from ladder-top when zero.
    // Ladder-top is the highest price we'd pay — conservative worst-case estimate.
    let avg_no = if position.no_qty > Decimal::ZERO {
        position.total_no_spent / position.no_qty
    } else if !no_ladder.is_empty() {
        no_ladder[0].price
    } else {
        Decimal::ZERO
    };
    let avg_yes = if position.yes_qty > Decimal::ZERO {
        position.total_yes_spent / position.yes_qty
    } else if !yes_ladder.is_empty() {
        yes_ladder[0].price
    } else {
        Decimal::ZERO
    };

    let imbalance = position.yes_qty - position.no_qty; // positive = heavy YES

    // Determine thresholds: heavy side gets strict guard, light side gets configurable cap.
    // Previously light side used $1.00, allowing breakeven/losing pairs.
    // Now uses light_side_max (default $0.99) → ensures at least 1c/pair profit.
    let (yes_threshold, no_threshold) = if both_zero {
        // Fresh position: strict max_combined for both sides.
        (max_combined, max_combined)
    } else if imbalance > Decimal::ZERO {
        // Heavy YES: strict for YES (heavy), relaxed for NO (light — we need it)
        (max_combined, light_side_max)
    } else if imbalance < Decimal::ZERO {
        // Heavy NO: relaxed for YES (light — we need it), strict for NO (heavy)
        (light_side_max, max_combined)
    } else {
        // Balanced: use strict max_combined for both sides.
        // Previously used light_side_max ($1.00) which allowed breakeven pairs.
        (max_combined, max_combined)
    };

    // In EV recovery mode (ev_position_negative fired, heavy side already cleared),
    // use light_side_max for the light side instead of skipping the guard entirely.
    // This still allows pair completion but prevents creating deeply unprofitable pairs.
    let (yes_threshold, no_threshold) = if ev_recovery_mode && !both_zero {
        if imbalance > Decimal::ZERO {
            (yes_threshold, light_side_max) // Heavy YES: relax NO (light)
        } else if imbalance < Decimal::ZERO {
            (light_side_max, no_threshold) // Heavy NO: relax YES (light)
        } else {
            (yes_threshold, no_threshold)
        }
    } else {
        (yes_threshold, no_threshold)
    };

    // Pair-completion mode: when one side has filled shares and the other has ZERO,
    // skip the combined cost guard for the empty side. Rationale (verified empirically):
    // - Pairing at ANY combined cost is EV-neutral vs holding one-sided (math proof:
    //   EV(hold) = p*0 + (1-p)*shares = -(1-p)*cost = -avg_cost*(1-FV_other)
    //   EV(pair at FV) = -(FV_other + avg_cost - 1.00)*shares = same)
    // - But pairing REDUCES VARIANCE: converts 96% chance of -$6.63 into guaranteed ~-$2
    // - Session 2 data: period with pair completion → +$0.03, without → -$6.59
    // - The real fix for one-sided accumulation is smaller orders (base_order_shares)
    //   and tighter thresholds (max_share_imbalance), not restricting pair completion.
    let yes_needs_pair_completion =
        position.yes_qty == Decimal::ZERO && position.no_qty > Decimal::ZERO;
    let no_needs_pair_completion =
        position.no_qty == Decimal::ZERO && position.yes_qty > Decimal::ZERO;

    // Apply guard using real avg cost or ladder-top estimates.
    // Skip for the side that needs pair completion — let it bid freely.
    if avg_no > Decimal::ZERO && !yes_needs_pair_completion {
        yes_ladder.retain(|level| level.price + avg_no <= yes_threshold);
    }
    if avg_yes > Decimal::ZERO && !no_needs_pair_completion {
        no_ladder.retain(|level| level.price + avg_yes <= no_threshold);
    }
}

/// Adjust ladder sizes based on position imbalance (cost-weighted).
///
/// Uses `|yes_cost - no_cost|` instead of `|yes_shares - no_shares|` so that
/// cheap deep-grid fills ($0.01-$0.15) don't trigger the same restrictions as
/// expensive regular-ladder fills ($0.30-$0.50).
///
/// - Hard block: if cost imbalance > max_abs_imbalance, clear the heavy side entirely.
/// - Soft reduction: if cost imbalance > soft_threshold, linearly scale down heavy side
///   and boost light side.
///
/// Thresholds are in USDC (time-adjusted, decaying toward resolution).
fn apply_balance_management(
    yes_ladder: &mut Vec<LadderLevel>,
    no_ladder: &mut Vec<LadderLevel>,
    position: &Position,
    max_abs_imbalance: Decimal,
    soft_imbalance_threshold: Decimal,
) {
    let imbalance = position.cost_imbalance(); // positive = heavy YES spending
    let abs_imbalance = imbalance.abs();

    if abs_imbalance > max_abs_imbalance {
        // Hard block: clear the heavy side
        if imbalance > Decimal::ZERO {
            yes_ladder.clear();
        } else {
            no_ladder.clear();
        }
        return;
    }

    if abs_imbalance > soft_imbalance_threshold {
        // Linear scale-down for heavy side, scale-up for light side
        // scale = 1.0 - (abs_imbalance - soft_threshold) / (max_abs - soft_threshold)
        let range = max_abs_imbalance - soft_imbalance_threshold;
        if range > Decimal::ZERO {
            let excess_over_soft = abs_imbalance - soft_imbalance_threshold;
            let scale_down = Decimal::ONE - (excess_over_soft / range);
            let scale_down = scale_down.max(Decimal::ZERO);
            // Boost the light side slightly (inverse)
            let scale_up = Decimal::ONE + (excess_over_soft / range) * dec!(0.5);

            if imbalance > Decimal::ZERO {
                // Heavy YES
                for level in yes_ladder.iter_mut() {
                    level.size = (level.size * scale_down).floor();
                }
                for level in no_ladder.iter_mut() {
                    level.size = (level.size * scale_up).floor();
                }
                // Remove zero-size levels
                yes_ladder.retain(|l| l.size > Decimal::ZERO);
            } else {
                // Heavy NO
                for level in no_ladder.iter_mut() {
                    level.size = (level.size * scale_down).floor();
                }
                for level in yes_ladder.iter_mut() {
                    level.size = (level.size * scale_up).floor();
                }
                no_ladder.retain(|l| l.size > Decimal::ZERO);
            }
        }
    }
}

/// Diff the target ladder against resting orders to determine what to place and cancel.
///
/// Returns `(to_place, to_cancel)`:
/// - `to_place`: levels in the target ladder with no matching resting order at `(outcome, price)`.
/// - `to_cancel`: resting order IDs NOT in the ladder AND further than `stale_distance_ticks`
///   from the ladder's lowest price. Orders above the ladder top are always cancelled
///   (unless they're below `fv_cancel_min_price`, which protects cheap deep levels).
///
/// `deep_stale_ticks`: wider stale distance for orders below `fv_cancel_min_price`.
/// This prevents deep levels from being cancelled just because they're far from the
/// ask-anchored ladder bottom.
fn diff_ladder_vs_resting(
    ladder: &[LadderLevel],
    resting_orders: &HashMap<(Outcome, Decimal), RestingLadderOrder>,
    tick_size: Decimal,
    stale_ticks: u32,
    deep_stale_ticks: u32,
    fv_cancel_min_price: Decimal,
) -> (Vec<LadderLevel>, Vec<OrderId>) {
    // Empty ladder means "don't place anything on this side" — do NOT cancel
    // the opposite side's orders.  Return early with nothing to place/cancel.
    if ladder.is_empty() {
        return (Vec::new(), Vec::new());
    }

    // Levels that need to be placed (no matching resting order)
    let to_place: Vec<LadderLevel> = ladder
        .iter()
        .filter(|level| !resting_orders.contains_key(&(level.outcome, level.price)))
        .cloned()
        .collect();

    // Determine ladder boundaries for stale detection
    let stale_distance = tick_size * Decimal::from(stale_ticks);
    let deep_stale_distance = tick_size * Decimal::from(deep_stale_ticks);

    // Get ladder price range per outcome (safe — ladder is non-empty here)
    let ladder_top = ladder.first().unwrap().price;
    let ladder_bottom = ladder.last().unwrap().price;
    let ladder_outcome = ladder.first().unwrap().outcome;

    // Build set of target prices for quick lookup
    let target_prices: std::collections::HashSet<(Outcome, Decimal)> =
        ladder.iter().map(|l| (l.outcome, l.price)).collect();

    let to_cancel: Vec<OrderId> = resting_orders
        .iter()
        .filter(|((outcome, price), _)| {
            // Only consider orders on the same side as this ladder
            if *outcome != ladder_outcome {
                return false;
            }
            // If this order is in the target ladder, keep it
            if target_prices.contains(&(*outcome, *price)) {
                return false;
            }
            // Deep level protection: orders below fv_cancel_min_price use wider
            // stale distance and are NOT cancelled for being above ladder top
            // (they're cheap and catch panic dumps).
            let is_deep = *price < fv_cancel_min_price;
            // Orders above the ladder top: cancel (FV moved down)
            // but protect deep levels from this aggressive cancel
            if *price > ladder_top && !is_deep {
                return true;
            }
            // Orders below the ladder: cancel if too far from bottom
            // Deep levels get a wider stale distance
            let effective_stale = if is_deep {
                deep_stale_distance
            } else {
                stale_distance
            };
            if *price < ladder_bottom - effective_stale {
                return true;
            }
            false
        })
        .map(|(_, order)| order.order_id.clone())
        .collect();

    (to_place, to_cancel)
}

// ═══════════════════════════════════════════════════════════════════════
// CLOB REST Book Poller (paper mode)
// ═══════════════════════════════════════════════════════════════════════

const CLOB_BOOK_URL: &str = "https://clob.polymarket.com/book";

#[derive(Deserialize)]
struct ClobBookResponse {
    bids: Vec<ClobLevel>,
    asks: Vec<ClobLevel>,
}

#[derive(Deserialize)]
struct ClobLevel {
    price: String,
    size: String,
}

/// Fetch orderbook snapshot from the CLOB REST API (blocking — run in spawn_blocking).
fn fetch_clob_book(token_id: &str) -> Option<OrderBookSnapshot> {
    let url = format!("{CLOB_BOOK_URL}?token_id={token_id}");
    let resp = ureq::get(&url).call().ok()?;
    let body = resp.into_body().read_to_string().ok()?;
    let book: ClobBookResponse = serde_json::from_str(&body).ok()?;

    let mut bids = std::collections::BTreeMap::new();
    for level in &book.bids {
        if let (Ok(p), Ok(s)) = (
            level.price.parse::<Decimal>(),
            level.size.parse::<Decimal>(),
        ) {
            bids.insert(p, s);
        }
    }
    let mut asks = std::collections::BTreeMap::new();
    for level in &book.asks {
        if let (Ok(p), Ok(s)) = (
            level.price.parse::<Decimal>(),
            level.size.parse::<Decimal>(),
        ) {
            asks.insert(p, s);
        }
    }

    Some(OrderBookSnapshot {
        asset_id: token_id.to_string(),
        bids,
        asks,
        timestamp: Utc::now(),
    })
}

// ═══════════════════════════════════════════════════════════════════════
// Binance Kline (candle) REST fetch — for hourly market btc_open
// ═══════════════════════════════════════════════════════════════════════

/// Fetch the candle open price from Binance REST API for a specific timestamp.
/// `symbol` is e.g. "BTCUSDT", `interval` is e.g. "1h",
/// `start_ms` is the candle open time in epoch milliseconds.
/// Returns the candle open price, or None on any failure.
fn fetch_binance_kline_open(symbol: &str, interval: &str, start_ms: i64) -> Option<f64> {
    let url = format!(
        "https://api.binance.com/api/v3/klines?symbol={}&interval={}&startTime={}&limit=1",
        symbol.to_uppercase(),
        interval,
        start_ms
    );
    let resp = ureq::get(&url).call().ok()?;
    let body = resp.into_body().read_to_string().ok()?;
    // Klines response: [[open_time, open, high, low, close, ...], ...]
    let klines: Vec<Vec<serde_json::Value>> = serde_json::from_str(&body).ok()?;
    let candle = klines.first()?;
    // Index 1 is the open price (string)
    candle.get(1)?.as_str()?.parse::<f64>().ok()
}

// ═══════════════════════════════════════════════════════════════════════
// OrchestratorV2
// ═══════════════════════════════════════════════════════════════════════

pub struct OrchestratorV2 {
    config: ValidatedConfig,
    v2: V2Config,
    db: Arc<Database>,
    inventory: Arc<InventoryManager>,
    emergency: Arc<EmergencyHandler>,
    time_manager: TimeManager,
    fill_handler: FillHandler,
    onchain: Arc<OnChainManager>,
    sdk: Option<Arc<SdkClients>>,
    orderbooks: SharedOrderBooks,
    book_notify: BookNotify,
    market_trade_signals: Arc<RwLock<HashMap<ConditionId, VecDeque<MarketTradeSignal>>>>,
    active_markets: HashMap<ConditionId, MarketV2State>,
    alert_tx: mpsc::UnboundedSender<AlertMessage>,
    fill_tx: mpsc::Sender<FillEvent>,
    order_update_tx: mpsc::Sender<(OrderId, String)>,
    strategy_heartbeat_tx: mpsc::Sender<()>,
    shutdown_tx: broadcast::Sender<()>,
    // V2-specific
    asset: Asset,
    asset_price: SharedAssetPrice,
    // Dashboard + paper sim + control
    dashboard: SharedDashboard,
    bot_control: SharedBotControl,
    paper_sim: PaperSimulator,
    start_time: Instant,
    markets_discovered_total: u32,
    // Per-period file logger
    period_logger: PeriodLogger,
    run_id: String,
    // Session-level state for summary tracking
    session_start: String,
    cumulative_session_pnl: Decimal,
    // Volatility circuit breaker hysteresis
    vol_breaker_active: bool,
    // Emergency auto-cancel: true once resting orders have been cancelled after emergency trip
    emergency_orders_cancelled: bool,
    // Per-minute order rate limiter: timestamps of recent order submissions
    order_timestamps: VecDeque<Instant>,
    // Adaptive throttle: pause quote cycles after 429/425/503 errors
    throttle_until: Option<Instant>,
    throttle_backoff_secs: u64,
    /// Current suspend reason (if any). Determines what actions are allowed.
    suspend_reason: Option<TradingSuspendReason>,
    /// Whether we need to reconcile state after an engine restart (425).
    needs_post_restart_reconcile: bool,
    // Watchdog: track last successful quote cycle for liveness detection
    last_quote_cycle: Instant,
    // Canary deployment: track successful periods for auto-escalation
    canary_active: bool,
    canary_successful_periods: u32,
    canary_original_max_position: Decimal,
    // Per-asset rolling performance and auto-guard state
    recent_period_health: VecDeque<PeriodHealthSample>,
    asset_guard_active_until: Option<Instant>,
    last_asset_guard_log: Option<Instant>,
    // Order lifecycle audit trail: ring buffer of completed orders
    order_lifecycle: VecDeque<OrderLifecycle>,
    // Task death monitoring: track critical spawned task handles
    binance_handle: Option<tokio::task::JoinHandle<()>>,
    ws_handles: HashMap<ConditionId, tokio::task::JoinHandle<()>>,
    /// Phase 1: Notify handle for price-shock fast-path wakeup.
    /// Binance WS task signals this when price moves by more than threshold
    /// in a short window, bypassing the normal debounce.
    price_shock_notify: Arc<tokio::sync::Notify>,
    /// Consecutive CLOB heartbeat probe failures. At 3+, all local resting
    /// order state is suspect (Polymarket auto-cancels after ~15s without heartbeat).
    consecutive_heartbeat_failures: u32,
    /// Shared tick_size updates from WS tick_size_change events.
    /// WS tasks write here; main loop reads + applies to MarketV2State.
    ws_tick_sizes: SharedTickSizes,
    /// VPIN toxic flow tracker (per-asset, resets on period boundaries).
    vpin_tracker: crate::vpin::VpinTracker,
    /// Pipeline latency tracker (shared across all orchestrators + web).
    latency_tracker: Arc<crate::latency::LatencyTracker>,
}

impl OrchestratorV2 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        asset: Asset,
        config: ValidatedConfig,
        v2_config: V2Config,
        run_id: String,
        db: Arc<Database>,
        inventory: Arc<InventoryManager>,
        emergency: Arc<EmergencyHandler>,
        onchain: Arc<OnChainManager>,
        sdk: Option<Arc<SdkClients>>,
        alert_tx: mpsc::UnboundedSender<AlertMessage>,
        fill_tx: mpsc::Sender<FillEvent>,
        order_update_tx: mpsc::Sender<(OrderId, String)>,
        strategy_heartbeat_tx: mpsc::Sender<()>,
        shutdown_tx: broadcast::Sender<()>,
        dashboard: SharedDashboard,
        bot_control: SharedBotControl,
        latency_tracker: Arc<crate::latency::LatencyTracker>,
    ) -> Self {
        let time_manager = TimeManager::new(config.resolution_safety_margin_secs);
        let fill_handler = FillHandler::new(&config, inventory.clone(), db.clone());
        let asset_price = Arc::new(RwLock::new(AssetPriceState::new(v2_config.vol_window_secs)));
        let log_dir = format!("{}/{}", config.mode.artifact_root(), asset.display_name());

        let canary_active = config.canary_mode;
        let canary_original_max_position = config.max_position_per_market;

        let mut cfg = config;
        // If canary mode, override max_position with canary budget
        if canary_active {
            if let Some(budget) = cfg.canary_budget {
                info!(
                    canary_budget = %budget,
                    full_budget = %canary_original_max_position,
                    canary_periods = cfg.canary_periods,
                    "[v2] Canary mode active — reduced budget until {} successful periods",
                    cfg.canary_periods
                );
                cfg.max_position_per_market = budget;
            }
        }

        let vpin_cfg = crate::vpin::VpinConfig {
            bucket_volume: v2_config.vpin_bucket_volume,
            n_buckets: v2_config.vpin_n_buckets,
            widen_threshold: v2_config.vpin_widen_threshold,
            pullback_threshold: v2_config.vpin_pullback_threshold,
            max_spread_multiplier: v2_config.vpin_max_spread_multiplier,
        };

        Self {
            config: cfg,
            v2: v2_config,
            db,
            inventory,
            emergency,
            time_manager,
            fill_handler,
            onchain,
            sdk,
            orderbooks: Arc::new(RwLock::new(HashMap::new())),
            book_notify: Arc::new(tokio::sync::Notify::new()),
            market_trade_signals: Arc::new(RwLock::new(HashMap::new())),
            active_markets: HashMap::new(),
            alert_tx,
            fill_tx,
            order_update_tx,
            strategy_heartbeat_tx,
            shutdown_tx,
            asset,
            asset_price,
            dashboard,
            bot_control,
            paper_sim: PaperSimulator::new(),
            start_time: Instant::now(),
            markets_discovered_total: 0,
            period_logger: PeriodLogger::new(&log_dir, &run_id),
            run_id,
            session_start: Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
            cumulative_session_pnl: Decimal::ZERO,
            vol_breaker_active: false,
            emergency_orders_cancelled: false,
            order_timestamps: VecDeque::new(),
            throttle_until: None,
            throttle_backoff_secs: 2,
            suspend_reason: None,
            needs_post_restart_reconcile: false,
            last_quote_cycle: Instant::now(),
            canary_active,
            canary_successful_periods: 0,
            canary_original_max_position,
            recent_period_health: VecDeque::with_capacity(64),
            asset_guard_active_until: None,
            last_asset_guard_log: None,
            order_lifecycle: VecDeque::with_capacity(500),
            binance_handle: None,
            ws_handles: HashMap::new(),
            price_shock_notify: Arc::new(tokio::sync::Notify::new()),
            consecutive_heartbeat_failures: 0,
            ws_tick_sizes: Arc::new(RwLock::new(HashMap::new())),
            vpin_tracker: crate::vpin::VpinTracker::new(vpin_cfg),
            latency_tracker,
        }
    }

    pub fn orderbooks(&self) -> SharedOrderBooks {
        self.orderbooks.clone()
    }

    pub fn book_notify(&self) -> BookNotify {
        self.book_notify.clone()
    }

    pub fn shutdown_tx(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }

    fn mode_label(&self) -> &'static str {
        self.config.mode.as_str()
    }

    fn update_period_telemetry(
        &mut self,
        condition_id: &str,
        position: &Position,
        yes_ladder: &[LadderLevel],
        no_ladder: &[LadderLevel],
        suppression_reasons: &[&str],
    ) {
        if let Some(ms) = self.active_markets.get_mut(condition_id) {
            let abs_excess = (position.yes_qty - position.no_qty).abs();
            if abs_excess > ms.max_excess_seen {
                ms.max_excess_seen = abs_excess;
            }
            ms.max_quote_levels_yes = ms.max_quote_levels_yes.max(yes_ladder.len() as u32);
            ms.max_quote_levels_no = ms.max_quote_levels_no.max(no_ladder.len() as u32);
            for reason in suppression_reasons {
                *ms.suppression_reason_counts
                    .entry((*reason).to_string())
                    .or_insert(0) += 1;
            }
        }
    }

    fn suppression_reason_counts_csv(ms: &MarketV2State) -> String {
        let mut entries: Vec<_> = ms.suppression_reason_counts.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        entries
            .into_iter()
            .map(|(reason, count)| format!("{reason}:{count}"))
            .collect::<Vec<_>>()
            .join("|")
    }

    fn settlement_mode(ms: &MarketV2State, position: &Position) -> String {
        let mut modes = Vec::new();
        if ms.sell_realized_pnl != Decimal::ZERO {
            modes.push("sell");
        }
        if ms.cumulative_merged_pairs > Decimal::ZERO || ms.merge_realized_pnl != Decimal::ZERO {
            modes.push("merge");
        }
        if position.yes_qty > Decimal::ZERO || position.no_qty > Decimal::ZERO {
            modes.push("redeem");
        }
        if modes.is_empty() {
            "none".to_string()
        } else {
            modes.join("+")
        }
    }

    fn note_cancel_all_event(&mut self, condition_id: &str) {
        if let Some(ms) = self.active_markets.get_mut(condition_id) {
            ms.cancel_all_count += 1;
        }
    }

    fn directional_trade_flow_summary(&self, condition_id: &str) -> DirectionalTradeFlowSummary {
        let long_window = Duration::from_secs(self.v2.directional_skew_flow_window_secs.max(1));
        let short_window =
            Duration::from_secs(self.v2.directional_skew_short_flow_window_secs.max(1));
        let now = Instant::now();
        let signals = self.market_trade_signals.read();
        let Some(entries) = signals.get(condition_id) else {
            return DirectionalTradeFlowSummary::default();
        };

        let mut summary = DirectionalTradeFlowSummary::default();
        for signal in entries.iter().rev() {
            let age = now.saturating_duration_since(signal.received_at);
            if age > long_window {
                break;
            }
            summary.long_flow_up_notional += signal.signed_up_notional;
            summary.large_flow_up_notional += signal.signed_up_large_notional;
            if age <= short_window {
                summary.short_flow_up_notional += signal.signed_up_notional;
            }
        }
        summary
    }

    fn directional_skew_decision(
        &self,
        condition_id: &str,
        remaining_secs: f64,
        btc_open: f64,
        btc_current: f64,
        yes_book: &OrderBookSnapshot,
        no_book: &OrderBookSnapshot,
    ) -> Option<DirectionalSkewDecision> {
        if !self.v2.directional_skew_enabled {
            return None;
        }

        let flow_summary = self.directional_trade_flow_summary(condition_id);
        let up_best_imbalance = yes_book.best_level_imbalance();
        let down_best_imbalance = no_book.best_level_imbalance();
        let snapshot = DirectionalSkewSnapshot {
            spot_ret_from_start_bps: spot_return_bps(btc_open, btc_current),
            long_flow_up_notional: flow_summary.long_flow_up_notional,
            short_flow_up_notional: flow_summary.short_flow_up_notional,
            large_flow_up_notional: flow_summary.large_flow_up_notional,
            up_best_imbalance,
            imbalance_diff: up_best_imbalance - down_best_imbalance,
        };

        evaluate_directional_skew(&self.v2, remaining_secs, snapshot)
    }

    /// Spawn the Binance WS price feed. Must be called before `run()`.
    pub fn start_binance_feed(&mut self) {
        let state = self.asset_price.clone();
        let url = format!(
            "wss://stream.binance.com:9443/ws/{}@trade",
            self.asset.binance_symbol()
        );
        let asset_name = self.asset.display_name().to_string();
        let shock_notify = self.price_shock_notify.clone();
        // Use the smaller threshold (5m) as the shock detection trigger.
        // The orchestrator will decide per-market whether to act.
        let shock_threshold = self.v2.price_shock_threshold_5m;
        let handle = tokio::spawn(binance_ws_loop(
            state,
            url,
            asset_name,
            shock_notify,
            shock_threshold,
        ));
        self.binance_handle = Some(handle);
        info!("[v2-{}] Binance price feed started", self.asset);
    }

    /// Spawn the optional RTDS Chainlink oracle feed for cross-validation.
    /// Failure is non-fatal — Binance remains the primary price source.
    pub fn start_rtds_feed(&mut self) {
        let pair = match self.asset {
            Asset::BTC => "btc/usd",
            Asset::ETH => "eth/usd",
            Asset::SOL => "sol/usd",
            Asset::XRP => "xrp/usd",
        };
        let state = self.asset_price.clone();
        tokio::spawn(rtds_chainlink_loop(state, pair.to_string()));
        info!(
            "[v2-{}] RTDS Chainlink feed started (pair: {})",
            self.asset, pair
        );
    }

    // ─── Main Event Loop ─────────────────────────────────────────────

    pub async fn run(
        &mut self,
        mut market_rx: mpsc::Receiver<TrackedMarket>,
        mut fill_rx: mpsc::Receiver<FillEvent>,
        mut order_update_rx: mpsc::Receiver<(OrderId, String)>,
        mut shutdown_rx: broadcast::Receiver<()>,
        shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
    ) {
        let mut quote_interval =
            tokio::time::interval(Duration::from_millis(self.v2.quote_refresh_ms));
        let mut health_interval =
            tokio::time::interval(Duration::from_secs(self.config.health_check_interval_secs));
        let mut book_snapshot_interval = tokio::time::interval(Duration::from_secs(5));
        let mut flush_interval = tokio::time::interval(Duration::from_secs(30));
        // Reduced from 300s to 60s for live trading hardening.
        // 300s was too slow: for 5-minute markets, reconciliation might never fire.
        // Also catches phantom orders (orders we think are resting but aren't) faster.
        let mut recon_interval = tokio::time::interval(Duration::from_secs(60));
        let mut rest_validation_interval = tokio::time::interval(Duration::from_secs(60));
        // Debounce: minimum 50ms between event-driven quote cycles to prevent CPU thrash
        let mut last_event_cycle = Instant::now() - Duration::from_secs(1);
        const EVENT_DEBOUNCE: Duration = Duration::from_millis(50);

        info!("[v2] Orchestrator main loop starting");
        let _ = self
            .alert_tx
            .send(AlertMessage::System("V2 Bot started".into()));

        loop {
            // Check if dashboard requested shutdown
            if shutdown_flag.load(std::sync::atomic::Ordering::Relaxed) {
                info!("[v2] Dashboard shutdown signal received");
                self.graceful_shutdown().await;
                break;
            }

            // ── External kill file check ──
            if std::path::Path::new(&self.v2.kill_file_path).exists() {
                error!(
                    path = %self.v2.kill_file_path,
                    "[v2] Kill file detected — triggering emergency shutdown"
                );
                self.emergency
                    .trigger_emergency(crate::risk::emergency::EmergencyTrigger::CtrlC);
                self.graceful_shutdown().await;
                break;
            }

            // CRITICAL: `biased;` ensures deterministic arm priority.
            // Without it, Tokio's select! uses random fairness — fills and order updates
            // can be starved by timer ticks. Missing a fill causes inventory drift,
            // which is the most dangerous state for a live market maker.
            // Priority order: fills > order updates > shutdown > timers > housekeeping.
            tokio::select! {
                biased;

                Some(fill) = fill_rx.recv() => {
                    // HIGHEST PRIORITY: fills represent events that already happened on the
                    // exchange. Dropping or delaying fill processing during emergency causes
                    // inventory drift at the worst possible time.
                    let fill_start = Instant::now();
                    self.handle_fill_event(fill).await;
                    self.latency_tracker.record("fill_process", fill_start.elapsed().as_secs_f64() * 1000.0);
                }
                Some((order_id, status)) = order_update_rx.recv() => {
                    // HIGH PRIORITY: order state changes (cancelled, matched, etc.)
                    // must be processed promptly to keep resting_orders in sync.
                    self.handle_order_update(&order_id, &status).await;
                }
                _ = shutdown_rx.recv() => {
                    info!("[v2] Orchestrator received shutdown signal");
                    self.graceful_shutdown().await;
                    break;
                }
                _ = quote_interval.tick() => {
                    // Always update dashboard so the UI stays alive even during emergency
                    self.update_dashboard();

                    // ── Emergency circuit breaker: cancel all resting orders on first detection ──
                    if self.emergency.is_emergency() {
                        if !self.emergency_orders_cancelled {
                            error!("[v2] Emergency triggered — cancelling ALL resting orders");
                            self.cancel_all_resting_orders().await;
                            if let Some(sdk) = &self.sdk {
                                if let Err(e) = sdk.cancel_all_orders().await {
                                    error!("[v2] Emergency cancel-all failed: {e}");
                                }
                            }

                            // ── Emergency position exit: attempt to sell all inventory ──
                            // After cancelling orders, try to dump positions at market.
                            // This prevents uncontrolled exposure bleeding to resolution.
                            if let Some(sdk) = &self.sdk {
                                let market_ids: Vec<String> = self.active_markets.keys().cloned().collect();
                                for cid in &market_ids {
                                    let (market, position) = {
                                        let ms = match self.active_markets.get(cid.as_str()) {
                                            Some(ms) => ms,
                                            None => continue,
                                        };
                                        (ms.market.clone(), self.inventory.get_position(cid))
                                    };
                                    let position = match position {
                                        Some(p) => p,
                                        None => continue,
                                    };

                                    // Read best bids from orderbook snapshot. Lock MUST be
                                    // dropped (via block scope) before any .await — parking_lot
                                    // guards are !Send and can't cross await points.
                                    let (yes_best_bid, no_best_bid) = {
                                        let books = self.orderbooks.read();
                                        let yb = books.get(&market.token_id_yes)
                                            .and_then(|b| b.best_bid())
                                            .map(|(p, _)| p);
                                        let nb = books.get(&market.token_id_no)
                                            .and_then(|b| b.best_bid())
                                            .map(|(p, _)| p);
                                        (yb, nb)
                                    }; // guard dropped here

                                    // Try to sell YES excess
                                    if position.yes_qty >= dec!(5) {
                                        if let Some(bid) = yes_best_bid {
                                            let sell_price = (bid - market.tick_size).max(market.tick_size);
                                            let sell_size = position.yes_qty;
                                            info!(
                                                condition_id = %cid,
                                                side = "YES",
                                                size = %sell_size,
                                                price = %sell_price,
                                                "[v2] Emergency position exit — selling"
                                            );
                                            match sdk.place_emergency_sell(
                                                &market.token_id_yes, sell_price, sell_size, market.tick_size
                                            ).await {
                                                Ok(()) => info!("[v2] Emergency YES sell placed for {cid}"),
                                                Err(e) => warn!("[v2] Emergency YES sell failed for {cid}: {e}"),
                                            }
                                        }
                                    }

                                    // Try to sell NO excess
                                    if position.no_qty >= dec!(5) {
                                        if let Some(bid) = no_best_bid {
                                            let sell_price = (bid - market.tick_size).max(market.tick_size);
                                            let sell_size = position.no_qty;
                                            info!(
                                                condition_id = %cid,
                                                side = "NO",
                                                size = %sell_size,
                                                price = %sell_price,
                                                "[v2] Emergency position exit — selling"
                                            );
                                            match sdk.place_emergency_sell(
                                                &market.token_id_no, sell_price, sell_size, market.tick_size
                                            ).await {
                                                Ok(()) => info!("[v2] Emergency NO sell placed for {cid}"),
                                                Err(e) => warn!("[v2] Emergency NO sell failed for {cid}: {e}"),
                                            }
                                        }
                                    }
                                }
                            }

                            self.emergency_orders_cancelled = true;
                        }
                        // Reset watchdog — the bot is alive, just in emergency mode
                        self.last_quote_cycle = Instant::now();
                        // Send heartbeat so emergency monitor doesn't trigger StrategyHang
                        // while we wait for the cooldown to clear the emergency flag.
                        let _ = self.strategy_heartbeat_tx.try_send(());
                        continue;
                    } else if self.emergency_orders_cancelled {
                        // Emergency cleared — reset flag for next emergency
                        info!("[v2] Emergency cleared — resuming normal operation");
                        self.emergency_orders_cancelled = false;
                    }

                    // ── Bot control state check ──
                    let bot_status = self.bot_control.read().status;
                    match bot_status {
                        BotStatus::Paused => {
                            // Paused: skip trading, dashboard still updates.
                            // FIX: Still run phase progression so Closing/Resolved
                            // transitions happen for any active markets.
                            let market_ids: Vec<String> = self.active_markets.keys().cloned().collect();
                            for cid in &market_ids {
                                if let Some(ms) = self.active_markets.get(cid.as_str()) {
                                    let market = ms.market.clone();
                                    let phase = self.time_manager.phase_for_duration(
                                        market.end_date,
                                        market.effective_duration_secs_15m_fallback(),
                                    );
                                    if phase == MarketPhase::Closing || phase == MarketPhase::Resolved {
                                        self.handle_market_closing(cid, phase).await;
                                    }
                                }
                            }
                            // Reset watchdog — the bot is alive, just intentionally paused.
                            self.last_quote_cycle = Instant::now();
                            continue;
                        }
                        BotStatus::Stopping => {
                            // Stopping: check if current market period ended
                            if self.active_markets.is_empty() {
                                // No active markets — transition to Paused
                                info!("[v2] Stop complete — no active markets, transitioning to Paused");
                                self.bot_control.write().status = BotStatus::Paused;
                                self.last_quote_cycle = Instant::now();
                                continue;
                            }
                            // FIX: Still have active markets — cancel new quotes but
                            // MUST run phase progression so Closing/Resolved transitions
                            // happen. Without this, markets can never resolve and Stopping
                            // deadlocks forever.
                            self.cancel_all_resting_orders().await;
                            // Run phase progression for all active markets
                            let market_ids: Vec<String> = self.active_markets.keys().cloned().collect();
                            for cid in &market_ids {
                                if let Some(ms) = self.active_markets.get(cid.as_str()) {
                                    let market = ms.market.clone();
                                    let phase = self.time_manager.phase_for_duration(
                                        market.end_date,
                                        market.effective_duration_secs_15m_fallback(),
                                    );
                                    if phase == MarketPhase::Closing || phase == MarketPhase::Resolved {
                                        self.handle_market_closing(cid, phase).await;
                                    }
                                }
                            }
                            self.last_quote_cycle = Instant::now();
                            continue;
                        }
                        BotStatus::Running => {
                            // Normal operation
                        }
                    }

                    let _quote_start = Instant::now();
                    self.quote_refresh_cycle_v2().await;
                    let elapsed = _quote_start.elapsed().as_secs_f64() * 1000.0;
                    metrics::histogram!("quote_cycle_duration_ms", "asset" => self.asset.display_name()).record(elapsed);
                    self.latency_tracker.record("quote_cycle", elapsed);
                    self.last_quote_cycle = Instant::now();
                    last_event_cycle = Instant::now();
                    let _ = self.strategy_heartbeat_tx.try_send(());
                }
                _ = self.book_notify.notified() => {
                    // Event-driven: WS delivered fresh orderbook data — run quote cycle immediately.
                    // Debounce: skip if we ran a cycle very recently (prevents CPU thrash on rapid WS updates).
                    if last_event_cycle.elapsed() < EVENT_DEBOUNCE {
                        continue;
                    }
                    // Reset the timer so it doesn't double-fire right after this event-driven cycle.
                    quote_interval.reset();

                    self.update_dashboard();
                    if self.emergency.is_emergency() {
                        self.last_quote_cycle = Instant::now();
                        continue;
                    }
                    let bot_status = self.bot_control.read().status;
                    if bot_status != BotStatus::Running {
                        self.last_quote_cycle = Instant::now();
                        continue;
                    }

                    let _quote_start = Instant::now();
                    self.quote_refresh_cycle_v2().await;
                    let elapsed = _quote_start.elapsed().as_secs_f64() * 1000.0;
                    metrics::histogram!("quote_cycle_duration_ms", "asset" => self.asset.display_name()).record(elapsed);
                    self.latency_tracker.record("quote_cycle", elapsed);
                    self.last_quote_cycle = Instant::now();
                    last_event_cycle = Instant::now();
                    let _ = self.strategy_heartbeat_tx.try_send(());
                }
                _ = self.price_shock_notify.notified() => {
                    // Phase 1: Price-shock fast-path — Binance detected a large price move.
                    // Bypass EVENT_DEBOUNCE and run an immediate quote cycle to requote.
                    // The Binance feed fires at the 5m threshold. For 15m-only markets,
                    // check if the move exceeds the 15m threshold before proceeding.
                    quote_interval.reset();

                    self.update_dashboard();
                    if self.emergency.is_emergency() {
                        self.last_quote_cycle = Instant::now();
                        continue;
                    }
                    let bot_status = self.bot_control.read().status;
                    if bot_status != BotStatus::Running {
                        self.last_quote_cycle = Instant::now();
                        continue;
                    }

                    // Duration-aware shock filtering: use the threshold for the
                    // shortest active market duration (most sensitive).
                    let min_duration_secs = self.active_markets.values()
                        .map(|ms| ms.market.effective_duration_secs_15m_fallback())
                        .min()
                        .unwrap_or(900);
                    let shock_threshold = if min_duration_secs <= 300 {
                        self.v2.price_shock_threshold_5m
                    } else if min_duration_secs <= 900 {
                        self.v2.price_shock_threshold_15m
                    } else {
                        self.v2.price_shock_threshold_60m
                    };
                    {
                        let price_delta = self.asset_price.read()
                            .price_change_over(10)
                            .map(|d| d.abs())
                            .unwrap_or(0.0);
                        if price_delta < shock_threshold {
                            debug!(
                                price_delta,
                                threshold = shock_threshold,
                                "[v2-{}] Price shock below duration threshold — skipping requote",
                                self.asset
                            );
                            self.last_quote_cycle = Instant::now();
                            continue;
                        }
                    }

                    info!("[v2-{}] Price-shock wakeup — running emergency requote", self.asset);
                    metrics::counter!("price_shock_wakeups", "asset" => self.asset.display_name()).increment(1);

                    // Cancel THIS asset's resting orders only (not account-wide).
                    // cancel_all_orders() is account-wide and would nuke other assets' quotes.
                    if self.v2.price_shock_use_cancel_all {
                        self.cancel_all_resting_orders().await;
                    }

                    let _quote_start = Instant::now();
                    self.quote_refresh_cycle_v2().await;
                    let elapsed = _quote_start.elapsed().as_secs_f64() * 1000.0;
                    metrics::histogram!("quote_cycle_duration_ms", "asset" => self.asset.display_name()).record(elapsed);
                    self.latency_tracker.record("quote_cycle", elapsed);
                    self.last_quote_cycle = Instant::now();
                    last_event_cycle = Instant::now();
                    let _ = self.strategy_heartbeat_tx.try_send(());
                }
                Some(market) = market_rx.recv() => {
                    let disc_start = Instant::now();
                    self.handle_market_discovered(market).await;
                    self.latency_tracker.record("market_discovery", disc_start.elapsed().as_secs_f64() * 1000.0);
                }
                _ = health_interval.tick() => {
                    self.health_check().await;
                }
                _ = book_snapshot_interval.tick() => {
                    self.log_book_snapshots();
                }
                _ = flush_interval.tick() => {
                    self.period_logger.flush_all();
                }
                _ = recon_interval.tick() => {
                    self.reconcile_positions().await;
                }
                _ = rest_validation_interval.tick() => {
                    self.validate_orderbooks().await;
                }
            }
        }
    }

    // ─── V2 Quote Refresh Cycle ──────────────────────────────────────
    //
    // This is the core v2 change: fair-value pricing with Binance feed.

    async fn quote_refresh_cycle_v2(&mut self) {
        // ── Per-minute order rate limiter ──
        if self.v2.max_orders_per_minute > 0 {
            let cutoff = Instant::now() - Duration::from_secs(60);
            while self
                .order_timestamps
                .front()
                .map(|&t| t < cutoff)
                .unwrap_or(false)
            {
                self.order_timestamps.pop_front();
            }
            if self.order_timestamps.len() >= self.v2.max_orders_per_minute as usize {
                warn!(
                    count = self.order_timestamps.len(),
                    max = self.v2.max_orders_per_minute,
                    "[v2] Rate limit: order count in last 60s exceeds max — skipping quote cycle"
                );
                return;
            }
        }

        // ── Adaptive throttle (429 / 425 / 503) ──
        if let Some(until) = self.throttle_until {
            if Instant::now() < until {
                let reason_str = self
                    .suspend_reason
                    .map(|r| r.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                debug!(
                    secs_remaining = until.saturating_duration_since(Instant::now()).as_secs(),
                    reason = %reason_str,
                    "[v2-{}] Throttled ({}) — skipping quote cycle",
                    self.asset, reason_str
                );
                return;
            }
            // Throttle expired — clear it
            let was_restart = self.suspend_reason == Some(TradingSuspendReason::EngineRestart);
            self.throttle_until = None;
            self.suspend_reason = None;

            // After engine restart (425), run reconciliation before resuming trading
            if was_restart && self.needs_post_restart_reconcile {
                info!(
                    "[v2-{}] Post-425 cooldown expired — running reconciliation",
                    self.asset
                );
                self.needs_post_restart_reconcile = false;
                self.reconcile_positions().await;
            }
        }

        // Snapshot Binance state (blended sigma: alpha*rv_1m + (1-alpha)*rv_all)
        let (btc_current, vol_all, vol_1m, sample_count, price_stale) = {
            let bs = self.asset_price.read();
            (
                bs.current_price,
                bs.realized_vol_per_sec(),
                bs.realized_vol_over(60),
                bs.sample_count(),
                bs.is_price_stale(Duration::from_secs(MAX_BTC_PRICE_AGE_SECS)),
            )
        };

        let btc_current = match btc_current {
            Some(p) if !price_stale => p,
            Some(_) => {
                warn!(
                    "[v2] Binance price stale (>{}s since last update) — pausing trading",
                    MAX_BTC_PRICE_AGE_SECS
                );
                return;
            }
            None => {
                info!("[v2] Waiting for Binance price feed...");
                return;
            }
        };

        // Sigma warm-up guard: wait for enough samples to avoid noisy estimates
        if sample_count < self.v2.min_sigma_samples as usize {
            info!(
                samples = sample_count,
                min = self.v2.min_sigma_samples,
                "[v2] Waiting for sigma warmup"
            );
            return;
        }

        let sigma = match vol_all {
            Some(v_all) => {
                // Blend 1-minute vol with full-window vol for faster response to spikes
                let v_1m = vol_1m.unwrap_or(v_all);
                let alpha = self.v2.sigma_blend_alpha;
                let blended = alpha * v_1m + (1.0 - alpha) * v_all;
                if blended >= self.v2.min_vol_per_sec {
                    blended
                } else {
                    let floor_vol = self.v2.min_vol_per_sec;
                    info!(
                        actual_vol = blended,
                        floor = floor_vol,
                        "[v2] Vol below threshold, using floor vol"
                    );
                    floor_vol
                }
            }
            None => {
                info!("[v2] Waiting for vol estimate (need 10 samples)...");
                return;
            }
        };

        // ── Volatility circuit breaker (hysteresis) ──
        // Suppress new order placement when sigma is too high. Resting orders
        // are kept alive — they may still fill, and cancel/sell-back/pair-completion
        // continue to run normally.
        let vol_breaker_suppressing = if self.v2.max_sigma > 0.0 {
            if self.vol_breaker_active {
                // Already tripped: only resume when sigma drops below threshold * resume_factor
                let resume_threshold = self.v2.max_sigma * self.v2.max_sigma_resume_factor;
                if sigma < resume_threshold {
                    info!(
                        sigma = format!("{sigma:.8}"),
                        resume_threshold = format!("{resume_threshold:.8}"),
                        "[v2] Vol breaker: sigma dropped below resume threshold — resuming"
                    );
                    self.vol_breaker_active = false;
                    false
                } else {
                    true
                }
            } else if sigma > self.v2.max_sigma {
                // Trip the breaker
                warn!(
                    sigma = format!("{sigma:.8}"),
                    max_sigma = format!("{:.8}", self.v2.max_sigma),
                    "[v2] Vol breaker: sigma exceeds max — suppressing new orders"
                );
                self.vol_breaker_active = true;
                true
            } else {
                false
            }
        } else {
            false
        };
        let asset_guard_suppressing = self.asset_guard_suppressing();

        let market_ids: Vec<ConditionId> = self.active_markets.keys().cloned().collect();

        for condition_id in market_ids {
            let (market, period_name, btc_open_snapshot, reconciliation_blocked) =
                match self.active_markets.get(&condition_id) {
                    Some(ms) => (
                        ms.market.clone(),
                        ms.period_name.clone(),
                        ms.btc_open,
                        ms.reconciliation_blocked,
                    ),
                    None => continue,
                };
            let market_total_secs = market_total_secs_f64(&market);
            let duration_mins = (market_total_secs / 60.0).round() as u32;
            let resolved_ladder_levels = self.v2.ladder_levels_for_duration(duration_mins);

            // ── Continuous liquidity tapering: reduce levels as settlement approaches ──
            // remaining_secs not yet computed here, so use a quick inline calc
            let taper_remaining = self.time_manager.seconds_remaining(market.end_date) as f64;
            let (tapered_levels, taper_size_factor) = if self.v2.taper_enabled {
                let taper = liquidity_taper_factor(
                    taper_remaining,
                    market_total_secs,
                    self.v2.taper_min_factor,
                );
                let levels = ((resolved_ladder_levels as f64) * taper).ceil().max(1.0) as u32;
                (levels, taper)
            } else {
                (resolved_ladder_levels, 1.0)
            };

            // Lazy btc_open: if Binance wasn't ready when market was discovered,
            // capture it now on first available price.
            let btc_open = match btc_open_snapshot {
                Some(p) => p,
                None => {
                    // Set it now and log
                    if let Some(ms_mut) = self.active_markets.get_mut(&condition_id) {
                        ms_mut.btc_open = Some(btc_current);
                        info!(
                            condition_id = %condition_id,
                            btc_open = btc_current,
                            "[v2] Late btc_open capture"
                        );
                    }
                    btc_current
                }
            };

            // Phase check (duration-aware for 5-min / 15-min markets)
            let phase = self.time_manager.phase_for_duration(
                market.end_date,
                market.effective_duration_secs_15m_fallback(),
            );
            if phase == MarketPhase::Closing || phase == MarketPhase::Resolved {
                self.handle_market_closing(&condition_id, phase).await;
                continue;
            }

            // Reconciliation safety stop: do not trade markets with position drift.
            if reconciliation_blocked {
                let now = Instant::now();
                let mut should_log = false;
                let mut reason: Option<String> = None;
                if let Some(ms_mut) = self.active_markets.get_mut(&condition_id) {
                    let cooldown = Duration::from_secs(30);
                    should_log = ms_mut
                        .last_reconciliation_block_log
                        .map(|last| now.duration_since(last) >= cooldown)
                        .unwrap_or(true);
                    if should_log {
                        ms_mut.last_reconciliation_block_log = Some(now);
                        reason = ms_mut.reconciliation_block_reason.clone();
                    }
                }
                if should_log {
                    warn!(
                        condition_id = %condition_id,
                        reason = ?reason,
                        "[v2] Trading paused for market due to reconciliation mismatch"
                    );
                }
                continue;
            }

            // Need orderbooks for depth checking
            let (yes_book, no_book) = {
                let books = self.orderbooks.read();
                let yes = books.get(&market.token_id_yes).cloned();
                let no = books.get(&market.token_id_no).cloned();
                match (yes, no) {
                    (Some(y), Some(n)) => (y, n),
                    _ => {
                        debug!(
                            condition_id = %condition_id,
                            has_yes = books.contains_key(&market.token_id_yes),
                            has_no = books.contains_key(&market.token_id_no),
                            "[v2] Skipping tick: orderbook data missing"
                        );
                        continue;
                    }
                }
            };

            // Check book freshness
            let now_utc = Utc::now();
            let yes_age_ms = (now_utc - yes_book.timestamp).num_milliseconds();
            let no_age_ms = (now_utc - no_book.timestamp).num_milliseconds();
            if yes_age_ms > self.config.max_book_age_ms as i64
                || no_age_ms > self.config.max_book_age_ms as i64
            {
                debug!(
                    condition_id = %condition_id,
                    yes_age_ms, no_age_ms,
                    max_age_ms = self.config.max_book_age_ms,
                    "[v2] Skipping tick: orderbook stale"
                );
                continue;
            }

            // ── Market readiness gate: warmup delay + minimum book depth ──
            {
                let ms_ref = self.active_markets.get(&condition_id);
                let (book_ready, warmup_elapsed) = ms_ref
                    .map(|ms| (ms.book_ready, ms.discovered_at.elapsed()))
                    .unwrap_or((false, Duration::ZERO));

                if !book_ready {
                    let duration_mins = market.effective_duration_minutes_15m_fallback();
                    let warmup_secs = self.v2.market_warmup_secs_for_duration(duration_mins);
                    let warmup_ok = warmup_elapsed >= Duration::from_secs(warmup_secs);
                    let has_depth = yes_book.best_ask().is_some()
                        && yes_book.best_bid().is_some()
                        && no_book.best_ask().is_some()
                        && no_book.best_bid().is_some();

                    if warmup_ok && has_depth {
                        if let Some(ms_mut) = self.active_markets.get_mut(&condition_id) {
                            ms_mut.book_ready = true;
                            info!(
                                condition_id = %condition_id,
                                warmup_ms = warmup_elapsed.as_millis() as u64,
                                "[v2] Market ready: warmup complete + orderbook depth confirmed"
                            );
                        }
                    } else {
                        debug!(
                            condition_id = %condition_id,
                            warmup_ok,
                            has_depth,
                            warmup_elapsed_ms = warmup_elapsed.as_millis() as u64,
                            "[v2] Skipping tick: market not ready (warmup={warmup_ok}, depth={has_depth})"
                        );
                        continue;
                    }
                }
            }

            // ── Trading window gate: observation → active → wind-down ──
            // Compute elapsed_pct for this market and determine phase.
            // During observation: skip all order placement (data is collecting).
            // During wind-down: only sell/exit logic runs (handled later via flag).
            let tw_remaining_secs = (market.end_date - Utc::now()).num_seconds().max(0) as f64;
            let tw_elapsed_pct = elapsed_pct_from_remaining(tw_remaining_secs, market_total_secs);
            let in_observation_phase = tw_elapsed_pct < self.v2.trading_window_start_pct;
            let in_wind_down_phase = tw_elapsed_pct > self.v2.trading_window_end_pct;

            if in_observation_phase {
                debug!(
                    condition_id = %condition_id,
                    elapsed_pct = format!("{:.1}%", tw_elapsed_pct * 100.0),
                    window_start = format!("{:.0}%", self.v2.trading_window_start_pct * 100.0),
                    "[v2] Observation phase — collecting data, no orders"
                );
                metrics::counter!("trading_window_skip_total", "asset" => self.asset.display_name(), "phase" => "observation").increment(1);
                continue;
            }

            // ── Apply WS tick_size_change events (real-time, no REST round-trip) ──
            {
                let pending_tick = self.ws_tick_sizes.read().get(&condition_id).copied();
                if let Some(new_tick) = pending_tick {
                    if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                        if ms.market.tick_size != new_tick {
                            warn!(
                                condition_id = %condition_id,
                                old = %ms.market.tick_size,
                                new = %new_tick,
                                "[v2-{}] Applying WS tick_size_change — cancelling stale orders",
                                self.asset
                            );
                            ms.market.tick_size = new_tick;
                            // Cancel all resting orders since they may have wrong tick alignment
                            self.cancel_all_resting_orders().await;
                        }
                    }
                    self.ws_tick_sizes.write().remove(&condition_id);
                }
            }

            // ── Periodic market params refresh (fees + tick size, every 2 min) ──
            {
                const PARAMS_REFRESH_SECS: u64 = 120;
                let needs_refresh = self
                    .active_markets
                    .get(&condition_id)
                    .map(|ms| {
                        ms.fee_last_fetched
                            .map(|t| t.elapsed().as_secs() >= PARAMS_REFRESH_SECS)
                            .unwrap_or(true)
                    })
                    .unwrap_or(false);
                if needs_refresh {
                    if let Some(sdk) = &self.sdk {
                        match sdk.get_market_params(&condition_id).await {
                            Ok((_maker, taker, tick)) => {
                                if let Some(ms_mut) = self.active_markets.get_mut(&condition_id) {
                                    if ms_mut.taker_fee_rate != Some(taker) {
                                        info!(
                                            condition_id = %condition_id,
                                            old = ?ms_mut.taker_fee_rate,
                                            new = %taker,
                                            "[v2] Fee rate updated"
                                        );
                                    }
                                    if ms_mut.market.tick_size != tick {
                                        warn!(
                                            condition_id = %condition_id,
                                            old = %ms_mut.market.tick_size,
                                            new = %tick,
                                            "[v2] Tick size changed mid-period!"
                                        );
                                        ms_mut.market.tick_size = tick;
                                    }
                                    ms_mut.taker_fee_rate = Some(taker);
                                    ms_mut.fee_last_fetched = Some(Instant::now());
                                }
                            }
                            Err(e) => {
                                warn!(
                                    condition_id = %condition_id,
                                    error = %e,
                                    "[v2] Market params refresh failed"
                                );
                                // Prevent retry storm — update timestamp even on failure
                                if let Some(ms_mut) = self.active_markets.get_mut(&condition_id) {
                                    ms_mut.fee_last_fetched = Some(Instant::now());
                                }
                            }
                        }
                    }
                }
            }

            // Load position early so it's available for merge and budget override.
            let mut position = self
                .inventory
                .get_position(&condition_id)
                .unwrap_or_default();

            // === Continuous merge: free USDC from completed pairs mid-period ===
            if self.v2.continuous_merge_enabled {
                let should_merge = self
                    .active_markets
                    .get(&condition_id)
                    .map(|ms| {
                        ms.last_merge_time
                            .map(|t| t.elapsed().as_secs() >= self.v2.merge_interval_secs)
                            .unwrap_or(true) // first merge: always eligible
                    })
                    .unwrap_or(false);

                if should_merge {
                    let complete_pairs = position.complete_pairs();
                    let min_pairs = Decimal::from(self.v2.merge_min_pairs);
                    let reserve = Decimal::from(self.v2.merge_reserve_pairs);

                    if complete_pairs >= min_pairs {
                        // Skip merge if avg combined cost is too high (would realize a loss)
                        let avg_cc = position.avg_combined_cost();
                        let profit_per_pair = Decimal::ONE - avg_cc;
                        if profit_per_pair < self.v2.merge_min_profit_per_pair {
                            info!(
                                condition_id = %condition_id,
                                avg_combined_cost = %avg_cc,
                                profit_per_pair = %profit_per_pair,
                                min_required = %self.v2.merge_min_profit_per_pair,
                                "[v2] MERGE_SKIP: avg combined cost too high, would realize loss"
                            );
                        } else {
                            let mergeable = (complete_pairs - reserve).max(Decimal::ZERO);
                            if let Some(pairs_u64) = mergeable.to_u64().filter(|&p| p > 0) {
                                // Cancel all resting sell orders before merge to prevent
                                // orphaned sells on tokens that will be burned.
                                let sells_to_cancel: Vec<(OrderId, Outcome, Decimal, Decimal)> =
                                    self.active_markets
                                        .get(&condition_id)
                                        .map(|ms| {
                                            ms.resting_sells
                                                .iter()
                                                .map(|((outcome, price), o)| {
                                                    (o.order_id.clone(), *outcome, *price, o.size)
                                                })
                                                .collect()
                                        })
                                        .unwrap_or_default();

                                if !sells_to_cancel.is_empty() {
                                    info!(
                                        condition_id = %condition_id,
                                        count = sells_to_cancel.len(),
                                        "[v2] Cancelling resting sell orders before merge"
                                    );
                                    // Batch cancel sell orders before merge
                                    if self.config.mode == TradingMode::Paper {
                                        for (oid, _, _, _) in &sells_to_cancel {
                                            self.paper_sim.cancel(oid);
                                        }
                                    } else if let Some(sdk) = &self.sdk {
                                        let ids: Vec<&str> = sells_to_cancel
                                            .iter()
                                            .map(|(oid, _, _, _)| oid.as_str())
                                            .collect();
                                        if let Err(e) = sdk.cancel_orders(&ids).await {
                                            warn!(
                                                count = ids.len(),
                                                "[v2] Batch cancel pre-merge sells failed: {e}"
                                            );
                                        }
                                    }
                                    for (oid, outcome, price, size) in &sells_to_cancel {
                                        self.period_logger.log_order_event(
                                            &period_name,
                                            oid,
                                            "CANCELLED",
                                            *outcome,
                                            *price,
                                            *size,
                                            *size,
                                            "pre_merge_cancel",
                                        );
                                    }
                                    if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                                        let cancel_set: std::collections::HashSet<&str> =
                                            sells_to_cancel
                                                .iter()
                                                .map(|(oid, _, _, _)| oid.as_str())
                                                .collect();
                                        ms.resting_sells.retain(|_, o| {
                                            !cancel_set.contains(o.order_id.as_str())
                                        });
                                        ms.orders_cancelled += sells_to_cancel.len() as u32;
                                    }
                                }

                                if self.config.mode == TradingMode::Paper {
                                    // Paper mode: simulate merge directly
                                    let merged_dec = Decimal::from(pairs_u64);
                                    let avg_combined_cost = position.avg_combined_cost();
                                    let merge_profit =
                                        merged_dec * (Decimal::ONE - avg_combined_cost);
                                    let released_cost_basis = merged_dec * avg_combined_cost;
                                    self.inventory.record_merge(&condition_id, merged_dec);
                                    if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                                        ms.merge_realized_pnl += merge_profit;
                                        ms.merge_cost_basis_released += released_cost_basis;
                                        ms.cumulative_merged_pairs += merged_dec;
                                        ms.last_merge_time = Some(Instant::now());
                                    }
                                    info!(
                                        condition_id = %condition_id,
                                        pairs = pairs_u64,
                                        avg_combined_cost = %avg_combined_cost,
                                        merge_profit = %merge_profit,
                                        cumulative_merge_pnl = %self.active_markets.get(&condition_id).map(|ms| ms.merge_realized_pnl).unwrap_or_default(),
                                        "[v2] MERGE_PROFIT: continuous merge (paper)"
                                    );
                                    // Re-read position after merge
                                    position = self
                                        .inventory
                                        .get_position(&condition_id)
                                        .unwrap_or_default();
                                    // Clear exit_buy_block if post-merge cost excess is below threshold
                                    if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                                        if ms.exit_buy_block.is_some() {
                                            let post_merge_cost_excess =
                                                position.cost_imbalance().abs();
                                            if post_merge_cost_excess < self.v2.exit_soft_excess {
                                                info!(
                                                    condition_id = %condition_id,
                                                    post_merge_cost_excess = %post_merge_cost_excess,
                                                    threshold = %self.v2.exit_soft_excess,
                                                    "[v2] exit_buy_block CLEARED after merge (paper)"
                                                );
                                                ms.exit_buy_block = None;
                                            }
                                        }
                                    }
                                } else if self.config.eoa_mode {
                                    if let Some(sdk) = &self.sdk {
                                        let rpc_url = self.onchain.rpc_url().to_string();
                                        info!(
                                            condition_id = %condition_id,
                                            pairs = pairs_u64,
                                            "[v2] Continuous merge — merging pairs on-chain"
                                        );
                                        match sdk
                                            .merge_positions(&rpc_url, &condition_id, pairs_u64)
                                            .await
                                        {
                                            Ok(tx_hash) => {
                                                let merged_dec = Decimal::from(pairs_u64);
                                                let avg_combined_cost =
                                                    position.avg_combined_cost();
                                                let merge_profit =
                                                    merged_dec * (Decimal::ONE - avg_combined_cost);
                                                let released_cost_basis =
                                                    merged_dec * avg_combined_cost;
                                                self.inventory
                                                    .record_merge(&condition_id, merged_dec);
                                                self.onchain.invalidate_balance_cache();
                                                if let Some(ms) =
                                                    self.active_markets.get_mut(&condition_id)
                                                {
                                                    ms.merge_realized_pnl += merge_profit;
                                                    ms.merge_cost_basis_released +=
                                                        released_cost_basis;
                                                    ms.cumulative_merged_pairs += merged_dec;
                                                    ms.last_merge_time = Some(Instant::now());
                                                }
                                                info!(
                                                    condition_id = %condition_id,
                                                    %tx_hash,
                                                    pairs = pairs_u64,
                                                    avg_combined_cost = %avg_combined_cost,
                                                    merge_profit = %merge_profit,
                                                    cumulative_merge_pnl = %self.active_markets.get(&condition_id).map(|ms| ms.merge_realized_pnl).unwrap_or_default(),
                                                    "[v2] MERGE_PROFIT: continuous merge successful"
                                                );
                                                // Re-read position after merge
                                                position = self
                                                    .inventory
                                                    .get_position(&condition_id)
                                                    .unwrap_or_default();
                                                // Clear exit_buy_block if post-merge cost excess is below threshold
                                                if let Some(ms) =
                                                    self.active_markets.get_mut(&condition_id)
                                                {
                                                    if ms.exit_buy_block.is_some() {
                                                        let post_merge_cost_excess =
                                                            position.cost_imbalance().abs();
                                                        if post_merge_cost_excess
                                                            < self.v2.exit_soft_excess
                                                        {
                                                            info!(
                                                                condition_id = %condition_id,
                                                                post_merge_cost_excess = %post_merge_cost_excess,
                                                                threshold = %self.v2.exit_soft_excess,
                                                                "[v2] exit_buy_block CLEARED after merge (live)"
                                                            );
                                                            ms.exit_buy_block = None;
                                                        }
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                warn!(
                                                    condition_id = %condition_id,
                                                    error = %e,
                                                    "[v2] Continuous merge failed"
                                                );
                                                if let Some(ms) =
                                                    self.active_markets.get_mut(&condition_id)
                                                {
                                                    ms.last_merge_time = Some(Instant::now());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } // else: profit check
                    } else {
                        // Not enough pairs — update timestamp to avoid checking every tick
                        if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                            ms.last_merge_time = Some(Instant::now());
                        }
                    }
                }
            }

            // Remaining risk capacity, adjusted for sell-back budget withdrawal.
            // When sell-back sells excess, record_sell reduces total_*_spent which
            // increases remaining_capacity. We subtract the freed cost basis so
            // that USDC from sell-backs is treated as withdrawn, not recycled into
            // new buys (which would flip the imbalance to the opposite side).
            let raw_capacity = self.inventory.remaining_capacity(&condition_id);
            let sell_withdrawn = self
                .active_markets
                .get(&condition_id)
                .map(|ms| ms.sell_cost_basis_freed)
                .unwrap_or(Decimal::ZERO);
            let capacity = (raw_capacity - sell_withdrawn).max(Decimal::ZERO);
            if capacity <= Decimal::ZERO {
                if sell_withdrawn > Decimal::ZERO {
                    debug!(
                        condition_id = %condition_id,
                        raw_capacity = %raw_capacity,
                        sell_withdrawn = %sell_withdrawn,
                        "[v2] Skipping tick: capacity exhausted (includes sell-back budget withdrawal)"
                    );
                } else {
                    warn!(
                        condition_id = %condition_id,
                        capacity = %raw_capacity,
                        "[v2] Skipping tick: zero remaining capacity (check max_total_exposure / max_position_per_market vs reconciled positions)"
                    );
                }
                continue;
            }

            // ── Rebalance budget override: extra capacity for light-side pair completion ──
            let rebalance_extra_capacity = if self.v2.rebalance_budget_override {
                let excess = (position.yes_qty - position.no_qty).abs();
                if excess > Decimal::ZERO && position.avg_combined_cost() < dec!(0.98) {
                    let heavy_avg = if position.yes_qty > position.no_qty {
                        position.avg_yes_cost()
                    } else {
                        position.avg_no_cost()
                    };
                    (excess * heavy_avg).min(self.v2.rebalance_max_extra_budget)
                } else {
                    Decimal::ZERO
                }
            } else {
                Decimal::ZERO
            };

            // ── Fair-value computation ──
            let remaining_secs = self.time_manager.seconds_remaining(market.end_date) as f64;
            let raw_fv_up = fair_value_up(btc_open, btc_current, sigma, remaining_secs);

            // Optionally blend BS fair value with book midpoint for more market-aligned pricing
            let (fv_up, fv_down) = if self.v2.fv_book_blend_weight > 0.0 {
                let yes_mid = match (yes_book.best_bid(), yes_book.best_ask()) {
                    (Some((b, _)), Some((a, _))) => {
                        let mid = (b + a) / dec!(2);
                        mid.to_f64().unwrap_or(raw_fv_up)
                    }
                    _ => raw_fv_up,
                };
                let w = self.v2.fv_book_blend_weight;
                let blended_up = ((1.0 - w) * raw_fv_up + w * yes_mid).clamp(0.02, 0.98);
                (blended_up, 1.0 - blended_up)
            } else {
                (raw_fv_up, 1.0 - raw_fv_up)
            };

            // ── Paper mode: check fills on EXISTING orders BEFORE replacing ──
            if self.config.mode == TradingMode::Paper {
                let market_label = {
                    let q = &market.question;
                    if let Some(dash_idx) = q.find(" - ") {
                        q[dash_idx + 3..].to_string()
                    } else {
                        q.chars().take(20).collect::<String>()
                    }
                };

                // Burst fill protection: use time-adjusted imbalance limit (cost-based USDC)
                let pre_fill_pos = self
                    .inventory
                    .get_position(&condition_id)
                    .unwrap_or_default();
                let (burst_max_imb, _) =
                    time_adjusted_imbalance_limits(remaining_secs, market_total_secs, &self.v2);
                let paper_start = std::time::Instant::now();
                let fill_result = self.paper_sim.check_fills_with_book_limited(
                    &condition_id,
                    yes_book.best_ask().map(|(p, _)| p),
                    no_book.best_ask().map(|(p, _)| p),
                    yes_book.best_bid().map(|(p, _)| p),
                    no_book.best_bid().map(|(p, _)| p),
                    fv_up,
                    fv_down,
                    Some(burst_max_imb),
                    pre_fill_pos.total_yes_spent,
                    pre_fill_pos.total_no_spent,
                );
                let paper_ms = paper_start.elapsed().as_millis();

                // Handle PostOnly rejections: clean up resting orders
                for rejected in &fill_result.postonly_rejections {
                    if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                        ms.resting_orders
                            .retain(|_, o| o.order_id != rejected.order_id);
                        ms.resting_sells
                            .retain(|_, o| o.order_id != rejected.order_id);
                        ms.orders_cancelled += 1;
                    }
                    self.fill_handler
                        .unregister_order(&condition_id, &rejected.order_id);
                    self.period_logger.log_order_event(
                        &period_name,
                        &rejected.order_id,
                        "CANCELLED",
                        rejected.outcome,
                        rejected.price,
                        rejected.size,
                        rejected.size,
                        "postonly_rejection",
                    );
                }

                if !fill_result.fills.is_empty() {
                    self.period_logger.log_latency(
                        &period_name,
                        "paper_fill_check",
                        paper_ms,
                        true,
                        None,
                    );
                }
                for fill in fill_result.fills {
                    self.process_paper_fill(fill, &market_label).await;
                }
            }

            // ── Compute target ladder ──
            // Re-read position AND capacity in case paper fills updated inventory above.
            position = self
                .inventory
                .get_position(&condition_id)
                .unwrap_or_default();
            let raw_capacity = self.inventory.remaining_capacity(&condition_id);
            let sell_withdrawn = self
                .active_markets
                .get(&condition_id)
                .map(|ms| ms.sell_cost_basis_freed)
                .unwrap_or(Decimal::ZERO);
            let capacity = (raw_capacity - sell_withdrawn).max(Decimal::ZERO);

            // ── EV Circuit Breaker (position-aware) ──
            let merge_pnl = self
                .active_markets
                .get(&condition_id)
                .map(|ms| ms.merge_realized_pnl)
                .unwrap_or(Decimal::ZERO);
            let ev = compute_position_ev(&position, fv_up, merge_pnl);

            // Time-adaptive EV breaker: scale up thresholds early in period
            // so the breaker is more tolerant during pair accumulation.
            let ev_time_scale = {
                let period_total = market_total_secs;
                let elapsed_pct = elapsed_pct_from_remaining(remaining_secs, period_total);
                let end_pct = self.v2.ev_early_period_end_pct;
                if elapsed_pct < end_pct && end_pct > 0.0 {
                    let t = elapsed_pct / end_pct;
                    let mult = self.v2.ev_early_period_multiplier.to_f64().unwrap_or(3.0);
                    // Linear interp: mult at t=0, 1.0 at t=1
                    let scale = mult + t * (1.0 - mult);
                    Decimal::from_f64(scale.max(1.0)).unwrap_or(Decimal::ONE)
                } else {
                    Decimal::ONE
                }
            };

            // Original check: excess EV exceeds locked profit
            // Floor: don't fire unless |excess_ev| > ev_min_excess_threshold * ev_time_scale
            // (prevents tripping on tiny FV noise during startup / pair-building)
            let ev_vs_locked = self.v2.ev_circuit_breaker_enabled
                && ev.locked_profit > Decimal::ZERO
                && ev.excess_ev < Decimal::ZERO
                && ev.excess_ev.abs()
                    > ((ev.locked_profit * self.v2.ev_stop_buying_ratio)
                        .max(self.v2.ev_min_excess_threshold))
                        * ev_time_scale;

            // Secondary check: even with zero locked profit, stop buying the HEAVY side
            // when we have a net-negative EV position. Also subject to the minimum
            // threshold so the first few fills don't immediately trigger it.
            let ev_position_negative = self.v2.ev_circuit_breaker_enabled
                && ev.excess_shares > Decimal::ZERO
                && ev.net_ev < Decimal::ZERO
                && ev.excess_ev.abs() > self.v2.ev_min_excess_threshold * ev_time_scale;

            let ev_breaker_tripped = ev_vs_locked || ev_position_negative;
            // Recovery mode whenever breaker is active — relaxes combined cost guard
            let ev_recovery_mode = ev_breaker_tripped;
            let mut ev_breaker_secs = 0.0;

            // Track EV breaker duration per market for exit escalation.
            if let Some(ms_mut) = self.active_markets.get_mut(&condition_id) {
                if ev_breaker_tripped {
                    let now = Instant::now();
                    let since = ms_mut.ev_breaker_since.get_or_insert(now);
                    ev_breaker_secs = now.duration_since(*since).as_secs_f64();
                } else {
                    ms_mut.ev_breaker_since = None;
                    ms_mut.last_ev_breaker_log = None;
                }
            }

            // Granular suppression tracking: record which filters cleared ladders
            let mut suppression_reasons: Vec<&str> = Vec::new();
            let mut directional_skew_label: Option<String> = None;

            let (mut yes_ladder, mut no_ladder) = if ev_breaker_tripped {
                let mut should_log_ev_warn = true;
                if let Some(ms_mut) = self.active_markets.get_mut(&condition_id) {
                    let now = Instant::now();
                    let cooldown = Duration::from_secs(self.v2.ev_log_cooldown_secs.max(1));
                    should_log_ev_warn = ms_mut
                        .last_ev_breaker_log
                        .map(|last| now.duration_since(last) >= cooldown)
                        .unwrap_or(true);
                    if should_log_ev_warn {
                        ms_mut.last_ev_breaker_log = Some(now);
                    }
                }
                if should_log_ev_warn {
                    if ev_vs_locked {
                        warn!(
                            condition_id = %condition_id,
                            locked_profit = %ev.locked_profit,
                            excess_ev = %ev.excess_ev,
                            excess_shares = %ev.excess_shares,
                            "[v2] EV circuit breaker: excess risk exceeds locked profit — no new buys"
                        );
                    } else {
                        warn!(
                            condition_id = %condition_id,
                            net_ev = %ev.net_ev,
                            excess_ev = %ev.excess_ev,
                            excess_shares = %ev.excess_shares,
                            "[v2] EV circuit breaker: net-negative position — suppressing heavy side"
                        );
                    }
                }
                let ev_breaker_reason = if ev_vs_locked {
                    "ev_circuit_breaker:vs_locked"
                } else {
                    "ev_circuit_breaker:position_negative"
                };
                suppression_reasons.push(ev_breaker_reason);

                // Partial suppression for both ev_vs_locked and ev_position_negative:
                // clear heavy side only, allow light-side buying for pair completion.
                // When balanced, allow both sides (other guards still protect).
                {
                    let mut ladders = compute_bid_ladder(
                        fv_up,
                        fv_down,
                        market.tick_size,
                        &position,
                        &self.v2,
                        Some(tapered_levels),
                    );
                    let imbalance = position.yes_qty - position.no_qty;
                    if imbalance > Decimal::ZERO {
                        ladders.0.clear(); // Clear YES (heavy)
                    } else if imbalance < Decimal::ZERO {
                        ladders.1.clear(); // Clear NO (heavy)
                    }
                    ladders
                }
            } else {
                compute_bid_ladder(
                    fv_up,
                    fv_down,
                    market.tick_size,
                    &position,
                    &self.v2,
                    Some(tapered_levels),
                )
            };

            // Check if FV dead zone cleared both sides from compute_bid_ladder
            if !ev_breaker_tripped
                && yes_ladder.is_empty()
                && no_ladder.is_empty()
                && fv_up < self.v2.fv_dead_threshold
                && fv_down < self.v2.fv_dead_threshold
            {
                suppression_reasons.push("fv_dead_both_sides");
            }

            // ── Volatility circuit breaker: suppress new orders in high-vol regimes ──
            // Resting orders are NOT cancelled — they may still fill.
            // Exception: when there are unpaired shares, allow the light side to
            // continue bidding for pair completion. Unpaired exposure in high-vol
            // is MORE dangerous than bidding for pairs.
            if vol_breaker_suppressing {
                if !yes_ladder.is_empty() || !no_ladder.is_empty() {
                    let light = position.light_side();
                    let has_unpaired = position.yes_qty != position.no_qty
                        && (position.yes_qty > Decimal::ZERO || position.no_qty > Decimal::ZERO);
                    if has_unpaired {
                        // Only suppress the heavy side; keep light side for pair completion
                        match light {
                            Some(Outcome::Yes) => {
                                no_ladder.clear();
                                suppression_reasons.push("vol_breaker");
                            }
                            Some(Outcome::No) => {
                                yes_ladder.clear();
                                suppression_reasons.push("vol_breaker");
                            }
                            None => {
                                yes_ladder.clear();
                                no_ladder.clear();
                                suppression_reasons.push("vol_breaker");
                            }
                        }
                    } else {
                        yes_ladder.clear();
                        no_ladder.clear();
                        suppression_reasons.push("vol_breaker");
                    }
                }
            }

            // ── Buy ladder anchoring ──
            // For the Phase 7 BTC 5m profile, re-anchor directly to the best bid
            // so level 0 / level 1 track `bid_1` and `bid_1 - 1 tick`.
            // Other profiles keep the existing ask-buffer anchoring.
            let effective_buffer_ticks = if self.v2.vpin_enabled {
                let mult = self.vpin_tracker.spread_multiplier();
                ((self.v2.postonly_regen_buffer_ticks as f64) * mult).ceil() as u32
            } else {
                self.v2.postonly_regen_buffer_ticks
            };
            {
                let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                let use_best_bid_anchor = duration_mins <= 7 && self.v2.best_bid_anchor_5m;
                if use_best_bid_anchor {
                    if let Some((yes_bid, _)) = yes_book.best_bid() {
                        let min_f64 = (fv_up * self.v2.min_bid_fv_ratio).max(
                            self.v2
                                .min_bid_absolute_floor
                                .to_string()
                                .parse::<f64>()
                                .unwrap_or(0.02),
                        );
                        let min_bid =
                            Decimal::from_f64(min_f64).unwrap_or(self.v2.min_bid_absolute_floor);
                        let yes_levels = match (yes_book.best_bid(), yes_book.best_ask()) {
                            (Some((bid, _)), Some((ask, _))) if ask - bid < dec!(0.01) => 1,
                            _ => tapered_levels,
                        };
                        yes_ladder.clear();
                        for i in 0..yes_levels {
                            let offset = ladder_price_offset(
                                i,
                                market.tick_size,
                                self.v2.ladder_tick_spacing,
                                self.v2.deep_ladder_tick_spacing,
                                self.v2.deep_ladder_start_level,
                            );
                            let price = yes_bid - offset;
                            if price < min_bid || price <= Decimal::ZERO {
                                break;
                            }
                            yes_ladder.push(LadderLevel {
                                outcome: Outcome::Yes,
                                price,
                                size: ladder_size_at_level_and_price(
                                    self.v2.level_order_size,
                                    i,
                                    self.v2.ladder_size_decay,
                                    price,
                                ),
                            });
                        }
                    }
                    if let Some((no_bid, _)) = no_book.best_bid() {
                        let min_f64 = (fv_down * self.v2.min_bid_fv_ratio).max(
                            self.v2
                                .min_bid_absolute_floor
                                .to_string()
                                .parse::<f64>()
                                .unwrap_or(0.02),
                        );
                        let min_bid =
                            Decimal::from_f64(min_f64).unwrap_or(self.v2.min_bid_absolute_floor);
                        let no_levels = match (no_book.best_bid(), no_book.best_ask()) {
                            (Some((bid, _)), Some((ask, _))) if ask - bid < dec!(0.01) => 1,
                            _ => tapered_levels,
                        };
                        no_ladder.clear();
                        for i in 0..no_levels {
                            let offset = ladder_price_offset(
                                i,
                                market.tick_size,
                                self.v2.ladder_tick_spacing,
                                self.v2.deep_ladder_tick_spacing,
                                self.v2.deep_ladder_start_level,
                            );
                            let price = no_bid - offset;
                            if price < min_bid || price <= Decimal::ZERO {
                                break;
                            }
                            no_ladder.push(LadderLevel {
                                outcome: Outcome::No,
                                price,
                                size: ladder_size_at_level_and_price(
                                    self.v2.level_order_size,
                                    i,
                                    self.v2.ladder_size_decay,
                                    price,
                                ),
                            });
                        }
                    }
                } else {
                    if let Some(yes_ask) = yes_book.best_ask().map(|(p, _)| p) {
                        let buffer = market.tick_size * Decimal::from(effective_buffer_ticks);
                        let max_bid = round_down_to_tick(
                            (yes_ask - buffer).max(Decimal::ZERO),
                            market.tick_size,
                        );
                        let yes_top = yes_ladder.first().map(|l| l.price).unwrap_or(Decimal::ZERO);
                        let needs_reanchor =
                            !yes_ladder.is_empty() && (yes_top >= yes_ask || yes_top < max_bid);
                        if needs_reanchor {
                            let min_f64 = (fv_up * self.v2.min_bid_fv_ratio).max(
                                self.v2
                                    .min_bid_absolute_floor
                                    .to_string()
                                    .parse::<f64>()
                                    .unwrap_or(0.02),
                            );
                            let min_bid = Decimal::from_f64(min_f64)
                                .unwrap_or(self.v2.min_bid_absolute_floor);
                            yes_ladder.clear();
                            for i in 0..tapered_levels {
                                let offset = ladder_price_offset(
                                    i,
                                    market.tick_size,
                                    self.v2.ladder_tick_spacing,
                                    self.v2.deep_ladder_tick_spacing,
                                    self.v2.deep_ladder_start_level,
                                );
                                let price = max_bid - offset;
                                if price < min_bid || price <= Decimal::ZERO {
                                    break;
                                }
                                yes_ladder.push(LadderLevel {
                                    outcome: Outcome::Yes,
                                    price,
                                    size: ladder_size_at_level_and_price(
                                        self.v2.level_order_size,
                                        i,
                                        self.v2.ladder_size_decay,
                                        price,
                                    ),
                                });
                            }
                        }
                    }
                    if let Some(no_ask) = no_book.best_ask().map(|(p, _)| p) {
                        let buffer = market.tick_size * Decimal::from(effective_buffer_ticks);
                        let max_bid = round_down_to_tick(
                            (no_ask - buffer).max(Decimal::ZERO),
                            market.tick_size,
                        );
                        let no_top = no_ladder.first().map(|l| l.price).unwrap_or(Decimal::ZERO);
                        let needs_reanchor =
                            !no_ladder.is_empty() && (no_top >= no_ask || no_top < max_bid);
                        if needs_reanchor {
                            let min_f64 = (fv_down * self.v2.min_bid_fv_ratio).max(
                                self.v2
                                    .min_bid_absolute_floor
                                    .to_string()
                                    .parse::<f64>()
                                    .unwrap_or(0.02),
                            );
                            let min_bid = Decimal::from_f64(min_f64)
                                .unwrap_or(self.v2.min_bid_absolute_floor);
                            no_ladder.clear();
                            for i in 0..tapered_levels {
                                let offset = ladder_price_offset(
                                    i,
                                    market.tick_size,
                                    self.v2.ladder_tick_spacing,
                                    self.v2.deep_ladder_tick_spacing,
                                    self.v2.deep_ladder_start_level,
                                );
                                let price = max_bid - offset;
                                if price < min_bid || price <= Decimal::ZERO {
                                    break;
                                }
                                no_ladder.push(LadderLevel {
                                    outcome: Outcome::No,
                                    price,
                                    size: ladder_size_at_level_and_price(
                                        self.v2.level_order_size,
                                        i,
                                        self.v2.ladder_size_decay,
                                        price,
                                    ),
                                });
                            }
                        }
                    }
                }
                if (pre_yes > 0 && yes_ladder.is_empty()) || (pre_no > 0 && no_ladder.is_empty()) {
                    suppression_reasons.push("postonly_regen");
                }
            }

            // ── Post-anchoring inventory skew (Phase 1 Change 2) ──
            // After ask-anchoring has positioned the ladder near the market ask,
            // shift the heavy-side ladder deeper (away from the ask) proportional
            // to the excess. This makes it harder to accumulate more on the wrong side.
            //
            // Two modes:
            //   (a) as_skew_enabled: Avellaneda-Stoikov formula — skew scales with
            //       inventory, volatility, AND time remaining. Adapts automatically.
            //   (b) Original fixed tick-based skew (backward compatible).
            if self.v2.post_anchor_skew_enabled || self.v2.as_skew_enabled {
                let excess = position.yes_qty - position.no_qty;
                let abs_excess = excess.abs();

                let shift = if self.v2.as_skew_enabled {
                    // A-S formula: skew = |q| * gamma * sigma^2 * (T-t)
                    let q = abs_excess.to_f64().unwrap_or(0.0);
                    let raw_skew = q * self.v2.as_gamma * sigma * sigma * remaining_secs;
                    let max_shift = market.tick_size * Decimal::from(self.v2.max_skew_ticks);
                    let shift_d = Decimal::from_f64(raw_skew).unwrap_or(Decimal::ZERO);
                    round_down_to_tick(shift_d.min(max_shift), market.tick_size)
                } else if abs_excess > self.v2.skew_activation_threshold {
                    // Original fixed tick-based skew
                    let skew_shares = abs_excess - self.v2.skew_activation_threshold;
                    let raw_ticks = if self.v2.shares_per_skew_tick > Decimal::ZERO {
                        (skew_shares / self.v2.shares_per_skew_tick)
                            .to_u32()
                            .unwrap_or(0)
                    } else {
                        0
                    };
                    let skew_ticks = raw_ticks.min(self.v2.max_skew_ticks);
                    market.tick_size * Decimal::from(skew_ticks)
                } else {
                    Decimal::ZERO
                };

                // Apply randomized noise to the shift (anti-pattern-detection)
                let shift = if self.v2.skew_noise_enabled && shift > Decimal::ZERO {
                    use rand::Rng;
                    let amp = self.v2.skew_noise_amplitude;
                    let noise: f64 = rand::rng().random_range(-amp..=amp);
                    let noisy = shift.to_f64().unwrap_or(0.0) * (1.0 + noise);
                    let noisy_d = Decimal::from_f64(noisy.max(0.0)).unwrap_or(Decimal::ZERO);
                    round_down_to_tick(noisy_d, market.tick_size)
                } else {
                    shift
                };

                if shift > Decimal::ZERO {
                    let min_bid = self.v2.min_bid_absolute_floor;
                    let heavy_ladder = if excess > Decimal::ZERO {
                        &mut yes_ladder
                    } else {
                        &mut no_ladder
                    };
                    for level in heavy_ladder.iter_mut() {
                        level.price -= shift;
                    }
                    heavy_ladder.retain(|l| l.price >= min_bid && l.price > Decimal::ZERO);
                }
            }

            // ── VPIN toxic flow integration ──
            // (a) Toxic pullback: truncate to level-0 only under extreme informed flow
            // (b) Size reduction: scale down sizes proportional to VPIN
            if self.v2.vpin_enabled {
                if self.vpin_tracker.should_pullback() {
                    yes_ladder.truncate(1);
                    no_ladder.truncate(1);
                    suppression_reasons.push("vpin_pullback");
                }
                let vpin_size = self.vpin_tracker.size_factor();
                if vpin_size < 1.0 {
                    let factor = Decimal::from_f64(vpin_size).unwrap_or(Decimal::ONE);
                    for level in yes_ladder.iter_mut().chain(no_ladder.iter_mut()) {
                        level.size = quantize_order_size((level.size * factor).max(Decimal::ONE));
                    }
                }
            }

            // ── Taper size reduction: scale sizes as settlement approaches ──
            if self.v2.taper_enabled && taper_size_factor < 1.0 {
                let factor = Decimal::from_f64(taper_size_factor).unwrap_or(Decimal::ONE);
                for level in yes_ladder.iter_mut().chain(no_ladder.iter_mut()) {
                    level.size = quantize_order_size((level.size * factor).max(Decimal::ONE));
                }
            }

            // ── Per-side sell cooldown ──
            {
                let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                if self.v2.sell_buy_cooldown_secs > 0 {
                    if let Some(ms) = self.active_markets.get(&condition_id) {
                        let cooldown = Duration::from_secs(self.v2.sell_buy_cooldown_secs);
                        let now = Instant::now();
                        if let Some(t) = ms.last_sell_time.get(&Outcome::Yes) {
                            if now.duration_since(*t) < cooldown {
                                yes_ladder.clear();
                            }
                        }
                        if let Some(t) = ms.last_sell_time.get(&Outcome::No) {
                            if now.duration_since(*t) < cooldown {
                                no_ladder.clear();
                            }
                        }
                    }
                }
                if (pre_yes > 0 && yes_ladder.is_empty()) || (pre_no > 0 && no_ladder.is_empty()) {
                    suppression_reasons.push("sell_buy_cooldown");
                }
            }

            // ── Wind-down phase: stop new buys, allow pair-completion only ──
            if in_wind_down_phase {
                let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                if self.v2.wind_down_allow_pair_completion {
                    // Only allow buys on the light side (pair completion)
                    match position.light_side() {
                        Some(Outcome::Yes) => no_ladder.clear(),
                        Some(Outcome::No) => yes_ladder.clear(),
                        None => {
                            // Balanced — no buys needed
                            yes_ladder.clear();
                            no_ladder.clear();
                        }
                    }
                } else {
                    yes_ladder.clear();
                    no_ladder.clear();
                }
                if (pre_yes > 0 && yes_ladder.is_empty()) || (pre_no > 0 && no_ladder.is_empty()) {
                    suppression_reasons.push("wind_down_phase");
                }
            }

            // ── Persistent exit buy block ──
            // Once the exit system fires on a heavy side, block buying that side
            // until the excess is fully resolved.  This prevents the churn loop:
            //   sell heavy → cooldown expires → buy heavy → sell heavy → …
            {
                let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                let excess = position.yes_qty - position.no_qty;
                if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                    // Clear block once excess for the blocked side is resolved
                    if let Some(blocked) = ms.exit_buy_block {
                        let still_heavy = match blocked {
                            Outcome::Yes => excess > Decimal::ZERO,
                            Outcome::No => excess < Decimal::ZERO,
                        };
                        if !still_heavy {
                            ms.exit_buy_block = None;
                        }
                    }
                    // Suppress buys on the blocked side
                    if let Some(blocked) = ms.exit_buy_block {
                        match blocked {
                            Outcome::Yes => yes_ladder.clear(),
                            Outcome::No => no_ladder.clear(),
                        }
                    }
                }
                if (pre_yes > 0 && yes_ladder.is_empty()) || (pre_no > 0 && no_ladder.is_empty()) {
                    suppression_reasons.push("exit_buy_block");
                }
            }

            // ── Per-order combined cost guard ──
            {
                let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                apply_combined_cost_guard(
                    &mut yes_ladder,
                    &mut no_ladder,
                    &position,
                    self.v2.max_per_order_combined,
                    self.v2.light_side_max_combined,
                    ev_recovery_mode,
                );
                if (pre_yes > 0 && yes_ladder.is_empty()) || (pre_no > 0 && no_ladder.is_empty()) {
                    suppression_reasons.push("combined_cost_guard");
                }
            }

            // ── One-sided position guard (cost-weighted) ──
            // Uses total_spent instead of qty so cheap deep-grid fills don't trigger.
            {
                let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                let one_sided_threshold = self.v2.one_sided_threshold;
                if position.complete_pairs() == Decimal::ZERO {
                    if position.total_yes_spent >= one_sided_threshold
                        && position.no_qty == Decimal::ZERO
                    {
                        yes_ladder.clear();
                    } else if position.total_no_spent >= one_sided_threshold
                        && position.yes_qty == Decimal::ZERO
                    {
                        no_ladder.clear();
                    }
                }
                if (pre_yes > 0 && yes_ladder.is_empty()) || (pre_no > 0 && no_ladder.is_empty()) {
                    suppression_reasons.push("one_sided_guard");
                }
            }

            // ── Trending market suppression (Phase 1: duration-aware) ──
            // Select threshold by market duration: 5-min markets need much tighter
            // thresholds ($25) than 15-min ($75). Crossover to negative EV at $20 for 5-min.
            {
                let trend_threshold = match duration_mins {
                    0..=7 => self
                        .v2
                        .trend_threshold_5m
                        .unwrap_or(self.v2.trend_threshold_dollars),
                    8..=30 => self
                        .v2
                        .trend_threshold_15m
                        .unwrap_or(self.v2.trend_threshold_dollars),
                    _ => self
                        .v2
                        .trend_threshold_60m
                        .unwrap_or(self.v2.trend_threshold_dollars),
                };
                if trend_threshold > 0.0 {
                    let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                    let btc_change = {
                        let bs = self.asset_price.read();
                        bs.price_change_over(self.v2.trend_window_secs)
                    };
                    if let Some(change) = btc_change {
                        if change > trend_threshold {
                            if !no_ladder.is_empty() {
                                info!(
                                    btc_change = format!("{:+.1}", change),
                                    threshold = trend_threshold,
                                    duration_mins,
                                    "[v2] Trending UP: suppressing DOWN buys"
                                );
                                no_ladder.clear();
                            }
                        } else if change < -trend_threshold {
                            if !yes_ladder.is_empty() {
                                info!(
                                    btc_change = format!("{:+.1}", change),
                                    threshold = trend_threshold,
                                    duration_mins,
                                    "[v2] Trending DOWN: suppressing UP buys"
                                );
                                yes_ladder.clear();
                            }
                        }
                    }
                    if (pre_yes > 0 && yes_ladder.is_empty())
                        || (pre_no > 0 && no_ladder.is_empty())
                    {
                        if btc_change.map(|c| c > 0.0).unwrap_or(false) {
                            suppression_reasons.push("trending_up");
                        } else {
                            suppression_reasons.push("trending_down");
                        }
                    }
                }
            }

            // ── Rebalance size multiplier: boost light-side order sizes ──
            // Uses variable decay (larger near center) and caps total at abs_excess.
            // Threshold is cost-based (USDC); sizing stays share-based.
            if self.v2.rebalance_size_multiplier > 1 {
                let excess = position.yes_qty - position.no_qty;
                let abs_excess = excess.abs();
                let abs_cost_excess = position.cost_imbalance().abs();
                if abs_cost_excess >= self.v2.exit_soft_excess {
                    let multiplier = Decimal::from(self.v2.rebalance_size_multiplier);
                    let boosted_base = (self.v2.level_order_size * multiplier).min(abs_excess);
                    let light_ladder = if excess > Decimal::ZERO {
                        &mut no_ladder
                    } else {
                        &mut yes_ladder
                    };
                    let mut remaining = abs_excess;
                    for (i, level) in light_ladder.iter_mut().enumerate() {
                        if remaining <= Decimal::ZERO {
                            level.size = Decimal::ZERO;
                            continue;
                        }
                        let decayed =
                            ladder_size_at_level(boosted_base, i as u32, self.v2.ladder_size_decay);
                        let capped = decayed.min(remaining);
                        level.size = capped;
                        remaining -= capped;
                    }
                    light_ladder.retain(|l| l.size > Decimal::ZERO);
                }
            }

            // ── Time-decaying balance management ──
            {
                let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                let (time_max_abs, time_soft) =
                    time_adjusted_imbalance_limits(remaining_secs, market_total_secs, &self.v2);
                apply_balance_management(
                    &mut yes_ladder,
                    &mut no_ladder,
                    &position,
                    time_max_abs,
                    time_soft,
                );
                if (pre_yes > 0 && yes_ladder.is_empty()) || (pre_no > 0 && no_ladder.is_empty()) {
                    suppression_reasons.push("balance_management");
                }
            }

            // ── VeryLate phase: hard block heavy side buys ──
            {
                let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                if remaining_secs <= self.v2.very_late_phase_secs as f64
                    && remaining_secs > self.config.resolution_safety_margin_secs as f64
                {
                    let excess = position.yes_qty - position.no_qty;
                    if excess > Decimal::ZERO {
                        yes_ladder.clear();
                    } else if excess < Decimal::ZERO {
                        no_ladder.clear();
                    }
                }
                if (pre_yes > 0 && yes_ladder.is_empty()) || (pre_no > 0 && no_ladder.is_empty()) {
                    suppression_reasons.push("very_late_phase");
                }
            }

            // ── Asset guard suppression ──
            if asset_guard_suppressing {
                if !yes_ladder.is_empty() || !no_ladder.is_empty() {
                    yes_ladder.clear();
                    no_ladder.clear();
                    suppression_reasons.push("asset_guard");
                }
            }

            // ── Cancel-churn breaker ──
            // If cancel ratio spikes in this period, reduce aggressiveness:
            // - keep fewer ladder levels
            // - require larger center movement before reprice
            let mut churn_breaker_active = false;
            if self.v2.churn_breaker_enabled {
                let mut ratio = 0.0_f64;
                let mut orders_placed = 0_u32;
                if let Some(ms) = self.active_markets.get(&condition_id) {
                    orders_placed = ms.orders_placed;
                    if orders_placed > 0 {
                        ratio = ms.orders_cancelled as f64 / orders_placed as f64;
                    }
                }
                if orders_placed >= self.v2.churn_breaker_min_orders
                    && ratio >= self.v2.churn_breaker_cancel_ratio
                {
                    churn_breaker_active = true;
                    if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                        let now = Instant::now();
                        let cooldown = Duration::from_secs(30);
                        let should_log = ms
                            .last_churn_breaker_log
                            .map(|last| now.duration_since(last) >= cooldown)
                            .unwrap_or(true);
                        if should_log {
                            ms.last_churn_breaker_log = Some(now);
                            warn!(
                                condition_id = %condition_id,
                                cancel_ratio = format!("{ratio:.4}"),
                                threshold = format!("{:.4}", self.v2.churn_breaker_cancel_ratio),
                                keep_levels = self.v2.churn_breaker_keep_levels,
                                "[v2] Cancel-churn breaker active: reducing ladder aggressiveness"
                            );
                        }
                    }

                    let keep = self.v2.churn_breaker_keep_levels as usize;
                    if yes_ladder.len() > keep || no_ladder.len() > keep {
                        yes_ladder.truncate(keep);
                        no_ladder.truncate(keep);
                        suppression_reasons.push("cancel_churn_breaker");
                    }
                }
            }

            // ── Pair-opportunity restore ──
            // When the market offers profitable pairs (combined asks < target) but one
            // or both sides were suppressed by FV dead zone / trend / other filters,
            // restore bids near the ask on the suppressed side(s).
            // This is the core mechanism for buying at ANY price to complete pairs:
            // e.g. UP ask=0.82, DOWN ask=0.19 → combined=1.01 (skip), but
            //      UP ask=0.80, DOWN ask=0.18 → combined=0.98 → buy BOTH.
            {
                let yes_ask_opt = yes_book.best_ask().map(|(p, _)| p);
                let no_ask_opt = no_book.best_ask().map(|(p, _)| p);
                if let (Some(yes_ask), Some(no_ask)) = (yes_ask_opt, no_ask_opt) {
                    let buffer = market.tick_size * Decimal::from(effective_buffer_ticks);
                    let potential_yes_bid =
                        round_down_to_tick((yes_ask - buffer).max(Decimal::ZERO), market.tick_size);
                    let potential_no_bid =
                        round_down_to_tick((no_ask - buffer).max(Decimal::ZERO), market.tick_size);
                    let combined = potential_yes_bid + potential_no_bid;
                    // Use light_side_max_combined (0.99) instead of target_combined (0.97):
                    // real market combined asks are ~1.01-1.03, so after subtracting
                    // the 2-tick buffer, combined bids land at ~0.97-1.01.
                    // At 0.97 threshold, this almost never triggers. At 0.99, it
                    // triggers when combined asks <= 1.03 (common), ensuring ≥1c/pair profit.
                    let max_combined = self.v2.light_side_max_combined;

                    // Only restore if combined cost is actually profitable
                    if combined <= max_combined
                        && potential_yes_bid > Decimal::ZERO
                        && potential_no_bid > Decimal::ZERO
                    {
                        // Restore YES side if it was suppressed
                        if yes_ladder.is_empty() {
                            for i in 0..tapered_levels {
                                let offset = ladder_price_offset(
                                    i,
                                    market.tick_size,
                                    self.v2.ladder_tick_spacing,
                                    self.v2.deep_ladder_tick_spacing,
                                    self.v2.deep_ladder_start_level,
                                );
                                let price = potential_yes_bid - offset;
                                if price <= Decimal::ZERO || price < self.v2.min_bid_absolute_floor
                                {
                                    break;
                                }
                                yes_ladder.push(LadderLevel {
                                    outcome: Outcome::Yes,
                                    price,
                                    size: ladder_size_at_level_and_price(
                                        self.v2.level_order_size,
                                        i,
                                        self.v2.ladder_size_decay,
                                        price,
                                    ),
                                });
                            }
                            if !yes_ladder.is_empty() {
                                suppression_reasons.push("pair_opp_restore_yes");
                            }
                        }

                        // Restore NO side if it was suppressed
                        if no_ladder.is_empty() {
                            for i in 0..tapered_levels {
                                let offset = ladder_price_offset(
                                    i,
                                    market.tick_size,
                                    self.v2.ladder_tick_spacing,
                                    self.v2.deep_ladder_tick_spacing,
                                    self.v2.deep_ladder_start_level,
                                );
                                let price = potential_no_bid - offset;
                                if price <= Decimal::ZERO || price < self.v2.min_bid_absolute_floor
                                {
                                    break;
                                }
                                no_ladder.push(LadderLevel {
                                    outcome: Outcome::No,
                                    price,
                                    size: ladder_size_at_level_and_price(
                                        self.v2.level_order_size,
                                        i,
                                        self.v2.ladder_size_decay,
                                        price,
                                    ),
                                });
                            }
                            if !no_ladder.is_empty() {
                                suppression_reasons.push("pair_opp_restore_no");
                            }
                        }
                    }

                    // ── Pair-completion boost ──
                    // When imbalanced and buying the light side at market creates
                    // profitable pairs with the heavy side's avg cost, boost size.
                    // Uses variable decay (larger near center, smaller at edges) and
                    // caps total boosted shares at abs_excess to avoid over-exposure.
                    let excess = position.yes_qty - position.no_qty;
                    let abs_excess = excess.abs();
                    if abs_excess >= self.v2.level_order_size {
                        let (heavy_avg, light_bid, light_ladder) = if excess > Decimal::ZERO {
                            (position.avg_yes_cost(), potential_no_bid, &mut no_ladder)
                        } else {
                            (position.avg_no_cost(), potential_yes_bid, &mut yes_ladder)
                        };
                        let pair_combined = heavy_avg + light_bid;
                        if pair_combined < Decimal::ONE
                            && pair_combined <= self.v2.light_side_max_combined
                        {
                            // Boosted base = base_size × multiplier, capped at abs_excess
                            let boosted_base = (self.v2.level_order_size
                                * Decimal::from(self.v2.rebalance_size_multiplier.max(2)))
                            .min(abs_excess);
                            // Apply decay per level and cap total shares at the imbalance
                            let mut remaining = abs_excess;
                            for (i, level) in light_ladder.iter_mut().enumerate() {
                                if remaining <= Decimal::ZERO {
                                    level.size = Decimal::ZERO;
                                    continue;
                                }
                                let decayed = ladder_size_at_level(
                                    boosted_base,
                                    i as u32,
                                    self.v2.ladder_size_decay,
                                );
                                let capped = decayed.min(remaining);
                                level.size = capped;
                                remaining -= capped;
                            }
                            // Remove zero-size levels
                            light_ladder.retain(|l| l.size > Decimal::ZERO);
                        }
                    }
                }
            }

            // ── Re-apply combined cost guard after pair-opp restore ──
            // Pair-opp restore creates ladders that bypass the earlier cost guard (line ~3510).
            // Re-apply here to ensure no restored bid creates a losing pair with existing positions.
            {
                let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                apply_combined_cost_guard(
                    &mut yes_ladder,
                    &mut no_ladder,
                    &position,
                    self.v2.max_per_order_combined,
                    self.v2.light_side_max_combined,
                    ev_recovery_mode,
                );
                if (pre_yes > 0 && yes_ladder.is_empty()) || (pre_no > 0 && no_ladder.is_empty()) {
                    suppression_reasons.push("pair_opp_cost_guard");
                }
            }

            // ── Late-phase directional skew ──
            // Research-calibrated for the BTC 5m launch profile. This is a size-only
            // transform applied after ladder construction so every existing safety,
            // pair-quality, and exposure guard still wins over the skew.
            if let Some(decision) = self.directional_skew_decision(
                &condition_id,
                remaining_secs,
                btc_open,
                btc_current,
                &yes_book,
                &no_book,
            ) {
                apply_directional_skew_to_ladders(&mut yes_ladder, &mut no_ladder, decision);
                directional_skew_label = Some(decision.label());
                metrics::counter!(
                    "directional_skew_active_total",
                    "asset" => self.asset.display_name(),
                    "stage" => decision.stage.as_str().to_string(),
                )
                .increment(1);
            }

            // ── Phase 2 period-level risk controls ──
            {
                let elapsed_pct = elapsed_pct_from_remaining(remaining_secs, market_total_secs);
                let pair_ratio = position_pair_ratio(&position);

                let mut pair_quality_block_active = false;
                let mut buy_commitment_usdc = Decimal::ZERO;
                let mut sell_realized_pnl = Decimal::ZERO;

                if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                    sell_realized_pnl = ms.sell_realized_pnl;

                    // Pair-quality hysteresis uses current position's paired inventory quality.
                    if self.v2.period_pair_quality_max_combined > Decimal::ZERO {
                        let pairs = position.complete_pairs();
                        if pairs >= self.v2.period_pair_quality_min_pairs {
                            let avg_combined = position.avg_combined_cost();
                            if !ms.pair_quality_block_active
                                && avg_combined >= self.v2.period_pair_quality_max_combined
                            {
                                ms.pair_quality_block_active = true;
                            } else if ms.pair_quality_block_active
                                && avg_combined <= self.v2.period_pair_quality_resume_combined
                            {
                                ms.pair_quality_block_active = false;
                            }
                        } else {
                            ms.pair_quality_block_active = false;
                        }
                    } else {
                        ms.pair_quality_block_active = false;
                    }
                    pair_quality_block_active = ms.pair_quality_block_active;

                    let resting_buy_usdc = resting_buy_notional(&ms.resting_orders);
                    let net_buy_filled_usdc = (ms.gross_buy_filled_usdc
                        - ms.merge_cost_basis_released)
                        .max(Decimal::ZERO);
                    buy_commitment_usdc = net_buy_filled_usdc + resting_buy_usdc;
                }

                let bounds = compute_terminal_pnl_bounds(&position, sell_realized_pnl, merge_pnl);
                if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                    ms.min_worst_case_pnl_seen =
                        ms.min_worst_case_pnl_seen.min(bounds.worst_case_pnl);
                }

                if pair_quality_block_active {
                    let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                    match position.heavy_side() {
                        Some(Outcome::Yes) => yes_ladder.clear(),
                        Some(Outcome::No) => no_ladder.clear(),
                        None => {
                            yes_ladder.clear();
                            no_ladder.clear();
                        }
                    }
                    if (pre_yes > 0 && yes_ladder.is_empty())
                        || (pre_no > 0 && no_ladder.is_empty())
                    {
                        suppression_reasons.push("period_pair_quality");
                    }
                }

                if elapsed_pct >= self.v2.early_phase_pct
                    && position.total_qty() >= self.v2.pair_ratio_eval_min_total_shares
                    && pair_ratio < self.v2.period_min_pair_ratio_for_heavy_add
                {
                    let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                    match position.heavy_side() {
                        Some(Outcome::Yes) => yes_ladder.clear(),
                        Some(Outcome::No) => no_ladder.clear(),
                        None => {}
                    }
                    if (pre_yes > 0 && yes_ladder.is_empty())
                        || (pre_no > 0 && no_ladder.is_empty())
                    {
                        suppression_reasons.push("pair_ratio_heavy_add_guard");
                    }
                }

                if self.v2.period_worst_case_loss_cap_usdc > Decimal::ZERO {
                    let worst_case_loss = (-bounds.worst_case_pnl).max(Decimal::ZERO);
                    if worst_case_loss > self.v2.period_worst_case_loss_cap_usdc {
                        let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                        match position.heavy_side() {
                            Some(Outcome::Yes) => yes_ladder.clear(),
                            Some(Outcome::No) => no_ladder.clear(),
                            None => {
                                yes_ladder.clear();
                                no_ladder.clear();
                            }
                        }
                        if (pre_yes > 0 && yes_ladder.is_empty())
                            || (pre_no > 0 && no_ladder.is_empty())
                        {
                            suppression_reasons.push("worst_case_loss_cap");
                        }
                    }
                }

                if self.v2.period_gross_buy_cap_usdc > Decimal::ZERO
                    && buy_commitment_usdc >= self.v2.period_gross_buy_cap_usdc
                {
                    let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                    yes_ladder.clear();
                    no_ladder.clear();
                    if (pre_yes > 0 && yes_ladder.is_empty())
                        || (pre_no > 0 && no_ladder.is_empty())
                    {
                        suppression_reasons.push("period_buy_commitment_cap");
                    }
                } else if elapsed_pct < self.v2.early_phase_pct
                    && self.v2.early_phase_gross_buy_cap_usdc > Decimal::ZERO
                    && buy_commitment_usdc >= self.v2.early_phase_gross_buy_cap_usdc
                {
                    let (pre_yes, pre_no) = (yes_ladder.len(), no_ladder.len());
                    yes_ladder.clear();
                    no_ladder.clear();
                    if (pre_yes > 0 && yes_ladder.is_empty())
                        || (pre_no > 0 && no_ladder.is_empty())
                    {
                        suppression_reasons.push("early_buy_commitment_cap");
                    }
                }
            }

            // ── Ladder churn reduction ──
            // Skip cancel/replace cycle if ladder centers haven't moved enough.
            // Still run sell-back and pair completion below.
            let new_yes_center = yes_ladder.first().map(|l| l.price);
            let new_no_center = no_ladder.first().map(|l| l.price);
            let should_reprice = {
                let threshold = if churn_breaker_active {
                    self.v2.ladder_reprice_threshold
                        * Decimal::from(self.v2.churn_breaker_reprice_multiplier.max(1))
                } else {
                    self.v2.ladder_reprice_threshold
                };
                let ms = self.active_markets.get(&condition_id);
                let yes_stable = match (new_yes_center, ms.and_then(|m| m.last_yes_center)) {
                    (Some(new), Some(old)) => (new - old).abs() < threshold,
                    (None, None) => true,
                    _ => false,
                };
                let no_stable = match (new_no_center, ms.and_then(|m| m.last_no_center)) {
                    (Some(new), Some(old)) => (new - old).abs() < threshold,
                    (None, None) => true,
                    _ => false,
                };
                !yes_stable || !no_stable
            };

            // ── Compute sell-back ladder ──
            // Grace period: suppress sell-back for N seconds after first order placed.
            // Uses first_order_placed_at so grace is relative to actual trading start,
            // not period start (which would waste time during observation phase).
            let in_grace_period = self
                .active_markets
                .get(&condition_id)
                .and_then(|ms| ms.first_order_placed_at)
                .map(|t| t.elapsed().as_secs() < self.v2.sellback_grace_period_secs)
                .unwrap_or(true); // No orders placed yet → still in grace

            // Anti-oscillation: don't sell a side if we're also buying it this tick.
            // The sell ladder only sells the heavy side, so check if buys exist there.
            let excess_for_sell = position.yes_qty - position.no_qty;
            let heavy_side_has_buys = if excess_for_sell > Decimal::ZERO {
                !yes_ladder.is_empty() // heavy YES and we're buying YES → skip sells
            } else if excess_for_sell < Decimal::ZERO {
                !no_ladder.is_empty() // heavy NO and we're buying NO → skip sells
            } else {
                false // balanced, no sells needed anyway
            };

            let exit_plan = compute_excess_exit_plan(
                &position,
                fv_up,
                fv_down,
                yes_book.best_bid().map(|(p, _)| p),
                no_book.best_bid().map(|(p, _)| p),
                yes_book.best_ask().map(|(p, _)| p),
                no_book.best_ask().map(|(p, _)| p),
                market.tick_size,
                &self.v2,
                remaining_secs,
                ev_breaker_secs,
                in_grace_period,
                heavy_side_has_buys,
            );
            let (sell_ladder, taker_exit) = match &exit_plan {
                ExitPlan::Skip { .. } => (Vec::new(), None),
                ExitPlan::Maker { levels, .. } => (levels.clone(), None),
                ExitPlan::Taker {
                    heavy_outcome,
                    size,
                    price,
                    ..
                } => (Vec::new(), Some((*heavy_outcome, *size, *price))),
            };
            let exit_heavy_outcome = match &exit_plan {
                ExitPlan::Maker { heavy_outcome, .. } | ExitPlan::Taker { heavy_outcome, .. } => {
                    Some(*heavy_outcome)
                }
                ExitPlan::Skip { .. } => None,
            };

            // Exit mode safety: do not keep buying the side we are trying to reduce.
            // Cancel existing heavy-side resting buys and suppress new heavy-side buys.
            // Also set the persistent exit_buy_block so that even after the sell fills
            // and excess drops below exit_soft_excess, we don't re-buy the heavy side.
            if let Some(heavy_outcome) = exit_heavy_outcome {
                if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                    ms.exit_buy_block = Some(heavy_outcome);
                }
                let blocked_new_buys = match heavy_outcome {
                    Outcome::Yes => {
                        let had = !yes_ladder.is_empty();
                        yes_ladder.clear();
                        had
                    }
                    Outcome::No => {
                        let had = !no_ladder.is_empty();
                        no_ladder.clear();
                        had
                    }
                };
                if blocked_new_buys {
                    suppression_reasons.push("exit_mode_block_heavy_buy");
                }

                let heavy_side_buys_to_cancel: Vec<(OrderId, Decimal, Decimal)> = self
                    .active_markets
                    .get(&condition_id)
                    .map(|ms| {
                        ms.resting_orders
                            .iter()
                            .filter(|((outcome, _), _)| *outcome == heavy_outcome)
                            .map(|((_, price), order)| (order.order_id.clone(), *price, order.size))
                            .collect()
                    })
                    .unwrap_or_default();

                if !heavy_side_buys_to_cancel.is_empty() {
                    // Paper-sim cancels (in-memory, no network)
                    if self.config.mode == TradingMode::Paper {
                        for (oid, _, _) in &heavy_side_buys_to_cancel {
                            self.paper_sim.cancel(oid);
                        }
                    } else if let Some(sdk) = &self.sdk {
                        let ids: Vec<&str> = heavy_side_buys_to_cancel
                            .iter()
                            .map(|(oid, _, _)| oid.as_str())
                            .collect();
                        if let Err(e) = sdk.cancel_orders(&ids).await {
                            warn!(
                                count = ids.len(),
                                "[v2] Batch cancel heavy-side buys failed: {e}"
                            );
                        }
                    }
                    // Update local state
                    for (oid, price, size) in &heavy_side_buys_to_cancel {
                        self.period_logger.log_order_event(
                            &period_name,
                            oid,
                            "CANCELLED",
                            heavy_outcome,
                            *price,
                            *size,
                            *size,
                            "exit_mode_heavy_buy_cancel",
                        );
                        self.fill_handler.unregister_order(&condition_id, oid);
                    }
                    if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                        ms.orders_cancelled += heavy_side_buys_to_cancel.len() as u32;
                        let cancel_ids: std::collections::HashSet<&str> = heavy_side_buys_to_cancel
                            .iter()
                            .map(|(oid, _, _)| oid.as_str())
                            .collect();
                        ms.resting_orders
                            .retain(|_, order| !cancel_ids.contains(order.order_id.as_str()));
                    }
                }
            }

            // ── Running combined cost for dashboard ──
            let avg_yes = if position.yes_qty > Decimal::ZERO {
                position.total_yes_spent / position.yes_qty
            } else {
                Decimal::ZERO
            };
            let avg_no = if position.no_qty > Decimal::ZERO {
                position.total_no_spent / position.no_qty
            } else {
                Decimal::ZERO
            };
            let running_combined = avg_yes + avg_no;

            // ── Update dashboard signal data ──
            let top_bid_yes = yes_ladder.first().map(|l| l.price).unwrap_or(Decimal::ZERO);
            let top_bid_no = no_ladder.first().map(|l| l.price).unwrap_or(Decimal::ZERO);
            {
                let mut dash = self.dashboard.write();
                dash.fv_up = fv_up;
                dash.fv_down = 1.0 - fv_up;
                dash.sigma = sigma;
                dash.bid_yes = top_bid_yes;
                dash.bid_no = top_bid_no;
                dash.combined_bid = top_bid_yes + top_bid_no;
                dash.market_best_bid_up =
                    yes_book.best_bid().map(|(p, _)| p).unwrap_or(Decimal::ZERO);
                dash.market_best_ask_up =
                    yes_book.best_ask().map(|(p, _)| p).unwrap_or(Decimal::ZERO);
                dash.market_best_bid_down =
                    no_book.best_bid().map(|(p, _)| p).unwrap_or(Decimal::ZERO);
                dash.market_best_ask_down =
                    no_book.best_ask().map(|(p, _)| p).unwrap_or(Decimal::ZERO);
                dash.running_combined_cost = running_combined;
                dash.pipeline.cex_feed_ok = true;
                dash.pipeline.pm_odds_ok = true;
                let edge = (dec!(1) - top_bid_yes - top_bid_no)
                    .to_string()
                    .parse::<f64>()
                    .unwrap_or(0.0);
                dash.pipeline.edge_found = edge > 0.0;
                dash.pipeline.last_edge = edge;
                dash.pipeline.kelly_ok = !yes_ladder.is_empty() || !no_ladder.is_empty();
                dash.pipeline.last_kelly = dash.pipeline.last_edge * 2.0;
                dash.pipeline.exec_ok = !yes_ladder.is_empty() || !no_ladder.is_empty();
            }

            // ── Log prices to CSV (enhanced with vol/spread data) ──
            let (sigma_source, realized_vol_1m, realized_vol_5m, btc_price_1m_ago) = {
                let bs = self.asset_price.read();
                (
                    bs.sigma_source(),
                    bs.realized_vol_over(60).unwrap_or(0.0),
                    bs.realized_vol_over(300).unwrap_or(0.0),
                    bs.price_at_offset(60).unwrap_or(0.0),
                )
            };
            self.period_logger.log_prices(
                &period_name,
                btc_current,
                btc_open,
                fv_up,
                1.0 - fv_up,
                sigma,
                remaining_secs,
                yes_book.best_bid().map(|(p, _)| p).unwrap_or(Decimal::ZERO),
                yes_book.best_ask().map(|(p, _)| p).unwrap_or(Decimal::ZERO),
                no_book.best_bid().map(|(p, _)| p).unwrap_or(Decimal::ZERO),
                no_book.best_ask().map(|(p, _)| p).unwrap_or(Decimal::ZERO),
                position.yes_qty,
                position.no_qty,
                position.complete_pairs(),
                position.locked_profit(),
                sigma_source,
                realized_vol_1m,
                realized_vol_5m,
                btc_price_1m_ago,
                raw_fv_up,
            );

            // ── Decision logging ──
            // Log the decision point AFTER all ladder filters have been applied.
            // This captures the net effect of all guards (EV breaker, balance mgmt,
            // postonly, cooldown, trending, time-decay) in a single row.
            {
                let bb_up = yes_book.best_bid().map(|(p, _)| p).unwrap_or(Decimal::ZERO);
                let ba_up = yes_book.best_ask().map(|(p, _)| p).unwrap_or(Decimal::ZERO);
                let bb_dn = no_book.best_bid().map(|(p, _)| p).unwrap_or(Decimal::ZERO);
                let ba_dn = no_book.best_ask().map(|(p, _)| p).unwrap_or(Decimal::ZERO);
                let budget_used = position.total_yes_spent + position.total_no_spent;
                let budget_limit = self.config.max_position_per_market;

                if yes_ladder.is_empty() && no_ladder.is_empty() {
                    // All ladders suppressed — use granular per-filter reasons
                    let reason =
                        if remaining_secs <= self.config.resolution_safety_margin_secs as f64 {
                            "resolution_safety_margin".to_string()
                        } else if !suppression_reasons.is_empty() {
                            suppression_reasons.join("+")
                        } else {
                            "all_filters_suppressed".to_string()
                        };
                    let reason = reason.as_str();
                    self.period_logger.log_decision(
                        &period_name,
                        "TICK",
                        "SKIP",
                        "BOTH",
                        Decimal::ZERO,
                        Decimal::ZERO,
                        reason,
                        fv_up,
                        1.0 - fv_up,
                        sigma,
                        btc_current,
                        btc_open,
                        remaining_secs,
                        bb_up,
                        ba_up,
                        bb_dn,
                        ba_dn,
                        position.yes_qty,
                        position.no_qty,
                        position.complete_pairs(),
                        budget_used,
                        budget_limit,
                    );
                } else {
                    // Log what will be placed
                    let yes_top = yes_ladder.first().map(|l| l.price).unwrap_or(Decimal::ZERO);
                    let no_top = no_ladder.first().map(|l| l.price).unwrap_or(Decimal::ZERO);
                    let yes_levels = yes_ladder.len();
                    let no_levels = no_ladder.len();
                    let mut reason = format!("yes_levels={yes_levels}_no_levels={no_levels}");
                    if let Some(label) = &directional_skew_label {
                        reason.push('_');
                        reason.push_str(label);
                    }
                    self.period_logger.log_decision(
                        &period_name,
                        "TICK",
                        "PLACE_LADDER",
                        "BOTH",
                        yes_top,
                        no_top.max(self.v2.level_order_size),
                        &reason,
                        fv_up,
                        1.0 - fv_up,
                        sigma,
                        btc_current,
                        btc_open,
                        remaining_secs,
                        bb_up,
                        ba_up,
                        bb_dn,
                        ba_dn,
                        position.yes_qty,
                        position.no_qty,
                        position.complete_pairs(),
                        budget_used,
                        budget_limit,
                    );
                }

                // ── Prometheus: suppression counters (atomic, no I/O) ──
                for reason in &suppression_reasons {
                    metrics::counter!("suppression_total", "asset" => self.asset.display_name(), "reason" => (*reason).to_string()).increment(1);
                }
                if yes_ladder.is_empty() && no_ladder.is_empty() {
                    metrics::counter!("tick_skipped_total", "asset" => self.asset.display_name())
                        .increment(1);
                } else {
                    metrics::counter!("tick_placed_total", "asset" => self.asset.display_name())
                        .increment(1);
                }

                // Exit planner observability: maker/taker/skip reasons.
                match &exit_plan {
                    ExitPlan::Skip {
                        reason,
                        heavy_outcome,
                        ..
                    } => {
                        let outcome = match heavy_outcome {
                            Some(Outcome::Yes) => "UP",
                            Some(Outcome::No) => "DOWN",
                            None => "BOTH",
                        };
                        self.period_logger.log_decision(
                            &period_name,
                            "EXIT",
                            "SKIP_EXIT",
                            outcome,
                            Decimal::ZERO,
                            Decimal::ZERO,
                            reason,
                            fv_up,
                            1.0 - fv_up,
                            sigma,
                            btc_current,
                            btc_open,
                            remaining_secs,
                            bb_up,
                            ba_up,
                            bb_dn,
                            ba_dn,
                            position.yes_qty,
                            position.no_qty,
                            position.complete_pairs(),
                            budget_used,
                            budget_limit,
                        );
                    }
                    ExitPlan::Maker {
                        reason,
                        heavy_outcome,
                        levels,
                        ..
                    } => {
                        let price = levels.first().map(|l| l.price).unwrap_or(Decimal::ZERO);
                        let size: Decimal = levels.iter().map(|l| l.size).sum();
                        let outcome = match heavy_outcome {
                            Outcome::Yes => "UP",
                            Outcome::No => "DOWN",
                        };
                        self.period_logger.log_decision(
                            &period_name,
                            "EXIT",
                            "PLACE_SELL_MAKER",
                            outcome,
                            price,
                            size,
                            reason,
                            fv_up,
                            1.0 - fv_up,
                            sigma,
                            btc_current,
                            btc_open,
                            remaining_secs,
                            bb_up,
                            ba_up,
                            bb_dn,
                            ba_dn,
                            position.yes_qty,
                            position.no_qty,
                            position.complete_pairs(),
                            budget_used,
                            budget_limit,
                        );
                    }
                    ExitPlan::Taker {
                        reason,
                        heavy_outcome,
                        size,
                        price,
                        ..
                    } => {
                        let outcome = match heavy_outcome {
                            Outcome::Yes => "UP",
                            Outcome::No => "DOWN",
                        };
                        self.period_logger.log_decision(
                            &period_name,
                            "EXIT",
                            "PLACE_SELL_TAKER",
                            outcome,
                            *price,
                            *size,
                            reason,
                            fv_up,
                            1.0 - fv_up,
                            sigma,
                            btc_current,
                            btc_open,
                            remaining_secs,
                            bb_up,
                            ba_up,
                            bb_dn,
                            ba_dn,
                            position.yes_qty,
                            position.no_qty,
                            position.complete_pairs(),
                            budget_used,
                            budget_limit,
                        );
                    }
                }
            }

            // Note: do NOT `continue` when both ladders are empty — we still
            // need to cancel stale resting orders, run sell-back, and pair completion.

            // Gate the cancel/place cycle on whether ladder centers have moved enough
            if should_reprice {
                // ── FV-stale cancel: nuke all resting on a side when FV shifts away ──
                // If the highest resting bid on a side exceeds the new ladder top by
                // more than fv_stale_cancel_cents, cancel ALL resting on that side.
                // This reacts to FV shifts faster than the per-order stale_distance_ticks.
                if self.v2.fv_stale_cancel_cents > Decimal::ZERO {
                    let threshold = self.v2.fv_stale_cancel_cents;
                    let yes_ladder_top =
                        yes_ladder.first().map(|l| l.price).unwrap_or(Decimal::ZERO);
                    let no_ladder_top = no_ladder.first().map(|l| l.price).unwrap_or(Decimal::ZERO);

                    // Collect IDs to cancel while holding immutable borrow, then release
                    // Deep level protection: only cancel orders ABOVE fv_cancel_min_price.
                    // Orders at very cheap prices ($0.01-$0.15) are kept alive even when
                    // FV shifts — they catch panic dumps and cost almost nothing.
                    let fv_cancel_floor = self.v2.fv_cancel_min_price;

                    let (nuke_yes, nuke_no) = if let Some(ms) =
                        self.active_markets.get(&condition_id)
                    {
                        let has_yes_stale =
                            ms.resting_orders.iter().any(|((outcome, price), _)| {
                                *outcome == Outcome::Yes && *price > yes_ladder_top + threshold
                            });
                        let has_no_stale = ms.resting_orders.iter().any(|((outcome, price), _)| {
                            *outcome == Outcome::No && *price > no_ladder_top + threshold
                        });

                        let yes_to_nuke: Vec<OrderId> = if has_yes_stale {
                            ms.resting_orders
                                .iter()
                                .filter(|((outcome, price), _)| {
                                    *outcome == Outcome::Yes && *price >= fv_cancel_floor
                                })
                                .map(|(_, o)| o.order_id.clone())
                                .collect()
                        } else {
                            Vec::new()
                        };
                        let no_to_nuke: Vec<OrderId> = if has_no_stale {
                            ms.resting_orders
                                .iter()
                                .filter(|((outcome, price), _)| {
                                    *outcome == Outcome::No && *price >= fv_cancel_floor
                                })
                                .map(|(_, o)| o.order_id.clone())
                                .collect()
                        } else {
                            Vec::new()
                        };
                        (yes_to_nuke, no_to_nuke)
                    } else {
                        (Vec::new(), Vec::new())
                    };
                    // Immutable borrow on active_markets is now dropped

                    if !nuke_yes.is_empty() {
                        // Collect order info for event logging before mutating
                        let yes_order_info: Vec<(OrderId, Decimal, Decimal)> = self
                            .active_markets
                            .get(&condition_id)
                            .map(|ms| {
                                ms.resting_orders
                                    .iter()
                                    .filter(|((o, _), _)| *o == Outcome::Yes)
                                    .map(|((_, price), o)| (o.order_id.clone(), *price, o.size))
                                    .collect()
                            })
                            .unwrap_or_default();
                        // Batch cancel YES orders — only update state for confirmed cancels
                        if self.config.mode == TradingMode::Paper {
                            for oid in &nuke_yes {
                                self.paper_sim.cancel(oid);
                            }
                        }
                        let yes_ids: Vec<&str> = nuke_yes.iter().map(|s| s.as_str()).collect();
                        let confirmed = self.batch_cancel_confirmed(&yes_ids, "fv_stale_yes").await;
                        for oid in &nuke_yes {
                            if confirmed.contains(oid.as_str()) {
                                self.fill_handler.unregister_order(&condition_id, oid);
                            }
                        }
                        for (oid, price, size) in &yes_order_info {
                            if confirmed.contains(oid.as_str()) {
                                self.period_logger.log_order_event(
                                    &period_name,
                                    oid,
                                    "CANCELLED",
                                    Outcome::Yes,
                                    *price,
                                    *size,
                                    *size,
                                    "fv_stale_shift",
                                );
                            }
                        }
                        if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                            ms.orders_cancelled += confirmed.len() as u32;
                            ms.resting_orders.retain(|(outcome, _), o| {
                                !(*outcome == Outcome::Yes
                                    && confirmed.contains(o.order_id.as_str()))
                            });
                        }
                        info!(
                            condition_id = %condition_id,
                            cancelled = nuke_yes.len(),
                            "[v2] FV-stale: cancelled all YES resting (FV shifted)"
                        );
                    }
                    if !nuke_no.is_empty() {
                        let no_order_info: Vec<(OrderId, Decimal, Decimal)> = self
                            .active_markets
                            .get(&condition_id)
                            .map(|ms| {
                                ms.resting_orders
                                    .iter()
                                    .filter(|((o, _), _)| *o == Outcome::No)
                                    .map(|((_, price), o)| (o.order_id.clone(), *price, o.size))
                                    .collect()
                            })
                            .unwrap_or_default();
                        // Batch cancel NO orders — only update state for confirmed cancels
                        if self.config.mode == TradingMode::Paper {
                            for oid in &nuke_no {
                                self.paper_sim.cancel(oid);
                            }
                        }
                        let no_ids: Vec<&str> = nuke_no.iter().map(|s| s.as_str()).collect();
                        let confirmed = self.batch_cancel_confirmed(&no_ids, "fv_stale_no").await;
                        for oid in &nuke_no {
                            if confirmed.contains(oid.as_str()) {
                                self.fill_handler.unregister_order(&condition_id, oid);
                            }
                        }
                        for (oid, price, size) in &no_order_info {
                            if confirmed.contains(oid.as_str()) {
                                self.period_logger.log_order_event(
                                    &period_name,
                                    oid,
                                    "CANCELLED",
                                    Outcome::No,
                                    *price,
                                    *size,
                                    *size,
                                    "fv_stale_shift",
                                );
                            }
                        }
                        if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                            ms.orders_cancelled += confirmed.len() as u32;
                            ms.resting_orders.retain(|(outcome, _), o| {
                                !(*outcome == Outcome::No
                                    && confirmed.contains(o.order_id.as_str()))
                            });
                        }
                        info!(
                            condition_id = %condition_id,
                            cancelled = nuke_no.len(),
                            "[v2] FV-stale: cancelled all NO resting (FV shifted)"
                        );
                    }
                }

                // ── Diff ladder vs resting orders ──
                let resting = self
                    .active_markets
                    .get(&condition_id)
                    .map(|ms| &ms.resting_orders)
                    .cloned()
                    .unwrap_or_default();

                let (mut yes_to_place, yes_to_cancel) = diff_ladder_vs_resting(
                    &yes_ladder,
                    &resting,
                    market.tick_size,
                    self.v2.stale_distance_ticks,
                    self.v2.deep_level_stale_distance,
                    self.v2.fv_cancel_min_price,
                );
                let (mut no_to_place, no_to_cancel) = diff_ladder_vs_resting(
                    &no_ladder,
                    &resting,
                    market.tick_size,
                    self.v2.stale_distance_ticks,
                    self.v2.deep_level_stale_distance,
                    self.v2.fv_cancel_min_price,
                );

                if let Some(limit) = self.v2.buy_level_activation_limit_for_duration(
                    market.effective_duration_minutes_15m_fallback(),
                ) {
                    let throttled = yes_to_place.len() > limit || no_to_place.len() > limit;
                    yes_to_place.truncate(limit);
                    no_to_place.truncate(limit);
                    if throttled {
                        suppression_reasons.push("buy_activation_throttle_5m");
                    }
                }

                self.update_period_telemetry(
                    &condition_id,
                    &position,
                    &yes_ladder,
                    &no_ladder,
                    &suppression_reasons,
                );

                // ── Cancel stale orders (batch) ──
                let all_stale: Vec<&String> =
                    yes_to_cancel.iter().chain(no_to_cancel.iter()).collect();
                if !all_stale.is_empty() {
                    // Collect order info for event logging before cancelling
                    let order_infos: Vec<(String, Outcome, Decimal, Decimal)> = all_stale
                        .iter()
                        .filter_map(|oid| {
                            self.active_markets.get(&condition_id).and_then(|ms| {
                                ms.resting_orders
                                    .iter()
                                    .find(|(_, o)| o.order_id == **oid)
                                    .map(|((outcome, price), o)| {
                                        (oid.to_string(), *outcome, *price, o.size)
                                    })
                            })
                        })
                        .collect();

                    // Paper-sim cancels (in-memory)
                    if self.config.mode == TradingMode::Paper {
                        for oid in &all_stale {
                            self.paper_sim.cancel(oid);
                        }
                    }
                    let ids: Vec<&str> = all_stale.iter().map(|s| s.as_str()).collect();
                    let confirmed = self.batch_cancel_confirmed(&ids, "stale_distance").await;

                    // Update local state — only for confirmed cancels
                    for (oid, outcome, price, size) in &order_infos {
                        if confirmed.contains(oid.as_str()) {
                            self.period_logger.log_order_event(
                                &period_name,
                                oid,
                                "CANCELLED",
                                *outcome,
                                *price,
                                *size,
                                *size,
                                "stale_distance",
                            );
                        }
                    }
                    for oid in &all_stale {
                        if confirmed.contains(oid.as_str()) {
                            self.fill_handler.unregister_order(&condition_id, oid);
                        }
                    }
                    if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                        ms.orders_cancelled += confirmed.len() as u32;
                        ms.resting_orders
                            .retain(|_, o| !confirmed.contains(o.order_id.as_str()));
                    }
                }

                // ── Place missing levels ──
                // PRICE-PROPORTIONAL budget split: allocate dollars proportional
                // to FV prices so both sides target EQUAL SHARE COUNTS.
                // E.g., FV UP=0.30, DOWN=0.70 → YES gets 30% of budget, NO gets 70%.
                // This maximizes pair formation (the core arb mechanism).
                struct PreparedLevel {
                    outcome: Outcome,
                    price: Decimal,
                    size: Decimal,
                }

                // Compute per-side resting notional to prevent cross-cycle leakage
                // Include BOTH ask-anchored ladder AND static deep grid orders
                let deep_grid_resting = self
                    .active_markets
                    .get(&condition_id)
                    .map(|ms| ms.resting_deep_grid.clone())
                    .unwrap_or_default();

                let yes_resting_notional: Decimal = resting
                    .iter()
                    .chain(deep_grid_resting.iter())
                    .filter(|((outcome, _), _)| *outcome == Outcome::Yes)
                    .map(|((_, price), order)| *price * order.size)
                    .sum();
                let no_resting_notional: Decimal = resting
                    .iter()
                    .chain(deep_grid_resting.iter())
                    .filter(|((outcome, _), _)| *outcome == Outcome::No)
                    .map(|((_, price), order)| *price * order.size)
                    .sum();

                // Compute per-side resting SHARES for share-based ceiling
                let yes_resting_shares: Decimal = resting
                    .iter()
                    .chain(deep_grid_resting.iter())
                    .filter(|((outcome, _), _)| *outcome == Outcome::Yes)
                    .map(|(_, order)| order.size)
                    .sum();
                let no_resting_shares: Decimal = resting
                    .iter()
                    .chain(deep_grid_resting.iter())
                    .filter(|((outcome, _), _)| *outcome == Outcome::No)
                    .map(|(_, order)| order.size)
                    .sum();

                // Price-proportional split: allocate budget so both sides target
                // the same number of shares (= maximum pairs).
                let max_budget = self.config.max_position_per_market;
                let fv_up_dec = Decimal::from_f64(fv_up).unwrap_or(dec!(0.5));
                let fv_down_dec = Decimal::from_f64(fv_down).unwrap_or(dec!(0.5));
                let combined_fv = (fv_up_dec + fv_down_dec).max(dec!(0.10));
                // target_pairs = budget / combined_fv (how many pairs we can afford)
                let target_pairs = (max_budget / combined_fv).floor();
                // Per-side budget proportional to its FV price
                let yes_total_budget = target_pairs * fv_up_dec;
                let no_total_budget = target_pairs * fv_down_dec;

                // In paper mode, pair-completion orders live in paper_sim
                // but NOT in ms.resting_orders.  Use the max of both sources
                // so pair-completion orders are counted without double-counting
                // ladder orders that exist in both.
                let (yes_all_resting, no_all_resting) = if self.config.mode == TradingMode::Paper {
                    (
                        yes_resting_shares.max(
                            self.paper_sim
                                .resting_buy_shares(&condition_id, Outcome::Yes),
                        ),
                        no_resting_shares.max(
                            self.paper_sim
                                .resting_buy_shares(&condition_id, Outcome::No),
                        ),
                    )
                } else {
                    (yes_resting_shares, no_resting_shares)
                };

                // Hard ceiling: already-filled + resting shares must not exceed target_pairs
                let yes_committed_shares = position.yes_qty + yes_all_resting;
                let no_committed_shares = position.no_qty + no_all_resting;
                let yes_share_room = (target_pairs - yes_committed_shares).max(Decimal::ZERO);
                let no_share_room = (target_pairs - no_committed_shares).max(Decimal::ZERO);

                // Dollar room: total budget minus filled minus resting notional
                let yes_dollar_room =
                    (yes_total_budget - position.total_yes_spent - yes_resting_notional)
                        .max(Decimal::ZERO);
                let no_dollar_room =
                    (no_total_budget - position.total_no_spent - no_resting_notional)
                        .max(Decimal::ZERO);

                // Use the tighter of share-room and dollar-room
                // (share_room × price converts to dollar equivalent)
                let yes_base = yes_dollar_room.min(yes_share_room * fv_up_dec.max(dec!(0.05)));
                let no_base = no_dollar_room.min(no_share_room * fv_down_dec.max(dec!(0.05)));

                // Rebalance extra capacity DISABLED: with price-proportional
                // budget split, both sides already target equal share counts.
                // Adding rebalance bypassed the share-room constraint, causing
                // the light side to overshoot target_pairs (e.g., 25 YES when
                // target was 15).
                let _is_yes_light = position.light_side() == Some(Outcome::Yes);
                let _is_no_light = position.light_side() == Some(Outcome::No);

                // Hard share cap: never allow dollar budget to exceed what
                // share_room permits (prevents any future bypass path).
                let yes_base = yes_base.min(yes_share_room * fv_up_dec.max(dec!(0.05)));
                let no_base = no_base.min(no_share_room * fv_down_dec.max(dec!(0.05)));

                // Cap combined to remaining capacity (safety)
                let total_base = yes_base + no_base;
                let (yes_budget, no_budget) = if total_base > capacity && total_base > Decimal::ZERO
                {
                    let scale = capacity / total_base;
                    (yes_base * scale, no_base * scale)
                } else {
                    (yes_base, no_base)
                };

                let mut prepared_levels: Vec<PreparedLevel> = Vec::new();

                // Process YES side with its budget AND share cap
                let mut yes_used = Decimal::ZERO;
                let mut yes_shares_placed = Decimal::ZERO;
                for level in yes_to_place.iter() {
                    let remaining_cap = (yes_budget - yes_used).max(Decimal::ZERO);
                    let remaining_shares = (yes_share_room - yes_shares_placed).max(Decimal::ZERO);
                    let size = level
                        .size
                        .min(remaining_cap / level.price.max(dec!(0.01)))
                        .min(remaining_shares)
                        .floor();
                    let size = cap_buy_size_for_notional(
                        size,
                        level.price,
                        self.v2.single_order_notional_cap_usdc,
                    );
                    if size < MIN_ORDER_SHARES {
                        continue;
                    }
                    yes_used += size * level.price;
                    yes_shares_placed += size;
                    prepared_levels.push(PreparedLevel {
                        outcome: level.outcome,
                        price: level.price,
                        size,
                    });
                }

                // Process NO side with its budget AND share cap
                let mut no_used = Decimal::ZERO;
                let mut no_shares_placed = Decimal::ZERO;
                for level in no_to_place.iter() {
                    let remaining_cap = (no_budget - no_used).max(Decimal::ZERO);
                    let remaining_shares = (no_share_room - no_shares_placed).max(Decimal::ZERO);
                    let size = level
                        .size
                        .min(remaining_cap / level.price.max(dec!(0.01)))
                        .min(remaining_shares)
                        .floor();
                    let size = cap_buy_size_for_notional(
                        size,
                        level.price,
                        self.v2.single_order_notional_cap_usdc,
                    );
                    if size < MIN_ORDER_SHARES {
                        continue;
                    }
                    no_used += size * level.price;
                    no_shares_placed += size;
                    prepared_levels.push(PreparedLevel {
                        outcome: level.outcome,
                        price: level.price,
                        size,
                    });
                }

                // Stamp first_order_placed_at once on the first order placement
                if !prepared_levels.is_empty() {
                    if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                        if ms.first_order_placed_at.is_none() {
                            ms.first_order_placed_at = Some(Instant::now());
                        }
                    }
                }

                if self.config.mode == TradingMode::Paper {
                    // Paper mode: place individually (paper sim doesn't benefit from batching)
                    for pl in &prepared_levels {
                        let oid =
                            self.paper_sim
                                .place_buy(&condition_id, pl.outcome, pl.price, pl.size);
                        self.period_logger.log_order(
                            &period_name,
                            "buy",
                            pl.outcome,
                            pl.price,
                            pl.size,
                            btc_current,
                            btc_open,
                            fv_up,
                            1.0 - fv_up,
                            sigma,
                            remaining_secs,
                            &condition_id,
                            &oid,
                            "paper",
                        );
                        self.period_logger.log_order_event(
                            &period_name,
                            &oid,
                            "PLACED",
                            pl.outcome,
                            pl.price,
                            pl.size,
                            pl.size,
                            "ladder_buy",
                        );
                        self.fill_handler
                            .register_order(&condition_id, oid.clone(), pl.outcome);
                        if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                            ms.orders_placed += 1;
                            self.order_timestamps.push_back(Instant::now());
                            ms.resting_orders.insert(
                                (pl.outcome, pl.price),
                                RestingLadderOrder {
                                    order_id: oid,
                                    size: pl.size,
                                    placed_at: Instant::now(),
                                },
                            );
                        }
                    }
                } else if let Some(sdk) = &self.sdk {
                    // Live mode: batch all buy orders, chunked to stay within
                    // Polymarket's 15-order-per-request limit.
                    const MAX_BATCH_SIZE: usize = 15;
                    if !prepared_levels.is_empty() {
                        let all_entries: Vec<(&str, Decimal, Decimal, Decimal)> = prepared_levels
                            .iter()
                            .map(|pl| {
                                (
                                    market.token_id(pl.outcome),
                                    pl.price,
                                    pl.size,
                                    market.tick_size,
                                )
                            })
                            .collect();
                        let expiration = Some(market.end_date - chrono::Duration::seconds(60));

                        // Submit in chunks of MAX_BATCH_SIZE
                        let mut all_results: Vec<(usize, String)> = Vec::new();
                        let mut chunk_offset = 0usize;
                        for chunk in all_entries.chunks(MAX_BATCH_SIZE) {
                            let batch = chunk.to_vec();
                            let start = Instant::now();
                            match sdk.place_batch_orders(batch, expiration).await {
                                Ok(results) => {
                                    self.latency_tracker.record(
                                        "order_place",
                                        start.elapsed().as_secs_f64() * 1000.0,
                                    );
                                    // Remap indices back to prepared_levels
                                    for (idx_in_chunk, oid) in results {
                                        all_results.push((chunk_offset + idx_in_chunk, oid));
                                    }
                                }
                                Err(e) => {
                                    let latency_ms = start.elapsed().as_millis();
                                    self.latency_tracker
                                        .record("order_place", latency_ms as f64);
                                    warn!(
                                        %e,
                                        chunk_offset,
                                        chunk_size = chunk.len(),
                                        "[v2] Batch chunk failed"
                                    );
                                    self.period_logger.log_latency(
                                        &period_name,
                                        "place_batch",
                                        latency_ms,
                                        false,
                                        Some(&format!("{e}")),
                                    );
                                }
                            }
                            chunk_offset += chunk.len();
                        }

                        // Process all successful results from all chunks
                        for (orig_idx, oid) in &all_results {
                            let pl = &prepared_levels[*orig_idx];
                            metrics::counter!("orders_placed_total", "asset" => self.asset.display_name(), "outcome" => pl.outcome.to_string(), "type" => "buy").increment(1);
                            self.period_logger.log_order(
                                &period_name,
                                "buy",
                                pl.outcome,
                                pl.price,
                                pl.size,
                                btc_current,
                                btc_open,
                                fv_up,
                                1.0 - fv_up,
                                sigma,
                                remaining_secs,
                                &condition_id,
                                oid,
                                "live",
                            );
                            self.period_logger.log_order_event(
                                &period_name,
                                oid,
                                "PLACED",
                                pl.outcome,
                                pl.price,
                                pl.size,
                                pl.size,
                                "ladder_buy",
                            );
                            self.fill_handler.register_order(
                                &condition_id,
                                oid.clone(),
                                pl.outcome,
                            );
                            if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                                ms.orders_placed += 1;
                                self.order_timestamps.push_back(Instant::now());
                                ms.resting_orders.insert(
                                    (pl.outcome, pl.price),
                                    RestingLadderOrder {
                                        order_id: oid.clone(),
                                        size: pl.size,
                                        placed_at: Instant::now(),
                                    },
                                );
                            }
                            if let Err(e) = self
                                .db
                                .insert_order(
                                    oid,
                                    &condition_id,
                                    pl.outcome,
                                    pl.price,
                                    pl.size,
                                    "buy",
                                )
                                .await
                            {
                                warn!("[v2] Failed to persist ladder order: {e}");
                            }
                        }
                    }
                }

                // Update last ladder centers after successful reprice
                if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                    ms.last_yes_center = new_yes_center;
                    ms.last_no_center = new_no_center;
                }
            } // end if should_reprice

            // ── Static deep grid: one-shot placement ──
            // Place fixed-price bids at $0.01-$0.15 on BOTH Up and Down.
            // These are independent of the ask-anchored ladder, NOT cancelled on
            // FV moves, and catch panic dumps at deep discounts.
            if self.v2.deep_static_grid_enabled {
                let already_placed = self
                    .active_markets
                    .get(&condition_id)
                    .map(|ms| ms.deep_grid_placed)
                    .unwrap_or(true);
                if !already_placed {
                    let (yes_deep, no_deep) = compute_static_deep_grid(&self.v2, market.tick_size);

                    // Filter by combined cost guard: for each price level, both sides
                    // place at the same price, so combined = 2 * price.
                    let max_combined = self.v2.deep_static_max_combined;
                    let mut deep_levels: Vec<LadderLevel> = Vec::new();
                    for level in yes_deep.into_iter().chain(no_deep.into_iter()) {
                        // Combined cost check: this_price + same_price_other_side
                        if level.price * dec!(2) <= max_combined {
                            // Skip if we already have a resting order at this (outcome, price)
                            let key = (level.outcome, level.price);
                            let already_resting = self
                                .active_markets
                                .get(&condition_id)
                                .map(|ms| {
                                    ms.resting_orders.contains_key(&key)
                                        || ms.resting_deep_grid.contains_key(&key)
                                })
                                .unwrap_or(false);
                            if !already_resting {
                                deep_levels.push(level);
                            }
                        }
                    }

                    if !deep_levels.is_empty() {
                        // Stamp first_order_placed_at
                        if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                            if ms.first_order_placed_at.is_none() {
                                ms.first_order_placed_at = Some(Instant::now());
                            }
                        }

                        if self.config.mode == TradingMode::Paper {
                            for dl in &deep_levels {
                                let oid = self.paper_sim.place_buy(
                                    &condition_id,
                                    dl.outcome,
                                    dl.price,
                                    dl.size,
                                );
                                self.period_logger.log_order_event(
                                    &period_name,
                                    &oid,
                                    "PLACED",
                                    dl.outcome,
                                    dl.price,
                                    dl.size,
                                    dl.size,
                                    "deep_grid_buy",
                                );
                                self.fill_handler.register_order(
                                    &condition_id,
                                    oid.clone(),
                                    dl.outcome,
                                );
                                if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                                    ms.orders_placed += 1;
                                    self.order_timestamps.push_back(Instant::now());
                                    ms.resting_deep_grid.insert(
                                        (dl.outcome, dl.price),
                                        RestingLadderOrder {
                                            order_id: oid,
                                            size: dl.size,
                                            placed_at: Instant::now(),
                                        },
                                    );
                                }
                            }
                        } else if let Some(sdk) = &self.sdk {
                            const MAX_BATCH_SIZE: usize = 15;
                            let all_entries: Vec<(&str, Decimal, Decimal, Decimal)> = deep_levels
                                .iter()
                                .map(|dl| {
                                    (
                                        market.token_id(dl.outcome),
                                        dl.price,
                                        dl.size,
                                        market.tick_size,
                                    )
                                })
                                .collect();
                            let expiration = Some(market.end_date - chrono::Duration::seconds(60));

                            let mut all_results: Vec<(usize, String)> = Vec::new();
                            let mut chunk_offset = 0usize;
                            for chunk in all_entries.chunks(MAX_BATCH_SIZE) {
                                let batch = chunk.to_vec();
                                match sdk.place_batch_orders(batch, expiration).await {
                                    Ok(results) => {
                                        for (idx, oid) in results {
                                            all_results.push((chunk_offset + idx, oid));
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            chunk_offset,
                                            chunk_size = chunk.len(),
                                            "[v2] Deep grid batch chunk failed: {e}"
                                        );
                                    }
                                }
                                chunk_offset += chunk.len();
                            }

                            for (orig_idx, oid) in &all_results {
                                let dl = &deep_levels[*orig_idx];
                                self.period_logger.log_order_event(
                                    &period_name,
                                    oid,
                                    "PLACED",
                                    dl.outcome,
                                    dl.price,
                                    dl.size,
                                    dl.size,
                                    "deep_grid_buy",
                                );
                                self.fill_handler.register_order(
                                    &condition_id,
                                    oid.clone(),
                                    dl.outcome,
                                );
                                if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                                    ms.orders_placed += 1;
                                    self.order_timestamps.push_back(Instant::now());
                                    ms.resting_deep_grid.insert(
                                        (dl.outcome, dl.price),
                                        RestingLadderOrder {
                                            order_id: oid.clone(),
                                            size: dl.size,
                                            placed_at: Instant::now(),
                                        },
                                    );
                                }
                            }
                        }

                        info!(
                            condition_id = %condition_id,
                            levels = deep_levels.len(),
                            "[v2] Static deep grid placed"
                        );
                    }

                    // Mark as placed even if no levels passed the guard
                    if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                        ms.deep_grid_placed = true;
                    }
                }
            }

            // ── Sell-back order management ──
            {
                let resting_sells = self
                    .active_markets
                    .get(&condition_id)
                    .map(|ms| ms.resting_sells.clone())
                    .unwrap_or_default();

                // Cancel stale sell orders not in the new sell ladder
                let sell_target_keys: std::collections::HashSet<(Outcome, Decimal)> =
                    sell_ladder.iter().map(|l| (l.outcome, l.price)).collect();
                let sells_to_cancel: Vec<OrderId> = resting_sells
                    .iter()
                    .filter(|(key, _)| !sell_target_keys.contains(key))
                    .map(|(_, o)| o.order_id.clone())
                    .collect();

                if !sells_to_cancel.is_empty() {
                    // Collect order info for event logging
                    let sell_infos: Vec<(String, Outcome, Decimal, Decimal)> = sells_to_cancel
                        .iter()
                        .filter_map(|oid| {
                            resting_sells.iter().find(|(_, o)| o.order_id == *oid).map(
                                |((outcome, price), o)| (oid.clone(), *outcome, *price, o.size),
                            )
                        })
                        .collect();

                    // Batch cancel
                    if self.config.mode == TradingMode::Paper {
                        for oid in &sells_to_cancel {
                            self.paper_sim.cancel(oid);
                        }
                    } else if let Some(sdk) = &self.sdk {
                        let ids: Vec<&str> = sells_to_cancel.iter().map(|s| s.as_str()).collect();
                        if let Err(e) = sdk.cancel_orders(&ids).await {
                            warn!(
                                count = ids.len(),
                                "[v2] Batch cancel stale sells failed: {e}"
                            );
                        }
                    }

                    // Update local state
                    for (oid, outcome, price, size) in &sell_infos {
                        self.period_logger.log_order_event(
                            &period_name,
                            oid,
                            "CANCELLED",
                            *outcome,
                            *price,
                            *size,
                            *size,
                            "sell_stale",
                        );
                    }
                    if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                        ms.orders_cancelled += sell_infos.len() as u32;
                        let cancel_set: std::collections::HashSet<&str> =
                            sells_to_cancel.iter().map(|s| s.as_str()).collect();
                        ms.resting_sells
                            .retain(|_, o| !cancel_set.contains(o.order_id.as_str()));
                    }
                }

                // Place new sell levels
                for level in &sell_ladder {
                    if resting_sells.contains_key(&(level.outcome, level.price)) {
                        continue; // already have a sell at this price
                    }

                    // Orchestrator-level naked sell guard: verify we hold enough to sell
                    let available = self
                        .inventory
                        .available_to_sell(&condition_id, level.outcome);
                    if level.size > available {
                        warn!(
                            condition_id = %condition_id,
                            outcome = %level.outcome,
                            requested = %level.size,
                            available = %available,
                            "[v2] BLOCKED sell-back: would exceed available position"
                        );
                        continue;
                    }

                    if self.config.mode == TradingMode::Paper {
                        let oid = self.paper_sim.place_sell(
                            &condition_id,
                            level.outcome,
                            level.price,
                            level.size,
                        );
                        self.period_logger.log_order(
                            &period_name,
                            "sell",
                            level.outcome,
                            level.price,
                            level.size,
                            btc_current,
                            btc_open,
                            fv_up,
                            1.0 - fv_up,
                            sigma,
                            remaining_secs,
                            &condition_id,
                            &oid,
                            "paper",
                        );
                        self.period_logger.log_order_event(
                            &period_name,
                            &oid,
                            "PLACED",
                            level.outcome,
                            level.price,
                            level.size,
                            level.size,
                            "sell_back",
                        );
                        if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                            ms.orders_placed += 1;
                            self.order_timestamps.push_back(Instant::now());
                            ms.resting_sells.insert(
                                (level.outcome, level.price),
                                RestingLadderOrder {
                                    order_id: oid,
                                    size: level.size,
                                    placed_at: Instant::now(),
                                },
                            );
                        }
                    } else {
                        let token_id = market.token_id(level.outcome);
                        if let Some(sdk) = &self.sdk {
                            let start = Instant::now();
                            match sdk
                                .place_limit_sell(
                                    token_id,
                                    level.price,
                                    level.size,
                                    market.tick_size,
                                    Some(market.end_date - chrono::Duration::seconds(60)),
                                )
                                .await
                            {
                                Ok(oid) => {
                                    metrics::counter!("orders_placed_total", "asset" => self.asset.display_name(), "outcome" => level.outcome.to_string(), "type" => "sell").increment(1);
                                    let latency_ms = start.elapsed().as_millis();
                                    self.period_logger.log_latency(
                                        &period_name,
                                        "place_order",
                                        latency_ms,
                                        true,
                                        None,
                                    );
                                    self.record_latency_success(&condition_id, latency_ms);
                                    self.period_logger.log_order(
                                        &period_name,
                                        "sell",
                                        level.outcome,
                                        level.price,
                                        level.size,
                                        btc_current,
                                        btc_open,
                                        fv_up,
                                        1.0 - fv_up,
                                        sigma,
                                        remaining_secs,
                                        &condition_id,
                                        &oid,
                                        "live",
                                    );
                                    self.period_logger.log_order_event(
                                        &period_name,
                                        &oid,
                                        "PLACED",
                                        level.outcome,
                                        level.price,
                                        level.size,
                                        level.size,
                                        "sell_back",
                                    );
                                    if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                                        ms.orders_placed += 1;
                                        self.order_timestamps.push_back(Instant::now());
                                        ms.resting_sells.insert(
                                            (level.outcome, level.price),
                                            RestingLadderOrder {
                                                order_id: oid.clone(),
                                                size: level.size,
                                                placed_at: Instant::now(),
                                            },
                                        );
                                    }
                                    if let Err(e) = self
                                        .db
                                        .insert_order(
                                            &oid,
                                            &condition_id,
                                            level.outcome,
                                            level.price,
                                            level.size,
                                            "sell",
                                        )
                                        .await
                                    {
                                        warn!("[v2] Failed to persist sell order: {e}");
                                    }
                                }
                                Err(e) => {
                                    let latency_ms = start.elapsed().as_millis();
                                    self.period_logger.log_latency(
                                        &period_name,
                                        "place_order",
                                        latency_ms,
                                        false,
                                        Some(&e.to_string()),
                                    );
                                    self.check_rate_limit_error(&e);
                                    warn!(
                                        condition_id = %condition_id,
                                        outcome = %level.outcome,
                                        price = %level.price,
                                        "[v2] Sell order failed: {e}"
                                    );
                                }
                            }
                        }
                    }
                }
            }

            // ── Exit escalation: taker reduction when risk persists or time is short ──
            if let Some((outcome, mut shares, price)) = taker_exit {
                // Orchestrator-level naked sell guard: clamp to available position
                let available = self.inventory.available_to_sell(&condition_id, outcome);
                if shares > available {
                    warn!(
                        condition_id = %condition_id,
                        outcome = %outcome,
                        requested = %shares,
                        available = %available,
                        "[v2] Clamping exit taker sell to available position"
                    );
                    shares = available;
                }
                if shares > Decimal::ZERO && price > Decimal::ZERO {
                    if !self.can_place_emergency_sell(&condition_id, outcome) {
                        continue;
                    }
                    if self.config.mode == TradingMode::Paper {
                        let oid = self
                            .paper_sim
                            .place_sell(&condition_id, outcome, price, shares);
                        self.period_logger.log_order(
                            &period_name,
                            "sell_taker",
                            outcome,
                            price,
                            shares,
                            btc_current,
                            btc_open,
                            fv_up,
                            1.0 - fv_up,
                            sigma,
                            remaining_secs,
                            &condition_id,
                            &oid,
                            "paper",
                        );
                        self.period_logger.log_order_event(
                            &period_name,
                            &oid,
                            "PLACED",
                            outcome,
                            price,
                            shares,
                            shares,
                            "emergency_sell",
                        );
                        if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                            ms.orders_placed += 1;
                            self.order_timestamps.push_back(Instant::now());
                            ms.resting_sells.insert(
                                (outcome, price),
                                RestingLadderOrder {
                                    order_id: oid,
                                    size: shares,
                                    placed_at: Instant::now(),
                                },
                            );
                        }
                        self.mark_emergency_sell_placed(&condition_id);
                    } else if let Some(sdk) = &self.sdk {
                        let token_id = market.token_id(outcome);
                        let start = Instant::now();
                        match sdk
                            .place_emergency_sell(token_id, price, shares, market.tick_size)
                            .await
                        {
                            Ok(()) => {
                                let latency_ms = start.elapsed().as_millis();
                                self.period_logger.log_latency(
                                    &period_name,
                                    "place_order",
                                    latency_ms,
                                    true,
                                    None,
                                );
                                self.record_latency_success(&condition_id, latency_ms);
                                let oid = format!(
                                    "emergency_{}_{}",
                                    condition_id.chars().take(8).collect::<String>(),
                                    Utc::now().timestamp_millis()
                                );
                                self.period_logger.log_order(
                                    &period_name,
                                    "sell_taker",
                                    outcome,
                                    price,
                                    shares,
                                    btc_current,
                                    btc_open,
                                    fv_up,
                                    1.0 - fv_up,
                                    sigma,
                                    remaining_secs,
                                    &condition_id,
                                    &oid,
                                    "live",
                                );
                                self.period_logger.log_order_event(
                                    &period_name,
                                    &oid,
                                    "PLACED",
                                    outcome,
                                    price,
                                    shares,
                                    shares,
                                    "emergency_sell",
                                );
                                if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                                    ms.orders_placed += 1;
                                    self.order_timestamps.push_back(Instant::now());
                                }
                                self.mark_emergency_sell_placed(&condition_id);
                            }
                            Err(e) => {
                                let latency_ms = start.elapsed().as_millis();
                                self.period_logger.log_latency(
                                    &period_name,
                                    "place_order",
                                    latency_ms,
                                    false,
                                    Some(&e.to_string()),
                                );
                                self.check_rate_limit_error(&e);
                                warn!(
                                    condition_id = %condition_id,
                                    outcome = %outcome,
                                    price = %price,
                                    size = %shares,
                                    "[v2] Emergency taker sell failed: {e}"
                                );
                            }
                        }
                    }
                }
            }

            // ── Late-phase pair completion: cross the spread to lock profit ──
            if (phase == MarketPhase::Late || remaining_secs <= self.v2.very_late_phase_secs as f64)
                && remaining_secs > self.config.resolution_safety_margin_secs as f64
                && !asset_guard_suppressing
            {
                let max_per_cycle = dec!(20);
                let mut allow_pair_completion = true;
                if let Some(ms_mut) = self.active_markets.get_mut(&condition_id) {
                    let imbalance_now = (position.yes_qty - position.no_qty).abs();
                    if imbalance_now < Decimal::ONE {
                        ms_mut.pair_completion_attempts = 0;
                        ms_mut.last_pair_completion_attempt = None;
                    }
                    if ms_mut.pair_completion_attempts >= self.v2.pair_completion_max_attempts {
                        allow_pair_completion = false;
                    }
                    if self.v2.pair_completion_retry_secs > 0 {
                        if let Some(last) = ms_mut.last_pair_completion_attempt {
                            let retry_gap = Duration::from_secs(self.v2.pair_completion_retry_secs);
                            if Instant::now().duration_since(last) < retry_gap {
                                allow_pair_completion = false;
                            }
                        }
                    }
                }

                if allow_pair_completion {
                    let taker_fee_rate = self
                        .active_markets
                        .get(&condition_id)
                        .and_then(|ms| ms.taker_fee_rate);
                    if let Some((outcome, shares, price)) = compute_pair_completion(
                        &position,
                        yes_book.best_ask().map(|(p, _)| p),
                        no_book.best_ask().map(|(p, _)| p),
                        max_per_cycle,
                        taker_fee_rate,
                        self.v2.pair_fee_buffer,
                    ) {
                        // In paper mode, pair completion places regular bids (not FOK),
                        // so previous attempts may still be resting. Subtract those to
                        // avoid doubling the light-side position.
                        let already_resting = if self.config.mode == TradingMode::Paper {
                            self.paper_sim.resting_buy_shares(&condition_id, outcome)
                        } else {
                            Decimal::ZERO
                        };
                        let shares =
                            quantize_order_size((shares - already_resting).max(Decimal::ZERO));
                        let shares = cap_buy_size_for_notional(
                            shares,
                            price,
                            self.v2.single_order_notional_cap_usdc,
                        );
                        if shares < MIN_ORDER_SHARES {
                            debug!(
                                condition_id = %condition_id,
                                outcome = %outcome,
                                price = %price,
                                capped_size = %shares,
                                "[v2] Pair completion skipped after single-order notional cap"
                            );
                        } else {
                            let mut attempted_pair_completion = false;

                            if self.config.mode == TradingMode::Paper {
                                attempted_pair_completion = true;
                                // Place the buy order; the NEXT tick's burst-protected fill
                                // check will handle the fill. Do NOT call check_fills_with_book
                                // here — that would bypass burst protection and fill ALL resting
                                // orders without imbalance limits.
                                let _oid =
                                    self.paper_sim
                                        .place_buy(&condition_id, outcome, price, shares);
                                self.period_logger.log_order(
                                    &period_name,
                                    "buy_fok",
                                    outcome,
                                    price,
                                    shares,
                                    btc_current,
                                    btc_open,
                                    fv_up,
                                    1.0 - fv_up,
                                    sigma,
                                    remaining_secs,
                                    &condition_id,
                                    &_oid,
                                    "paper",
                                );
                                self.period_logger.log_order_event(
                                    &period_name,
                                    &_oid,
                                    "PLACED",
                                    outcome,
                                    price,
                                    shares,
                                    shares,
                                    "pair_completion",
                                );
                                if let Some(ms) = self.active_markets.get_mut(&condition_id) {
                                    ms.orders_placed += 1;
                                    ms.pair_completion_successes += 1;
                                    self.order_timestamps.push_back(Instant::now());
                                }
                                info!(
                                    condition_id = %condition_id,
                                    outcome = %outcome,
                                    shares = %shares,
                                    price = %price,
                                    "[v2] Pair completion: crossing spread to buy light side"
                                );
                            } else if let Some(sdk) = &self.sdk {
                                attempted_pair_completion = true;
                                let token_id = market.token_id(outcome);
                                let start = Instant::now();
                                match sdk
                                    .place_fok_buy(token_id, price, shares, market.tick_size)
                                    .await
                                {
                                    Ok(oid) => {
                                        metrics::counter!("orders_placed_total", "asset" => self.asset.display_name(), "outcome" => outcome.to_string(), "type" => "buy_fok").increment(1);
                                        let latency_ms = start.elapsed().as_millis();
                                        self.period_logger.log_latency(
                                            &period_name,
                                            "place_order",
                                            latency_ms,
                                            true,
                                            None,
                                        );
                                        self.record_latency_success(&condition_id, latency_ms);
                                        self.period_logger.log_order(
                                            &period_name,
                                            "buy_fok",
                                            outcome,
                                            price,
                                            shares,
                                            btc_current,
                                            btc_open,
                                            fv_up,
                                            1.0 - fv_up,
                                            sigma,
                                            remaining_secs,
                                            &condition_id,
                                            &oid,
                                            "live",
                                        );
                                        self.period_logger.log_order_event(
                                            &period_name,
                                            &oid,
                                            "PLACED",
                                            outcome,
                                            price,
                                            shares,
                                            shares,
                                            "pair_completion",
                                        );
                                        self.fill_handler.register_order(
                                            &condition_id,
                                            oid.clone(),
                                            outcome,
                                        );
                                        if let Some(ms) = self.active_markets.get_mut(&condition_id)
                                        {
                                            ms.orders_placed += 1;
                                            ms.pair_completion_successes += 1;
                                            self.order_timestamps.push_back(Instant::now());
                                        }
                                        if let Err(e) = self
                                            .db
                                            .insert_order(
                                                &oid,
                                                &condition_id,
                                                outcome,
                                                price,
                                                shares,
                                                "buy_fok",
                                            )
                                            .await
                                        {
                                            warn!(
                                                "[v2] Failed to persist pair completion order: {e}"
                                            );
                                        }
                                        info!(
                                            condition_id = %condition_id,
                                            outcome = %outcome,
                                            shares = %shares,
                                            price = %price,
                                            "[v2] Pair completion: crossing spread to buy light side"
                                        );
                                    }
                                    Err(e) => {
                                        let latency_ms = start.elapsed().as_millis();
                                        self.period_logger.log_latency(
                                            &period_name,
                                            "place_order",
                                            latency_ms,
                                            false,
                                            Some(&e.to_string()),
                                        );
                                        self.check_rate_limit_error(&e);
                                        debug!(
                                            condition_id = %condition_id,
                                            "[v2] Pair completion FOK failed (expected if no liquidity): {e}"
                                        );
                                    }
                                }
                            }

                            if attempted_pair_completion {
                                if let Some(ms_mut) = self.active_markets.get_mut(&condition_id) {
                                    ms_mut.last_pair_completion_attempt = Some(Instant::now());
                                    ms_mut.pair_completion_attempts += 1;
                                }
                            }
                        }
                    }
                }
            }

            // ── Update resting bids display ──
            {
                let mut dash = self.dashboard.write();
                dash.resting_bids.clear();
                if let Some(ms) = self.active_markets.get(&condition_id) {
                    for ((outcome, price), order) in &ms.resting_orders {
                        let outcome_str = match outcome {
                            Outcome::Yes => "UP",
                            Outcome::No => "DOWN",
                        };
                        dash.resting_bids.push(RestingBid {
                            outcome: outcome_str.into(),
                            price: *price,
                            size: order.size,
                        });
                    }
                    // Also show sell orders with SELL prefix
                    for ((outcome, price), order) in &ms.resting_sells {
                        let outcome_str = match outcome {
                            Outcome::Yes => "SELL-UP",
                            Outcome::No => "SELL-DN",
                        };
                        dash.resting_bids.push(RestingBid {
                            outcome: outcome_str.into(),
                            price: *price,
                            size: order.size,
                        });
                    }
                }
                // Sort by outcome then descending price for display
                dash.resting_bids
                    .sort_by(|a, b| a.outcome.cmp(&b.outcome).then(b.price.cmp(&a.price)));
            }
        }

        // If we got through a full cycle without hitting the throttle, reset backoff
        if self.throttle_until.is_none() {
            self.reset_throttle_backoff();
        }
    }

    // ─── Fill Handling ───────────────────────────────────────────────

    fn record_period_fill_counters(
        ms: &mut MarketV2State,
        is_buy: bool,
        outcome: Outcome,
        price: Decimal,
        size: Decimal,
        fill_edge_sample: Option<(f64, f64)>,
        is_full_fill: bool,
    ) {
        // Only count fully filled orders to avoid partial fills inflating fill-rate >1.0.
        // Previously every fill event (including partials) incremented this counter.
        if is_full_fill {
            ms.orders_filled += 1;
        }
        ms.gross_cost += price * size;
        if is_buy {
            ms.gross_buy_filled_usdc += price * size;
        }
        if let Some((edge_notional, size_sum)) = fill_edge_sample {
            ms.fill_edge_notional_sum += edge_notional;
            ms.fill_edge_size_sum += size_sum;
        }
        match outcome {
            Outcome::Yes => ms.total_up_shares_filled += size,
            Outcome::No => ms.total_down_shares_filled += size,
        }
    }

    async fn handle_fill_event(&mut self, fill: FillEvent) {
        // Check active-market BEFORE dedup to avoid "burning" fill IDs for markets
        // not yet inserted into active_markets (race during market discovery).
        // If the market isn't tracked yet, return without marking the fill as seen
        // so it can be processed if/when it arrives again after the market is active.
        if !self.active_markets.contains_key(&fill.condition_id) {
            return;
        }

        // FIX: Dedup BEFORE the closing_position late-fill branch.
        // Previously, the late-fill path returned before dedup, so duplicate WS
        // deliveries could double-apply fills to closing_position and double-count
        // sell_realized_pnl in the aggregate P&L.
        let dedup_key = format!("{}:{}", fill.trade_id, fill.order_id);
        if self.fill_handler.is_duplicate_trade(&dedup_key) {
            return;
        }

        // FIX: Apply late fills to closing_position snapshot instead of the main inventory.
        // After Closing phase frees the inventory position, late fills (from cancel/fill
        // races) must update the snapshot so resolution P&L is correct. We don't touch
        // the main inventory (which would create phantom positions).
        if let Some(ms) = self.active_markets.get_mut(&fill.condition_id) {
            if let Some(ref mut closing_pos) = ms.closing_position {
                match fill.side {
                    FillSide::Buy => match fill.outcome {
                        Outcome::Yes => {
                            closing_pos.yes_qty += fill.size;
                            closing_pos.total_yes_spent += fill.price * fill.size;
                        }
                        Outcome::No => {
                            closing_pos.no_qty += fill.size;
                            closing_pos.total_no_spent += fill.price * fill.size;
                        }
                    },
                    FillSide::Sell => {
                        let realized_pnl = match fill.outcome {
                            Outcome::Yes => {
                                let sold = fill.size.min(closing_pos.yes_qty);
                                if sold > Decimal::ZERO && closing_pos.yes_qty > Decimal::ZERO {
                                    let avg_cost =
                                        closing_pos.total_yes_spent / closing_pos.yes_qty;
                                    closing_pos.yes_qty -= sold;
                                    closing_pos.total_yes_spent -= avg_cost * sold;
                                    (fill.price - avg_cost) * sold
                                } else {
                                    Decimal::ZERO
                                }
                            }
                            Outcome::No => {
                                let sold = fill.size.min(closing_pos.no_qty);
                                if sold > Decimal::ZERO && closing_pos.no_qty > Decimal::ZERO {
                                    let avg_cost = closing_pos.total_no_spent / closing_pos.no_qty;
                                    closing_pos.no_qty -= sold;
                                    closing_pos.total_no_spent -= avg_cost * sold;
                                    (fill.price - avg_cost) * sold
                                } else {
                                    Decimal::ZERO
                                }
                            }
                        };
                        ms.sell_realized_pnl += realized_pnl;
                        // Update aggregate P&L for emergency handler
                        self.inventory.update_aggregate_pnl(realized_pnl);
                    }
                }
                warn!(
                    trade_id = %fill.trade_id,
                    order_id = %fill.order_id,
                    condition_id = %fill.condition_id,
                    outcome = %fill.outcome,
                    side = ?fill.side,
                    price = %fill.price,
                    size = %fill.size,
                    "[v2] Late fill after Closing — applied to closing_position snapshot"
                );
                return;
            }
        }

        // (Dedup already done above, before the closing_position branch.)

        // Sanity check: warn on unexpectedly large fills but NEVER reject.
        // A fill is an on-chain fact — rejecting it creates position drift which
        // triggers reconciliation freeze and emergency shutdown.
        // Only reject truly impossible values (astronomical).
        let max_sane_fill =
            self.v2.level_order_size * Decimal::from(self.v2.ladder_levels) * dec!(2);
        if fill.size > max_sane_fill {
            warn!(
                trade_id = %fill.trade_id,
                order_id = %fill.order_id,
                fill_size = %fill.size,
                max_sane = %max_sane_fill,
                "[v2] Unexpectedly large fill — processing anyway (WS may aggregate multiple maker fills)"
            );
        }
        if fill.size > dec!(10000) {
            error!(
                trade_id = %fill.trade_id,
                fill_size = %fill.size,
                "[v2] REJECTED fill — size astronomically large, likely corrupted WS data"
            );
            return;
        }
        if fill.price > Decimal::ONE || fill.price <= Decimal::ZERO {
            error!(
                trade_id = %fill.trade_id,
                fill_price = %fill.price,
                "[v2] REJECTED fill — price outside valid range (0, 1.0]"
            );
            return;
        }
        // Note: We no longer gate on has_registered_order() here.
        // The WS task already filters by API key owner, so any fill reaching
        // this point IS ours. Orders may be unregistered before the fill arrives
        // (cancel/fill race condition), but the fill is still real.
        if !self.fill_handler.has_registered_order(&fill.order_id) {
            info!(
                order_id = %fill.order_id,
                condition_id = %fill.condition_id,
                "[v2] Fill for cancelled/unregistered order — processing anyway (late WS delivery)"
            );
        }

        // Feed fill into VPIN tracker (buy fills = someone buying from us = sell pressure on us)
        if self.v2.vpin_enabled {
            let vol = fill.size.to_f64().unwrap_or(0.0);
            let is_buy = matches!(fill.side, FillSide::Buy);
            self.vpin_tracker.record_trade(vol, is_buy);
        }

        info!(
            trade_id = %fill.trade_id,
            condition_id = %fill.condition_id,
            outcome = %fill.outcome,
            price = %fill.price,
            size = %fill.size,
            side = ?fill.side,
            "[v2] Fill received"
        );
        metrics::counter!("fills_total", "asset" => self.asset.display_name(), "outcome" => fill.outcome.to_string()).increment(1);

        let (action, sell_realized_pnl) = self.fill_handler.handle_fill(&fill).await;
        // Track sell-back realized P&L, cost basis freed, and both-side cooldown
        if !sell_realized_pnl.is_zero() {
            if let Some(ms) = self.active_markets.get_mut(&fill.condition_id) {
                ms.sell_realized_pnl += sell_realized_pnl;
                // Cost basis freed = avg_cost * sold = fill_price * size - realized_pnl.
                // Withdraw this from the trading budget so it can't be recycled into
                // new buys that would flip the imbalance to the opposite side.
                let cost_basis_freed =
                    (fill.price * fill.size - sell_realized_pnl).max(Decimal::ZERO);
                ms.sell_cost_basis_freed += cost_basis_freed;
                // Both-side cooldown: suppress ALL new buys for cooldown_secs after
                // a sell-back fill. Without this, the freed budget gets immediately
                // re-spent on the opposite side within the same or next tick.
                let now = Instant::now();
                ms.last_sell_time.insert(Outcome::Yes, now);
                ms.last_sell_time.insert(Outcome::No, now);
            }
        }

        // Push fill to dashboard order feed (fixes live-mode fills not appearing)
        let market_label = self
            .active_markets
            .get(&fill.condition_id)
            .map(|ms| {
                let q = &ms.market.question;
                if let Some(dash_idx) = q.find(" - ") {
                    q[dash_idx + 3..].to_string()
                } else {
                    q.chars().take(20).collect::<String>()
                }
            })
            .unwrap_or_else(|| fill.condition_id.chars().take(8).collect::<String>());

        let outcome_str = match fill.outcome {
            Outcome::Yes => "UP",
            Outcome::No => "DOWN",
        };
        let side_str = match fill.side {
            FillSide::Buy => "BUY",
            FillSide::Sell => "SELL",
        };

        // Build detailed entry before taking dashboard lock
        let detailed = self.build_detailed_entry(
            fill.timestamp,
            &market_label,
            &fill.condition_id,
            side_str,
            outcome_str,
            fill.price,
            fill.size,
            "FILLED",
        );

        {
            let mut dash = self.dashboard.write();
            dash.push_order(OrderFeedEntry {
                time: fill.timestamp,
                market: market_label,
                side: side_str.to_string(),
                outcome: outcome_str.to_string(),
                price: fill.price,
                size: fill.size,
                status: OrderStatus::Filled,
            });
            dash.push_detailed_order(detailed);
            dash.total_fills += 1;
        }
        // Persist fill count to DB (fire and forget)
        let db_clone = self.db.clone();
        tokio::spawn(async move {
            let _ = db_clone.increment_session_fills(1).await;
        });

        match action {
            FillAction::None => {
                let pos = self.inventory.get_position(&fill.condition_id);
                if let Some(pos) = pos {
                    let pairs = pos.complete_pairs();
                    let locked = pos.locked_profit();
                    if pairs > Decimal::ZERO {
                        info!(
                            condition_id = %fill.condition_id,
                            pairs = %pairs,
                            locked_profit = %locked,
                            yes = %pos.yes_qty,
                            no = %pos.no_qty,
                            "[v2] Complete pairs status"
                        );
                    }
                }
            }
            FillAction::SkewQuotes => {
                debug!("[v2] Fill handled, quotes will be skewed on next refresh");
            }
            FillAction::CancelOverweightSide { outcome, order_ids } => {
                info!(
                    ?outcome,
                    count = order_ids.len(),
                    "[v2] Cancelling overweight side orders"
                );
                if self.config.mode == TradingMode::Live {
                    let ids: Vec<&str> = order_ids.iter().map(|s| s.as_str()).collect();
                    let confirmed = self.batch_cancel_confirmed(&ids, "overweight_side").await;
                    for oid in &order_ids {
                        if confirmed.contains(oid.as_str()) {
                            if let Err(e) = self.db.update_order_status(oid, "cancelled").await {
                                warn!("[v2] Failed to update cancelled order in DB: {e}");
                            }
                        }
                    }
                }
            }
            FillAction::EmergencySell {
                outcome,
                excess_qty,
                order_ids_to_cancel,
            } => {
                warn!(
                    ?outcome,
                    %excess_qty,
                    "[v2] Emergency imbalance detected — forcing sell reduction"
                );

                if !order_ids_to_cancel.is_empty() {
                    // Batch cancel via SDK — only update state for confirmed cancels
                    if self.config.mode == TradingMode::Paper {
                        for oid in &order_ids_to_cancel {
                            self.paper_sim.cancel(oid);
                        }
                    }
                    let ids: Vec<&str> = order_ids_to_cancel.iter().map(|s| s.as_str()).collect();
                    let confirmed = self
                        .batch_cancel_confirmed(&ids, "emergency_sell_pre_cancel")
                        .await;
                    for oid in &order_ids_to_cancel {
                        if confirmed.contains(oid.as_str()) {
                            if let Err(e) = self.db.update_order_status(oid, "cancelled").await {
                                warn!("[v2] Failed to update cancelled order in DB: {e}");
                            }
                            self.fill_handler.unregister_order(&fill.condition_id, oid);
                        }
                    }
                    if let Some(ms) = self.active_markets.get_mut(&fill.condition_id) {
                        ms.resting_orders
                            .retain(|_, o| !confirmed.contains(o.order_id.as_str()));
                        ms.resting_sells
                            .retain(|_, o| !confirmed.contains(o.order_id.as_str()));
                        ms.orders_cancelled += confirmed.len() as u32;
                    }
                }

                if excess_qty > Decimal::ZERO {
                    // Orchestrator-level naked sell guard: clamp to available position
                    let available = self
                        .inventory
                        .available_to_sell(&fill.condition_id, outcome);
                    let clamped_excess = excess_qty.min(available);
                    if clamped_excess <= Decimal::ZERO {
                        warn!(
                            condition_id = %fill.condition_id,
                            ?outcome,
                            requested = %excess_qty,
                            available = %available,
                            "[v2] BLOCKED emergency sell: no position available to sell"
                        );
                    }
                    let market_ctx = if clamped_excess > Decimal::ZERO
                        && self.can_place_emergency_sell(&fill.condition_id, outcome)
                    {
                        self.active_markets.get(&fill.condition_id).map(|ms| {
                            (
                                ms.market.clone(),
                                ms.period_name.clone(),
                                ms.btc_open.unwrap_or(0.0),
                                self.time_manager.seconds_remaining(ms.market.end_date) as f64,
                            )
                        })
                    } else {
                        None
                    };
                    if let Some((market, period_name, btc_open_raw, remaining_secs)) = market_ctx {
                        let best_bid = {
                            let books = self.orderbooks.read();
                            books
                                .get(market.token_id(outcome))
                                .and_then(|book| book.best_bid().map(|(p, _)| p))
                        };
                        if let Some(best_bid) = best_bid {
                            if best_bid > Decimal::ZERO {
                                let size = quantize_order_size(
                                    clamped_excess.min(self.v2.sell_level_size),
                                );
                                if size < MIN_ORDER_SHARES {
                                    debug!(
                                        condition_id = %fill.condition_id,
                                        ?outcome,
                                        raw_excess = %excess_qty,
                                        quantized_size = %size,
                                        "[v2] Emergency sell skipped: below minimum order size"
                                    );
                                } else {
                                    let (btc_current_opt, vol_opt) = {
                                        let bs = self.asset_price.read();
                                        (bs.current_price, bs.realized_vol_per_sec())
                                    };
                                    let btc_current = btc_current_opt.unwrap_or(0.0);
                                    let btc_open = if btc_open_raw > 0.0 {
                                        btc_open_raw
                                    } else {
                                        btc_current
                                    };
                                    let sigma = vol_opt.unwrap_or(self.v2.min_vol_per_sec);
                                    let fv_up =
                                        fair_value_up(btc_open, btc_current, sigma, remaining_secs);

                                    if self.config.mode == TradingMode::Paper {
                                        let oid = self.paper_sim.place_sell(
                                            &fill.condition_id,
                                            outcome,
                                            best_bid,
                                            size,
                                        );
                                        self.period_logger.log_order(
                                            &period_name,
                                            "sell_taker",
                                            outcome,
                                            best_bid,
                                            size,
                                            btc_current,
                                            btc_open,
                                            fv_up,
                                            1.0 - fv_up,
                                            sigma,
                                            remaining_secs,
                                            &fill.condition_id,
                                            &oid,
                                            "paper",
                                        );
                                        self.period_logger.log_order_event(
                                            &period_name,
                                            &oid,
                                            "PLACED",
                                            outcome,
                                            best_bid,
                                            size,
                                            size,
                                            "emergency_sell",
                                        );
                                        if let Some(ms) =
                                            self.active_markets.get_mut(&fill.condition_id)
                                        {
                                            ms.orders_placed += 1;
                                            self.order_timestamps.push_back(Instant::now());
                                            ms.resting_sells.insert(
                                                (outcome, best_bid),
                                                RestingLadderOrder {
                                                    order_id: oid,
                                                    size,
                                                    placed_at: Instant::now(),
                                                },
                                            );
                                        }
                                        self.mark_emergency_sell_placed(&fill.condition_id);
                                    } else if let Some(sdk) = &self.sdk {
                                        let token_id = market.token_id(outcome);
                                        let start = Instant::now();
                                        match sdk
                                            .place_emergency_sell(
                                                token_id,
                                                best_bid,
                                                size,
                                                market.tick_size,
                                            )
                                            .await
                                        {
                                            Ok(()) => {
                                                let latency_ms = start.elapsed().as_millis();
                                                self.period_logger.log_latency(
                                                    &period_name,
                                                    "place_order",
                                                    latency_ms,
                                                    true,
                                                    None,
                                                );
                                                self.record_latency_success(
                                                    &fill.condition_id,
                                                    latency_ms,
                                                );
                                                let oid = format!(
                                                    "emergency_{}_{}",
                                                    fill.condition_id
                                                        .chars()
                                                        .take(8)
                                                        .collect::<String>(),
                                                    Utc::now().timestamp_millis()
                                                );
                                                self.period_logger.log_order(
                                                    &period_name,
                                                    "sell_taker",
                                                    outcome,
                                                    best_bid,
                                                    size,
                                                    btc_current,
                                                    btc_open,
                                                    fv_up,
                                                    1.0 - fv_up,
                                                    sigma,
                                                    remaining_secs,
                                                    &fill.condition_id,
                                                    &oid,
                                                    "live",
                                                );
                                                self.period_logger.log_order_event(
                                                    &period_name,
                                                    &oid,
                                                    "PLACED",
                                                    outcome,
                                                    best_bid,
                                                    size,
                                                    size,
                                                    "emergency_sell",
                                                );
                                                if let Some(ms) =
                                                    self.active_markets.get_mut(&fill.condition_id)
                                                {
                                                    ms.orders_placed += 1;
                                                    self.order_timestamps.push_back(Instant::now());
                                                }
                                                self.mark_emergency_sell_placed(&fill.condition_id);
                                            }
                                            Err(e) => {
                                                let latency_ms = start.elapsed().as_millis();
                                                self.period_logger.log_latency(
                                                    &period_name,
                                                    "place_order",
                                                    latency_ms,
                                                    false,
                                                    Some(&e.to_string()),
                                                );
                                                self.check_rate_limit_error(&e);
                                                warn!(
                                                    condition_id = %fill.condition_id,
                                                    ?outcome,
                                                    price = %best_bid,
                                                    size = %size,
                                                    "[v2] Emergency sell execution failed: {e}"
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            warn!(
                                condition_id = %fill.condition_id,
                                ?outcome,
                                "[v2] Emergency sell skipped: no best bid liquidity"
                            );
                        }
                    }
                }
            }
        }

        // Log fill to per-period CSV + order event
        let mut fill_edge_sample: Option<(f64, f64)> = None;
        if let Some(ms) = self.active_markets.get(&fill.condition_id) {
            let period_name = ms.period_name.clone();
            let (btc_current_opt, vol_opt) = {
                let bs = self.asset_price.read();
                (bs.current_price, bs.realized_vol_per_sec())
            };
            let btc_current = btc_current_opt.unwrap_or(0.0);
            let btc_open = ms.btc_open.unwrap_or(btc_current);
            let sigma = vol_opt.unwrap_or(self.v2.min_vol_per_sec);
            let remaining_secs = self.time_manager.seconds_remaining(ms.market.end_date) as f64;
            let fv_up = fair_value_up(btc_open, btc_current, sigma, remaining_secs);
            let fill_side = match fill.side {
                FillSide::Buy => "buy",
                FillSide::Sell => "sell",
            };
            let fill_price = fill.price.to_f64().unwrap_or(0.0);
            let fill_size = fill.size.to_f64().unwrap_or(0.0);
            let fair = if fill.outcome == Outcome::Yes {
                fv_up
            } else {
                1.0 - fv_up
            };
            let edge = if fill_side == "buy" {
                fair - fill_price
            } else {
                fill_price - fair
            };
            if fill_size > 0.0 {
                fill_edge_sample = Some((edge * fill_size, fill_size));
                // Prometheus: per-fill edge in cents (positive = favorable)
                metrics::histogram!("fill_edge_cents", "asset" => self.asset.display_name(), "side" => fill_side).record(edge * 100.0);
            }
            let mode = self.mode_label();
            self.period_logger.log_fill(
                &period_name,
                fill_side,
                fill.outcome,
                fill.price,
                fill.size,
                btc_current,
                btc_open,
                fv_up,
                1.0 - fv_up,
                sigma,
                remaining_secs,
                &fill.condition_id,
                &fill.order_id,
                mode,
            );
            self.period_logger.log_order_event(
                &period_name,
                &fill.order_id,
                "FILLED",
                fill.outcome,
                fill.price,
                fill.size,
                Decimal::ZERO,
                "maker_fill",
            );
        }

        // Update period counters + adjust resting order size for partial fills
        if let Some(ms) = self.active_markets.get_mut(&fill.condition_id) {
            // Reduce resting order size by fill amount; remove only when fully filled.
            // Compute fully_filled BEFORE record_period_fill_counters so we can pass it.
            let dust = Decimal::new(1, 1); // 0.1 shares — below min order size
            let mut fully_filled = false;
            let mut is_deep_grid_fill = false;

            // Check deep grid first (separate tracking)
            for (_, order) in ms.resting_deep_grid.iter_mut() {
                if order.order_id == fill.order_id {
                    order.size -= fill.size;
                    is_deep_grid_fill = true;
                    if order.size <= dust {
                        fully_filled = true;
                    }
                    break;
                }
            }
            if is_deep_grid_fill {
                if fully_filled {
                    ms.resting_deep_grid
                        .retain(|_, order| order.order_id != fill.order_id);
                }
                // Track deep grid fill metrics separately
                match fill.outcome {
                    Outcome::Yes => ms.deep_grid_fills_up += 1,
                    Outcome::No => ms.deep_grid_fills_down += 1,
                }
                ms.deep_grid_fill_shares += fill.size;
                ms.deep_grid_fill_cost += fill.price * fill.size;
            } else {
                // Regular resting orders + sells
                for map in [&mut ms.resting_orders, &mut ms.resting_sells] {
                    for (_, order) in map.iter_mut() {
                        if order.order_id == fill.order_id {
                            order.size -= fill.size;
                            if order.size <= dust {
                                fully_filled = true;
                            }
                            break;
                        }
                    }
                    if fully_filled {
                        map.retain(|_, order| order.order_id != fill.order_id);
                    }
                }
            }

            Self::record_period_fill_counters(
                ms,
                matches!(fill.side, FillSide::Buy),
                fill.outcome,
                fill.price,
                fill.size,
                fill_edge_sample,
                fully_filled,
            );

            // Unregister fully filled orders from FillHandler to prevent stale IDs
            // from accumulating in order_states (which causes phantom cancel attempts).
            if fully_filled {
                let cid = fill.condition_id.clone();
                self.fill_handler.unregister_order(&cid, &fill.order_id);
            }
        }

        // Refresh positions immediately so UI doesn't lag behind order feed
        self.update_positions_display();
    }

    // ─── Market Lifecycle ────────────────────────────────────────────

    async fn handle_market_discovered(&mut self, mut market: TrackedMarket) {
        let condition_id = market.condition_id.clone();
        if self.active_markets.contains_key(&condition_id) {
            return;
        }

        self.markets_discovered_total += 1;

        // Don't accept new markets when stopping or paused
        let bot_status = self.bot_control.read().status;
        if bot_status == BotStatus::Stopping || bot_status == BotStatus::Paused {
            debug!(
                condition_id = %condition_id,
                ?bot_status,
                "[v2] Skipping new market (bot is {:?})", bot_status
            );
            return;
        }

        // ── Active-market filtering: only track the currently active window ──
        // Prefer explicit market.start_date metadata; fall back to end_date - 15m.
        let now = Utc::now();
        let start_time = market.effective_start_date_15m_fallback();
        if now < start_time {
            // Market hasn't started yet — skip it, discovery will pick it up later
            debug!(
                condition_id = %condition_id,
                question = %market.question,
                start_time = %start_time,
                "[v2] Skipping future market (not yet active)"
            );
            return;
        }

        // Skip if too far into the period (late-entry guard)
        // Use trading_window_end_pct: market discovered after the active window closes is useless.
        let period_duration_secs = market_total_secs_f64(&market);
        let remaining_secs = (market.end_date - now).num_seconds().max(0) as f64;
        let elapsed_pct = elapsed_pct_from_remaining(remaining_secs, period_duration_secs);
        if elapsed_pct > self.v2.trading_window_end_pct {
            info!(
                condition_id = %condition_id,
                question = %market.question,
                remaining_secs = remaining_secs as u32,
                elapsed_pct = format!("{:.0}%", elapsed_pct * 100.0),
                window_end = format!("{:.0}%", self.v2.trading_window_end_pct * 100.0),
                "[v2] Skipping late-entry market ({:.0}% elapsed > {:.0}% window end)",
                elapsed_pct * 100.0, self.v2.trading_window_end_pct * 100.0
            );
            return;
        }

        // Also skip if already in closing/resolved phase (duration-aware)
        let phase = self.time_manager.phase_for_duration(
            market.end_date,
            market.effective_duration_secs_15m_fallback(),
        );
        if phase == MarketPhase::Closing || phase == MarketPhase::Resolved {
            debug!(
                condition_id = %condition_id,
                "[v2] Skipping market already in {:?} phase", phase
            );
            return;
        }

        // FIX: Capture btc_open BEFORE the USDC wait (which can take up to 5s).
        // For hourly markets (duration >= 60 min), the bot may discover the market
        // well into the period, so the live price can be hundreds of dollars off the
        // actual Binance candle open. Use Binance klines REST API instead.
        let duration_secs = market.effective_duration_secs_15m_fallback();
        let btc_open = if duration_secs >= 3600 {
            // Hourly market: fetch the real candle open from Binance REST API
            let symbol = self.asset.binance_symbol().to_uppercase();
            let interval = "1h";
            let start_ms = market
                .effective_start_date_15m_fallback()
                .timestamp_millis();
            let kline_open = tokio::task::spawn_blocking(move || {
                fetch_binance_kline_open(&symbol, interval, start_ms)
            })
            .await
            .ok()
            .flatten();
            match kline_open {
                Some(price) => {
                    let live = self.asset_price.read().current_price;
                    info!(
                        condition_id = %condition_id,
                        kline_open = price,
                        live_price = ?live,
                        delta = ?(live.map(|l| l - price)),
                        "[v2] Using Binance kline open for hourly market (NOT live price)"
                    );
                    Some(price)
                }
                None => {
                    warn!(
                        condition_id = %condition_id,
                        "[v2] Failed to fetch Binance kline open — falling back to live price"
                    );
                    self.asset_price.read().current_price
                }
            }
        } else {
            // 5m/15m markets: live price is close enough to candle open
            self.asset_price.read().current_price
        };

        // === USDC balance refresh before entering new period ===
        // After closing merges/redeems, on-chain USDC may still be settling.
        // Check balance and wait briefly if needed to avoid entering with empty wallet.
        if self.config.mode == TradingMode::Live && self.config.eoa_mode {
            self.onchain.invalidate_balance_cache();
            let min_trading_usdc = dec!(10);
            let max_wait_secs = 5u64;
            let poll_interval_ms = 500u64;
            let mut waited = 0u64;

            loop {
                match self.onchain.get_usdc_balance().await {
                    Ok(balance) => {
                        if balance >= min_trading_usdc || waited >= max_wait_secs * 1000 {
                            info!(
                                condition_id = %condition_id,
                                usdc_balance = %balance,
                                waited_ms = waited,
                                "[v2] USDC balance check before new period"
                            );
                            if balance < min_trading_usdc {
                                warn!(
                                    condition_id = %condition_id,
                                    usdc_balance = %balance,
                                    "[v2] Low USDC balance (< ${min_trading_usdc}) — entering period anyway, merges may still be settling"
                                );
                            }
                            break;
                        }
                        // Balance too low, wait for pending merge/redeem to confirm
                        debug!(
                            condition_id = %condition_id,
                            usdc_balance = %balance,
                            waited_ms = waited,
                            "[v2] USDC below ${min_trading_usdc}, waiting for merge/redeem settlement..."
                        );
                        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
                        waited += poll_interval_ms;
                        self.onchain.invalidate_balance_cache();
                    }
                    Err(e) => {
                        warn!(
                            condition_id = %condition_id,
                            error = %e,
                            "[v2] Failed to check USDC balance before new period — continuing"
                        );
                        break;
                    }
                }
            }
        }

        info!(
            condition_id = %condition_id,
            question = %market.question,
            end_date = %market.end_date,
            btc_open = ?btc_open,
            "[v2] New market discovered (ACTIVE)"
        );

        if let Err(e) = self.db.upsert_market(&market).await {
            error!("[v2] Failed to persist market: {e}");
        }

        // Fetch taker fee rate + tick size from CLOB API (non-blocking on failure)
        let (taker_fee_rate, live_tick_size) = if let Some(sdk) = &self.sdk {
            match sdk.get_market_params(&condition_id).await {
                Ok((_maker, taker, tick)) => {
                    info!(
                        condition_id = %condition_id,
                        taker_fee = %taker,
                        tick_size = %tick,
                        "[v2] Fetched market params"
                    );
                    if tick != market.tick_size {
                        warn!(
                            condition_id = %condition_id,
                            gamma_tick = %market.tick_size,
                            clob_tick = %tick,
                            "[v2] Tick size mismatch at discovery — using CLOB value"
                        );
                    }
                    (Some(taker), Some(tick))
                }
                Err(e) => {
                    warn!(
                        condition_id = %condition_id,
                        error = %e,
                        "[v2] Failed to fetch market params — using Gamma fallbacks"
                    );
                    (None, None)
                }
            }
        } else {
            (None, None) // Paper mode — no SDK available
        };

        // Override tick size with CLOB's authoritative value if available
        if let Some(tick) = live_tick_size {
            market.tick_size = tick;
        }

        // FIX: Re-check phase after USDC wait + SDK calls. Up to ~6s may have
        // passed since the first phase check. If the market has crossed into
        // Closing, inserting it would start a quoting cycle that immediately
        // gets cancelled — wasting gas and API calls.
        let phase_recheck = self.time_manager.phase_for_duration(
            market.end_date,
            market.effective_duration_secs_15m_fallback(),
        );
        if phase_recheck == MarketPhase::Closing || phase_recheck == MarketPhase::Resolved {
            warn!(
                condition_id = %condition_id,
                phase = ?phase_recheck,
                "[v2] Market crossed into {:?} during USDC/SDK setup — aborting insertion", phase_recheck
            );
            return;
        }

        // Register token_id → (condition_id, outcome) in inventory for SDK sell guard
        self.inventory
            .register_tokens(&condition_id, &market.token_id_yes, &market.token_id_no);

        let period_name = PeriodLogger::period_name(&market.question);
        let condition_id_for_dash = condition_id.clone();
        self.active_markets.insert(
            condition_id,
            MarketV2State {
                market,
                btc_open,
                resting_orders: HashMap::new(),
                resting_sells: HashMap::new(),
                resting_deep_grid: HashMap::new(),
                deep_grid_placed: false,
                deep_grid_fills_up: 0,
                deep_grid_fills_down: 0,
                deep_grid_fill_shares: Decimal::ZERO,
                deep_grid_fill_cost: Decimal::ZERO,
                last_sell_time: HashMap::new(),
                ev_breaker_since: None,
                last_ev_breaker_log: None,
                last_pair_completion_attempt: None,
                pair_completion_attempts: 0,
                pair_completion_successes: 0,
                period_name,
                last_yes_center: None,
                last_no_center: None,
                orders_placed: 0,
                orders_filled: 0,
                orders_cancelled: 0,
                orders_expired: 0,
                total_up_shares_filled: Decimal::ZERO,
                total_down_shares_filled: Decimal::ZERO,
                gross_cost: Decimal::ZERO,
                gross_buy_filled_usdc: Decimal::ZERO,
                fill_edge_notional_sum: 0.0,
                fill_edge_size_sum: 0.0,
                latency_success_sum_ms: 0.0,
                latency_success_count: 0,
                sell_realized_pnl: Decimal::ZERO,
                merge_realized_pnl: Decimal::ZERO,
                sell_cost_basis_freed: Decimal::ZERO,
                merge_cost_basis_released: Decimal::ZERO,
                closing_position: None,
                exit_buy_block: None,
                last_merge_time: None,
                cumulative_merged_pairs: Decimal::ZERO,
                taker_fee_rate,
                fee_last_fetched: if taker_fee_rate.is_some() {
                    Some(Instant::now())
                } else {
                    None
                },
                first_order_placed_at: None,
                discovered_at: Instant::now(),
                book_ready: false,
                reconciliation_blocked: false,
                reconciliation_block_reason: None,
                last_reconciliation_block_log: None,
                last_emergency_sell_at: None,
                emergency_sell_placements: 0,
                last_churn_breaker_log: None,
                max_excess_seen: Decimal::ZERO,
                max_quote_levels_yes: 0,
                max_quote_levels_no: 0,
                suppression_reason_counts: HashMap::new(),
                cancel_all_count: 0,
                pair_quality_block_active: false,
                min_worst_case_pnl_seen: Decimal::ZERO,
                closing_expired_logged: false,
            },
        );

        // Subscribe to WS AFTER inserting into active_markets so that any fills
        // arriving on the stream are not dropped by the active-market check in
        // handle_fill_event. This closes a race where WS subscription started
        // before the market was in active_markets, causing early fills to be
        // deduped away and permanently lost.
        // Clone the market since subscribe_market_ws takes &mut self.
        let market_for_ws = self.active_markets[&condition_id_for_dash].market.clone();
        if self.sdk.is_some() {
            self.subscribe_market_ws(&market_for_ws).await;
        } else {
            self.spawn_book_poller(&market_for_ws);
        }

        // Reset period summary scope for the new market
        {
            let mut dash = self.dashboard.write();
            dash.current_period_condition_id = Some(condition_id_for_dash);
            // Clear detailed_order_log so period summary only shows current period
            dash.detailed_order_log.clear();
            dash.period_summary = PeriodSummary::default();
        }
    }

    async fn handle_market_closing(&mut self, condition_id: &str, phase: MarketPhase) {
        if phase == MarketPhase::Closing {
            // Reset VPIN tracker on period boundary so new period starts fresh
            if self.v2.vpin_enabled {
                self.vpin_tracker.reset();
            }

            // Only cancel + log if there are still resting orders to cancel
            let has_orders = self
                .active_markets
                .get(condition_id)
                .map(|ms| {
                    !ms.resting_orders.is_empty()
                        || !ms.resting_sells.is_empty()
                        || !ms.resting_deep_grid.is_empty()
                })
                .unwrap_or(false);
            if has_orders {
                info!(condition_id, "[v2] Closing phase — cancelling all orders");
            }

            // FIX: Emit EXPIRED events only once. On repeated Closing ticks (when
            // unconfirmed cancels keep orders in resting), we'd otherwise log duplicate
            // EXPIRED events and inflate orders_expired counters.
            if let Some(ms) = self.active_markets.get_mut(condition_id) {
                if !ms.closing_expired_logged {
                    ms.closing_expired_logged = true;
                    let period_name = ms.period_name.clone();
                    for ((outcome, price), order) in &ms.resting_orders {
                        self.period_logger.log_order_event(
                            &period_name,
                            &order.order_id,
                            "EXPIRED",
                            *outcome,
                            *price,
                            order.size,
                            order.size,
                            "period_end",
                        );
                        ms.orders_expired += 1;
                    }
                    for ((outcome, price), order) in &ms.resting_sells {
                        self.period_logger.log_order_event(
                            &period_name,
                            &order.order_id,
                            "EXPIRED",
                            *outcome,
                            *price,
                            order.size,
                            order.size,
                            "period_end",
                        );
                        ms.orders_expired += 1;
                    }
                    for ((outcome, price), order) in &ms.resting_deep_grid {
                        self.period_logger.log_order_event(
                            &period_name,
                            &order.order_id,
                            "EXPIRED",
                            *outcome,
                            *price,
                            order.size,
                            order.size,
                            "period_end_deep_grid",
                        );
                        ms.orders_expired += 1;
                    }
                }
            }

            // Cancel paper orders too
            self.paper_sim.cancel_market(condition_id);
            // Cancel all resting ladder orders (buys + sells + deep grid)
            // Collect IDs first (immutable borrow), then cancel, then update state (mutable borrow).
            let resting: Vec<OrderId> = self
                .active_markets
                .get(condition_id)
                .map(|ms| {
                    ms.resting_orders
                        .values()
                        .chain(ms.resting_sells.values())
                        .chain(ms.resting_deep_grid.values())
                        .map(|o| o.order_id.clone())
                        .collect()
                })
                .unwrap_or_default();
            if !resting.is_empty() {
                if self.config.mode == TradingMode::Paper {
                    for oid in &resting {
                        self.paper_sim.cancel(oid);
                    }
                }
                let ids: Vec<&str> = resting.iter().map(|s| s.as_str()).collect();
                let confirmed = self.batch_cancel_confirmed(&ids, "closing_phase").await;
                for oid in &resting {
                    if confirmed.contains(oid.as_str()) {
                        if let Err(e) = self.db.update_order_status(oid, "cancelled").await {
                            warn!("[v2] Failed to update cancelled order in DB: {e}");
                        }
                    }
                }
                // Remove only confirmed orders; keep unconfirmed for reconciliation
                if let Some(ms) = self.active_markets.get_mut(condition_id) {
                    ms.resting_orders
                        .retain(|_, o| !confirmed.contains(o.order_id.as_str()));
                    ms.resting_sells
                        .retain(|_, o| !confirmed.contains(o.order_id.as_str()));
                    ms.resting_deep_grid
                        .retain(|_, o| !confirmed.contains(o.order_id.as_str()));
                }
            } else if let Some(ms) = self.active_markets.get_mut(condition_id) {
                ms.resting_orders.clear();
                ms.resting_sells.clear();
                ms.resting_deep_grid.clear();
            }
            self.cancel_market_orders(condition_id).await;

            // === Sell unmatched shares at period end ===
            // When one side filled more than the other, sell the excess at best bid
            // instead of holding to expiry. Converts one-leg risk into partial recovery.
            // 0xd0d605 makes $25K/day from sells alone — this is critical for deep ladders.
            if self.v2.sell_unmatched_enabled {
                let position = self
                    .inventory
                    .get_position(condition_id)
                    .unwrap_or_default();
                let excess = position.yes_qty - position.no_qty;
                let abs_excess = excess.abs();

                if abs_excess >= self.v2.sell_unmatched_min_excess {
                    let (outcome, shares) = if excess > Decimal::ZERO {
                        (Outcome::Yes, abs_excess)
                    } else {
                        (Outcome::No, abs_excess)
                    };

                    // Compute loss floor: don't sell below avg_cost - max_loss
                    let avg_cost = if outcome == Outcome::Yes {
                        if position.yes_qty > Decimal::ZERO {
                            position.total_yes_spent / position.yes_qty
                        } else {
                            Decimal::ZERO
                        }
                    } else if position.no_qty > Decimal::ZERO {
                        position.total_no_spent / position.no_qty
                    } else {
                        Decimal::ZERO
                    };
                    let loss_floor = (avg_cost - self.v2.sell_unmatched_max_loss).max(dec!(0.01));

                    // Get best bid from order book
                    let market = self.active_markets.get(condition_id);
                    let token_id = market.map(|m| m.market.token_id(outcome));
                    let best_bid = token_id.and_then(|tid| {
                        let books = self.orderbooks.read();
                        books.get(tid).and_then(|b| b.best_bid().map(|(p, _)| p))
                    });

                    if let Some(bid_price) = best_bid {
                        if bid_price >= loss_floor {
                            let period_name = self
                                .active_markets
                                .get(condition_id)
                                .map(|ms| ms.period_name.clone())
                                .unwrap_or_default();

                            info!(
                                condition_id,
                                %outcome,
                                %shares,
                                %bid_price,
                                %avg_cost,
                                %loss_floor,
                                "[v2] SELL_UNMATCHED: selling excess at period end"
                            );

                            if self.config.mode == TradingMode::Paper {
                                let oid = self.paper_sim.place_sell(
                                    condition_id,
                                    outcome,
                                    bid_price,
                                    shares,
                                );
                                // Simulate immediate fill for paper mode FOK
                                let realized = self.inventory.record_sell(
                                    condition_id,
                                    outcome,
                                    bid_price,
                                    shares,
                                );
                                if let Some(ms) = self.active_markets.get_mut(condition_id) {
                                    ms.sell_realized_pnl += realized;
                                }
                                self.period_logger.log_order_event(
                                    &period_name,
                                    &oid,
                                    "FILLED",
                                    outcome,
                                    bid_price,
                                    shares,
                                    shares,
                                    "sell_unmatched",
                                );
                                info!(
                                    condition_id,
                                    %realized,
                                    "[v2] SELL_UNMATCHED: paper fill complete"
                                );
                            } else if let Some(sdk) = &self.sdk {
                                if let Some(ms) = self.active_markets.get(condition_id) {
                                    let tid = ms.market.token_id(outcome).to_string();
                                    let tick_sz = ms.market.tick_size;
                                    match tokio::time::timeout(
                                        Duration::from_secs(10),
                                        sdk.place_limit_sell(
                                            &tid, bid_price, shares, tick_sz,
                                            None, // No expiration for closing sell
                                        ),
                                    )
                                    .await
                                    {
                                        Ok(Ok(order_id)) => {
                                            info!(
                                                condition_id,
                                                %order_id,
                                                "[v2] SELL_UNMATCHED: sell order placed"
                                            );
                                            // Note: fill will be recorded via fill_rx channel
                                        }
                                        Ok(Err(e)) => {
                                            warn!(
                                                condition_id,
                                                error = %e,
                                                "[v2] SELL_UNMATCHED: sell order failed"
                                            );
                                        }
                                        Err(_) => {
                                            warn!(
                                                condition_id,
                                                "[v2] SELL_UNMATCHED: sell order timed out after 10s"
                                            );
                                        }
                                    }
                                }
                            }
                        } else {
                            info!(
                                condition_id,
                                %outcome,
                                %shares,
                                %bid_price,
                                %loss_floor,
                                "[v2] SELL_UNMATCHED: skipping — bid below loss floor"
                            );
                        }
                    } else {
                        info!(
                            condition_id,
                            %outcome,
                            %shares,
                            "[v2] SELL_UNMATCHED: skipping — no bid on book"
                        );
                    }
                }
            }

            // === Merge complete pairs on-chain if enabled ===
            if self.v2.merge_at_closing
                && self.config.eoa_mode
                && self.config.mode == TradingMode::Live
            {
                if let Some(sdk) = &self.sdk {
                    let position = self
                        .inventory
                        .get_position(condition_id)
                        .unwrap_or_default();
                    let complete_pairs = position.complete_pairs();

                    if complete_pairs > Decimal::ZERO {
                        // Skip closing merge if it would realize a loss
                        let avg_cc = position.avg_combined_cost();
                        let profit_per_pair = Decimal::ONE - avg_cc;
                        if profit_per_pair < self.v2.merge_min_profit_per_pair {
                            info!(
                                condition_id,
                                avg_combined_cost = %avg_cc,
                                profit_per_pair = %profit_per_pair,
                                min_required = %self.v2.merge_min_profit_per_pair,
                                "[v2] MERGE_SKIP: closing merge skipped — avg combined cost too high, letting resolution handle payout"
                            );
                        } else {
                            let pairs_u64 = complete_pairs.to_u64().unwrap_or(0);
                            if pairs_u64 > 0 {
                                let rpc_url = self.onchain.rpc_url().to_string();
                                info!(
                                    condition_id,
                                    pairs = pairs_u64,
                                    "[v2] Closing merge — merging complete pairs on-chain"
                                );

                                match tokio::time::timeout(
                                    Duration::from_secs(20),
                                    sdk.merge_positions(&rpc_url, condition_id, pairs_u64),
                                )
                                .await
                                {
                                    Ok(Ok(tx_hash)) => {
                                        // Update position: reduce both sides by merged amount
                                        let merged_dec = Decimal::from(pairs_u64);
                                        let avg_combined_cost = position.avg_combined_cost();
                                        let merge_profit =
                                            merged_dec * (Decimal::ONE - avg_combined_cost);
                                        let released_cost_basis = merged_dec * avg_combined_cost;
                                        self.inventory.record_merge(condition_id, merged_dec);

                                        if let Some(ms) = self.active_markets.get_mut(condition_id)
                                        {
                                            ms.merge_realized_pnl += merge_profit;
                                            ms.merge_cost_basis_released += released_cost_basis;
                                            ms.cumulative_merged_pairs += merged_dec;
                                        }

                                        info!(
                                            condition_id,
                                            %tx_hash,
                                            pairs = pairs_u64,
                                            avg_combined_cost = %avg_combined_cost,
                                            merge_profit = %merge_profit,
                                            cumulative_merge_pnl = %self.active_markets.get(condition_id).map(|ms| ms.merge_realized_pnl).unwrap_or_default(),
                                            "[v2] MERGE_PROFIT: closing merge successful"
                                        );

                                        // Invalidate balance cache so next read reflects merge
                                        self.onchain.invalidate_balance_cache();
                                    }
                                    Ok(Err(e)) => {
                                        warn!(
                                            condition_id,
                                            error = %e,
                                            "[v2] Closing merge failed — continuing without merge"
                                        );
                                    }
                                    Err(_) => {
                                        warn!(
                                        condition_id,
                                        "[v2] Closing merge timed out after 20s — continuing without merge"
                                    );
                                    }
                                }
                            }
                        } // else: profit check
                    }
                }
            }

            // Snapshot position and free inventory capacity immediately so new
            // markets are not blocked while this one waits for Resolved phase.
            if let Some(ms) = self.active_markets.get_mut(condition_id) {
                if ms.closing_position.is_none() {
                    let pos = self.inventory.remove_position(condition_id);
                    if let Some(ref p) = pos {
                        info!(
                            condition_id,
                            yes_spent = %p.total_yes_spent,
                            no_spent = %p.total_no_spent,
                            "[v2] Freed position from inventory at Closing (saved for PnL)"
                        );
                    }
                    ms.closing_position = pos;
                    if let Err(e) = self.db.delete_position(condition_id).await {
                        warn!("[v2] Failed to delete position from DB at Closing: {e}");
                    }
                }
            }
        } else if phase == MarketPhase::Resolved {
            info!(condition_id, "[v2] Market resolved — cleanup");
            self.cancel_market_orders(condition_id).await;
            let mut period_health_sample: Option<(String, String, Decimal, u32, u32)> = None;
            // Pre-computed resolution P&L for the DB pnl_log.
            // record_resolution() in the spawned handler returns (0,0) because
            // closing_position was already freed, so we compute the real values here.
            let mut db_pnl: (Decimal, Decimal, Decimal) =
                (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);

            // Write period result and session summary BEFORE closing log files
            if let Some(ms) = self.active_markets.get(condition_id) {
                let period_name = ms.period_name.clone();
                let btc_open_opt = ms.btc_open;
                let btc_close_opt = self.asset_price.read().current_price;
                // FIX: Don't default missing prices to 0.0 — that would make every
                // market resolve as DOWN when the price feed is unavailable.
                let (btc_open, btc_close, result) = match (btc_open_opt, btc_close_opt) {
                    (Some(open), Some(close)) => {
                        let r = if close >= open { "UP" } else { "DOWN" };
                        (open, close, r)
                    }
                    _ => {
                        error!(
                            condition_id,
                            btc_open = ?btc_open_opt,
                            btc_close = ?btc_close_opt,
                            "[v2] Missing price data at resolution — cannot determine winner, defaulting to UNKNOWN"
                        );
                        // Use 0.0 for DB fields but mark result as UNKNOWN so PnL
                        // computation treats it as a loss (conservative).
                        (
                            btc_open_opt.unwrap_or(0.0),
                            btc_close_opt.unwrap_or(0.0),
                            "UNKNOWN",
                        )
                    }
                };
                // Use the position snapshot from Closing phase if available,
                // otherwise fall back to inventory (direct Resolved without Closing).
                let position = ms
                    .closing_position
                    .clone()
                    .or_else(|| self.inventory.get_position(condition_id))
                    .unwrap_or_default();
                let complete_pairs = position.complete_pairs();
                let locked_profit = position.locked_profit();

                // TRUE economic PnL:
                // period_pnl = locked_profit (remaining unmerged pairs)
                //            + merge_realized_pnl (from all merges during period)
                //            + sell_realized_pnl (from exit sells)
                //            - excess_loss (excess shares that resolved to $0)
                // Computed as: (winning_payout - remaining_cost) captures
                // locked_profit - excess_loss on the remaining position.
                let winning_payout = if result == "UNKNOWN" {
                    Decimal::ZERO // No winner known — assume total loss (conservative)
                } else if btc_close >= btc_open {
                    position.yes_qty // UP wins: YES shares pay $1
                } else {
                    position.no_qty // DOWN wins: NO shares pay $1
                };
                let remaining_cost = position.total_yes_spent + position.total_no_spent;
                let resolution_pnl = winning_payout - remaining_cost;
                let period_pnl = resolution_pnl + ms.merge_realized_pnl + ms.sell_realized_pnl;

                // Compute resolution P&L for DB pnl_log (matches record_resolution semantics).
                // The spawned handler can't compute this because the position is already freed.
                let resolution_fee = if resolution_pnl > Decimal::ZERO {
                    resolution_pnl * dec!(0.02)
                } else {
                    Decimal::ZERO
                };
                db_pnl = (
                    resolution_pnl,
                    resolution_fee,
                    resolution_pnl - resolution_fee,
                );

                // Deep grid fill metrics
                let _deep_grid_total_fills = ms.deep_grid_fills_up + ms.deep_grid_fills_down;
                let avg_deep_fill_price = if ms.deep_grid_fill_shares > Decimal::ZERO {
                    (ms.deep_grid_fill_cost / ms.deep_grid_fill_shares)
                        .to_f64()
                        .unwrap_or(0.0)
                } else {
                    0.0
                };

                info!(
                    condition_id,
                    %locked_profit,
                    %resolution_pnl,
                    merge_realized_pnl = %ms.merge_realized_pnl,
                    sell_realized_pnl = %ms.sell_realized_pnl,
                    merged_pairs = %ms.cumulative_merged_pairs,
                    %period_pnl,
                    yes_qty = %position.yes_qty,
                    no_qty = %position.no_qty,
                    total_yes_spent = %position.total_yes_spent,
                    total_no_spent = %position.total_no_spent,
                    deep_grid_fills_up = ms.deep_grid_fills_up,
                    deep_grid_fills_down = ms.deep_grid_fills_down,
                    deep_grid_fill_shares = %ms.deep_grid_fill_shares,
                    avg_deep_fill_price,
                    "[v2] TRUE period PnL calculation"
                );
                self.cumulative_session_pnl += period_pnl;

                // Update the shared aggregate P&L counter so the emergency handler
                // sees resolution losses. The spawned handler uses pre-computed db_pnl
                // (passed at spawn time) for the DB pnl_log since the position is freed.
                // IMPORTANT: exclude sell_realized_pnl because record_sell() already
                // pushed each sell's P&L to the aggregate counter at fill time.
                // Including it here would double-count sell P&L.
                self.inventory
                    .update_aggregate_pnl(resolution_pnl + ms.merge_realized_pnl);

                self.period_logger.log_period_result(
                    &period_name,
                    condition_id,
                    btc_open,
                    btc_close,
                    result,
                    position.yes_qty,
                    position.no_qty,
                    complete_pairs,
                    locked_profit,
                    ms.sell_realized_pnl,
                    ms.merge_realized_pnl,
                    ms.cumulative_merged_pairs,
                    period_pnl,
                    self.cumulative_session_pnl,
                    ms.deep_grid_fills_up,
                    ms.deep_grid_fills_down,
                    ms.deep_grid_fill_shares,
                    avg_deep_fill_price,
                );

                let mode = self.mode_label();
                let avg_fill_edge = if ms.fill_edge_size_sum > 0.0 {
                    ms.fill_edge_notional_sum / ms.fill_edge_size_sum
                } else {
                    0.0
                };
                let avg_latency_ms = if ms.latency_success_count > 0 {
                    ms.latency_success_sum_ms / ms.latency_success_count as f64
                } else {
                    0.0
                };
                let suppression_reason_counts = Self::suppression_reason_counts_csv(&ms);
                let settlement_mode = Self::settlement_mode(&ms, &position);
                self.period_logger.log_session_summary(
                    &self.session_start.clone(),
                    &period_name,
                    condition_id,
                    btc_open,
                    btc_close,
                    result,
                    ms.orders_placed,
                    ms.orders_filled,
                    ms.orders_cancelled,
                    ms.orders_expired,
                    ms.total_up_shares_filled,
                    ms.total_down_shares_filled,
                    complete_pairs,
                    locked_profit,
                    ms.gross_cost,
                    period_pnl,
                    self.cumulative_session_pnl,
                    ms.max_excess_seen,
                    ms.max_quote_levels_yes,
                    ms.max_quote_levels_no,
                    ms.pair_completion_attempts,
                    ms.pair_completion_successes,
                    &suppression_reason_counts,
                    ms.cancel_all_count,
                    &settlement_mode,
                    avg_fill_edge,
                    avg_latency_ms,
                    mode,
                    ms.deep_grid_fills_up,
                    ms.deep_grid_fills_down,
                    ms.deep_grid_fill_shares,
                    avg_deep_fill_price,
                );

                // Flush all log files before closing
                self.period_logger.flush_all();
                self.period_logger.close_period(&period_name);

                // ── Prometheus: period completion counters ──
                {
                    let result_label = if period_pnl > Decimal::ZERO {
                        "win"
                    } else {
                        "loss"
                    };
                    metrics::counter!("periods_completed_total", "asset" => self.asset.display_name(), "result" => result_label).increment(1);
                    metrics::histogram!("period_pnl_usd", "asset" => self.asset.display_name())
                        .record(period_pnl.to_f64().unwrap_or(0.0));
                    metrics::histogram!("period_complete_pairs", "asset" => self.asset.display_name()).record(complete_pairs.to_f64().unwrap_or(0.0));
                    metrics::counter!("period_fills_total", "asset" => self.asset.display_name())
                        .increment(ms.orders_filled as u64);
                    if ms.cumulative_merged_pairs > Decimal::ZERO {
                        metrics::counter!("merges_total", "asset" => self.asset.display_name())
                            .increment(1);
                        metrics::histogram!("merge_pairs_count", "asset" => self.asset.display_name()).record(ms.cumulative_merged_pairs.to_f64().unwrap_or(0.0));
                    }
                }

                // ── Persist period result + session stats to SQLite ──
                let won = period_pnl > Decimal::ZERO;
                let pairs_i64 = complete_pairs.to_i64().unwrap_or(0);
                let yes_qty = position.yes_qty;
                let no_qty = position.no_qty;
                let excess = if yes_qty > no_qty {
                    (yes_qty - no_qty).to_i64().unwrap_or(0)
                } else {
                    (no_qty - yes_qty).to_i64().unwrap_or(0)
                };
                let _fills_count = (ms.orders_filled) as i64;

                // INSERT period_results
                let merged_pairs_i64 = ms.cumulative_merged_pairs.to_i64().unwrap_or(0);
                if let Err(e) = self
                    .db
                    .insert_period_result(
                        &period_name,
                        condition_id,
                        result,
                        won,
                        pairs_i64,
                        excess,
                        locked_profit,
                        ms.sell_realized_pnl,
                        ms.merge_realized_pnl,
                        merged_pairs_i64,
                        period_pnl,
                        btc_open,
                        btc_close,
                        &self.run_id,
                        self.asset.display_name(),
                    )
                    .await
                {
                    warn!("[v2-{}] Failed to persist period result: {e}", self.asset);
                }

                // UPDATE session_stats
                // FIX: Pass 0 for fills — each fill already calls increment_session_fills(1)
                // in real-time, so adding fills_count here would double-count.
                let today_str = chrono::Utc::now().format("%Y-%m-%d").to_string();
                if let Err(e) = self
                    .db
                    .record_period_in_session_stats(
                        period_pnl,
                        won,
                        0, // fills already counted per-fill via increment_session_fills
                        merged_pairs_i64,
                        &today_str,
                    )
                    .await
                {
                    warn!("[v2] Failed to update session stats: {e}");
                }

                // FIX: Persist aggregate daily P&L (including sell P&L) so restarts
                // don't lose same-day sell-fill realized P&L from the loss counter.
                {
                    let today_str_pnl = chrono::Utc::now().format("%Y-%m-%d").to_string();
                    if let Some(cents) = self.inventory.aggregate_daily_pnl_cents() {
                        if let Err(e) = self.db.persist_daily_pnl_cents(&today_str_pnl, cents).await
                        {
                            warn!("[v2] Failed to persist aggregate daily P&L: {e}");
                        }
                    }
                }

                // INSERT equity_curve point
                if let Err(e) = self
                    .db
                    .insert_equity_point(
                        self.cumulative_session_pnl,
                        "period_close",
                        &self.run_id,
                        self.asset.display_name(),
                    )
                    .await
                {
                    warn!("[v2-{}] Failed to insert equity point: {e}", self.asset);
                }

                // Update dashboard with TRUE period PnL (not the resolution handler PnL)
                {
                    let mut dash = self.dashboard.write();
                    dash.record_trade_result(period_pnl);
                    // Also push equity point for real-time chart
                    let cum_pnl = dash.total_pnl.to_f64().unwrap_or(0.0);
                    dash.push_equity(cum_pnl);
                    dash.push_pnl(cum_pnl);
                }
                // Refresh period history + equity curve from DB (outside lock)
                if let Ok(results) = self.db.get_period_results().await {
                    self.dashboard.write().period_history = results;
                }
                if let Ok(points) = self.db.get_equity_curve().await {
                    self.dashboard.write().equity_curve_db = points;
                }

                // ── Canary mode: track CONSECUTIVE profitable periods and auto-escalate ──
                if self.canary_active {
                    if period_pnl > Decimal::ZERO {
                        self.canary_successful_periods += 1;
                        info!(
                            consecutive = self.canary_successful_periods,
                            required = self.config.canary_periods,
                            "[v2] Canary: consecutive profitable period #{}",
                            self.canary_successful_periods
                        );
                        if self.canary_successful_periods >= self.config.canary_periods {
                            info!(
                                old_budget = %self.config.max_position_per_market,
                                full_budget = %self.canary_original_max_position,
                                "[v2] Canary: auto-escalating to full budget after {} successful periods",
                                self.canary_successful_periods
                            );
                            self.config.max_position_per_market = self.canary_original_max_position;
                            // Also update the inventory manager so it actually allows the full budget.
                            // Without this, the inventory manager retains the canary budget and blocks orders.
                            self.inventory
                                .set_max_position_per_market(self.canary_original_max_position);
                            self.canary_active = false;
                            let _ = self.alert_tx.send(AlertMessage::System(format!(
                                "Canary graduated to full budget ({}) after {} periods",
                                self.canary_original_max_position, self.canary_successful_periods
                            )));
                        }
                    } else {
                        // Reset consecutive counter on any non-profitable period
                        if self.canary_successful_periods > 0 {
                            info!(
                                was = self.canary_successful_periods,
                                period_pnl = %period_pnl,
                                "[v2] Canary: resetting consecutive count (losing period)"
                            );
                            self.canary_successful_periods = 0;
                        }
                        if let Some(canary_budget) = self.config.canary_budget {
                            // Check for catastrophic canary loss (LIVE MODE ONLY).
                            // In paper mode, single-period losses of $25+ are normal variance
                            // (44% of periods have combined cost > 1.0 per leaderboard analysis).
                            // Triggering emergency in paper mode permanently stalls the bot.
                            let loss_threshold = canary_budget * Decimal::new(5, 1); // 50%
                            if period_pnl.abs() > loss_threshold {
                                if self.config.mode == TradingMode::Live {
                                    error!(
                                        loss = %period_pnl,
                                        threshold = %loss_threshold,
                                        "[v2] Canary: catastrophic loss — halting"
                                    );
                                    self.emergency.trigger_emergency(
                                        crate::risk::emergency::EmergencyTrigger::DailyLossLimit,
                                    );
                                    let _ = self.alert_tx.send(AlertMessage::Emergency(
                                    format!("Canary halted: period loss {} exceeds 50% of canary budget {}",
                                        period_pnl, canary_budget)
                                ));
                                } else {
                                    warn!(
                                        loss = %period_pnl,
                                        threshold = %loss_threshold,
                                        "[v2] Canary: large loss (paper mode, not halting)"
                                    );
                                }
                            }
                        }
                    }
                }

                period_health_sample = Some((
                    period_name,
                    condition_id.to_string(),
                    period_pnl,
                    ms.orders_placed,
                    ms.orders_filled,
                ));
            }

            if let Some((period_name, cid, period_pnl, orders_placed, orders_filled)) =
                period_health_sample
            {
                self.evaluate_asset_guard_after_period(
                    &period_name,
                    &cid,
                    period_pnl,
                    orders_placed,
                    orders_filled,
                )
                .await;
            }

            let ms = self.active_markets.remove(condition_id);
            self.fill_handler.remove_market(condition_id);

            // Clean up WS handle for this market (abort if still running)
            if let Some(handle) = self.ws_handles.remove(condition_id) {
                handle.abort();
            }

            // Clean up stale orderbook entries and unregister tokens for this market
            if let Some(ref ms) = ms {
                let mut books = self.orderbooks.write();
                books.remove(&ms.market.token_id_yes);
                books.remove(&ms.market.token_id_no);
                drop(books);
                self.inventory
                    .unregister_tokens(&ms.market.token_id_yes, &ms.market.token_id_no);
            }

            // Remove position from inventory if it wasn't already freed at Closing.
            // This handles the case where a market jumps directly to Resolved.
            if let Some(removed_pos) = self.inventory.remove_position(condition_id) {
                info!(
                    condition_id,
                    yes_spent = %removed_pos.total_yes_spent,
                    no_spent = %removed_pos.total_no_spent,
                    "[v2] Removed resolved position from exposure tracking"
                );
            }
            // Always clean DB position (idempotent)
            if let Err(e) = self.db.delete_position(condition_id).await {
                warn!("[v2] Failed to delete position from DB at Resolved: {e}");
            }

            if let Some(ref ms) = ms {
                {
                    let mut books = self.orderbooks.write();
                    books.remove(&ms.market.token_id_yes);
                    books.remove(&ms.market.token_id_no);
                }

                // Batch cancel any remaining resting orders (buys + sells).
                // FIX: Only mark as cancelled in DB for confirmed cancels (or paper mode).
                let remaining_ids: Vec<String> = ms
                    .resting_orders
                    .values()
                    .chain(ms.resting_sells.values())
                    .map(|o| o.order_id.clone())
                    .collect();
                if !remaining_ids.is_empty() {
                    let confirmed_ids: Vec<String> = if self.config.mode == TradingMode::Paper {
                        for oid in &remaining_ids {
                            self.paper_sim.cancel(oid);
                        }
                        remaining_ids.clone()
                    } else if let Some(sdk) = &self.sdk {
                        let ids: Vec<&str> = remaining_ids.iter().map(|s| s.as_str()).collect();
                        match sdk.cancel_orders(&ids).await {
                            Ok(confirmed) => confirmed,
                            Err(e) => {
                                warn!(count = ids.len(), "[v2] Batch cancel remaining orders failed: {e} — NOT marking as cancelled in DB");
                                vec![]
                            }
                        }
                    } else {
                        remaining_ids.clone()
                    };
                    for oid in &confirmed_ids {
                        if let Err(e) = self.db.update_order_status(oid, "cancelled").await {
                            warn!("[v2] Failed to update cancelled order in DB: {e}");
                        }
                    }
                }
            }

            if let (Some(sdk), Some(ref ms)) = (&self.sdk, &ms) {
                if let (Ok(yes_u256), Ok(no_u256)) = (
                    U256::from_str(&ms.market.token_id_yes),
                    U256::from_str(&ms.market.token_id_no),
                ) {
                    let _ = sdk.unsubscribe_orderbook(&[yes_u256, no_u256]);
                }
            }

            if let Some(ms) = ms {
                // Pass position snapshot so the handler can recompute PnL if winner
                // was UNKNOWN at Resolved phase but is later confirmed via API.
                let closing_pos = ms
                    .closing_position
                    .clone()
                    .or_else(|| self.inventory.get_position(condition_id));
                self.spawn_resolution_handler(
                    condition_id.to_string(),
                    ms.market,
                    ms.btc_open,
                    db_pnl,
                    closing_pos,
                );
            }
        }
    }

    fn spawn_resolution_handler(
        &self,
        condition_id: String,
        market: TrackedMarket,
        btc_open: Option<f64>,
        db_pnl: (Decimal, Decimal, Decimal), // (gross, fee, net) pre-computed at Resolved phase
        closing_position: Option<Position>,  // FIX: position snapshot for PnL recomputation
    ) {
        let sdk = self.sdk.clone();
        let db = self.db.clone();
        let onchain = self.onchain.clone();
        let alert_tx = self.alert_tx.clone();
        let btc_price = self.asset_price.clone();
        let is_paper = self.config.mode == TradingMode::Paper;

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(30)).await;

            // Retry winner detection up to 3 times with backoff to handle timing races
            // where the market is resolved but the API hasn't updated yet.
            let winning_outcome = if let Some(ref sdk) = sdk {
                let mut winner_result = None;
                for attempt in 0..3u32 {
                    match sdk.get_market_resolution(&condition_id).await {
                        Ok(Some(winner)) => {
                            info!(%condition_id, ?winner, attempt, "[v2] Market winner determined");
                            winner_result = Some(winner);
                            break;
                        }
                        Ok(None) => {
                            if attempt < 2 {
                                warn!(%condition_id, attempt, "[v2] No winner yet — retrying in 15s");
                                tokio::time::sleep(Duration::from_secs(15)).await;
                            } else {
                                error!(%condition_id, "[v2] No winner after 3 attempts — skipping redemption");
                            }
                        }
                        Err(e) => {
                            if attempt < 2 {
                                warn!(%condition_id, attempt, "[v2] Resolution query failed: {e} — retrying in 15s");
                                tokio::time::sleep(Duration::from_secs(15)).await;
                            } else {
                                error!(%condition_id, "[v2] Resolution query failed after 3 attempts: {e}");
                            }
                        }
                    }
                }
                winner_result
            } else if is_paper {
                // Paper mode: determine winner from BTC price vs open
                let btc_now = btc_price.read().current_price;
                match (btc_open, btc_now) {
                    (Some(open), Some(now)) => {
                        let winner = if now >= open {
                            Outcome::Yes
                        } else {
                            Outcome::No
                        };
                        info!(
                            %condition_id, btc_open = open, btc_close = now, ?winner,
                            "[v2] Paper mode: winner determined from BTC price"
                        );
                        Some(winner)
                    }
                    _ => {
                        warn!(%condition_id, "[v2] Paper mode: no BTC data for resolution");
                        None
                    }
                }
            } else {
                None
            };

            // Only attempt redemption if we know the winner. Redeeming with unknown
            // outcome can fail or waste gas. The global redeem_all sweep will catch
            // any positions that become redeemable later.
            if winning_outcome.is_some() {
                if let Some(ref sdk) = sdk {
                    let neg_risk_amounts = if market.neg_risk {
                        let yes_bal = onchain
                            .get_token_balance(&market.token_id_yes)
                            .await
                            .unwrap_or_default();
                        let no_bal = onchain
                            .get_token_balance(&market.token_id_no)
                            .await
                            .unwrap_or_default();
                        let yes_u256 = U256::from_str(&yes_bal.to_string()).unwrap_or_default();
                        let no_u256 = U256::from_str(&no_bal.to_string()).unwrap_or_default();
                        Some(vec![yes_u256, no_u256])
                    } else {
                        None
                    };

                    let rpc_url = onchain.rpc_url();
                    match sdk
                        .redeem_positions(rpc_url, &condition_id, market.neg_risk, neg_risk_amounts)
                        .await
                    {
                        Ok(tx_hash) => info!(%condition_id, %tx_hash, "[v2] Positions redeemed"),
                        Err(e) => error!(%condition_id, "[v2] Redeem failed: {e}"),
                    }
                }
            } else {
                warn!(
                    %condition_id,
                    "[v2] Skipping redemption — winner unknown. Global redeem sweep will catch this later."
                );
            }

            // Use pre-computed P&L from Resolved phase (position already freed from inventory).
            // Only persist pnl_log when the winner is confirmed — writing with
            // winning_outcome=None produces misleading P&L data.
            // Always mark the market resolved so it doesn't stay orphaned as "active" in DB.
            if let Some(winner) = winning_outcome {
                // FIX: If the original db_pnl was computed with UNKNOWN (all zeros because
                // price feed was unavailable), recompute from the position snapshot now that
                // we know the actual winner.
                let final_pnl = if db_pnl == (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO) {
                    if let Some(ref pos) = closing_position {
                        let winning_payout = match winner {
                            Outcome::Yes => pos.yes_qty,
                            Outcome::No => pos.no_qty,
                        };
                        let remaining_cost = pos.total_yes_spent + pos.total_no_spent;
                        let gross = winning_payout - remaining_cost;
                        let fee = if gross > Decimal::ZERO {
                            gross * dec!(0.02)
                        } else {
                            Decimal::ZERO
                        };
                        let net = gross - fee;
                        info!(
                            %condition_id, %gross, %fee, %net, ?winner,
                            "[v2] Recomputed PnL from position snapshot (was UNKNOWN at Resolved phase)"
                        );
                        (gross, fee, net)
                    } else {
                        db_pnl // No position snapshot available, use original
                    }
                } else {
                    db_pnl // Original computation was valid
                };
                let (gross_pnl, fees, net_pnl) = final_pnl;

                if let Err(e) = db
                    .insert_pnl(&condition_id, gross_pnl, fees, net_pnl, Some(winner))
                    .await
                {
                    error!("[v2] Failed to persist P&L: {e}");
                }
            } else {
                warn!(
                    %condition_id,
                    "[v2] Skipping pnl_log — winner unknown after retries. Market will still be marked resolved."
                );
            }
            // Always mark resolved — the market is done regardless of whether we confirmed the winner.
            // Without this, the DB record stays in "active" state permanently.
            if let Err(e) = db.mark_market_resolved(&condition_id).await {
                error!("[v2] Failed to mark resolved: {e}");
            }

            onchain.invalidate_balance_cache();

            let (gross_pnl, fees, net_pnl) = db_pnl;
            info!(
                %condition_id,
                %gross_pnl,
                %fees,
                %net_pnl,
                ?winning_outcome,
                "[v2] Market resolution complete"
            );

            // NOTE: Dashboard stats (record_trade_result) are now updated at Resolved phase
            // with the TRUE period PnL, not the resolution handler PnL. This handler only
            // does on-chain redemption and pnl_log DB writes.

            let _ = alert_tx.send(AlertMessage::System(format!(
                "[v2] Market {condition_id} resolved: P&L = {net_pnl}"
            )));

            // Sweep ALL redeemable positions (catches any previously missed redemptions)
            if let Some(ref sdk) = sdk {
                let rpc = onchain.rpc_url();
                match sdk.redeem_all_redeemable(rpc).await {
                    Ok((s, f)) if s > 0 || f > 0 => {
                        info!(
                            success = s,
                            failed = f,
                            "[v2] redeem_all sweep after period end"
                        );
                    }
                    Ok(_) => {} // nothing to redeem
                    Err(e) => warn!("[v2] redeem_all sweep failed: {e}"),
                }
                onchain.invalidate_balance_cache();
            }
        });
    }

    async fn handle_order_update(&mut self, order_id: &str, status: &str) {
        debug!(order_id, status, "[v2] Order status update");

        if let Err(e) = self.db.update_order_status(order_id, status).await {
            warn!("[v2] Failed to update order status in DB: {e}");
        }

        let status_upper = status.to_ascii_uppercase();
        let is_terminal =
            status_upper == "CANCELLED" || status_upper == "EXPIRED" || status_upper == "MATCHED";

        if is_terminal {
            // Unregister from FillHandler for ALL terminal states (not just cancel/expire).
            // MATCHED means fully filled — stale IDs in FillHandler cause phantom cancels.
            for (cid, _) in &self.active_markets {
                self.fill_handler.unregister_order(cid, order_id);
            }

            let reason = match status_upper.as_str() {
                "EXPIRED" => "ws_expired",
                "MATCHED" => "ws_matched",
                _ => "ws_cancelled",
            };
            let mut lifecycle_events: Vec<(String, Outcome, Decimal, Decimal)> = Vec::new();

            // Remove from resting orders tracking (buys + sells + deep grid)
            for (_, ms) in self.active_markets.iter_mut() {
                let mut matched_order: Option<(Outcome, Decimal, Decimal)> = None;
                for ((outcome, price), order) in ms
                    .resting_orders
                    .iter()
                    .chain(ms.resting_sells.iter())
                    .chain(ms.resting_deep_grid.iter())
                {
                    if order.order_id == order_id {
                        matched_order = Some((*outcome, *price, order.size));
                        break;
                    }
                }
                if let Some((outcome, price, size)) = matched_order {
                    lifecycle_events.push((ms.period_name.clone(), outcome, price, size));
                }
                ms.resting_orders.retain(|_, o| o.order_id != order_id);
                ms.resting_sells.retain(|_, o| o.order_id != order_id);
                ms.resting_deep_grid.retain(|_, o| o.order_id != order_id);
            }

            for (period_name, outcome, price, size) in lifecycle_events {
                self.period_logger.log_order_event(
                    &period_name,
                    order_id,
                    &status_upper,
                    outcome,
                    price,
                    size,
                    size,
                    reason,
                );
            }
        }
    }

    // ─── Order Placement ─────────────────────────────────────────────

    async fn place_order(
        &mut self,
        market: &TrackedMarket,
        outcome: Outcome,
        price: Decimal,
        size: Decimal,
    ) {
        let token_id = market.token_id(outcome);
        info!(
            condition_id = %market.condition_id,
            %outcome,
            %price,
            %size,
            "[v2] Placing order"
        );

        let order_id = if let Some(sdk) = &self.sdk {
            match sdk
                .place_limit_order(
                    token_id,
                    price,
                    size,
                    market.tick_size,
                    Some(market.end_date - chrono::Duration::seconds(60)),
                )
                .await
            {
                Ok(oid) => {
                    metrics::counter!("orders_placed_total", "asset" => self.asset.display_name(), "outcome" => outcome.to_string(), "type" => "buy").increment(1);
                    info!(order_id = %oid, "[v2] Order placed");
                    oid
                }
                Err(e) => {
                    self.check_rate_limit_error(&e);
                    error!(condition_id = %market.condition_id, "[v2] Order failed: {e}");
                    return;
                }
            }
        } else {
            format!(
                "v2_ord_{}_{}_{}",
                market.condition_id.chars().take(8).collect::<String>(),
                outcome,
                Utc::now().timestamp_millis()
            )
        };

        self.fill_handler
            .register_order(&market.condition_id, order_id.clone(), outcome);

        if let Err(e) = self
            .db
            .insert_order(&order_id, &market.condition_id, outcome, price, size, "buy")
            .await
        {
            warn!("[v2] Failed to persist order: {e}");
        }
    }

    // ─── Order Cancellation ──────────────────────────────────────────

    /// Record an order lifecycle event in the audit ring buffer.
    fn record_lifecycle(&mut self, lifecycle: OrderLifecycle) {
        const MAX_LIFECYCLE: usize = 500;
        if self.order_lifecycle.len() >= MAX_LIFECYCLE {
            self.order_lifecycle.pop_front();
        }
        self.order_lifecycle.push_back(lifecycle);
    }

    async fn cancel_order(&mut self, order_id: &str) {
        // Cancel in paper sim (no-op if not a paper order)
        self.paper_sim.cancel(order_id);

        let confirmed = if let Some(sdk) = &self.sdk {
            match tokio::time::timeout(Duration::from_secs(5), sdk.cancel_order(order_id)).await {
                Ok(Ok(())) => true,
                Ok(Err(e)) => {
                    error!(
                        order_id,
                        "[v2] Cancel failed: {e} — NOT updating DB (order may still be live)"
                    );
                    false
                }
                Err(_) => {
                    error!(
                        order_id,
                        "[v2] Cancel timed out after 5s — NOT updating DB (outcome unknown)"
                    );
                    false
                }
            }
        } else {
            true // Paper mode: always succeeds
        };
        if confirmed {
            if let Err(e) = self.db.update_order_status(order_id, "cancelled").await {
                warn!("[v2] Failed to update cancelled order in DB: {e}");
            }
        }
    }

    /// Cancel ALL open orders (buys and sells) for a market.
    /// Used at Closing/Resolved/reconciliation — all call sites need to cancel everything.
    async fn cancel_market_orders(&mut self, condition_id: &str) {
        // FIX: Collect order IDs from BOTH DB and local state (union).
        // If a DB insert failed earlier, the order exists locally but not in DB.
        // We must include those in the cancel batch to avoid orphan orders.
        let (yes_ids, no_ids) = match self.db.get_open_orders(condition_id).await {
            Ok(ids) => ids,
            Err(e) => {
                warn!("[v2] Failed to get open orders: {e}");
                (vec![], vec![])
            }
        };

        let mut all_ids_set: std::collections::HashSet<String> =
            yes_ids.into_iter().chain(no_ids).collect();
        // Also include order IDs from local resting maps (may be missing from DB)
        if let Some(ms) = self.active_markets.get(condition_id) {
            for order in ms.resting_orders.values() {
                all_ids_set.insert(order.order_id.clone());
            }
            for order in ms.resting_sells.values() {
                all_ids_set.insert(order.order_id.clone());
            }
        }

        let all_ids_vec: Vec<String> = all_ids_set.into_iter().collect();
        let all_ids: Vec<&str> = all_ids_vec.iter().map(|s| s.as_str()).collect();
        if !all_ids.is_empty() {
            if let Some(sdk) = &self.sdk {
                match tokio::time::timeout(Duration::from_secs(5), sdk.cancel_orders(&all_ids))
                    .await
                {
                    Ok(Ok(confirmed)) => {
                        // Update DB/fill_handler/local maps for confirmed cancels
                        for oid in &confirmed {
                            if let Err(e) = self.db.update_order_status(oid, "cancelled").await {
                                warn!("[v2] Failed to update cancelled order in DB: {e}");
                            }
                        }
                        for oid in &confirmed {
                            self.fill_handler.unregister_order(condition_id, oid);
                        }
                        // Clean up resting_orders/resting_sells for confirmed IDs
                        if let Some(ms) = self.active_markets.get_mut(condition_id) {
                            ms.resting_orders
                                .retain(|_, order| !confirmed.contains(&order.order_id));
                            ms.resting_sells
                                .retain(|_, order| !confirmed.contains(&order.order_id));
                        }
                    }
                    Ok(Err(e)) => {
                        error!(
                            count = all_ids.len(),
                            "[v2] Batch cancel market orders failed: {e} — NOT clearing local state (orders may still be live)"
                        );
                    }
                    Err(_) => {
                        error!(
                            count = all_ids.len(),
                            "[v2] Batch cancel market orders timed out after 5s — NOT clearing local state (orders may still be live)"
                        );
                    }
                }
            } else {
                // Paper mode: no SDK, all cancels succeed. Clean up DB, fill handler, and local maps.
                if let Err(e) = self.db.cancel_all_orders_for_market(condition_id).await {
                    warn!("[v2] Failed to cancel orders in DB (paper): {e}");
                }
                self.fill_handler.clear_market_orders(condition_id);
                if let Some(ms) = self.active_markets.get_mut(condition_id) {
                    ms.resting_orders.clear();
                    ms.resting_sells.clear();
                }
            }
        } else {
            // No open orders in DB — still clean up any stale state.
            // FIX: Also clear local resting maps. If a DB insert failed earlier,
            // orders exist locally but not in DB, so the DB returns nothing.
            // Without clearing local maps, these phantom orders persist.
            if let Err(e) = self.db.cancel_all_orders_for_market(condition_id).await {
                warn!("[v2] Failed to cancel orders in DB: {e}");
            }
            self.fill_handler.clear_market_orders(condition_id);
            if let Some(ms) = self.active_markets.get_mut(condition_id) {
                if !ms.resting_orders.is_empty() || !ms.resting_sells.is_empty() {
                    warn!(
                        condition_id,
                        resting_buys = ms.resting_orders.len(),
                        resting_sells = ms.resting_sells.len(),
                        "[v2] Clearing stale local resting orders (not in DB)"
                    );
                }
                ms.resting_orders.clear();
                ms.resting_sells.clear();
            }
        }
    }

    /// Batch-cancel orders via SDK and return the set of IDs **confirmed** cancelled.
    /// On network error, returns EMPTY set — caller must not clear state for unconfirmed cancels.
    /// Orders with unknown cancel status will be caught by the next reconciliation cycle.
    /// In paper mode, all cancels succeed.
    async fn batch_cancel_confirmed(
        &self,
        ids: &[&str],
        context: &str,
    ) -> std::collections::HashSet<String> {
        if ids.is_empty() {
            return std::collections::HashSet::new();
        }
        if let Some(ref sdk) = self.sdk {
            let cancel_start = Instant::now();
            match sdk.cancel_orders(ids).await {
                Ok(confirmed) => {
                    self.latency_tracker.record(
                        "order_cancel",
                        cancel_start.elapsed().as_secs_f64() * 1000.0,
                    );
                    let unconfirmed = ids.len() - confirmed.len();
                    if unconfirmed > 0 {
                        warn!(
                            confirmed = confirmed.len(),
                            unconfirmed,
                            context,
                            "[v2] Batch cancel: some orders not confirmed cancelled — keeping in local state"
                        );
                    }
                    confirmed.into_iter().collect()
                }
                Err(e) => {
                    self.latency_tracker.record(
                        "order_cancel",
                        cancel_start.elapsed().as_secs_f64() * 1000.0,
                    );
                    error!(
                        count = ids.len(),
                        context,
                        "[v2] Batch cancel failed: {e} — NOT clearing local state (orders may still be live)"
                    );
                    // Return empty set: no cancels confirmed. Phantom order risk is worse
                    // than stale-state risk — reconciliation will clean up on next cycle.
                    std::collections::HashSet::new()
                }
            }
        } else {
            // Paper mode: all cancels succeed
            ids.iter().map(|s| s.to_string()).collect()
        }
    }

    /// Cancel all resting orders across all active markets (used when entering Stopping/Paused state).
    async fn cancel_all_resting_orders(&mut self) {
        let market_ids: Vec<ConditionId> = self.active_markets.keys().cloned().collect();
        for condition_id in &market_ids {
            self.paper_sim.cancel_market(condition_id);
            // Cancel all resting ladder orders (buys + sells)
            if let Some(ms) = self.active_markets.get_mut(condition_id.as_str()) {
                let period_name = ms.period_name.clone();
                let resting: Vec<(OrderId, Outcome, Decimal, Decimal)> = ms
                    .resting_orders
                    .iter()
                    .chain(ms.resting_sells.iter())
                    .map(|((outcome, price), order)| {
                        (order.order_id.clone(), *outcome, *price, order.size)
                    })
                    .collect();
                // Batch cancel via SDK FIRST, then update local state
                if !resting.is_empty() {
                    ms.cancel_all_count += 1;
                    let mut cancelled_ids: std::collections::HashSet<String> =
                        std::collections::HashSet::new();
                    if let Some(sdk) = &self.sdk {
                        let ids: Vec<&str> =
                            resting.iter().map(|(oid, _, _, _)| oid.as_str()).collect();
                        match sdk.cancel_orders(&ids).await {
                            Ok(confirmed) => {
                                cancelled_ids = confirmed.into_iter().collect();
                            }
                            Err(e) => {
                                error!(
                                    count = ids.len(),
                                    condition_id,
                                    "[v2] Batch cancel resting orders failed: {e} — NOT clearing local state (orders may still be live)"
                                );
                                // Return empty set: no cancels confirmed. Orders with unknown
                                // status will be caught by reconciliation on next cycle.
                            }
                        }
                    } else {
                        // Paper mode: all cancels succeed
                        cancelled_ids = resting.iter().map(|(oid, _, _, _)| oid.clone()).collect();
                    }
                    // Only clear orders that were actually cancelled
                    for (oid, outcome, price, size) in &resting {
                        if cancelled_ids.contains(oid.as_str()) {
                            self.paper_sim.cancel(oid);
                            self.period_logger.log_order_event(
                                &period_name,
                                oid,
                                "CANCELLED",
                                *outcome,
                                *price,
                                *size,
                                *size,
                                "pause_or_stop",
                            );
                            if let Err(e) = self.db.update_order_status(oid, "cancelled").await {
                                warn!("[v2] Failed to update cancelled order in DB: {e}");
                            }
                        } else {
                            warn!(order_id = %oid, "[v2] Order NOT confirmed cancelled — keeping in local state");
                        }
                    }
                    // Clear only confirmed cancels from local state
                    ms.resting_orders
                        .retain(|_, o| !cancelled_ids.contains(o.order_id.as_str()));
                    ms.resting_sells
                        .retain(|_, o| !cancelled_ids.contains(o.order_id.as_str()));
                    ms.orders_cancelled += cancelled_ids.len() as u32;
                }
            }
            self.cancel_market_orders(condition_id).await;
        }
        // Clear resting bids display
        self.dashboard.write().resting_bids.clear();
    }

    // ─── Paper Mode Book Poller ────────────────────────────────────

    /// Spawn a REST-based orderbook poller for paper mode (no WS auth).
    /// Polls CLOB book endpoint every 2 seconds for both YES and NO tokens.
    fn spawn_book_poller(&self, market: &TrackedMarket) {
        let orderbooks = self.orderbooks.clone();
        let book_notify = self.book_notify.clone();
        let yes_token = market.token_id_yes.clone();
        let no_token = market.token_id_no.clone();
        let condition_id = market.condition_id.clone();
        let end_date = market.end_date;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            info!(condition_id = %condition_id, "[v2] Paper book poller started");

            loop {
                interval.tick().await;

                // Stop polling after market ends
                if Utc::now() >= end_date {
                    info!(condition_id = %condition_id, "[v2] Paper book poller stopped (market ended)");
                    break;
                }

                let yes_tok = yes_token.clone();
                let no_tok = no_token.clone();

                let (yes_book, no_book) = tokio::task::spawn_blocking(move || {
                    (fetch_clob_book(&yes_tok), fetch_clob_book(&no_tok))
                })
                .await
                .unwrap_or((None, None));

                {
                    let mut books = orderbooks.write();
                    if let Some(snap) = yes_book {
                        books.insert(yes_token.clone(), snap);
                    }
                    if let Some(snap) = no_book {
                        books.insert(no_token.clone(), snap);
                    }
                }
                book_notify.notify_one();
            }
        });
    }

    // ─── WebSocket Subscription (from v1) ────────────────────────────

    async fn subscribe_market_ws(&mut self, market: &TrackedMarket) {
        let Some(sdk) = self.sdk.clone() else { return };
        let my_api_key = sdk.clob.credentials().key();

        let yes_u256 = match U256::from_str(&market.token_id_yes) {
            Ok(v) => v,
            Err(e) => {
                warn!(token_id = %market.token_id_yes, "[v2] Invalid YES token: {e}");
                return;
            }
        };
        let no_u256 = match U256::from_str(&market.token_id_no) {
            Ok(v) => v,
            Err(e) => {
                warn!(token_id = %market.token_id_no, "[v2] Invalid NO token: {e}");
                return;
            }
        };

        // FIX: condition_b256 is required for trade/order WS streams.
        // If it fails to parse, we can't subscribe to fills — abort entirely.
        let condition_b256 = match alloy::primitives::B256::from_str(&market.condition_id) {
            Ok(v) => Some(v),
            Err(e) => {
                error!(
                    condition_id = %market.condition_id,
                    "[v2] Failed to parse condition_id as B256: {e} — cannot subscribe WS streams (blind trading risk)"
                );
                return;
            }
        };

        let orderbooks = self.orderbooks.clone();
        let book_notify = self.book_notify.clone();
        let fill_tx = self.fill_tx.clone();
        let order_update_tx = self.order_update_tx.clone();
        let market_trade_signals = self.market_trade_signals.clone();
        let ws_tick_sizes = self.ws_tick_sizes.clone();
        let ws_condition_id = market.condition_id.clone();
        let ws_handle_key = ws_condition_id.clone();
        let condition_id = market.condition_id.clone();
        let token_id_yes = market.token_id_yes.clone();
        let token_id_no = market.token_id_no.clone();
        let skew_signal_max_age = self
            .v2
            .directional_skew_flow_window_secs
            .max(self.v2.directional_skew_short_flow_window_secs)
            .max(1);
        let large_trade_min_usdc = self.v2.directional_skew_large_trade_min_usdc;

        let handle = tokio::spawn(async move {
            let mut backoff = Duration::from_secs(2);
            let max_backoff = Duration::from_secs(30);

            'reconnect: loop {
                let ob_result = sdk.ws.subscribe_orderbook(vec![yes_u256, no_u256]);

                let mut ob_stream = match ob_result {
                    Ok(s) => Some(Box::pin(s)),
                    Err(e) => {
                        warn!(
                            condition_id = %condition_id,
                            "[v2] Orderbook WS subscribe failed: {e}, retrying in {}s",
                            backoff.as_secs()
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                        continue 'reconnect;
                    }
                };

                // FIX: Trade and order WS streams are mandatory for safe quoting.
                // If either fails, the bot would be blind to its own fills or order status,
                // leading to uncontrolled position buildup. Trigger reconnect instead.
                let trade_result = if let Some(cb) = condition_b256 {
                    match sdk.ws.subscribe_trades(vec![cb]) {
                        Ok(stream) => Some(stream),
                        Err(e) => {
                            error!(
                                condition_id = %condition_id,
                                "[v2] Trade WS subscribe failed: {e} — reconnecting (cannot quote blind)"
                            );
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(max_backoff);
                            continue 'reconnect;
                        }
                    }
                } else {
                    None
                };
                let order_result = if let Some(cb) = condition_b256 {
                    match sdk.ws.subscribe_orders(vec![cb]) {
                        Ok(stream) => Some(stream),
                        Err(e) => {
                            error!(
                                condition_id = %condition_id,
                                "[v2] Order WS subscribe failed: {e} — reconnecting (cannot quote blind)"
                            );
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(max_backoff);
                            continue 'reconnect;
                        }
                    }
                } else {
                    None
                };

                let mut trade_stream = trade_result.map(|s| Box::pin(s));
                let mut order_stream = order_result.map(|s| Box::pin(s));

                // Subscribe to tick_size_change for real-time tick size updates.
                // Non-critical: if it fails, we fall back to periodic REST refresh.
                let tick_size_stream_result =
                    sdk.ws.subscribe_tick_size_change(vec![yes_u256, no_u256]);
                let mut tick_size_stream = match tick_size_stream_result {
                    Ok(s) => Some(Box::pin(s)),
                    Err(e) => {
                        warn!(
                            condition_id = %condition_id,
                            "[v2] tick_size_change WS subscribe failed: {e} — falling back to REST refresh"
                        );
                        None
                    }
                };

                info!(condition_id = %condition_id, "[v2] WS streams subscribed (all required streams active)");
                backoff = Duration::from_secs(2);

                loop {
                    tokio::select! {
                        Some(result) = async {
                            match ob_stream.as_mut() {
                                Some(s) => s.next().await,
                                None => std::future::pending().await,
                            }
                        } => {
                            match result {
                                Ok(book_update) => {
                                    let mut bids = std::collections::BTreeMap::new();
                                    for level in &book_update.bids {
                                        bids.insert(level.price, level.size);
                                    }
                                    let mut asks = std::collections::BTreeMap::new();
                                    for level in &book_update.asks {
                                        asks.insert(level.price, level.size);
                                    }
                                    // Use local clock for staleness detection. The server
                                    // timestamp only updates when book content changes, so
                                    // quiet markets would falsely trigger the staleness
                                    // guard even with a healthy WS connection.
                                    let snapshot = OrderBookSnapshot {
                                        asset_id: book_update.asset_id.to_string(),
                                        bids,
                                        asks,
                                        timestamp: Utc::now(),
                                    };
                                    let key = if book_update.asset_id == yes_u256 {
                                        token_id_yes.clone()
                                    } else {
                                        token_id_no.clone()
                                    };
                                    orderbooks.write().insert(key, snapshot);
                                    book_notify.notify_one();
                                }
                                Err(e) => {
                                    warn!(condition_id = %condition_id, "[v2] OB WS error: {e}");
                                }
                            }
                        }

                        Some(result) = async {
                            match trade_stream.as_mut() {
                                Some(s) => s.next().await,
                                None => std::future::pending().await,
                            }
                        } => {
                            match result {
                                Ok(trade) => {
                                    // trade.asset_id and trade.side are from the TAKER's perspective.
                                    // For maker fills, we must use maker_order fields instead.
                                    let taker_outcome = if trade.asset_id == yes_u256 {
                                        Outcome::Yes
                                    } else {
                                        Outcome::No
                                    };
                                    let trade_notional = trade.price * trade.size;
                                    let signed_up_notional = match (taker_outcome, trade.side) {
                                        (
                                            Outcome::Yes,
                                            polymarket_client_sdk::clob::types::Side::Buy,
                                        ) => trade_notional,
                                        (
                                            Outcome::Yes,
                                            polymarket_client_sdk::clob::types::Side::Sell,
                                        ) => -trade_notional,
                                        (
                                            Outcome::No,
                                            polymarket_client_sdk::clob::types::Side::Buy,
                                        ) => -trade_notional,
                                        (
                                            Outcome::No,
                                            polymarket_client_sdk::clob::types::Side::Sell,
                                        ) => trade_notional,
                                        _ => Decimal::ZERO,
                                    };
                                    if signed_up_notional != Decimal::ZERO {
                                        let received_at = Instant::now();
                                        let mut signals = market_trade_signals.write();
                                        let queue =
                                            if let Some(existing) = signals.get_mut(condition_id.as_str()) {
                                                existing
                                            } else {
                                                signals
                                                    .entry(condition_id.clone())
                                                    .or_insert_with(VecDeque::new)
                                            };
                                        queue.push_back(MarketTradeSignal {
                                            received_at,
                                            signed_up_notional,
                                            signed_up_large_notional: if trade_notional
                                                >= large_trade_min_usdc
                                            {
                                                signed_up_notional
                                            } else {
                                                Decimal::ZERO
                                            },
                                        });
                                        let cutoff = received_at
                                            .checked_sub(Duration::from_secs(skew_signal_max_age))
                                            .unwrap_or(received_at);
                                        while queue
                                            .front()
                                            .map(|signal| signal.received_at < cutoff)
                                            .unwrap_or(false)
                                        {
                                            queue.pop_front();
                                        }
                                    }
                                    let timestamp = trade
                                        .timestamp
                                        .and_then(DateTime::from_timestamp_millis)
                                        .unwrap_or_else(Utc::now);

                                    use polymarket_client_sdk::clob::types::TraderSide;
                                    let is_taker = matches!(
                                        trade.trader_side,
                                        Some(TraderSide::Taker)
                                    );

                                    if !is_taker && !trade.maker_orders.is_empty() {
                                        // We're the maker: use maker_order fields for correct perspective.
                                        // maker_order.outcome = "Yes"/"No" from OUR order's perspective
                                        // maker_order.asset_id = OUR token (may differ from trade.asset_id
                                        //   in complementary-token matches where taker bought the other token)
                                        for maker_order in &trade.maker_orders {
                                            if maker_order.owner != my_api_key {
                                                continue; // Not our order
                                            }
                                            // Outcome from our maker order's asset_id (reliable)
                                            // maker_order.outcome string may be "Yes"/"No" or "Up"/"Down"
                                            // depending on market type — asset_id comparison is unambiguous.
                                            let order_outcome = if maker_order.asset_id == yes_u256 {
                                                Outcome::Yes
                                            } else if maker_order.asset_id == no_u256 {
                                                Outcome::No
                                            } else {
                                                warn!(
                                                    asset_id = %maker_order.asset_id,
                                                    outcome_str = %maker_order.outcome,
                                                    "[v2] Unknown maker asset_id, skipping"
                                                );
                                                continue;
                                            };
                                            // Derive our fill side: trade.side is taker's side.
                                            // Same book (asset_ids match): our side = opposite of taker
                                            // Cross book (complementary match): our side = same as taker
                                            let same_book = maker_order.asset_id == trade.asset_id;
                                            let order_side = if same_book {
                                                match trade.side {
                                                    polymarket_client_sdk::clob::types::Side::Buy => FillSide::Sell,
                                                    polymarket_client_sdk::clob::types::Side::Sell => FillSide::Buy,
                                                    _ => FillSide::Buy,
                                                }
                                            } else {
                                                match trade.side {
                                                    polymarket_client_sdk::clob::types::Side::Buy => FillSide::Buy,
                                                    polymarket_client_sdk::clob::types::Side::Sell => FillSide::Sell,
                                                    _ => FillSide::Buy,
                                                }
                                            };
                                            let fill = FillEvent {
                                                trade_id: trade.id.clone(),
                                                order_id: maker_order.order_id.clone(),
                                                condition_id: condition_id.clone(),
                                                outcome: order_outcome,
                                                price: maker_order.price,
                                                size: maker_order.matched_amount,
                                                side: order_side,
                                                timestamp,
                                            };
                                            match fill_tx.try_send(fill) {
                                                Ok(()) => {}
                                                Err(tokio::sync::mpsc::error::TrySendError::Full(f)) => {
                                                    error!(
                                                        trade_id = %f.trade_id,
                                                        "[v2] Fill channel FULL — dropping fill! Increase buffer or speed up consumer"
                                                    );
                                                    metrics::counter!("fill_channel_overflow").increment(1);
                                                }
                                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                                    info!("[v2] Fill channel closed, stopping WS task");
                                                    return;
                                                }
                                            }
                                        }
                                    } else if is_taker {
                                        // We're the taker: trade-level fields are from our perspective
                                        let taker_side = match trade.side {
                                            polymarket_client_sdk::clob::types::Side::Buy => FillSide::Buy,
                                            polymarket_client_sdk::clob::types::Side::Sell => FillSide::Sell,
                                            _ => FillSide::Buy,
                                        };
                                        let fill = FillEvent {
                                            trade_id: trade.id.clone(),
                                            order_id: trade.taker_order_id
                                                .unwrap_or_else(|| trade.id.clone()),
                                            condition_id: condition_id.clone(),
                                            outcome: taker_outcome,
                                            price: trade.price,
                                            size: trade.size,
                                            side: taker_side,
                                            timestamp,
                                        };
                                        match fill_tx.try_send(fill) {
                                            Ok(()) => {}
                                            Err(tokio::sync::mpsc::error::TrySendError::Full(f)) => {
                                                error!(
                                                    trade_id = %f.trade_id,
                                                    "[v2] Fill channel FULL — dropping fill! Increase buffer or speed up consumer"
                                                );
                                                metrics::counter!("fill_channel_overflow").increment(1);
                                            }
                                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                                info!("[v2] Fill channel closed, stopping WS task");
                                                return;
                                            }
                                        }
                                    }
                                    // else: trader_side is None/Unknown with empty maker_orders — not our trade
                                }
                                Err(e) => {
                                    warn!(condition_id = %condition_id, "[v2] Trade WS error: {e}");
                                }
                            }
                        }

                        Some(result) = async {
                            match order_stream.as_mut() {
                                Some(s) => s.next().await,
                                None => std::future::pending().await,
                            }
                        } => {
                            match result {
                                Ok(order_msg) => {
                                    // Canonicalize SDK enum debug strings to consistent
                                    // DB-friendly values.  OrderStatusType variants:
                                    //   Live, Matched, Canceled, Delayed, Unmatched
                                    // OrderMessageType variants:
                                    //   Placement, Update, Cancellation
                                    let raw = order_msg
                                        .status
                                        .map(|s| format!("{s:?}"))
                                        .or_else(|| order_msg.msg_type.map(|t| format!("{t:?}")))
                                        .unwrap_or_else(|| "unknown".to_string());
                                    let status = match raw.as_str() {
                                        "Live" | "Placement" | "Update" => "open".to_string(),
                                        "Matched" => "matched".to_string(),
                                        "Canceled" | "Cancellation" => "cancelled".to_string(),
                                        "Delayed" => "delayed".to_string(),
                                        "Unmatched" => "unmatched".to_string(),
                                        other => other.to_ascii_lowercase(),
                                    };
                                    if order_update_tx.send((order_msg.id, status)).await.is_err() {
                                        info!("[v2] Order channel closed, stopping WS task");
                                        return;
                                    }
                                }
                                Err(e) => {
                                    warn!(condition_id = %condition_id, "[v2] Order WS error: {e}");
                                }
                            }
                        }

                        Some(result) = async {
                            match tick_size_stream.as_mut() {
                                Some(s) => s.next().await,
                                None => std::future::pending().await,
                            }
                        } => {
                            match result {
                                Ok(tsc) => {
                                    info!(
                                        condition_id = %condition_id,
                                        old_tick = %tsc.old_tick_size,
                                        new_tick = %tsc.new_tick_size,
                                        "[v2] WS tick_size_change event received"
                                    );
                                    ws_tick_sizes.write().insert(
                                        ws_condition_id.clone(),
                                        tsc.new_tick_size,
                                    );
                                    metrics::counter!("ws_tick_size_changes").increment(1);
                                }
                                Err(e) => {
                                    warn!(condition_id = %condition_id, "[v2] tick_size_change WS error: {e}");
                                }
                            }
                        }

                        else => {
                            warn!(condition_id = %condition_id, "[v2] All WS streams ended, reconnecting");
                            break;
                        }
                    }
                }

                {
                    let mut books = orderbooks.write();
                    books.remove(&token_id_yes);
                    books.remove(&token_id_no);
                }

                warn!(
                    condition_id = %condition_id,
                    "[v2] WS disconnected, reconnecting in {}s",
                    backoff.as_secs()
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        });

        self.ws_handles.insert(ws_handle_key, handle);
        info!(
            condition_id = %market.condition_id,
            "[v2] Spawned WS subscription task"
        );
    }

    fn orders_on_side_count(&self, condition_id: &str, outcome: Outcome) -> usize {
        self.fill_handler
            .order_state(condition_id)
            .map(|s| s.order_ids_for(outcome).len())
            .unwrap_or(0)
    }

    async fn process_paper_fill(&mut self, fill: PaperFill, market_label: &str) {
        let outcome_str = match fill.order.outcome {
            Outcome::Yes => "UP",
            Outcome::No => "DOWN",
        };

        info!(
            mode = "PAPER-FILL",
            order_id = %fill.order.order_id,
            condition_id = %fill.order.condition_id,
            outcome = %fill.order.outcome,
            price = %fill.fill_price,
            size = %fill.order.size,
            "[v2] Paper order FILLED"
        );

        // Record in inventory (buy adds position, sell reduces it)
        if fill.order.side == PaperSide::Buy {
            self.inventory.record_fill(
                &fill.order.condition_id,
                fill.order.outcome,
                fill.fill_price,
                fill.order.size,
            );
        } else {
            let sell_pnl = self.inventory.record_sell(
                &fill.order.condition_id,
                fill.order.outcome,
                fill.fill_price,
                fill.order.size,
            );
            // Record sell time for BOTH-SIDE buy cooldown + track realized P&L.
            // Both-side cooldown prevents the freed budget from immediately buying
            // the opposite side within the cooldown window, complementing the
            // sell_cost_basis_freed capacity withdrawal for inter-tick protection.
            if let Some(ms) = self.active_markets.get_mut(&fill.order.condition_id) {
                let now = Instant::now();
                ms.last_sell_time.insert(Outcome::Yes, now);
                ms.last_sell_time.insert(Outcome::No, now);
                ms.sell_realized_pnl += sell_pnl;
                // Withdraw freed cost basis from trading budget (prevents churn)
                let cost_basis_freed =
                    (fill.fill_price * fill.order.size - sell_pnl).max(Decimal::ZERO);
                ms.sell_cost_basis_freed += cost_basis_freed;
            }
        }

        // Persist fill + position
        let position = self.inventory.get_position(&fill.order.condition_id);
        if let Some(pos) = &position {
            let side_str = match fill.order.side {
                PaperSide::Buy => "buy",
                PaperSide::Sell => "sell",
            };
            let _ = self
                .db
                .write_fill_and_position(
                    &fill.order.order_id,
                    &fill.order.condition_id,
                    fill.order.outcome,
                    fill.fill_price,
                    fill.order.size,
                    side_str,
                    pos,
                )
                .await;
        }

        // Update dashboard
        let side_label = if fill.order.side == PaperSide::Buy {
            "BUY"
        } else {
            "SELL"
        };
        let detailed = self.build_detailed_entry(
            fill.fill_time,
            market_label,
            &fill.order.condition_id,
            side_label,
            outcome_str,
            fill.fill_price,
            fill.order.size,
            "FILLED",
        );
        {
            let mut dash = self.dashboard.write();
            dash.push_order(OrderFeedEntry {
                time: fill.fill_time,
                market: market_label.to_string(),
                side: side_label.to_string(),
                outcome: outcome_str.to_string(),
                price: fill.fill_price,
                size: fill.order.size,
                status: OrderStatus::Filled,
            });
            dash.push_detailed_order(detailed);
            dash.total_fills += 1;
        }
        // Persist fill count to DB (fire and forget)
        let db_clone2 = self.db.clone();
        tokio::spawn(async move {
            let _ = db_clone2.increment_session_fills(1).await;
        });

        // Log fill to per-period CSV + order event
        let mut fill_edge_sample: Option<(f64, f64)> = None;
        if let Some(ms) = self.active_markets.get(&fill.order.condition_id) {
            let period_name = ms.period_name.clone();
            let (btc_current_opt, vol_opt) = {
                let bs = self.asset_price.read();
                (bs.current_price, bs.realized_vol_per_sec())
            };
            let btc_current = btc_current_opt.unwrap_or(0.0);
            let btc_open = ms.btc_open.unwrap_or(btc_current);
            let sigma = vol_opt.unwrap_or(self.v2.min_vol_per_sec);
            let remaining_secs = self.time_manager.seconds_remaining(ms.market.end_date) as f64;
            let fv_up = fair_value_up(btc_open, btc_current, sigma, remaining_secs);
            let side_str = match fill.order.side {
                PaperSide::Buy => "buy",
                PaperSide::Sell => "sell",
            };
            let fill_price = fill.fill_price.to_f64().unwrap_or(0.0);
            let fill_size = fill.order.size.to_f64().unwrap_or(0.0);
            let fair = if fill.order.outcome == Outcome::Yes {
                fv_up
            } else {
                1.0 - fv_up
            };
            let edge = if side_str == "buy" {
                fair - fill_price
            } else {
                fill_price - fair
            };
            if fill_size > 0.0 {
                fill_edge_sample = Some((edge * fill_size, fill_size));
            }
            self.period_logger.log_fill(
                &period_name,
                side_str,
                fill.order.outcome,
                fill.fill_price,
                fill.order.size,
                btc_current,
                btc_open,
                fv_up,
                1.0 - fv_up,
                sigma,
                remaining_secs,
                &fill.order.condition_id,
                &fill.order.order_id,
                "paper",
            );
            self.period_logger.log_order_event(
                &period_name,
                &fill.order.order_id,
                "FILLED",
                fill.order.outcome,
                fill.fill_price,
                fill.order.size,
                Decimal::ZERO,
                "paper_fill",
            );
        }

        // Update period counters + remove filled order from resting maps (paper mode)
        // Paper fills are always full fills (no partial fills in paper sim)
        if let Some(ms) = self.active_markets.get_mut(&fill.order.condition_id) {
            Self::record_period_fill_counters(
                ms,
                fill.order.side == PaperSide::Buy,
                fill.order.outcome,
                fill.fill_price,
                fill.order.size,
                fill_edge_sample,
                true, // paper fills are always full fills
            );
            ms.resting_orders
                .retain(|_, order| order.order_id != fill.order.order_id);
            ms.resting_sells
                .retain(|_, order| order.order_id != fill.order.order_id);
        }

        // Update position display
        self.update_positions_display();
    }

    /// Log top-5 orderbook depth for all active markets (called every 5 seconds).
    fn log_book_snapshots(&mut self) {
        let market_data: Vec<(String, String, String)> = self
            .active_markets
            .values()
            .map(|ms| {
                (
                    ms.period_name.clone(),
                    ms.market.token_id_yes.clone(),
                    ms.market.token_id_no.clone(),
                )
            })
            .collect();

        let books = self.orderbooks.read();
        for (period_name, yes_token, no_token) in &market_data {
            // UP_BID: top 5 bids for YES token (descending price)
            if let Some(book) = books.get(yes_token) {
                let bids: Vec<(Decimal, Decimal)> = book
                    .bids
                    .iter()
                    .rev()
                    .take(5)
                    .map(|(p, s)| (*p, *s))
                    .collect();
                self.period_logger
                    .log_book_snapshot(period_name, "UP_BID", &bids);

                let asks: Vec<(Decimal, Decimal)> =
                    book.asks.iter().take(5).map(|(p, s)| (*p, *s)).collect();
                self.period_logger
                    .log_book_snapshot(period_name, "UP_ASK", &asks);
            }
            // DOWN_BID: top 5 bids for NO token
            if let Some(book) = books.get(no_token) {
                let bids: Vec<(Decimal, Decimal)> = book
                    .bids
                    .iter()
                    .rev()
                    .take(5)
                    .map(|(p, s)| (*p, *s))
                    .collect();
                self.period_logger
                    .log_book_snapshot(period_name, "DOWN_BID", &bids);

                let asks: Vec<(Decimal, Decimal)> =
                    book.asks.iter().take(5).map(|(p, s)| (*p, *s)).collect();
                self.period_logger
                    .log_book_snapshot(period_name, "DOWN_ASK", &asks);
            }
        }
        drop(books);
    }

    fn update_positions_display(&self) {
        let summary = self.inventory.portfolio_summary();
        let mut entries = Vec::new();

        // ── Position entries from inventory (only active markets) ──
        let mut inv_pairs = Decimal::ZERO;
        let mut inv_locked = Decimal::ZERO;

        for pos in &summary.positions {
            if !self.active_markets.contains_key(&pos.condition_id) {
                continue;
            }

            inv_pairs += pos.complete_pairs();
            inv_locked += pos.locked_profit();

            if pos.yes_qty > Decimal::ZERO {
                let avg = pos.total_yes_spent / pos.yes_qty;
                entries.push(PositionEntry {
                    market_label: pos.condition_id.chars().take(8).collect::<String>() + "…",
                    outcome: "UP".into(),
                    entry_price: avg,
                    size: pos.yes_qty,
                    pnl: Decimal::ZERO,
                    resolved: false,
                    winner: None,
                });
            }
            if pos.no_qty > Decimal::ZERO {
                let avg = pos.total_no_spent / pos.no_qty;
                entries.push(PositionEntry {
                    market_label: pos.condition_id.chars().take(8).collect::<String>() + "…",
                    outcome: "DOWN".into(),
                    entry_price: avg,
                    size: pos.no_qty,
                    pnl: Decimal::ZERO,
                    resolved: false,
                    winner: None,
                });
            }

            let pairs = pos.complete_pairs();
            if pairs > Decimal::ZERO {
                let locked = pos.locked_profit();
                for entry in entries.iter_mut() {
                    if entry
                        .market_label
                        .starts_with(&pos.condition_id.chars().take(8).collect::<String>())
                    {
                        entry.pnl = locked / Decimal::TWO;
                    }
                }
            }
        }

        // ── Period summary from detailed_order_log (cumulative, survives market removal) ──
        // Aggregate all BUY/SELL fills across the entire session.
        let dash_read = self.dashboard.read();
        let log = &dash_read.detailed_order_log;

        let mut buy_yes_qty = Decimal::ZERO;
        let mut buy_yes_cost = Decimal::ZERO;
        let mut sell_yes_qty = Decimal::ZERO;
        let mut sell_yes_revenue = Decimal::ZERO;
        let mut buy_no_qty = Decimal::ZERO;
        let mut buy_no_cost = Decimal::ZERO;
        let mut sell_no_qty = Decimal::ZERO;
        let mut sell_no_revenue = Decimal::ZERO;

        for e in log.iter() {
            let price = e.fill_price;
            let size = e.size;
            let cost = price * size;
            match (e.side.as_str(), e.outcome.as_str()) {
                ("BUY", "UP") => {
                    buy_yes_qty += size;
                    buy_yes_cost += cost;
                }
                ("SELL", "UP") => {
                    sell_yes_qty += size;
                    sell_yes_revenue += cost;
                }
                ("BUY", "DOWN") => {
                    buy_no_qty += size;
                    buy_no_cost += cost;
                }
                ("SELL", "DOWN") => {
                    sell_no_qty += size;
                    sell_no_revenue += cost;
                }
                _ => {}
            }
        }
        drop(dash_read);

        // Net quantities held
        let net_yes = buy_yes_qty - sell_yes_qty;
        let net_no = buy_no_qty - sell_no_qty;

        // Realized PnL from sells
        let yes_avg_buy = if buy_yes_qty > Decimal::ZERO {
            buy_yes_cost / buy_yes_qty
        } else {
            Decimal::ZERO
        };
        let no_avg_buy = if buy_no_qty > Decimal::ZERO {
            buy_no_cost / buy_no_qty
        } else {
            Decimal::ZERO
        };
        let yes_realized = sell_yes_revenue - (sell_yes_qty * yes_avg_buy);
        let no_realized = sell_no_revenue - (sell_no_qty * no_avg_buy);

        // Current market prices for unrealized value
        let mut yes_current_price = Decimal::ZERO;
        let mut no_current_price = Decimal::ZERO;
        {
            let books = self.orderbooks.read();
            if let Some(ms) = self.active_markets.values().next() {
                if let Some(book) = books.get(&ms.market.token_id_yes) {
                    yes_current_price = book.best_bid().map(|(p, _)| p).unwrap_or(Decimal::ZERO);
                }
                if let Some(book) = books.get(&ms.market.token_id_no) {
                    no_current_price = book.best_bid().map(|(p, _)| p).unwrap_or(Decimal::ZERO);
                }
            }
        }

        // Unrealized value of remaining position
        let yes_unrealized_value = net_yes * yes_current_price;
        let no_unrealized_value = net_no * no_current_price;
        // Net cost of remaining position
        let yes_net_cost = net_yes * yes_avg_buy;
        let no_net_cost = net_no * no_avg_buy;
        let yes_unrealized = yes_unrealized_value - yes_net_cost;
        let no_unrealized = no_unrealized_value - no_net_cost;

        // Total return = realized + unrealized
        let yes_total_return = yes_realized + yes_unrealized;
        let no_total_return = no_realized + no_unrealized;

        let yes_return_pct = if buy_yes_cost > Decimal::ZERO {
            (yes_total_return / buy_yes_cost)
                .to_string()
                .parse::<f64>()
                .unwrap_or(0.0)
                * 100.0
        } else {
            0.0
        };
        let no_return_pct = if buy_no_cost > Decimal::ZERO {
            (no_total_return / buy_no_cost)
                .to_string()
                .parse::<f64>()
                .unwrap_or(0.0)
                * 100.0
        } else {
            0.0
        };

        // Complete pairs = min of total bought across both outcomes
        let complete_pairs = buy_yes_qty.min(buy_no_qty);
        let locked_profit = if complete_pairs > Decimal::ZERO {
            complete_pairs - (complete_pairs * yes_avg_buy) - (complete_pairs * no_avg_buy)
        } else {
            Decimal::ZERO
        };

        let period = PeriodSummary {
            up: PeriodSummaryEntry {
                outcome: "Up".into(),
                qty: buy_yes_qty,
                avg_price: yes_avg_buy,
                cost: buy_yes_cost,
                current_price: yes_current_price,
                value: yes_unrealized_value + sell_yes_revenue,
                return_pnl: yes_total_return,
                return_pct: yes_return_pct,
            },
            down: PeriodSummaryEntry {
                outcome: "Down".into(),
                qty: buy_no_qty,
                avg_price: no_avg_buy,
                cost: buy_no_cost,
                current_price: no_current_price,
                value: no_unrealized_value + sell_no_revenue,
                return_pnl: no_total_return,
                return_pct: no_return_pct,
            },
            total_cost: buy_yes_cost + buy_no_cost,
            total_value: (yes_unrealized_value + sell_yes_revenue)
                + (no_unrealized_value + sell_no_revenue),
            total_return: yes_total_return + no_total_return,
            complete_pairs,
            locked_profit,
        };

        let mut dash = self.dashboard.write();
        dash.positions = entries;
        dash.period_summary = period;
        dash.open_positions = summary.active_markets as u32;
    }

    // ─── Detailed Order Log Helper ─────────────────────────────────────

    /// Build a DetailedOrderEntry capturing full context at fill time for CSV export.
    fn build_detailed_entry(
        &self,
        time: DateTime<Utc>,
        market: &str,
        condition_id: &str,
        side: &str,
        outcome: &str,
        fill_price: Decimal,
        size: Decimal,
        status: &str,
    ) -> DetailedOrderEntry {
        let btc_state = self.asset_price.read();
        let btc_price_at_fill = btc_state.current_price.unwrap_or(0.0);
        let sigma = btc_state.realized_vol_per_sec().unwrap_or(0.0);
        drop(btc_state);

        let ms = self.active_markets.get(condition_id);
        let btc_open = ms.and_then(|m| m.btc_open).unwrap_or(0.0);
        let end_date = ms.map(|m| m.market.end_date);
        let secs_remaining = end_date
            .map(|ed| self.time_manager.seconds_remaining(ed) as f64)
            .unwrap_or(0.0);

        // Read dashboard snapshot for current FV/bid state
        let dash = self.dashboard.read();
        let fv_up = dash.fv_up;
        let fv_down = dash.fv_down;
        let bid_yes = dash.bid_yes;
        let bid_no = dash.bid_no;
        let combined_bid = dash.combined_bid;
        let total_pnl = dash.total_pnl;
        let today_pnl = dash.today_pnl;
        drop(dash);

        // CLOB best prices
        let (best_ask_yes, best_ask_no, best_bid_yes, best_bid_no) = ms
            .map(|ms| {
                let books = self.orderbooks.read();
                let ba_yes = books
                    .get(&ms.market.token_id_yes)
                    .and_then(|b| b.best_ask())
                    .map(|(p, _)| p)
                    .unwrap_or(Decimal::ZERO);
                let ba_no = books
                    .get(&ms.market.token_id_no)
                    .and_then(|b| b.best_ask())
                    .map(|(p, _)| p)
                    .unwrap_or(Decimal::ZERO);
                let bb_yes = books
                    .get(&ms.market.token_id_yes)
                    .and_then(|b| b.best_bid())
                    .map(|(p, _)| p)
                    .unwrap_or(Decimal::ZERO);
                let bb_no = books
                    .get(&ms.market.token_id_no)
                    .and_then(|b| b.best_bid())
                    .map(|(p, _)| p)
                    .unwrap_or(Decimal::ZERO);
                (ba_yes, ba_no, bb_yes, bb_no)
            })
            .unwrap_or((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));

        // Position state
        let position = self
            .inventory
            .get_position(condition_id)
            .unwrap_or_default();
        let pos_yes_qty = position.yes_qty;
        let pos_no_qty = position.no_qty;
        let complete_pairs = position.complete_pairs();
        let locked_profit = position.locked_profit();

        DetailedOrderEntry {
            time,
            market: market.to_string(),
            condition_id: condition_id.to_string(),
            side: side.to_string(),
            outcome: outcome.to_string(),
            fill_price,
            size,
            status: status.to_string(),
            btc_price_at_fill,
            btc_open,
            fv_up,
            fv_down,
            sigma,
            bid_yes,
            bid_no,
            combined_bid,
            best_ask_yes,
            best_ask_no,
            best_bid_yes,
            best_bid_no,
            pos_yes_qty,
            pos_no_qty,
            complete_pairs,
            locked_profit,
            secs_remaining,
            total_pnl,
            today_pnl,
        }
    }

    // ─── Dashboard Update ─────────────────────────────────────────────

    fn update_dashboard(&self) {
        let btc_state = self.asset_price.read();
        let btc_price = btc_state.current_price.unwrap_or(0.0);
        let vol = btc_state.realized_vol_per_sec().unwrap_or(0.0);
        drop(btc_state);

        // Find the active market (the one currently being traded)
        let active = self.active_markets.values().next();
        let (question, end_date, btc_open) = match active {
            Some(ms) => (
                ms.market.question.clone(),
                Some(ms.market.end_date),
                ms.btc_open.unwrap_or(0.0),
            ),
            None => (String::new(), None, 0.0),
        };

        let bot_status_str = self.bot_control.read().status.as_str().to_string();

        let mut dash = self.dashboard.write();
        dash.bot_status = bot_status_str;
        dash.push_btc_price(btc_price);
        dash.btc_open = btc_open;
        dash.vol_per_sec = vol;
        // today_pnl is now persisted in DB and updated via record_trade_result()
        // Don't overwrite it with inventory.daily_pnl which is session-only
        dash.active_market_question = question;
        dash.active_market_end = end_date;
        dash.active_market_count = self.active_markets.len() as u32;
        dash.markets_discovered = self.markets_discovered_total;
        dash.uptime_secs = self.start_time.elapsed().as_secs();
        dash.last_update = Utc::now();

        // Always compute FV for dashboard display, even when quote cycle is blocked
        if btc_price > 0.0 && btc_open > 0.0 {
            if let Some(end) = end_date {
                let sigma = if vol > 0.0 {
                    vol
                } else {
                    self.v2.min_vol_per_sec
                };
                let remaining = self.time_manager.seconds_remaining(end) as f64;
                let fv = fair_value_up(btc_open, btc_price, sigma, remaining);
                dash.fv_up = fv;
                dash.fv_down = 1.0 - fv;
                dash.sigma = sigma;
            }
        }

        // Push equity/pnl chart points (total_pnl is persisted cumulative PnL)
        let pnl_val = dash.total_pnl.to_f64().unwrap_or(0.0);
        dash.push_equity(pnl_val);
        dash.push_pnl(pnl_val);
        drop(dash);

        // Refresh positions every tick (not just on fills)
        self.update_positions_display();
    }

    // ─── Health & Shutdown ───────────────────────────────────────────

    async fn health_check(&mut self) {
        let summary = self.inventory.portfolio_summary();
        let btc = self.asset_price.read().current_price;
        let vol = self.asset_price.read().realized_vol_per_sec();

        debug!(
            active_markets = summary.active_markets,
            total_exposure = %summary.total_exposure,
            daily_pnl = %summary.daily_pnl,
            btc_price = ?btc,
            vol_per_sec = ?vol,
            "[v2] Health check"
        );
        metrics::gauge!("active_markets", "asset" => self.asset.display_name())
            .set(summary.active_markets as f64);
        metrics::gauge!("daily_pnl_usd", "asset" => self.asset.display_name())
            .set(summary.daily_pnl.to_f64().unwrap_or(0.0));
        metrics::gauge!("total_exposure_usd", "asset" => self.asset.display_name())
            .set(summary.total_exposure.to_f64().unwrap_or(0.0));

        // ── Per-asset state gauges (Grafana) ──
        {
            let asset_label = self.asset.display_name();
            // Asset price + volatility
            let price_state = self.asset_price.read();
            if let Some(price) = price_state.current_price {
                metrics::gauge!("asset_price_usd", "asset" => asset_label).set(price);
            }
            if let Some(sigma) = price_state.realized_vol_per_sec() {
                metrics::gauge!("sigma_per_sec", "asset" => asset_label).set(sigma);
            }
            drop(price_state);

            // Aggregate position across all active markets for this asset
            let mut total_yes: f64 = 0.0;
            let mut total_no: f64 = 0.0;
            let mut total_pairs: f64 = 0.0;
            let mut total_budget_used: f64 = 0.0;
            for (cid, _ms) in &self.active_markets {
                if let Some(pos) = self.inventory.get_position(cid) {
                    total_yes += pos.yes_qty.to_f64().unwrap_or(0.0);
                    total_no += pos.no_qty.to_f64().unwrap_or(0.0);
                    total_pairs += pos.complete_pairs().to_f64().unwrap_or(0.0);
                    total_budget_used += (pos.total_yes_spent + pos.total_no_spent)
                        .to_f64()
                        .unwrap_or(0.0);
                }
            }
            metrics::gauge!("position_yes_shares", "asset" => asset_label).set(total_yes);
            metrics::gauge!("position_no_shares", "asset" => asset_label).set(total_no);
            metrics::gauge!("position_complete_pairs", "asset" => asset_label).set(total_pairs);
            metrics::gauge!("budget_used_usd", "asset" => asset_label).set(total_budget_used);
            metrics::gauge!("session_pnl_usd", "asset" => asset_label)
                .set(self.cumulative_session_pnl.to_f64().unwrap_or(0.0));

            // Trading suspend state (0 = active, 1 = engine restart, 2 = rate limited, 3 = cancel only, 4 = manual)
            let suspend_code = match self.suspend_reason {
                None => 0.0,
                Some(TradingSuspendReason::EngineRestart) => 1.0,
                Some(TradingSuspendReason::RateLimited) => 2.0,
                Some(TradingSuspendReason::CancelOnly) => 3.0,
                Some(TradingSuspendReason::Manual) => 4.0,
            };
            metrics::gauge!("trading_suspend_state", "asset" => asset_label).set(suspend_code);
        }

        // ── Liveness probes (watchdog) ──
        // Check if quote cycle is stuck (hung await or deadlock)
        let quote_age = self.last_quote_cycle.elapsed();
        if quote_age > Duration::from_secs(30) {
            error!(
                age_secs = quote_age.as_secs(),
                "[v2] CRITICAL: Quote cycle stalled for {}s — triggering emergency",
                quote_age.as_secs()
            );
            self.emergency
                .trigger_emergency(crate::risk::emergency::EmergencyTrigger::StrategyHang);
        }

        // Check if Binance price feed is stale (WS silently died)
        if self.config.mode == TradingMode::Live {
            let price_stale = self
                .asset_price
                .read()
                .is_price_stale(Duration::from_secs(30));
            if price_stale
                && self
                    .active_markets
                    .iter()
                    .any(|(_, ms)| !ms.resting_orders.is_empty())
            {
                error!("[v2] CRITICAL: Binance price feed stale >30s with resting orders — triggering emergency");
                self.emergency
                    .trigger_emergency(crate::risk::emergency::EmergencyTrigger::StrategyHang);
            }
        }

        // ── CLOB Heartbeat health probe ──
        // Polymarket auto-cancels all orders if heartbeats stop for ~15s.
        // The SDK runs heartbeats in the background, but if they silently fail
        // (network partition, auth expiry), we need to detect it fast.
        if self.config.mode == TradingMode::Live {
            if let Some(sdk) = &self.sdk {
                if sdk.heartbeats_active() {
                    if self.consecutive_heartbeat_failures > 0 {
                        info!(
                            "[v2-{}] Heartbeat recovered after {} failures",
                            self.asset, self.consecutive_heartbeat_failures
                        );
                    }
                    self.consecutive_heartbeat_failures = 0;
                } else {
                    self.consecutive_heartbeat_failures += 1;
                    error!(
                        consecutive = self.consecutive_heartbeat_failures,
                        "[v2-{}] CRITICAL: SDK heartbeat task is NOT running — orders may be auto-cancelled",
                        self.asset
                    );
                    metrics::counter!("heartbeat_task_dead", "asset" => self.asset.display_name())
                        .increment(1);
                }

                // After 3+ consecutive failures, assume all orders are cancelled.
                // Wipe local resting state so we don't have phantom orders.
                if self.consecutive_heartbeat_failures >= 3 {
                    error!(
                        "[v2-{}] 3+ heartbeat failures — clearing all local resting order state (assumed auto-cancelled)",
                        self.asset
                    );
                    metrics::counter!("heartbeat_state_wipe", "asset" => self.asset.display_name())
                        .increment(1);
                    for (_cid, ms) in &mut self.active_markets {
                        if !ms.resting_orders.is_empty() {
                            warn!(
                                condition_id = %_cid,
                                orders_wiped = ms.resting_orders.len(),
                                "[v2-{}] Wiping {} phantom resting orders for market {}",
                                self.asset, ms.resting_orders.len(), _cid
                            );
                            ms.resting_orders.clear();
                            ms.resting_sells.clear();
                        }
                    }
                }
            }
        }

        // ── Chainlink cross-validation ──
        // Compare Binance and Chainlink prices when both are available.
        // Use percentage-based thresholds (not absolute $) so this works for all assets
        // (BTC ~$90k, ETH ~$3k, SOL ~$130, XRP ~$0.50).
        {
            let state = self.asset_price.read();
            if let (Some(binance), Some(chainlink)) = (state.current_price, state.chainlink_price) {
                let binance_dec = Decimal::from_f64_retain(binance).unwrap_or(Decimal::ZERO);
                let divergence = (binance_dec - chainlink).abs();
                // Calculate divergence as percentage of the midpoint
                let midpoint = (binance_dec + chainlink) / Decimal::TWO;
                let divergence_pct = if midpoint > Decimal::ZERO {
                    (divergence / midpoint) * dec!(100)
                } else {
                    Decimal::ZERO
                };
                // Emergency at >0.5% divergence, warn at >0.1%
                // (0.5% of $90k BTC ≈ $450, 0.5% of $0.50 XRP ≈ $0.0025)
                if divergence_pct > dec!(0.5) {
                    drop(state);
                    error!(
                        binance = %binance_dec,
                        chainlink = %chainlink,
                        divergence = %divergence,
                        divergence_pct = %divergence_pct,
                        "[v2] CRITICAL: Binance/Chainlink price divergence >{:.1}% — triggering emergency",
                        divergence_pct
                    );
                    self.emergency
                        .trigger_emergency(crate::risk::emergency::EmergencyTrigger::StrategyHang);
                    let _ = self.alert_tx.send(AlertMessage::Emergency(
                        format!("Price feed divergence: Binance={binance_dec} Chainlink={chainlink} diff={divergence_pct:.2}%")
                    ));
                } else if divergence_pct > dec!(0.1) {
                    warn!(
                        binance = %binance_dec,
                        chainlink = %chainlink,
                        divergence = %divergence,
                        divergence_pct = %divergence_pct,
                        "[v2] WARNING: Binance/Chainlink price divergence >{:.2}%", divergence_pct
                    );
                    metrics::counter!("price_feed_divergence_total", "asset" => self.asset.display_name()).increment(1);
                }
            }
        }

        // ── Task death monitoring ──
        // Check global panic flag
        if crate::PANIC_EMERGENCY.load(std::sync::atomic::Ordering::Relaxed) {
            error!("[v2] CRITICAL: Panic detected in spawned task — triggering emergency");
            self.emergency
                .trigger_emergency(crate::risk::emergency::EmergencyTrigger::StrategyHang);
        }

        // Check Binance feed
        if let Some(ref handle) = self.binance_handle {
            if handle.is_finished() {
                error!("[v2] CRITICAL: Binance price feed task died — triggering emergency");
                self.emergency
                    .trigger_emergency(crate::risk::emergency::EmergencyTrigger::StrategyHang);
                self.binance_handle = None;
            }
        }

        // Check per-market WS tasks — trigger emergency if active market loses its WS feed.
        // FIX: Also cancel resting orders for affected markets to prevent blind quoting
        // after emergency auto-clears. Without this, the market stays in active_markets
        // with no WS feed, and the bot resumes quoting without seeing fills.
        let dead_markets: Vec<ConditionId> = self
            .ws_handles
            .iter()
            .filter(|(_, h)| h.is_finished())
            .map(|(cid, _)| cid.clone())
            .collect();
        let mut active_ws_died = false;
        for cid in dead_markets {
            self.ws_handles.remove(&cid);
            if self.active_markets.contains_key(&cid) {
                error!(
                    condition_id = %cid,
                    "[v2] CRITICAL: WS task died for active market — cancelling orders and freezing market"
                );
                self.cancel_market_orders(&cid).await;
                // Remove from active_markets so the bot doesn't try to quote this market
                // after emergency clears. The market will be re-discovered if still valid.
                let removed_ms = self.active_markets.remove(&cid);
                self.fill_handler.remove_market(&cid);
                if let Some(ref ms) = removed_ms {
                    // FIX: Do NOT unregister tokens or remove inventory position.
                    // The exchange may still hold the position. Removing it locally
                    // would hide exposure from risk checks (capacity, daily loss).
                    // Tokens must stay registered for the SDK sell guard.
                    // On re-discovery or next startup, reconciliation will handle it.
                    let mut books = self.orderbooks.write();
                    books.remove(&ms.market.token_id_yes);
                    books.remove(&ms.market.token_id_no);
                }
                active_ws_died = true;
            }
        }
        if active_ws_died {
            self.emergency
                .trigger_emergency(crate::risk::emergency::EmergencyTrigger::StrategyHang);
        }
    }

    /// Cross-validate WS orderbook data against REST snapshots.
    /// Detects ghost books (WS data stale/wrong) and forces re-subscribe on large divergence.
    async fn validate_orderbooks(&mut self) {
        // Only validate in live mode — paper mode already uses REST
        if self.config.mode != TradingMode::Live {
            return;
        }

        let market_tokens: Vec<(ConditionId, String, String)> = self
            .active_markets
            .iter()
            .map(|(cid, ms)| {
                (
                    cid.clone(),
                    ms.market.token_id_yes.clone(),
                    ms.market.token_id_no.clone(),
                )
            })
            .collect();

        for (cid, yes_token, no_token) in market_tokens {
            for (label, token_id) in [("YES", &yes_token), ("NO", &no_token)] {
                let rest_book = tokio::task::spawn_blocking({
                    let tid = token_id.clone();
                    move || fetch_clob_book(&tid)
                })
                .await
                .ok()
                .flatten();

                let Some(rest) = rest_book else { continue };

                let ws_book = self.orderbooks.read().get(token_id).cloned();
                let Some(ws) = ws_book else { continue };

                // Compare best bid/ask
                let divergence = |ws_val: Option<Decimal>, rest_val: Option<Decimal>| -> Decimal {
                    match (ws_val, rest_val) {
                        (Some(w), Some(r)) => (w - r).abs(),
                        _ => Decimal::ZERO,
                    }
                };

                let bid_div = divergence(
                    ws.best_bid().map(|(p, _)| p),
                    rest.best_bid().map(|(p, _)| p),
                );
                let ask_div = divergence(
                    ws.best_ask().map(|(p, _)| p),
                    rest.best_ask().map(|(p, _)| p),
                );

                let max_div = bid_div.max(ask_div);
                let severe_threshold = Decimal::new(15, 2); // 15 cents
                let warn_threshold = Decimal::new(5, 2); // 5 cents

                if max_div > severe_threshold {
                    error!(
                        condition_id = %cid,
                        side = label,
                        bid_divergence = %bid_div,
                        ask_divergence = %ask_div,
                        "[v2] REST validation: severe orderbook divergence — cancelling orders"
                    );
                    // Cancel all resting orders for this market
                    if let Some(ms) = self.active_markets.get_mut(cid.as_str()) {
                        let period_name = ms.period_name.clone();
                        let order_infos: Vec<(OrderId, Outcome, Decimal, Decimal)> = ms
                            .resting_orders
                            .iter()
                            .chain(ms.resting_sells.iter())
                            .map(|((outcome, price), order)| {
                                (order.order_id.clone(), *outcome, *price, order.size)
                            })
                            .collect();
                        // NOTE: Do NOT clear resting_orders/resting_sells yet — wait for
                        // cancel confirmation to avoid phantom orders on network failure.
                        drop(ms); // release borrow for batch_cancel_confirmed
                                  // Batch cancel divergent orders
                        if !order_infos.is_empty() {
                            if self.config.mode == TradingMode::Paper {
                                for (oid, _, _, _) in &order_infos {
                                    self.paper_sim.cancel(oid);
                                }
                            }
                            let ids: Vec<&str> = order_infos
                                .iter()
                                .map(|(oid, _, _, _)| oid.as_str())
                                .collect();
                            let confirmed = self
                                .batch_cancel_confirmed(&ids, "rest_validation_divergence")
                                .await;
                            // Only clear confirmed orders from local state
                            if let Some(ms) = self.active_markets.get_mut(cid.as_str()) {
                                for (oid, outcome, price, _size) in &order_infos {
                                    if confirmed.contains(oid.as_str()) {
                                        ms.resting_orders.remove(&(*outcome, *price));
                                        ms.resting_sells.remove(&(*outcome, *price));
                                        ms.orders_cancelled += 1;
                                        self.fill_handler.unregister_order(&cid, oid);
                                    }
                                }
                            }
                            for (oid, outcome, price, size) in &order_infos {
                                if confirmed.contains(oid.as_str()) {
                                    self.period_logger.log_order_event(
                                        &period_name,
                                        oid,
                                        "CANCELLED",
                                        *outcome,
                                        *price,
                                        *size,
                                        *size,
                                        "rest_validation_divergence",
                                    );
                                    if let Err(e) =
                                        self.db.update_order_status(oid, "cancelled").await
                                    {
                                        warn!("[v2] Failed to update cancelled order in DB: {e}");
                                    }
                                }
                            }
                        }
                    }
                    // Overwrite WS book with REST data
                    self.orderbooks.write().insert(token_id.clone(), rest);
                    metrics::counter!("ws_orderbook_divergence_total", "asset" => self.asset.display_name(), "severity" => "severe").increment(1);
                } else if max_div > warn_threshold {
                    warn!(
                        condition_id = %cid,
                        side = label,
                        bid_divergence = %bid_div,
                        ask_divergence = %ask_div,
                        "[v2] REST validation: moderate orderbook divergence"
                    );
                    metrics::counter!("ws_orderbook_divergence_total", "asset" => self.asset.display_name(), "severity" => "moderate").increment(1);
                }
            }
        }
    }

    /// Check if an order error indicates a retryable HTTP status and activate appropriate throttle.
    ///
    /// Detects and handles:
    /// - **425**: CLOB engine restarting. Orders may be wiped. 30s cooldown + reconcile.
    /// - **429**: Rate limited. Exponential backoff (2s → 4s → ... → 30s).
    /// - **503**: Cancel-only / maintenance mode. 15s cooldown, only cancels allowed.
    fn check_rate_limit_error(&mut self, err: &crate::error::BotError) {
        let msg = err.to_string();

        if msg.contains("425") {
            // Engine restart: orders may be auto-cancelled, need full state reconcile
            const ENGINE_RESTART_COOLDOWN_SECS: u64 = 30;
            self.suspend_reason = Some(TradingSuspendReason::EngineRestart);
            self.throttle_until =
                Some(Instant::now() + Duration::from_secs(ENGINE_RESTART_COOLDOWN_SECS));
            self.needs_post_restart_reconcile = true;
            error!(
                cooldown_secs = ENGINE_RESTART_COOLDOWN_SECS,
                "[v2-{}] 425 ENGINE RESTART — suspending for {}s, will reconcile after",
                self.asset,
                ENGINE_RESTART_COOLDOWN_SECS
            );
            metrics::counter!("engine_restart_425", "asset" => self.asset.display_name())
                .increment(1);

            // Optimistically wipe local resting state since engine restart cancels all orders
            for (_cid, ms) in &mut self.active_markets {
                ms.resting_orders.clear();
                ms.resting_sells.clear();
            }
        } else if msg.contains("503") {
            // Cancel-only mode: can still cancel but not place new orders
            const CANCEL_ONLY_COOLDOWN_SECS: u64 = 15;
            self.suspend_reason = Some(TradingSuspendReason::CancelOnly);
            self.throttle_until =
                Some(Instant::now() + Duration::from_secs(CANCEL_ONLY_COOLDOWN_SECS));
            warn!(
                cooldown_secs = CANCEL_ONLY_COOLDOWN_SECS,
                "[v2-{}] 503 CANCEL-ONLY mode — suspending placements for {}s (cancels still allowed)",
                self.asset, CANCEL_ONLY_COOLDOWN_SECS
            );
            metrics::counter!("cancel_only_503", "asset" => self.asset.display_name()).increment(1);
        } else if msg.contains("429")
            || msg.contains("rate limit")
            || msg.contains("Too Many Requests")
        {
            const MAX_BACKOFF_SECS: u64 = 30;
            self.suspend_reason = Some(TradingSuspendReason::RateLimited);
            self.throttle_backoff_secs = (self.throttle_backoff_secs * 2).min(MAX_BACKOFF_SECS);
            self.throttle_until =
                Some(Instant::now() + Duration::from_secs(self.throttle_backoff_secs));
            warn!(
                backoff_secs = self.throttle_backoff_secs,
                "[v2-{}] 429 rate limit — throttling for {}s",
                self.asset,
                self.throttle_backoff_secs
            );
            metrics::counter!("rate_limit_hits_total", "asset" => self.asset.display_name())
                .increment(1);
        }
    }

    /// Reset adaptive throttle backoff after a successful cycle with orders.
    fn reset_throttle_backoff(&mut self) {
        if self.throttle_backoff_secs > 2 {
            self.throttle_backoff_secs = 2;
        }
    }

    fn asset_guard_suppressing(&mut self) -> bool {
        let Some(until) = self.asset_guard_active_until else {
            return false;
        };
        let now = Instant::now();
        if now < until {
            let cooldown = Duration::from_secs(30);
            let should_log = self
                .last_asset_guard_log
                .map(|last| now.duration_since(last) >= cooldown)
                .unwrap_or(true);
            if should_log {
                self.last_asset_guard_log = Some(now);
                warn!(
                    asset = %self.asset,
                    remaining_secs = until.saturating_duration_since(now).as_secs(),
                    "[v2] Asset guard active: suppressing new bids due to degraded rolling performance"
                );
            }
            true
        } else {
            info!(
                asset = %self.asset,
                "[v2] Asset guard lifted: resuming new bid placement"
            );
            self.asset_guard_active_until = None;
            self.last_asset_guard_log = None;
            false
        }
    }

    async fn cancel_all_resting_buy_orders(&mut self, reason: &str) {
        let market_ids: Vec<ConditionId> = self.active_markets.keys().cloned().collect();
        for condition_id in &market_ids {
            let (period_name, buy_orders): (String, Vec<(OrderId, Outcome, Decimal, Decimal)>) =
                if let Some(ms) = self.active_markets.get(condition_id.as_str()) {
                    let period_name = ms.period_name.clone();
                    let buy_orders: Vec<(OrderId, Outcome, Decimal, Decimal)> = ms
                        .resting_orders
                        .iter()
                        .map(|((outcome, price), order)| {
                            (order.order_id.clone(), *outcome, *price, order.size)
                        })
                        .collect();
                    // NOTE: Do NOT clear resting_orders yet — wait for cancel confirmation.
                    (period_name, buy_orders)
                } else {
                    (String::new(), Vec::new())
                };

            if buy_orders.is_empty() {
                continue;
            }

            self.note_cancel_all_event(condition_id);

            // Batch cancel
            if self.config.mode == TradingMode::Paper {
                for (oid, _, _, _) in &buy_orders {
                    self.paper_sim.cancel(oid);
                }
            }
            let ids: Vec<&str> = buy_orders
                .iter()
                .map(|(oid, _, _, _)| oid.as_str())
                .collect();
            let confirmed = self.batch_cancel_confirmed(&ids, reason).await;
            // Only clear confirmed orders from local state
            if let Some(ms) = self.active_markets.get_mut(condition_id.as_str()) {
                for (oid, outcome, price, _) in &buy_orders {
                    if confirmed.contains(oid.as_str()) {
                        ms.resting_orders.remove(&(*outcome, *price));
                        ms.orders_cancelled += 1;
                    }
                }
            }
            for (oid, outcome, price, size) in &buy_orders {
                if confirmed.contains(oid.as_str()) {
                    self.period_logger.log_order_event(
                        &period_name,
                        oid,
                        "CANCELLED",
                        *outcome,
                        *price,
                        *size,
                        *size,
                        reason,
                    );
                    self.fill_handler.unregister_order(condition_id, oid);
                    if let Err(e) = self.db.update_order_status(oid, "cancelled").await {
                        warn!("[v2] Failed to update cancelled order in DB: {e}");
                    }
                }
            }
        }
    }

    async fn evaluate_asset_guard_after_period(
        &mut self,
        period_name: &str,
        condition_id: &str,
        period_pnl: Decimal,
        orders_placed: u32,
        orders_filled: u32,
    ) {
        let fill_rate = if orders_placed > 0 {
            orders_filled as f64 / orders_placed as f64
        } else {
            0.0
        };
        self.recent_period_health.push_back(PeriodHealthSample {
            pnl: period_pnl,
            fill_rate,
        });

        let keep = (self.v2.asset_guard_window_periods as usize)
            .saturating_mul(2)
            .max(8);
        while self.recent_period_health.len() > keep {
            self.recent_period_health.pop_front();
        }

        if !self.v2.asset_guard_enabled {
            return;
        }
        let window = self.v2.asset_guard_window_periods as usize;
        if window == 0 || self.recent_period_health.len() < window {
            return;
        }

        let recent = self
            .recent_period_health
            .iter()
            .rev()
            .take(window)
            .copied()
            .collect::<Vec<_>>();
        let rolling_pnl: Decimal = recent.iter().map(|s| s.pnl).sum();
        let rolling_fill_rate = recent.iter().map(|s| s.fill_rate).sum::<f64>() / window as f64;
        let fill_bad = rolling_fill_rate < self.v2.asset_guard_min_fill_rate;
        let pnl_bad = rolling_pnl < self.v2.asset_guard_min_rolling_pnl;

        if !(fill_bad || pnl_bad) {
            return;
        }

        let now = Instant::now();
        if self
            .asset_guard_active_until
            .map(|until| now < until)
            .unwrap_or(false)
        {
            return;
        }

        self.asset_guard_active_until =
            Some(now + Duration::from_secs(self.v2.asset_guard_pause_secs));
        self.last_asset_guard_log = None;

        error!(
            asset = %self.asset,
            period_name,
            condition_id,
            rolling_periods = window,
            rolling_fill_rate = format!("{rolling_fill_rate:.4}"),
            rolling_pnl = %rolling_pnl,
            fill_threshold = format!("{:.4}", self.v2.asset_guard_min_fill_rate),
            pnl_threshold = %self.v2.asset_guard_min_rolling_pnl,
            pause_secs = self.v2.asset_guard_pause_secs,
            "[v2] Asset guard TRIGGERED: suppressing new bids and cancelling resting buy orders"
        );
        let _ = self.alert_tx.send(AlertMessage::Emergency(format!(
            "[{}] Asset guard triggered: rolling_fill_rate={:.4}, rolling_pnl={} over {} periods. Suppressing new bids for {}s.",
            self.asset,
            rolling_fill_rate,
            rolling_pnl,
            window,
            self.v2.asset_guard_pause_secs
        )));
        self.cancel_all_resting_buy_orders("asset_guard_pause")
            .await;
    }

    fn can_place_emergency_sell(&self, condition_id: &str, outcome: Outcome) -> bool {
        if self.v2.emergency_sell_cooldown_secs == 0 {
            return true;
        }
        let Some(ms) = self.active_markets.get(condition_id) else {
            return true;
        };
        if let Some(last) = ms.last_emergency_sell_at {
            let cooldown = Duration::from_secs(self.v2.emergency_sell_cooldown_secs);
            if Instant::now().duration_since(last) < cooldown {
                debug!(
                    condition_id = %condition_id,
                    ?outcome,
                    cooldown_secs = self.v2.emergency_sell_cooldown_secs,
                    "[v2] Emergency sell skipped due to cooldown"
                );
                return false;
            }
        }
        true
    }

    fn mark_emergency_sell_placed(&mut self, condition_id: &str) {
        if let Some(ms) = self.active_markets.get_mut(condition_id) {
            ms.last_emergency_sell_at = Some(Instant::now());
            ms.emergency_sell_placements += 1;
        }
    }

    fn record_latency_success(&mut self, condition_id: &str, latency_ms: u128) {
        if let Some(ms) = self.active_markets.get_mut(condition_id) {
            ms.latency_success_sum_ms += latency_ms as f64;
            ms.latency_success_count += 1;
        }
        metrics::histogram!("order_latency_ms", "asset" => self.asset.display_name())
            .record(latency_ms as f64);
    }

    /// Periodic position reconciliation: compare internal inventory against Data API.
    /// On significant mismatch, adopt the API positions as truth (the exchange is
    /// the authoritative source), cancel resting orders for safety, and resume trading.
    async fn reconcile_positions(&mut self) {
        let sdk = match &self.sdk {
            Some(s) => s,
            None => return, // Paper mode — no SDK
        };

        let api_positions = match sdk.get_positions_from_api().await {
            Ok(p) => p,
            Err(e) => {
                warn!("[v2] Reconciliation failed to fetch positions: {e}");
                return;
            }
        };

        // Group API positions by condition_id → (yes_qty, no_qty)
        let mut api_map: HashMap<String, (Decimal, Decimal)> = HashMap::new();
        for ap in &api_positions {
            let cid = format!("{:#x}", ap.condition_id);
            // Skip resolved/redeemable positions
            if ap.redeemable {
                continue;
            }
            let entry = api_map.entry(cid).or_insert((Decimal::ZERO, Decimal::ZERO));
            // Map outcome string to yes/no. "Up"/"Yes" → yes, "Down"/"No" → no
            let outcome_lower = ap.outcome.to_lowercase();
            if outcome_lower == "yes" || outcome_lower == "up" {
                entry.0 += ap.size;
            } else if outcome_lower == "no" || outcome_lower == "down" {
                entry.1 += ap.size;
            }
        }

        // Compare with internal inventory for active markets only
        let mismatch_threshold = dec!(0.5);
        let mut corrected = 0u32;
        let active_ids: Vec<ConditionId> = self.active_markets.keys().cloned().collect();

        for condition_id in &active_ids {
            // Skip markets whose position was already freed (Closing/Resolved phase).
            // The internal inventory is intentionally zeroed while the API may still
            // report residual tokens from an incomplete merge.
            if let Some(ms) = self.active_markets.get(condition_id.as_str()) {
                if ms.closing_position.is_some() {
                    continue;
                }
            }

            let internal = self.inventory.get_position(condition_id);
            let (int_yes, int_no) = match &internal {
                Some(p) => (p.yes_qty, p.no_qty),
                None => (Decimal::ZERO, Decimal::ZERO),
            };

            let (api_yes, api_no) = api_map
                .get(condition_id)
                .copied()
                .unwrap_or((Decimal::ZERO, Decimal::ZERO));

            let yes_diff = (int_yes - api_yes).abs();
            let no_diff = (int_no - api_no).abs();

            if yes_diff > mismatch_threshold || no_diff > mismatch_threshold {
                // SAFETY: Only adopt API values that are HIGHER than internal.
                // The API can lag behind WS fills by seconds, so api_qty < int_qty
                // usually means stale data, NOT that we over-counted.  Adopting a
                // lower value zeros our position and causes overshoot (0xf0a8 incident).
                // API values HIGHER than internal may indicate fills we missed on WS.
                let adopt_yes = api_yes.max(int_yes);
                let adopt_no = api_no.max(int_no);
                let yes_increased = api_yes > int_yes;
                let no_increased = api_no > int_no;
                let yes_stale = api_yes < int_yes;
                let no_stale = api_no < int_no;

                if yes_stale || no_stale {
                    warn!(
                        condition_id = %condition_id,
                        int_yes = %int_yes, api_yes = %api_yes,
                        int_no = %int_no, api_no = %api_no,
                        yes_stale, no_stale,
                        "[v2] Position mismatch — API LOWER than internal (stale?), keeping internal values"
                    );
                }
                if yes_increased || no_increased {
                    warn!(
                        condition_id = %condition_id,
                        int_yes = %int_yes, api_yes = %api_yes,
                        int_no = %int_no, api_no = %api_no,
                        yes_increased, no_increased,
                        "[v2] Position mismatch — API HIGHER, adopting upward correction"
                    );
                }

                // Only update if there's an actual upward correction to apply
                if yes_increased || no_increased {
                    self.inventory
                        .force_update_position(condition_id, adopt_yes, adopt_no);
                }

                // Only cancel resting orders and alert when we adopted an upward
                // correction.  Stale-API-lower mismatches are benign — our internal
                // tracking from WS fills is authoritative for decreases.
                if yes_increased || no_increased {
                    let mut stale_orders: Vec<(OrderId, Outcome, Decimal, Decimal)> = Vec::new();
                    let mut period_name: Option<String> = None;
                    if let Some(ms) = self.active_markets.get_mut(condition_id.as_str()) {
                        if ms.reconciliation_blocked {
                            info!(
                                condition_id = %condition_id,
                                "[v2] Reconciliation unblocking previously frozen market"
                            );
                            ms.reconciliation_blocked = false;
                            ms.reconciliation_block_reason = None;
                        }
                        period_name = Some(ms.period_name.clone());
                        stale_orders = ms
                            .resting_orders
                            .iter()
                            .chain(ms.resting_sells.iter())
                            .map(|((outcome, price), order)| {
                                (order.order_id.clone(), *outcome, *price, order.size)
                            })
                            .collect();
                    }

                    // Batch cancel stale reconciliation orders
                    if !stale_orders.is_empty() {
                        if self.config.mode == TradingMode::Paper {
                            for (oid, _, _, _) in &stale_orders {
                                self.paper_sim.cancel(oid);
                            }
                        }
                        let ids: Vec<&str> = stale_orders
                            .iter()
                            .map(|(oid, _, _, _)| oid.as_str())
                            .collect();
                        let confirmed = self
                            .batch_cancel_confirmed(&ids, "reconciliation_correction")
                            .await;
                        if let Some(ms) = self.active_markets.get_mut(condition_id.as_str()) {
                            for (oid, outcome, price, _size) in &stale_orders {
                                if confirmed.contains(oid.as_str()) {
                                    ms.resting_orders.remove(&(*outcome, *price));
                                    ms.resting_sells.remove(&(*outcome, *price));
                                    ms.orders_cancelled += 1;
                                }
                            }
                        }
                        if let Some(ref period_name) = period_name {
                            for (oid, outcome, price, size) in &stale_orders {
                                if confirmed.contains(oid.as_str()) {
                                    self.period_logger.log_order_event(
                                        period_name,
                                        oid,
                                        "CANCELLED",
                                        *outcome,
                                        *price,
                                        *size,
                                        *size,
                                        "reconciliation_correction",
                                    );
                                    self.fill_handler.unregister_order(condition_id, oid);
                                    if let Err(e) =
                                        self.db.update_order_status(oid, "cancelled").await
                                    {
                                        warn!("[v2] Failed to update cancelled order in DB: {e}");
                                    }
                                }
                            }
                        }
                    }
                    self.cancel_market_orders(condition_id).await;

                    let _ = self.alert_tx.send(AlertMessage::Risk(
                        format!(
                            "Reconciliation UP-corrected {condition_id}: internal YES/NO was {int_yes}/{int_no}, adopted YES/NO={adopt_yes}/{adopt_no}. Orders cancelled."
                        ),
                    ));

                    corrected += 1;
                }
            }
        }

        if corrected > 0 {
            warn!(
                corrected,
                "[v2] Reconciliation complete — corrected positions from API"
            );
        } else {
            debug!("[v2] Reconciliation complete — all positions match");
        }

        // ── Order-existence validation ──
        // Detect phantom orders: orders we believe are resting but don't exist on the exchange.
        // This catches cases where the exchange silently removed orders (e.g., Feb 2026 order
        // attack, heartbeat expiry, settlement failure) that we never received a WS cancel for.
        if let Some(sdk) = &self.sdk {
            let mut phantoms_removed = 0u32;
            for condition_id in &active_ids {
                let ms = match self.active_markets.get(condition_id.as_str()) {
                    Some(ms) => ms,
                    None => continue,
                };
                if ms.closing_position.is_some() {
                    continue; // Skip markets in Closing/Resolved
                }

                // Collect local resting order IDs for this market
                let local_order_ids: Vec<String> = ms
                    .resting_orders
                    .values()
                    .chain(ms.resting_sells.values())
                    .map(|o| o.order_id.clone())
                    .collect();

                if local_order_ids.is_empty() {
                    continue;
                }

                // Query exchange for open orders on both tokens
                let mut exchange_order_ids = std::collections::HashSet::new();
                for token_id in [&ms.market.token_id_yes, &ms.market.token_id_no] {
                    match sdk.get_open_orders_for_token(token_id).await {
                        Ok(orders) => {
                            for o in &orders {
                                exchange_order_ids.insert(o.id.clone());
                            }
                        }
                        Err(e) => {
                            // If we can't query, skip validation for this market
                            warn!(
                                condition_id = %condition_id,
                                token_id = %token_id,
                                "[v2] Order validation query failed: {e} — skipping"
                            );
                            continue;
                        }
                    }
                }

                // Find phantoms: orders in local state but not on exchange
                let phantoms: Vec<String> = local_order_ids
                    .iter()
                    .filter(|oid| !exchange_order_ids.contains(oid.as_str()))
                    .cloned()
                    .collect();

                if !phantoms.is_empty() {
                    warn!(
                        condition_id = %condition_id,
                        count = phantoms.len(),
                        "[v2] Phantom orders detected — removing from local state"
                    );
                    if let Some(ms) = self.active_markets.get_mut(condition_id.as_str()) {
                        ms.resting_orders
                            .retain(|_, o| !phantoms.contains(&o.order_id));
                        ms.resting_sells
                            .retain(|_, o| !phantoms.contains(&o.order_id));
                    }
                    for oid in &phantoms {
                        self.fill_handler.unregister_order(condition_id, oid);
                    }
                    phantoms_removed += phantoms.len() as u32;
                    metrics::counter!("phantom_orders_removed", "asset" => self.asset.display_name())
                        .increment(phantoms.len() as u64);
                }
            }
            if phantoms_removed > 0 {
                warn!(
                    phantoms_removed,
                    "[v2] Order validation complete — removed phantom orders"
                );
            }
        }
    }

    async fn graceful_shutdown(&mut self) {
        info!("[v2] Starting graceful shutdown...");

        // FIX: Cancel only THIS asset's markets, not account-wide.
        // All orchestrators share the same SDK, so account-wide cancel_all_orders()
        // would be called once per asset (BTC, ETH, SOL, XRP) in parallel — wasteful
        // and potentially racy. Per-market cancel is safer and only affects our orders.
        // Account-wide cleanup happens once at next startup (main.rs cancel-all).
        let market_ids: Vec<ConditionId> = self.active_markets.keys().cloned().collect();
        for condition_id in &market_ids {
            self.cancel_market_orders(condition_id).await;
        }

        // ── Save active periods to history before exiting ──
        let btc_close = self.asset_price.read().current_price.unwrap_or(0.0);
        let mode = self.mode_label();
        let mut period_health_samples: Vec<(String, String, Decimal, u32, u32)> = Vec::new();

        for condition_id in &market_ids {
            if let Some(ms) = self.active_markets.get(condition_id.as_str()) {
                let period_name = ms.period_name.clone();
                let btc_open = ms.btc_open.unwrap_or(0.0);
                let result = if btc_close >= btc_open { "UP" } else { "DOWN" };

                let position = ms
                    .closing_position
                    .clone()
                    .or_else(|| self.inventory.get_position(condition_id))
                    .unwrap_or_default();

                // Preserve quoted-but-unfilled periods in shutdown history so shadow runs
                // still emit analyzable summaries when no inventory was accumulated.
                let observed_strategy_activity = ms.orders_placed > 0
                    || ms.orders_cancelled > 0
                    || ms.orders_expired > 0
                    || ms.max_quote_levels_yes > 0
                    || ms.max_quote_levels_no > 0
                    || ms.pair_completion_attempts > 0
                    || !ms.suppression_reason_counts.is_empty();

                if position.yes_qty.is_zero()
                    && position.no_qty.is_zero()
                    && ms.merge_realized_pnl.is_zero()
                    && ms.sell_realized_pnl.is_zero()
                    && !observed_strategy_activity
                {
                    continue;
                }

                let complete_pairs = position.complete_pairs();
                let locked_profit = position.locked_profit();

                // Compute PnL as if resolving now at current BTC price
                let winning_payout = if btc_close >= btc_open {
                    position.yes_qty
                } else {
                    position.no_qty
                };
                let remaining_cost = position.total_yes_spent + position.total_no_spent;
                let resolution_pnl = winning_payout - remaining_cost;
                let period_pnl = resolution_pnl + ms.merge_realized_pnl + ms.sell_realized_pnl;

                info!(
                    condition_id,
                    %period_pnl,
                    %locked_profit,
                    btc_open,
                    btc_close,
                    result,
                    "[v2] Saving interrupted period to history"
                );

                self.cumulative_session_pnl += period_pnl;

                // Deep grid fill metrics
                let avg_deep_fill_price = if ms.deep_grid_fill_shares > Decimal::ZERO {
                    (ms.deep_grid_fill_cost / ms.deep_grid_fill_shares)
                        .to_f64()
                        .unwrap_or(0.0)
                } else {
                    0.0
                };

                // CSV logs
                self.period_logger.log_period_result(
                    &period_name,
                    condition_id,
                    btc_open,
                    btc_close,
                    result,
                    position.yes_qty,
                    position.no_qty,
                    complete_pairs,
                    locked_profit,
                    ms.sell_realized_pnl,
                    ms.merge_realized_pnl,
                    ms.cumulative_merged_pairs,
                    period_pnl,
                    self.cumulative_session_pnl,
                    ms.deep_grid_fills_up,
                    ms.deep_grid_fills_down,
                    ms.deep_grid_fill_shares,
                    avg_deep_fill_price,
                );

                let suppression_reason_counts = Self::suppression_reason_counts_csv(&ms);
                let settlement_mode = Self::settlement_mode(&ms, &position);
                self.period_logger.log_session_summary(
                    &self.session_start.clone(),
                    &period_name,
                    condition_id,
                    btc_open,
                    btc_close,
                    result,
                    ms.orders_placed,
                    ms.orders_filled,
                    ms.orders_cancelled,
                    ms.orders_expired,
                    ms.total_up_shares_filled,
                    ms.total_down_shares_filled,
                    complete_pairs,
                    locked_profit,
                    ms.gross_cost,
                    period_pnl,
                    self.cumulative_session_pnl,
                    ms.max_excess_seen,
                    ms.max_quote_levels_yes,
                    ms.max_quote_levels_no,
                    ms.pair_completion_attempts,
                    ms.pair_completion_successes,
                    &suppression_reason_counts,
                    ms.cancel_all_count,
                    &settlement_mode,
                    if ms.fill_edge_size_sum > 0.0 {
                        ms.fill_edge_notional_sum / ms.fill_edge_size_sum
                    } else {
                        0.0
                    },
                    if ms.latency_success_count > 0 {
                        ms.latency_success_sum_ms / ms.latency_success_count as f64
                    } else {
                        0.0
                    },
                    mode,
                    ms.deep_grid_fills_up,
                    ms.deep_grid_fills_down,
                    ms.deep_grid_fill_shares,
                    avg_deep_fill_price,
                );

                self.period_logger.flush_all();
                self.period_logger.close_period(&period_name);

                // ── Prometheus: period completion counters (interrupted) ──
                {
                    let result_label = if period_pnl > Decimal::ZERO {
                        "win"
                    } else {
                        "loss"
                    };
                    metrics::counter!("periods_completed_total", "asset" => self.asset.display_name(), "result" => result_label).increment(1);
                    metrics::histogram!("period_pnl_usd", "asset" => self.asset.display_name())
                        .record(period_pnl.to_f64().unwrap_or(0.0));
                    metrics::histogram!("period_complete_pairs", "asset" => self.asset.display_name()).record(complete_pairs.to_f64().unwrap_or(0.0));
                }

                // DB writes
                let won = period_pnl > Decimal::ZERO;
                let pairs_i64 = complete_pairs.to_i64().unwrap_or(0);
                let excess = if position.yes_qty > position.no_qty {
                    (position.yes_qty - position.no_qty).to_i64().unwrap_or(0)
                } else {
                    (position.no_qty - position.yes_qty).to_i64().unwrap_or(0)
                };
                let _fills_count = ms.orders_filled as i64;

                let merged_pairs_i64 = ms.cumulative_merged_pairs.to_i64().unwrap_or(0);
                if let Err(e) = self
                    .db
                    .insert_period_result(
                        &period_name,
                        condition_id,
                        result,
                        won,
                        pairs_i64,
                        excess,
                        locked_profit,
                        ms.sell_realized_pnl,
                        ms.merge_realized_pnl,
                        merged_pairs_i64,
                        period_pnl,
                        btc_open,
                        btc_close,
                        &self.run_id,
                        self.asset.display_name(),
                    )
                    .await
                {
                    warn!(
                        "[v2-{}] Failed to persist shutdown period result: {e}",
                        self.asset
                    );
                }

                // FIX: Pass 0 for fills — already counted per-fill via increment_session_fills
                let today_str = chrono::Utc::now().format("%Y-%m-%d").to_string();
                if let Err(e) = self
                    .db
                    .record_period_in_session_stats(
                        period_pnl,
                        won,
                        0, // fills already counted per-fill via increment_session_fills
                        merged_pairs_i64,
                        &today_str,
                    )
                    .await
                {
                    warn!("[v2] Failed to update session stats at shutdown: {e}");
                }

                if let Err(e) = self
                    .db
                    .insert_equity_point(
                        self.cumulative_session_pnl,
                        "shutdown",
                        &self.run_id,
                        self.asset.display_name(),
                    )
                    .await
                {
                    warn!(
                        "[v2-{}] Failed to insert shutdown equity point: {e}",
                        self.asset
                    );
                }

                period_health_samples.push((
                    period_name,
                    condition_id.clone(),
                    period_pnl,
                    ms.orders_placed,
                    ms.orders_filled,
                ));
            }
        }

        for (period_name, condition_id, period_pnl, orders_placed, orders_filled) in
            period_health_samples
        {
            self.evaluate_asset_guard_after_period(
                &period_name,
                &condition_id,
                period_pnl,
                orders_placed,
                orders_filled,
            )
            .await;
        }

        let summary = self.inventory.portfolio_summary();
        info!(
            active_markets = summary.active_markets,
            total_exposure = %summary.total_exposure,
            daily_pnl = %summary.daily_pnl,
            "[v2] Final state at shutdown"
        );

        // FIX: Persist aggregate daily P&L at shutdown so restarts include sell P&L.
        {
            let today_str_pnl = chrono::Utc::now().format("%Y-%m-%d").to_string();
            if let Some(cents) = self.inventory.aggregate_daily_pnl_cents() {
                if let Err(e) = self.db.persist_daily_pnl_cents(&today_str_pnl, cents).await {
                    warn!("[v2] Failed to persist aggregate daily P&L at shutdown: {e}");
                }
            }
        }

        let _ = self.alert_tx.send(AlertMessage::System(format!(
            "[v2] Bot shutting down. Daily P&L: {}",
            summary.daily_pnl
        )));

        // Sweep all redeemable positions before exiting
        if let Some(sdk) = &self.sdk {
            info!("[v2] Running redeem_all sweep before shutdown...");
            let rpc = self.onchain.rpc_url();
            match sdk.redeem_all_redeemable(rpc).await {
                Ok((s, f)) => {
                    info!(
                        success = s,
                        failed = f,
                        "[v2] Shutdown redeem_all sweep complete"
                    );
                }
                Err(e) => warn!("[v2] Shutdown redeem_all sweep failed: {e}"),
            }
        }

        info!("[v2] Graceful shutdown complete");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn test_market_state() -> MarketV2State {
        let end_date = Utc::now();
        let market = TrackedMarket {
            condition_id: "cond_test".into(),
            token_id_yes: "yes_token".into(),
            token_id_no: "no_token".into(),
            question: "BTC Up/Down test".into(),
            start_date: Some(end_date - chrono::Duration::minutes(15)),
            end_date,
            tick_size: dec!(0.01),
            neg_risk: false,
        };

        MarketV2State {
            market,
            btc_open: Some(100_000.0),
            resting_orders: HashMap::new(),
            resting_sells: HashMap::new(),
            resting_deep_grid: HashMap::new(),
            deep_grid_placed: false,
            deep_grid_fills_up: 0,
            deep_grid_fills_down: 0,
            deep_grid_fill_shares: Decimal::ZERO,
            deep_grid_fill_cost: Decimal::ZERO,
            last_sell_time: HashMap::new(),
            ev_breaker_since: None,
            last_ev_breaker_log: None,
            last_pair_completion_attempt: None,
            pair_completion_attempts: 0,
            pair_completion_successes: 0,
            period_name: "test_period".into(),
            last_yes_center: None,
            last_no_center: None,
            orders_placed: 0,
            orders_filled: 0,
            orders_cancelled: 0,
            orders_expired: 0,
            total_up_shares_filled: Decimal::ZERO,
            total_down_shares_filled: Decimal::ZERO,
            gross_cost: Decimal::ZERO,
            gross_buy_filled_usdc: Decimal::ZERO,
            fill_edge_notional_sum: 0.0,
            fill_edge_size_sum: 0.0,
            latency_success_sum_ms: 0.0,
            latency_success_count: 0,
            sell_realized_pnl: Decimal::ZERO,
            merge_realized_pnl: Decimal::ZERO,
            sell_cost_basis_freed: Decimal::ZERO,
            merge_cost_basis_released: Decimal::ZERO,
            closing_position: None,
            exit_buy_block: None,
            last_merge_time: None,
            cumulative_merged_pairs: Decimal::ZERO,
            taker_fee_rate: None,
            fee_last_fetched: None,
            first_order_placed_at: None,
            discovered_at: Instant::now(),
            book_ready: false,
            reconciliation_blocked: false,
            reconciliation_block_reason: None,
            last_reconciliation_block_log: None,
            last_emergency_sell_at: None,
            emergency_sell_placements: 0,
            last_churn_breaker_log: None,
            max_excess_seen: Decimal::ZERO,
            max_quote_levels_yes: 0,
            max_quote_levels_no: 0,
            suppression_reason_counts: HashMap::new(),
            cancel_all_count: 0,
            pair_quality_block_active: false,
            min_worst_case_pnl_seen: Decimal::ZERO,
            closing_expired_logged: false,
        }
    }

    #[test]
    fn test_normal_cdf() {
        // Phi(0) = 0.5
        assert!((normal_cdf(0.0) - 0.5).abs() < 1e-6);
        // Phi(inf) ≈ 1
        assert!((normal_cdf(5.0) - 1.0).abs() < 1e-4);
        // Phi(-inf) ≈ 0
        assert!(normal_cdf(-5.0) < 1e-4);
        // Phi(1.96) ≈ 0.975
        assert!((normal_cdf(1.96) - 0.975).abs() < 1e-3);
        // Phi(-1.96) ≈ 0.025
        assert!((normal_cdf(-1.96) - 0.025).abs() < 1e-3);
        // Symmetry: Phi(x) + Phi(-x) = 1
        for x in [0.5, 1.0, 1.5, 2.0, 3.0] {
            assert!((normal_cdf(x) + normal_cdf(-x) - 1.0).abs() < 1e-6);
        }
    }

    #[test]
    fn test_fair_value_up_at_open() {
        // BTC hasn't moved → 50/50
        let fv = fair_value_up(97000.0, 97000.0, 0.0001, 600.0);
        assert!((fv - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_fair_value_up_btc_risen() {
        // BTC up significantly → high P(up)
        let fv = fair_value_up(97000.0, 97100.0, 0.0001, 600.0);
        assert!(fv > 0.6);
    }

    #[test]
    fn test_fair_value_up_btc_fallen() {
        // BTC down → low P(up)
        let fv = fair_value_up(97000.0, 96900.0, 0.0001, 600.0);
        assert!(fv < 0.4);
    }

    #[test]
    fn test_fair_value_clamped() {
        // Extreme values should be clamped to [0.02, 0.98]
        let fv = fair_value_up(97000.0, 98000.0, 0.00001, 10.0);
        assert!(fv <= 0.98);
        let fv = fair_value_up(97000.0, 96000.0, 0.00001, 10.0);
        assert!(fv >= 0.02);
    }

    #[test]
    fn test_btc_price_state_vol() {
        let mut state = AssetPriceState::new(120);

        // Need enough samples for vol calculation
        // Simulate 20 samples at varying prices
        for i in 0..20 {
            let price = 97000.0 + (i as f64 * 10.0) * if i % 2 == 0 { 1.0 } else { -1.0 };
            state.current_price = Some(price);
            state
                .price_samples
                .push_back((Instant::now() + Duration::from_millis(500 * i), price));
            state.last_sample_time = Some(Instant::now());
        }

        let vol = state.realized_vol_per_sec();
        assert!(vol.is_some());
        assert!(vol.unwrap() > 0.0);
    }

    #[test]
    fn test_round_down_to_tick() {
        assert_eq!(round_down_to_tick(dec!(0.437), dec!(0.01)), dec!(0.43));
        assert_eq!(round_down_to_tick(dec!(0.5), dec!(0.01)), dec!(0.50));
        assert_eq!(round_down_to_tick(dec!(0.999), dec!(0.01)), dec!(0.99));
    }

    #[test]
    fn test_v2_config_defaults() {
        let cfg = V2Config::default();
        assert_eq!(cfg.target_combined, dec!(0.93));
        assert!(cfg.min_vol_per_sec > 0.0);
        assert_eq!(cfg.level_order_size, dec!(15));
        assert_eq!(cfg.ladder_levels, 5);
        assert_eq!(cfg.max_abs_imbalance, dec!(15));
        assert_eq!(cfg.max_per_order_combined, dec!(0.99));
    }

    #[test]
    fn test_record_period_fill_counters_tracks_buy_commitment_only_for_buys() {
        let mut ms = test_market_state();

        OrchestratorV2::record_period_fill_counters(
            &mut ms,
            true,
            Outcome::Yes,
            dec!(0.40),
            dec!(10),
            Some((2.0, 10.0)),
            true, // full fill
        );

        assert_eq!(ms.orders_filled, 1);
        assert_eq!(ms.gross_cost, dec!(4.0));
        assert_eq!(ms.gross_buy_filled_usdc, dec!(4.0));
        assert_eq!(ms.total_up_shares_filled, dec!(10));
        assert_eq!(ms.total_down_shares_filled, Decimal::ZERO);
        assert_eq!(ms.fill_edge_notional_sum, 2.0);
        assert_eq!(ms.fill_edge_size_sum, 10.0);

        OrchestratorV2::record_period_fill_counters(
            &mut ms,
            false,
            Outcome::No,
            dec!(0.60),
            dec!(5),
            None,
            true, // full fill
        );

        assert_eq!(ms.orders_filled, 2);
        assert_eq!(ms.gross_cost, dec!(7.0));
        assert_eq!(
            ms.gross_buy_filled_usdc,
            dec!(4.0),
            "SELL fills must not increase buy commitment"
        );
        assert_eq!(ms.total_up_shares_filled, dec!(10));
        assert_eq!(ms.total_down_shares_filled, dec!(5));
    }

    #[test]
    fn test_compute_terminal_pnl_bounds_and_worst_case_loss() {
        let position = Position {
            condition_id: "cond_test".into(),
            yes_qty: dec!(10),
            no_qty: dec!(4),
            total_yes_spent: dec!(4.0),
            total_no_spent: dec!(1.2),
        };

        let bounds = compute_terminal_pnl_bounds(&position, dec!(0.5), dec!(0.1));
        assert_eq!(bounds.pnl_if_up, dec!(5.4));
        assert_eq!(bounds.pnl_if_down, dec!(-0.6));
        assert_eq!(bounds.worst_case_pnl, dec!(-0.6));
    }

    #[test]
    fn test_position_pair_ratio() {
        assert_eq!(position_pair_ratio(&Position::default()), 1.0);

        let balanced = Position {
            yes_qty: dec!(10),
            no_qty: dec!(10),
            ..Default::default()
        };
        assert_eq!(position_pair_ratio(&balanced), 1.0);

        let one_sided = Position {
            yes_qty: dec!(10),
            no_qty: Decimal::ZERO,
            ..Default::default()
        };
        assert_eq!(position_pair_ratio(&one_sided), 0.0);

        let mixed = Position {
            yes_qty: dec!(20),
            no_qty: dec!(5),
            ..Default::default()
        };
        assert!((position_pair_ratio(&mixed) - 0.4).abs() < 1e-9);
    }

    #[test]
    fn test_resting_buy_notional_and_single_order_notional_cap() {
        let mut resting = HashMap::new();
        resting.insert(
            (Outcome::Yes, dec!(0.40)),
            RestingLadderOrder {
                order_id: "o1".into(),
                size: dec!(10),
                placed_at: Instant::now(),
            },
        );
        resting.insert(
            (Outcome::No, dec!(0.50)),
            RestingLadderOrder {
                order_id: "o2".into(),
                size: dec!(8),
                placed_at: Instant::now(),
            },
        );

        assert_eq!(resting_buy_notional(&resting), dec!(8.0));
        assert_eq!(
            cap_buy_size_for_notional(dec!(100), dec!(0.50), dec!(12.5)),
            dec!(25)
        );
        assert_eq!(
            cap_buy_size_for_notional(dec!(100), dec!(0.50), Decimal::ZERO),
            dec!(100)
        );
    }

    #[test]
    fn test_dynamic_loss_cap_schedule() {
        assert_eq!(dynamic_loss_cap(600.0), dec!(0.03));
        assert_eq!(dynamic_loss_cap(360.0), dec!(0.06));
        assert_eq!(dynamic_loss_cap(240.0), dec!(0.10));
        assert_eq!(dynamic_loss_cap(150.0), dec!(0.15));
        // Desperation tier: final 2 minutes, sell at any price
        assert_eq!(dynamic_loss_cap(120.0), dec!(1.00));
        assert_eq!(dynamic_loss_cap(60.0), dec!(1.00));
        assert_eq!(dynamic_loss_cap(0.0), dec!(1.00));
    }

    #[test]
    fn test_select_exit_mode_rules() {
        let mut v2 = V2Config::default();
        v2.exit_soft_excess = dec!(5);
        v2.exit_hard_excess = dec!(8);
        v2.exit_taker_after_secs = 20;
        v2.exit_force_taker_remaining_secs = 240;

        // Soft excess before soft window opens: no exit.
        assert_eq!(select_exit_mode(dec!(6), 700.0, 0.0, &v2), ExitMode::None);
        // Soft excess after soft window opens: maker mode.
        assert_eq!(select_exit_mode(dec!(6), 590.0, 0.0, &v2), ExitMode::Maker);
        // Hard excess + persistent EV breaker: taker mode.
        assert_eq!(select_exit_mode(dec!(9), 700.0, 25.0, &v2), ExitMode::Taker);
        // Late phase force-taker rule.
        assert_eq!(select_exit_mode(dec!(5), 200.0, 0.0, &v2), ExitMode::Taker);
    }

    #[test]
    fn test_exit_plan_ignores_cooldown_when_excess_is_high() {
        let mut v2 = V2Config::default();
        v2.exit_soft_excess = dec!(5);
        v2.exit_hard_excess = dec!(8);
        v2.sell_level_size = dec!(5);
        v2.sell_levels = 2;

        // Heavy YES by 10 shares.
        let position = Position {
            condition_id: String::new(),
            yes_qty: dec!(20),
            no_qty: dec!(10),
            total_yes_spent: dec!(10),
            total_no_spent: dec!(5),
        };

        let plan = compute_excess_exit_plan(
            &position,
            0.45,
            0.55,
            Some(dec!(0.52)),
            Some(dec!(0.48)),
            Some(dec!(0.53)),
            Some(dec!(0.49)),
            dec!(0.01),
            &v2,
            600.0,
            0.0,
            false,
            true, // heavy side has buys this tick
        );

        // Despite heavy_side_has_buys=true, high excess should still produce an exit plan.
        assert!(matches!(
            plan,
            ExitPlan::Maker { .. } | ExitPlan::Taker { .. }
        ));
    }

    #[test]
    fn test_exit_plan_decoupled_from_sellback_min_excess() {
        let mut v2 = V2Config::default();
        v2.exit_soft_excess = dec!(5);
        v2.exit_hard_excess = dec!(8);
        v2.sellback_min_excess = dec!(999);
        v2.sell_level_size = dec!(5);
        v2.sell_levels = 1;

        // Cost excess = |10 - 4| = $6 > exit_soft_excess of $5
        let position = Position {
            condition_id: String::new(),
            yes_qty: dec!(20),
            no_qty: dec!(14),
            total_yes_spent: dec!(10),
            total_no_spent: dec!(4),
        };

        let plan = compute_excess_exit_plan(
            &position,
            0.45,
            0.55,
            Some(dec!(0.52)),
            Some(dec!(0.48)),
            Some(dec!(0.53)),
            Some(dec!(0.49)),
            dec!(0.01),
            &v2,
            590.0,
            0.0,
            false,
            false,
        );

        assert!(matches!(plan, ExitPlan::Maker { .. }));
    }

    #[test]
    fn test_exit_plan_soft_window_guard() {
        let mut v2 = V2Config::default();
        v2.exit_soft_excess = dec!(5);
        v2.exit_hard_excess = dec!(8);

        // Cost excess = |9 - 3| = $6 > exit_soft_excess of $5
        let position = Position {
            condition_id: String::new(),
            yes_qty: dec!(12),
            no_qty: dec!(6),
            total_yes_spent: dec!(9),
            total_no_spent: dec!(3),
        };

        let plan = compute_excess_exit_plan(
            &position,
            0.45,
            0.55,
            Some(dec!(0.52)),
            Some(dec!(0.48)),
            Some(dec!(0.53)),
            Some(dec!(0.49)),
            dec!(0.01),
            &v2,
            700.0,
            0.0,
            false,
            false,
        );

        assert!(matches!(
            plan,
            ExitPlan::Skip {
                reason: "soft_window_not_open",
                ..
            }
        ));
    }

    #[test]
    fn test_compute_bid_ladder_balanced() {
        let v2 = V2Config::default();
        let position = Position::default();
        let tick = dec!(0.01);
        let (yes, no) = compute_bid_ladder(0.5, 0.5, tick, &position, &v2, None);
        // Symmetric FV → both ladders should have same number of levels
        assert_eq!(yes.len(), no.len());
        assert!(!yes.is_empty());
        // Top of ladder should be near 0.48 (0.96 * 0.5)
        assert!(yes[0].price >= dec!(0.40));
        assert!(yes[0].price <= dec!(0.50));
        // Levels should be descending
        for w in yes.windows(2) {
            assert!(w[0].price > w[1].price);
        }
    }

    #[test]
    fn test_compute_bid_ladder_skewed_fv() {
        let v2 = V2Config::default();
        let position = Position::default();
        let tick = dec!(0.01);
        let (yes, no) = compute_bid_ladder(0.8, 0.2, tick, &position, &v2, None);
        // YES should be higher priced than NO
        assert!(yes[0].price > no[0].price);
    }

    #[test]
    fn test_ladder_size_at_level_decay() {
        let base = dec!(20);
        // Level 0 = full base size
        assert_eq!(ladder_size_at_level(base, 0, 0.10), dec!(20));
        // Level 1 = 90% of 20 = 18
        assert_eq!(ladder_size_at_level(base, 1, 0.10), dec!(18));
        // Level 4 = 60% of 20 = 12
        assert_eq!(ladder_size_at_level(base, 4, 0.10), dec!(12));
        // Level 8 = max(0.2, 1.0-0.8) = 0.2 → 20% of 20 = 4, but floored at MIN_ORDER_SHARES=5
        assert_eq!(ladder_size_at_level(base, 8, 0.10), MIN_ORDER_SHARES);
        // Level 10 = factor clamped to 0.2 → 20% of 20 = 4, floored at 5
        assert_eq!(ladder_size_at_level(base, 10, 0.10), MIN_ORDER_SHARES);
    }

    #[test]
    fn test_ladder_size_at_level_no_decay() {
        let base = dec!(15);
        // With 0% decay, all levels should be base size
        for i in 0..10 {
            assert_eq!(ladder_size_at_level(base, i, 0.0), base);
        }
    }

    #[test]
    fn test_compute_bid_ladder_variable_sizes() {
        let mut v2 = V2Config::default();
        v2.ladder_size_decay = 0.10;
        v2.ladder_levels = 5;
        v2.level_order_size = dec!(20);
        let position = Position::default();
        let tick = dec!(0.01);
        let (yes, no) = compute_bid_ladder(0.5, 0.5, tick, &position, &v2, None);
        assert!(yes.len() >= 2, "should have multiple levels");
        // First level should be the largest
        assert!(yes[0].size >= yes[yes.len() - 1].size);
        // Sizes should be non-increasing
        for w in yes.windows(2) {
            assert!(
                w[0].size >= w[1].size,
                "sizes should decay: {} >= {}",
                w[0].size,
                w[1].size
            );
        }
        // Same for NO side
        for w in no.windows(2) {
            assert!(
                w[0].size >= w[1].size,
                "NO sizes should decay: {} >= {}",
                w[0].size,
                w[1].size
            );
        }
    }

    #[test]
    fn test_combined_cost_guard() {
        let v2 = V2Config::default();
        let tick = dec!(0.01);
        // Position: imbalanced with fills on both sides (NOT pair completion)
        let position = Position {
            condition_id: String::new(),
            yes_qty: dec!(10),        // small YES position
            no_qty: dec!(100),        // heavy NO
            total_yes_spent: dec!(5), // avg_yes = 0.50
            total_no_spent: dec!(50), // avg_no = 0.50
        };
        let (mut yes, mut no) = compute_bid_ladder(0.5, 0.5, tick, &position, &v2, None);
        let before_yes = yes.len();
        apply_combined_cost_guard(&mut yes, &mut no, &position, dec!(0.995), dec!(0.99), false);
        // YES levels above 0.495 (0.995 - 0.50) should be removed
        // (YES is light side when heavy NO, uses light_side_max=0.99)
        for level in &yes {
            assert!(level.price + dec!(0.50) <= dec!(0.99));
        }
        // Some levels should have been removed
        assert!(yes.len() <= before_yes);
    }

    #[test]
    fn test_combined_cost_guard_zero_position() {
        // When both positions are zero (period start), the guard should use
        // ladder-top estimates to prevent ask-anchored bids from creating
        // losing pairs (combined > max_combined).
        let position = Position {
            condition_id: String::new(),
            yes_qty: Decimal::ZERO,
            no_qty: Decimal::ZERO,
            total_yes_spent: Decimal::ZERO,
            total_no_spent: Decimal::ZERO,
        };
        let max_combined = dec!(0.98);

        // Simulate ask-anchored ladders: YES near low ask, NO near high ask
        // In a one-sided market (e.g., BTC strongly down), NO ask ≈ 0.88, YES ask ≈ 0.13
        let mut yes_ladder = vec![
            LadderLevel {
                outcome: Outcome::Yes,
                price: dec!(0.12),
                size: dec!(15),
            },
            LadderLevel {
                outcome: Outcome::Yes,
                price: dec!(0.11),
                size: dec!(15),
            },
        ];
        let mut no_ladder = vec![
            LadderLevel {
                outcome: Outcome::No,
                price: dec!(0.87),
                size: dec!(15),
            },
            LadderLevel {
                outcome: Outcome::No,
                price: dec!(0.86),
                size: dec!(15),
            },
        ];

        apply_combined_cost_guard(
            &mut yes_ladder,
            &mut no_ladder,
            &position,
            max_combined,
            dec!(0.99),
            false,
        );

        // With max_combined = 0.98:
        // NO guard uses yes_top estimate (0.12): 0.87 + 0.12 = 0.99 > 0.98 → filtered
        // NO at 0.86: 0.86 + 0.12 = 0.98 → passes
        assert!(
            !no_ladder.is_empty(),
            "NO ladder should have surviving levels"
        );
        for level in &no_ladder {
            assert!(
                level.price + dec!(0.12) <= max_combined,
                "NO level {} + YES estimate 0.12 should be <= {}",
                level.price,
                max_combined
            );
        }
        assert!(
            no_ladder.iter().all(|l| l.price <= dec!(0.86)),
            "NO top should be trimmed to 0.86"
        );

        // YES guard uses no_top estimate (0.87): 0.12 + 0.87 = 0.99 > 0.98 → filtered
        // YES at 0.11: 0.11 + 0.87 = 0.98 → passes
        assert!(
            !yes_ladder.is_empty(),
            "YES ladder should have surviving levels"
        );
        for level in &yes_ladder {
            assert!(
                level.price + dec!(0.87) <= max_combined,
                "YES level {} + NO estimate 0.87 should be <= {}",
                level.price,
                max_combined
            );
        }
        assert!(
            yes_ladder.iter().all(|l| l.price <= dec!(0.11)),
            "YES top should be trimmed to 0.11"
        );
    }

    #[test]
    fn test_combined_cost_guard_light_side_capped() {
        // When imbalanced, the light side should be capped at light_side_max (0.99),
        // NOT $1.00. This prevents creating breakeven/losing pairs.
        let position = Position {
            condition_id: String::new(),
            yes_qty: dec!(20),         // heavy YES
            no_qty: dec!(5),           // light NO — we need more NO to complete pairs
            total_yes_spent: dec!(10), // avg_yes = 0.50
            total_no_spent: dec!(2.5), // avg_no = 0.50
        };
        let max_combined = dec!(0.98);
        let light_side_max = dec!(0.99);

        // YES ladder (heavy side): should use strict max_combined (0.98)
        let mut yes_ladder = vec![
            LadderLevel {
                outcome: Outcome::Yes,
                price: dec!(0.49),
                size: dec!(5),
            },
            LadderLevel {
                outcome: Outcome::Yes,
                price: dec!(0.48),
                size: dec!(5),
            },
        ];
        // NO ladder (light side): should use light_side_max (0.99), NOT $1.00
        let mut no_ladder = vec![
            LadderLevel {
                outcome: Outcome::No,
                price: dec!(0.50),
                size: dec!(5),
            }, // 0.50 + 0.50 = 1.00 > 0.99 → filtered
            LadderLevel {
                outcome: Outcome::No,
                price: dec!(0.49),
                size: dec!(5),
            }, // 0.49 + 0.50 = 0.99 → passes
            LadderLevel {
                outcome: Outcome::No,
                price: dec!(0.48),
                size: dec!(5),
            }, // 0.48 + 0.50 = 0.98 → passes
        ];

        apply_combined_cost_guard(
            &mut yes_ladder,
            &mut no_ladder,
            &position,
            max_combined,
            light_side_max,
            false,
        );

        // NO (light side): 0.50 should be filtered (0.50 + avg_yes 0.50 = 1.00 > 0.99)
        assert_eq!(
            no_ladder.len(),
            2,
            "NO@0.50 should be filtered by light_side_max"
        );
        assert!(
            no_ladder.iter().all(|l| l.price <= dec!(0.49)),
            "All NO levels should be <= 0.49"
        );

        // YES (heavy side): 0.49 should be filtered (0.49 + avg_no 0.50 = 0.99 > 0.98)
        assert_eq!(
            yes_ladder.len(),
            1,
            "YES@0.49 should be filtered by strict max_combined"
        );
        assert_eq!(yes_ladder[0].price, dec!(0.48));
    }

    #[test]
    fn test_combined_cost_guard_balanced_uses_strict() {
        // When balanced (|imbalance| <= 1), both sides should use strict max_combined,
        // NOT the relaxed light_side_max. This was a bug: balanced positions used $1.00.
        let position = Position {
            condition_id: String::new(),
            yes_qty: dec!(10),
            no_qty: dec!(10),         // balanced
            total_yes_spent: dec!(5), // avg_yes = 0.50
            total_no_spent: dec!(5),  // avg_no = 0.50
        };
        let max_combined = dec!(0.98);
        let light_side_max = dec!(0.99);

        let mut yes_ladder = vec![
            LadderLevel {
                outcome: Outcome::Yes,
                price: dec!(0.49),
                size: dec!(5),
            }, // 0.49 + 0.50 = 0.99 > 0.98
            LadderLevel {
                outcome: Outcome::Yes,
                price: dec!(0.48),
                size: dec!(5),
            }, // 0.48 + 0.50 = 0.98 → passes
        ];
        let mut no_ladder = vec![
            LadderLevel {
                outcome: Outcome::No,
                price: dec!(0.49),
                size: dec!(5),
            }, // 0.49 + 0.50 = 0.99 > 0.98
            LadderLevel {
                outcome: Outcome::No,
                price: dec!(0.48),
                size: dec!(5),
            }, // 0.48 + 0.50 = 0.98 → passes
        ];

        apply_combined_cost_guard(
            &mut yes_ladder,
            &mut no_ladder,
            &position,
            max_combined,
            light_side_max,
            false,
        );

        // Both sides should use strict max_combined (0.98)
        assert_eq!(
            yes_ladder.len(),
            1,
            "YES@0.49 should be filtered (balanced = strict)"
        );
        assert_eq!(
            no_ladder.len(),
            1,
            "NO@0.49 should be filtered (balanced = strict)"
        );
        assert_eq!(yes_ladder[0].price, dec!(0.48));
        assert_eq!(no_ladder[0].price, dec!(0.48));
    }

    #[test]
    fn test_combined_cost_guard_ev_recovery_uses_cap() {
        // In EV recovery mode, light side should use light_side_max (0.99),
        // NOT skip the guard entirely. Previously it skipped, allowing combined > $1.00.
        let position = Position {
            condition_id: String::new(),
            yes_qty: dec!(20), // heavy YES
            no_qty: dec!(5),
            total_yes_spent: dec!(10), // avg_yes = 0.50
            total_no_spent: dec!(2.5), // avg_no = 0.50
        };
        let max_combined = dec!(0.98);
        let light_side_max = dec!(0.99);

        let mut no_ladder = vec![
            LadderLevel {
                outcome: Outcome::No,
                price: dec!(0.52),
                size: dec!(5),
            }, // 0.52 + 0.50 = 1.02 > 0.99 → filtered
            LadderLevel {
                outcome: Outcome::No,
                price: dec!(0.49),
                size: dec!(5),
            }, // 0.49 + 0.50 = 0.99 → passes
        ];
        let mut yes_ladder = vec![LadderLevel {
            outcome: Outcome::Yes,
            price: dec!(0.48),
            size: dec!(5),
        }];

        apply_combined_cost_guard(
            &mut yes_ladder,
            &mut no_ladder,
            &position,
            max_combined,
            light_side_max,
            true, // ev_recovery = true
        );

        // NO (light side, EV recovery): 0.52 should still be filtered (0.52 + 0.50 = 1.02 > 0.99)
        assert_eq!(
            no_ladder.len(),
            1,
            "NO@0.52 must be filtered even in EV recovery mode"
        );
        assert_eq!(no_ladder[0].price, dec!(0.49));
    }

    #[test]
    fn test_combined_cost_guard_pair_completion_skips() {
        // Pair completion (one side has 0 shares) should skip the guard entirely.
        // Pairing at any cost is EV-neutral but variance-reducing vs holding one-sided.
        let position = Position {
            condition_id: String::new(),
            yes_qty: Decimal::ZERO, // needs pair completion
            no_qty: dec!(10),       // has 10 DOWN shares
            total_yes_spent: Decimal::ZERO,
            total_no_spent: dec!(4.2), // avg_no = 0.42
        };
        let max_combined = dec!(0.93);
        let light_side_max = dec!(1.05);

        let mut yes_ladder = vec![
            LadderLevel {
                outcome: Outcome::Yes,
                price: dec!(0.73),
                size: dec!(5),
            },
            LadderLevel {
                outcome: Outcome::Yes,
                price: dec!(0.60),
                size: dec!(5),
            },
            LadderLevel {
                outcome: Outcome::Yes,
                price: dec!(0.50),
                size: dec!(5),
            },
        ];
        let mut no_ladder = vec![LadderLevel {
            outcome: Outcome::No,
            price: dec!(0.48),
            size: dec!(5),
        }];

        apply_combined_cost_guard(
            &mut yes_ladder,
            &mut no_ladder,
            &position,
            max_combined,
            light_side_max,
            false,
        );

        // YES (pair completion): guard is skipped, all levels survive
        assert_eq!(
            yes_ladder.len(),
            3,
            "YES ladder should be fully intact in pair completion mode"
        );
    }

    #[test]
    fn test_balance_management_hard_block() {
        let v2 = V2Config::default();
        let tick = dec!(0.01);
        // Position: $30 YES cost, $0 NO cost → exceeds max_abs_imbalance of $15
        let position = Position {
            condition_id: String::new(),
            yes_qty: dec!(60),
            no_qty: Decimal::ZERO,
            total_yes_spent: dec!(30),
            total_no_spent: Decimal::ZERO,
        };
        let (mut yes, mut no) = compute_bid_ladder(0.5, 0.5, tick, &position, &v2, None);
        apply_balance_management(
            &mut yes,
            &mut no,
            &position,
            v2.max_abs_imbalance,
            v2.soft_imbalance_threshold,
        );
        // YES should be completely blocked
        assert!(yes.is_empty());
        // NO should still have levels
        assert!(!no.is_empty());
    }

    #[test]
    fn test_evaluate_directional_skew_terminal() {
        let v2 = V2Config {
            directional_skew_enabled: true,
            ..V2Config::default()
        };
        let snapshot = DirectionalSkewSnapshot {
            spot_ret_from_start_bps: 8.5,
            long_flow_up_notional: dec!(5200),
            short_flow_up_notional: dec!(1800),
            large_flow_up_notional: dec!(3400),
            up_best_imbalance: dec!(0.82),
            imbalance_diff: dec!(1.62),
        };

        let decision = evaluate_directional_skew(&v2, 12.0, snapshot)
            .expect("terminal-aligned snapshot should trigger skew");

        assert_eq!(decision.stage, DirectionalSkewStage::Terminal);
        assert_eq!(decision.favored_outcome, Outcome::Yes);
        assert_eq!(decision.favored_multiplier, dec!(1.75));
        assert_eq!(decision.unfavored_multiplier, dec!(0.25));
    }

    #[test]
    fn test_evaluate_directional_skew_requires_long_flow_alignment() {
        let v2 = V2Config {
            directional_skew_enabled: true,
            ..V2Config::default()
        };
        let snapshot = DirectionalSkewSnapshot {
            spot_ret_from_start_bps: 9.0,
            long_flow_up_notional: dec!(-5200),
            short_flow_up_notional: dec!(1800),
            large_flow_up_notional: dec!(3400),
            up_best_imbalance: dec!(0.82),
            imbalance_diff: dec!(1.62),
        };

        assert!(
            evaluate_directional_skew(&v2, 12.0, snapshot).is_none(),
            "opposed long-window flow should block skew"
        );
    }

    #[test]
    fn test_apply_directional_skew_terminal_keeps_top_unfavored() {
        let mut yes_ladder = vec![
            LadderLevel {
                outcome: Outcome::Yes,
                price: dec!(0.48),
                size: dec!(12),
            },
            LadderLevel {
                outcome: Outcome::Yes,
                price: dec!(0.47),
                size: dec!(10),
            },
        ];
        let mut no_ladder = vec![
            LadderLevel {
                outcome: Outcome::No,
                price: dec!(0.48),
                size: dec!(12),
            },
            LadderLevel {
                outcome: Outcome::No,
                price: dec!(0.47),
                size: dec!(10),
            },
        ];
        let decision = DirectionalSkewDecision {
            stage: DirectionalSkewStage::Terminal,
            favored_outcome: Outcome::Yes,
            favored_multiplier: dec!(1.75),
            unfavored_multiplier: dec!(0.25),
            cancel_deepest_unfavored: true,
        };

        apply_directional_skew_to_ladders(&mut yes_ladder, &mut no_ladder, decision);

        assert_eq!(yes_ladder.len(), 2);
        assert_eq!(yes_ladder[0].size, dec!(21));
        assert_eq!(no_ladder.len(), 1);
        assert_eq!(no_ladder[0].size, MIN_ORDER_SHARES);
    }

    #[test]
    fn test_diff_ladder_vs_resting() {
        let tick = dec!(0.01);
        let ladder = vec![
            LadderLevel {
                outcome: Outcome::Yes,
                price: dec!(0.48),
                size: dec!(10),
            },
            LadderLevel {
                outcome: Outcome::Yes,
                price: dec!(0.47),
                size: dec!(10),
            },
            LadderLevel {
                outcome: Outcome::Yes,
                price: dec!(0.46),
                size: dec!(10),
            },
        ];
        let mut resting = HashMap::new();
        // One matching, one stale (above ladder top), one slightly below
        resting.insert(
            (Outcome::Yes, dec!(0.48)),
            RestingLadderOrder {
                order_id: "a".into(),
                size: dec!(10),
                placed_at: Instant::now(),
            },
        );
        resting.insert(
            (Outcome::Yes, dec!(0.50)),
            RestingLadderOrder {
                order_id: "b".into(),
                size: dec!(10),
                placed_at: Instant::now(),
            },
        );
        let (to_place, to_cancel) =
            diff_ladder_vs_resting(&ladder, &resting, tick, 5, 15, dec!(0.15));
        // Should place 0.47 and 0.46 (missing from resting)
        assert_eq!(to_place.len(), 2);
        // Should cancel 0.50 (above ladder top)
        assert_eq!(to_cancel.len(), 1);
        assert_eq!(to_cancel[0], "b");
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn normal_cdf_range(x in -100.0_f64..100.0) {
            let result = normal_cdf(x);
            prop_assert!(result >= 0.0, "normal_cdf({}) = {} < 0", x, result);
            prop_assert!(result <= 1.0, "normal_cdf({}) = {} > 1", x, result);
        }

        #[test]
        fn normal_cdf_monotonic(x in -10.0_f64..10.0) {
            let eps = 0.001;
            let left = normal_cdf(x);
            let right = normal_cdf(x + eps);
            prop_assert!(right >= left - 1e-10,
                "normal_cdf not monotonic at x={}: {} > {}", x, left, right);
        }

        #[test]
        fn fair_value_up_range(
            open in 50_000.0_f64..150_000.0,
            current_pct in 0.95_f64..1.05,
            sigma in 0.0001_f64..0.01,
            remaining in 1.0_f64..900.0,
        ) {
            let current = open * current_pct;
            let fv = fair_value_up(open, current, sigma, remaining);
            prop_assert!(fv >= 0.02, "fv_up({},{},{},{}) = {} < 0.02",
                open, current, sigma, remaining, fv);
            prop_assert!(fv <= 0.98, "fv_up({},{},{},{}) = {} > 0.98",
                open, current, sigma, remaining, fv);
        }

        #[test]
        fn fair_value_up_direction(
            open in 80_000.0_f64..120_000.0,
            sigma in 0.0001_f64..0.005,
            remaining in 60.0_f64..600.0,
        ) {
            let fv_up_price = fair_value_up(open, open * 1.01, sigma, remaining);
            let fv_down_price = fair_value_up(open, open * 0.99, sigma, remaining);
            prop_assert!(fv_up_price > fv_down_price,
                "BTC up should have higher fv_up: {} vs {}", fv_up_price, fv_down_price);
        }
    }
}
