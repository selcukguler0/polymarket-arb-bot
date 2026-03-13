use rust_decimal::Decimal;
use serde::Deserialize;
use std::path::Path;
use std::str::FromStr;

use crate::error::{BotError, Result};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub general: GeneralConfig,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    pub timing: TimingConfig,
    pub alerting: AlertingConfig,
    /// V2-specific configuration (optional, only present in v2.toml)
    pub v2: Option<V2RawConfig>,
    /// On-chain contract addresses (defaults to current Polygon mainnet)
    #[serde(default)]
    pub onchain: OnchainConfig,
    /// Per-asset overrides (optional)
    pub btc: Option<AssetRawConfig>,
    pub eth: Option<AssetRawConfig>,
    pub sol: Option<AssetRawConfig>,
    pub xrp: Option<AssetRawConfig>,
}

/// Per-asset configuration overrides. Fields that are `Some` override the
/// corresponding V2RawConfig defaults for that asset.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AssetRawConfig {
    pub enabled: Option<bool>,
    pub budget: Option<String>,
    pub base_order_shares: Option<String>,
    pub max_sigma: Option<f64>,
    pub max_share_imbalance: Option<String>,
    pub imbalance_decay_floor_abs: Option<String>,
    pub one_sided_threshold: Option<String>,
    pub target_combined: Option<String>,
    pub ladder_levels: Option<u32>,
    pub ladder_levels_5m: Option<u32>,
    pub ladder_levels_15m: Option<u32>,
}

/// On-chain contract addresses configuration.
/// Defaults to current Polygon mainnet addresses (USDC.e era).
/// Update these when USDC.e migrates to native USDC.
#[derive(Debug, Clone, Deserialize)]
pub struct OnchainConfig {
    #[serde(default = "default_usdc_address")]
    pub usdc_address: String,
    #[serde(default = "default_ctf_exchange_address")]
    pub ctf_exchange_address: String,
    #[serde(default = "default_neg_risk_ctf_exchange_address")]
    pub neg_risk_ctf_exchange_address: String,
}

fn default_usdc_address() -> String {
    "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".to_string()
}

fn default_ctf_exchange_address() -> String {
    "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E".to_string()
}

fn default_neg_risk_ctf_exchange_address() -> String {
    "0xC5d563A36AE78145C45a50134d48A1215220f80a".to_string()
}

impl Default for OnchainConfig {
    fn default() -> Self {
        Self {
            usdc_address: default_usdc_address(),
            ctf_exchange_address: default_ctf_exchange_address(),
            neg_risk_ctf_exchange_address: default_neg_risk_ctf_exchange_address(),
        }
    }
}

/// Raw (string-based) V2 config section from TOML.
#[derive(Debug, Clone, Deserialize)]
pub struct V2RawConfig {
    pub target_combined: Option<String>,
    pub min_bid: Option<String>,
    pub max_combined_avg_cost: Option<String>,
    pub max_share_imbalance: Option<String>,
    pub soft_imbalance_threshold: Option<String>,
    pub imbalance_skew_per_share: Option<String>,
    pub min_vol_per_sec: Option<String>,
    pub vol_window_secs: Option<u64>,
    pub sellback_edge: Option<String>,
    pub sellback_min_excess: Option<String>,
    pub base_order_shares: Option<String>,
    pub ladder_levels: Option<u32>,
    pub ladder_size_decay: Option<String>,
    pub quote_refresh_ms: Option<u64>,
    // V2 improvements
    pub ev_circuit_breaker_enabled: Option<bool>,
    pub ev_stop_buying_ratio: Option<String>,
    pub ev_min_excess_threshold: Option<String>,
    pub fv_dead_threshold: Option<String>,
    pub min_bid_absolute_floor: Option<String>,
    pub min_bid_fv_ratio: Option<String>,
    pub sell_level_size: Option<String>,
    pub sell_levels: Option<u32>,
    pub imbalance_decay_floor_abs: Option<String>,
    pub imbalance_decay_floor_soft: Option<String>,
    pub very_late_phase_secs: Option<u64>,
    // Anti-oscillation
    pub postonly_regen_buffer_ticks: Option<u32>,
    pub sell_buy_cooldown_secs: Option<u64>,
    // Sell-back grace period
    pub sellback_grace_period_secs: Option<u64>,
    // Locked profit protection
    pub sellback_protect_locked_profit: Option<bool>,
    // Maximum loss per share on sell-back (e.g. "0.02" = 2c below avg cost)
    pub sellback_max_loss_cents: Option<String>,
    // Exit policy (maker-first, escalate to taker under sustained risk/time pressure)
    pub exit_soft_excess: Option<String>,
    pub exit_hard_excess: Option<String>,
    pub exit_taker_after_secs: Option<u64>,
    pub exit_force_taker_remaining_secs: Option<u64>,
    // EV breaker log rate-limit
    pub ev_log_cooldown_secs: Option<u64>,
    // Time-adaptive EV breaker: scale up thresholds early in period
    pub ev_early_period_multiplier: Option<String>,
    pub ev_early_period_end_pct: Option<f64>,
    // Trending market detection
    pub trend_window_secs: Option<u64>,
    pub trend_threshold_dollars: Option<f64>,
    /// Duration-aware trend thresholds (Phase 1): override trend_threshold_dollars per duration.
    pub trend_threshold_5m: Option<f64>,
    pub trend_threshold_15m: Option<f64>,
    pub trend_threshold_60m: Option<f64>,
    // FV-stale cancel
    pub fv_stale_cancel_cents: Option<String>,
    // Sigma blending
    pub sigma_blend_alpha: Option<String>,
    // Volatility circuit breaker
    pub max_sigma: Option<f64>,
    pub max_sigma_resume_factor: Option<f64>,
    // Fee-aware pair completion
    pub pair_fee_buffer: Option<String>,
    // Ladder churn reduction
    pub ladder_reprice_threshold: Option<String>,
    // One-sided position guard
    pub one_sided_threshold: Option<String>,
    // Hybrid FV (book midpoint blend)
    pub fv_book_blend_weight: Option<String>,
    // Sigma warm-up guard
    pub min_sigma_samples: Option<u32>,
    // Late-entry guard
    pub min_period_remaining_pct: Option<f64>,
    // Trading window: observation → active → wind-down phases
    pub trading_window_start_pct: Option<f64>,
    pub trading_window_end_pct: Option<f64>,
    pub wind_down_allow_pair_completion: Option<bool>,
    // Market duration filter: which durations to trade (minutes)
    pub allowed_durations: Option<Vec<u32>>,
    // Pair completion retry guard
    pub pair_completion_retry_secs: Option<u64>,
    pub pair_completion_max_attempts: Option<u32>,
    /// Merge complete YES+NO pairs on-chain during the Closing phase (requires eoa_mode).
    pub merge_at_closing: Option<bool>,
    // ── Dynamic Rebalancing ──
    /// Allow extra budget for light-side buying when imbalanced (pair completion).
    pub rebalance_budget_override: Option<bool>,
    /// Maximum extra USDC budget for rebalance buying.
    pub rebalance_max_extra_budget: Option<String>,
    /// Multiply light-side order sizes when imbalanced (1 = no multiplier).
    pub rebalance_size_multiplier: Option<u32>,
    // ── Continuous Merge ──
    /// Merge completed pairs mid-period to free USDC for more pair accumulation.
    pub continuous_merge_enabled: Option<bool>,
    /// Minimum seconds between merge attempts.
    pub merge_interval_secs: Option<u64>,
    /// Minimum complete pairs before merging.
    pub merge_min_pairs: Option<u32>,
    /// Reserve this many pairs (don't merge them) for resolution hedge.
    pub merge_reserve_pairs: Option<u32>,
    /// Minimum seconds after market discovery before placing orders.
    pub market_warmup_secs: Option<u64>,
    /// Override warmup specifically for 5-minute markets.
    pub market_warmup_secs_5m: Option<u64>,
    /// Maximum orders per minute before rate-limit circuit breaker trips.
    pub max_orders_per_minute: Option<u32>,
    /// Path to external kill file — if this file exists, bot triggers emergency.
    pub kill_file_path: Option<String>,
    /// Asset-local auto guard: suppress new bid placement when recent rolling
    /// performance degrades (e.g., sustained low fill rate / negative PnL).
    pub asset_guard_enabled: Option<bool>,
    pub asset_guard_window_periods: Option<u32>,
    pub asset_guard_min_fill_rate: Option<f64>,
    pub asset_guard_min_rolling_pnl: Option<String>,
    pub asset_guard_pause_secs: Option<u64>,
    /// Cancel-churn breaker: reduce aggressiveness when cancel ratio spikes.
    pub churn_breaker_enabled: Option<bool>,
    pub churn_breaker_min_orders: Option<u32>,
    pub churn_breaker_cancel_ratio: Option<f64>,
    pub churn_breaker_reprice_multiplier: Option<u32>,
    pub churn_breaker_keep_levels: Option<u32>,
    /// Cooldown between emergency sell placements on the same market.
    pub emergency_sell_cooldown_secs: Option<u64>,
    /// Light-side combined cost guard: max combined cost for the side we need
    /// to complete pairs (default: 0.99 — ensures at least 1c/pair profit).
    pub light_side_max_combined: Option<String>,
    /// Minimum profit per pair before allowing a merge (default: 0.0 = break-even).
    /// Skip merge if `1.0 - avg_combined_cost < this`.
    pub merge_min_profit_per_pair: Option<String>,
    // ── Period-level risk caps (Phase 2) ──
    /// Max buy commitment per period: gross buy fills + resting buy notional.
    pub period_gross_buy_cap_usdc: Option<String>,
    /// Fraction of period considered "early" for burst cap (0.0-1.0).
    pub early_phase_pct: Option<f64>,
    /// Max buy commitment during the early phase.
    pub early_phase_gross_buy_cap_usdc: Option<String>,
    /// Hard cap on worst-case terminal loss for the current period.
    pub period_worst_case_loss_cap_usdc: Option<String>,
    /// Per-buy-order notional cap (applies to maker buys and pair-completion buys).
    pub single_order_notional_cap_usdc: Option<String>,
    /// Minimum paired shares before period pair-quality hysteresis activates.
    pub period_pair_quality_min_pairs: Option<String>,
    /// Activate pair-quality block when avg combined cost reaches/exceeds this.
    pub period_pair_quality_max_combined: Option<String>,
    /// Clear pair-quality block when avg combined cost falls back below this.
    pub period_pair_quality_resume_combined: Option<String>,
    /// Minimum total position shares before pair-ratio guard evaluates.
    pub pair_ratio_eval_min_total_shares: Option<String>,
    /// After early phase, block heavy-side adds when pair_ratio falls below this.
    pub period_min_pair_ratio_for_heavy_add: Option<f64>,
    // ── Phase 1: Post-Anchoring Inventory Skew ──
    pub post_anchor_skew_enabled: Option<bool>,
    pub skew_activation_threshold: Option<String>,
    pub shares_per_skew_tick: Option<String>,
    pub max_skew_ticks: Option<u32>,
    // ── Phase 1: Price-Shock Fast-Path ──
    pub price_shock_threshold_5m: Option<f64>,
    pub price_shock_threshold_15m: Option<f64>,
    pub price_shock_threshold_60m: Option<f64>,
    pub price_shock_use_cancel_all: Option<bool>,
    // ── Phase 1: Duration-Aware Ladder Levels ──
    pub ladder_levels_5m: Option<u32>,
    pub ladder_levels_15m: Option<u32>,
    pub ladder_levels_60m: Option<u32>,
    /// Max new/updated buy levels to activate per side per cycle for 5m markets.
    pub buy_level_activation_limit_5m: Option<u32>,
    /// Re-anchor 5m buy ladders directly to best bid instead of ask-buffer logic.
    pub best_bid_anchor_5m: Option<bool>,
    // ── Late-Phase Directional Skew ──
    pub directional_skew_enabled: Option<bool>,
    pub directional_skew_mild_start_secs: Option<u64>,
    pub directional_skew_strong_start_secs: Option<u64>,
    pub directional_skew_terminal_start_secs: Option<u64>,
    pub directional_skew_flow_window_secs: Option<u64>,
    pub directional_skew_short_flow_window_secs: Option<u64>,
    pub directional_skew_spot_ret_threshold_bps: Option<f64>,
    pub directional_skew_flow_threshold_usdc: Option<String>,
    pub directional_skew_large_trade_min_usdc: Option<String>,
    pub directional_skew_large_flow_threshold_usdc: Option<String>,
    pub directional_skew_short_flow_threshold_usdc: Option<String>,
    pub directional_skew_terminal_imbalance_diff_threshold: Option<String>,
    pub directional_skew_terminal_best_imbalance_threshold: Option<String>,
    pub directional_skew_mild_favored_multiplier: Option<String>,
    pub directional_skew_mild_unfavored_multiplier: Option<String>,
    pub directional_skew_strong_favored_multiplier: Option<String>,
    pub directional_skew_strong_unfavored_multiplier: Option<String>,
    pub directional_skew_terminal_favored_multiplier: Option<String>,
    pub directional_skew_terminal_unfavored_multiplier: Option<String>,
    pub directional_skew_terminal_cancel_deepest_unfavored: Option<bool>,
    // ── VPIN Toxic Flow Detection ──
    pub vpin_enabled: Option<bool>,
    pub vpin_bucket_volume: Option<f64>,
    pub vpin_n_buckets: Option<usize>,
    pub vpin_widen_threshold: Option<f64>,
    pub vpin_pullback_threshold: Option<f64>,
    pub vpin_max_spread_multiplier: Option<f64>,
    // ── Avellaneda-Stoikov Volatility-Scaled Skew ──
    pub as_skew_enabled: Option<bool>,
    pub as_gamma: Option<f64>,
    // ── Continuous Liquidity Tapering ──
    pub taper_enabled: Option<bool>,
    pub taper_min_factor: Option<f64>,
    // ── Randomized Skew Noise ──
    pub skew_noise_enabled: Option<bool>,
    pub skew_noise_amplitude: Option<f64>,
    // ── Deep Discount Ladder ──
    pub deep_ladder_tick_spacing: Option<u32>,
    pub deep_ladder_start_level: Option<u32>,
    // ── Sell Unmatched at Period End ──
    pub sell_unmatched_enabled: Option<bool>,
    pub sell_unmatched_min_excess: Option<String>,
    pub sell_unmatched_max_loss: Option<String>,
    // ── Static Deep Grid ──
    pub deep_static_grid_enabled: Option<bool>,
    pub deep_static_levels: Option<Vec<f64>>,
    pub deep_static_size_below_05: Option<String>,
    pub deep_static_size_above_05: Option<String>,
    pub deep_static_max_combined: Option<String>,
    // ── Cancel Protection for Deep Levels ──
    pub fv_cancel_min_price: Option<String>,
    pub deep_level_stale_distance: Option<u32>,
    // ── 5-Minute Market Feature Flag ──
    pub enable_5m_markets: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeneralConfig {
    pub mode: String,
    #[serde(default = "default_db_path")]
    pub db_path: String,
    /// When true, use EOA (type 0) signing instead of Proxy (type 1).
    /// EOA mode makes the EOA both signer and fund holder, enabling on-chain merge/split/redeem.
    pub eoa_mode: Option<bool>,
    /// Canary mode: start with reduced budget, auto-escalate after N successful periods.
    #[serde(default)]
    pub canary_mode: Option<bool>,
    /// Budget override for canary mode (USDC, e.g. "10").
    #[serde(default)]
    pub canary_budget: Option<String>,
    /// Number of successful periods before auto-escalating from canary to full budget.
    #[serde(default = "default_canary_periods")]
    pub canary_periods: u32,
}

fn default_db_path() -> String {
    "data/polymarket-arb.db".to_string()
}

fn default_canary_periods() -> u32 {
    5
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    pub min_profit_threshold: String,
    pub max_combined_bid: String,
    pub order_size_usdc: String,
    pub max_orders_per_side: u32,
    pub ticks_below_mid: u32,
    pub aggressiveness: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    pub max_position_per_market: String,
    pub max_total_exposure: String,
    pub max_imbalance_ratio: String,
    pub pause_imbalance_ratio: String,
    pub emergency_imbalance_ratio: String,
    pub daily_loss_limit: String,
    #[serde(default)]
    pub session_loss_limit: Option<String>,
    pub resolution_safety_margin_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TimingConfig {
    pub max_book_age_ms: u64,
    pub quote_refresh_ms: u64,
    pub health_check_interval_secs: u64,
    #[serde(default = "default_discovery_interval")]
    pub market_discovery_interval_secs: u64,
}

fn default_discovery_interval() -> u64 {
    30
}

#[derive(Debug, Clone, Deserialize)]
pub struct AlertingConfig {
    pub enabled: bool,
    pub max_alerts_per_5min: u32,
}

/// Parsed and validated config with Decimal types
#[derive(Debug, Clone)]
pub struct ValidatedConfig {
    pub mode: TradingMode,
    pub db_path: String,
    pub eoa_mode: bool,
    pub min_profit_threshold: Decimal,
    pub max_combined_bid: Decimal,
    pub order_size_usdc: Decimal,
    pub max_orders_per_side: u32,
    pub ticks_below_mid: u32,
    pub aggressiveness: Decimal,
    pub max_position_per_market: Decimal,
    pub max_total_exposure: Decimal,
    pub max_imbalance_ratio: Decimal,
    pub pause_imbalance_ratio: Decimal,
    pub emergency_imbalance_ratio: Decimal,
    pub daily_loss_limit: Decimal,
    pub session_loss_limit: Option<Decimal>,
    pub resolution_safety_margin_secs: u64,
    pub max_book_age_ms: u64,
    pub quote_refresh_ms: u64,
    pub health_check_interval_secs: u64,
    pub market_discovery_interval_secs: u64,
    pub alerting_enabled: bool,
    pub max_alerts_per_5min: u32,
    // On-chain contract addresses
    pub usdc_address: String,
    pub ctf_exchange_address: String,
    pub neg_risk_ctf_exchange_address: String,
    // Canary deployment
    pub canary_mode: bool,
    pub canary_budget: Option<Decimal>,
    pub canary_periods: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingMode {
    Paper,
    Live,
    Shadow,
}

impl TradingMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Paper => "paper",
            Self::Live => "live",
            Self::Shadow => "shadow",
        }
    }

    pub fn is_live(self) -> bool {
        matches!(self, Self::Live)
    }

    pub fn uses_paper_sim(self) -> bool {
        matches!(self, Self::Paper)
    }

    pub fn starts_with_clean_positions(self) -> bool {
        matches!(self, Self::Paper | Self::Shadow)
    }

    pub fn artifact_root(self) -> &'static str {
        match self {
            Self::Live => "logs",
            Self::Paper => "logs_paper",
            Self::Shadow => "logs_shadow",
        }
    }
}

/// Environment secrets loaded from .env
#[derive(Debug, Clone)]
pub struct Secrets {
    pub private_key: String,
    pub wallet_address: String,
    pub polygon_rpc_url: String,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    /// Builder API credentials for order attribution + weekly USDC rewards.
    pub builder_key: String,
    pub builder_secret: String,
    pub builder_passphrase: String,
}

impl Secrets {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        let private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
            .map_err(|_| BotError::Config("POLYMARKET_PRIVATE_KEY not set".into()))?;

        let wallet_address = std::env::var("WALLET_ADDRESS")
            .map_err(|_| BotError::Config("WALLET_ADDRESS not set".into()))?;

        let polygon_rpc_url = std::env::var("POLYGON_RPC_URL")
            .map_err(|_| BotError::Config("POLYGON_RPC_URL not set".into()))?;

        let telegram_bot_token = std::env::var("TELEGRAM_BOT_TOKEN")
            .ok()
            .filter(|s| !s.is_empty());
        let telegram_chat_id = std::env::var("TELEGRAM_CHAT_ID")
            .ok()
            .filter(|s| !s.is_empty());

        let builder_key = std::env::var("POLY_BUILDER_KEY")
            .map_err(|_| BotError::Config("POLY_BUILDER_KEY not set".into()))?;
        let builder_secret = std::env::var("POLY_BUILDER_SECRET")
            .map_err(|_| BotError::Config("POLY_BUILDER_SECRET not set".into()))?;
        let builder_passphrase = std::env::var("POLY_BUILDER_PASSPHRASE")
            .map_err(|_| BotError::Config("POLY_BUILDER_PASSPHRASE not set".into()))?;

        Ok(Self {
            private_key,
            wallet_address,
            polygon_rpc_url,
            telegram_bot_token,
            telegram_chat_id,
            builder_key,
            builder_secret,
            builder_passphrase,
        })
    }
}

fn parse_decimal(name: &str, value: &str) -> Result<Decimal> {
    Decimal::from_str(value)
        .map_err(|e| BotError::Config(format!("{name}: invalid decimal '{value}': {e}")))
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| BotError::Config(format!("Failed to read config: {e}")))?;
        toml::from_str(&content)
            .map_err(|e| BotError::Config(format!("Failed to parse config: {e}")))
    }

    pub fn validate(self) -> Result<ValidatedConfig> {
        let mode = match self.general.mode.as_str() {
            "paper" => TradingMode::Paper,
            "live" => TradingMode::Live,
            "shadow" => TradingMode::Shadow,
            other => {
                return Err(BotError::Config(format!(
                    "Invalid mode '{other}', must be 'paper', 'live', or 'shadow'"
                )))
            }
        };

        let min_profit_threshold =
            parse_decimal("min_profit_threshold", &self.strategy.min_profit_threshold)?;
        let max_combined_bid = parse_decimal("max_combined_bid", &self.strategy.max_combined_bid)?;
        let order_size_usdc = parse_decimal("order_size_usdc", &self.strategy.order_size_usdc)?;
        let aggressiveness = parse_decimal("aggressiveness", &self.strategy.aggressiveness)?;

        let max_position_per_market = parse_decimal(
            "max_position_per_market",
            &self.risk.max_position_per_market,
        )?;
        let max_total_exposure =
            parse_decimal("max_total_exposure", &self.risk.max_total_exposure)?;
        let max_imbalance_ratio =
            parse_decimal("max_imbalance_ratio", &self.risk.max_imbalance_ratio)?;
        let pause_imbalance_ratio =
            parse_decimal("pause_imbalance_ratio", &self.risk.pause_imbalance_ratio)?;
        let emergency_imbalance_ratio = parse_decimal(
            "emergency_imbalance_ratio",
            &self.risk.emergency_imbalance_ratio,
        )?;
        let daily_loss_limit = parse_decimal("daily_loss_limit", &self.risk.daily_loss_limit)?;
        let session_loss_limit = self
            .risk
            .session_loss_limit
            .as_deref()
            .map(|value| parse_decimal("session_loss_limit", value))
            .transpose()?;

        // Validation rules
        if min_profit_threshold <= Decimal::ZERO {
            return Err(BotError::Config("min_profit_threshold must be > 0".into()));
        }
        if max_combined_bid >= Decimal::ONE {
            return Err(BotError::Config("max_combined_bid must be < 1.0".into()));
        }
        let only_5m_markets = self
            .v2
            .as_ref()
            .and_then(|v2| v2.allowed_durations.as_ref())
            .map(|durations| !durations.is_empty() && durations.iter().all(|d| *d <= 5))
            .unwrap_or(false);
        let min_resolution_safety_margin_secs = if only_5m_markets {
            15
        } else if mode.is_live() {
            120
        } else {
            20
        };
        if self.risk.resolution_safety_margin_secs < min_resolution_safety_margin_secs {
            return Err(BotError::Config(format!(
                "resolution_safety_margin_secs must be >= {} in {} mode",
                min_resolution_safety_margin_secs,
                mode.as_str()
            )));
        }
        if !(max_imbalance_ratio < pause_imbalance_ratio
            && pause_imbalance_ratio < emergency_imbalance_ratio)
        {
            return Err(BotError::Config(
                "Risk thresholds must be ordered: max < pause < emergency".into(),
            ));
        }
        if aggressiveness < Decimal::ZERO || aggressiveness > Decimal::ONE {
            return Err(BotError::Config(
                "aggressiveness must be between 0.0 and 1.0".into(),
            ));
        }

        Ok(ValidatedConfig {
            mode,
            db_path: self.general.db_path,
            eoa_mode: self.general.eoa_mode.unwrap_or(false),
            min_profit_threshold,
            max_combined_bid,
            order_size_usdc,
            max_orders_per_side: self.strategy.max_orders_per_side,
            ticks_below_mid: self.strategy.ticks_below_mid,
            aggressiveness,
            max_position_per_market,
            max_total_exposure,
            max_imbalance_ratio,
            pause_imbalance_ratio,
            emergency_imbalance_ratio,
            daily_loss_limit,
            session_loss_limit,
            resolution_safety_margin_secs: self.risk.resolution_safety_margin_secs,
            max_book_age_ms: self.timing.max_book_age_ms,
            quote_refresh_ms: self.timing.quote_refresh_ms,
            health_check_interval_secs: self.timing.health_check_interval_secs,
            market_discovery_interval_secs: self.timing.market_discovery_interval_secs,
            alerting_enabled: self.alerting.enabled,
            max_alerts_per_5min: self.alerting.max_alerts_per_5min,
            usdc_address: self.onchain.usdc_address,
            ctf_exchange_address: self.onchain.ctf_exchange_address,
            neg_risk_ctf_exchange_address: self.onchain.neg_risk_ctf_exchange_address,
            canary_mode: self.general.canary_mode.unwrap_or(false),
            canary_budget: self
                .general
                .canary_budget
                .as_deref()
                .map(|s| parse_decimal("canary_budget", s))
                .transpose()?,
            canary_periods: self.general.canary_periods,
        })
    }
}
