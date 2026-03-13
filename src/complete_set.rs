use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::U256;
use anyhow::Context;
use chrono::{DateTime, Utc};
use polymarket_client_sdk::auth::state::Unauthenticated;
use polymarket_client_sdk::clob::types::request::OrderBookSummaryRequest;
use polymarket_client_sdk::clob::types::response::OrderBookSummaryResponse;
use polymarket_client_sdk::clob::types::Side;
use polymarket_client_sdk::clob::{self};
use polymarket_client_sdk::gamma;
use polymarket_client_sdk::gamma::types::response::Event;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::{info, warn};

use crate::config::{OnchainConfig, Secrets, TradingMode};
use crate::error::{BotError, Result};
use crate::run_manifest::{CompleteSetRunProfile, RunManifest};
use crate::sdk::{MarketOrderResult, MarketOrderSpec, SdkClients};
use crate::types::{Asset, TrackedMarket};

#[derive(Debug, Clone, Deserialize)]
pub struct CompleteSetAppConfig {
    pub general: CompleteSetGeneralConfig,
    pub complete_set: CompleteSetRawConfig,
    #[serde(default)]
    pub onchain: OnchainConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompleteSetGeneralConfig {
    pub mode: String,
    #[serde(default)]
    pub eoa_mode: Option<bool>,
    #[serde(default)]
    pub wallet_address: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompleteSetRawConfig {
    pub asset: String,
    pub poll_interval_ms: u64,
    pub market_discovery_interval_secs: u64,
    pub trading_window_start_pct: f64,
    pub trading_window_end_pct: f64,
    pub allowed_durations: Vec<u32>,
    pub max_long_combined_ask: String,
    pub min_short_combined_bid: String,
    pub fee_buffer: String,
    pub cooldown_secs: u64,
    pub pairs_per_trade: String,
    pub max_trades_per_period: u32,
    #[serde(default = "default_true")]
    pub long_enabled: bool,
    #[serde(default = "default_true")]
    pub short_enabled: bool,
    #[serde(default = "default_true")]
    pub merge_enabled: bool,
    #[serde(default = "default_true")]
    pub split_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct CompleteSetConfig {
    pub mode: TradingMode,
    pub eoa_mode: bool,
    pub wallet_address: Option<String>,
    pub onchain: OnchainConfig,
    pub asset: Asset,
    pub poll_interval_ms: u64,
    pub market_discovery_interval_secs: u64,
    pub trading_window_start_pct: f64,
    pub trading_window_end_pct: f64,
    pub allowed_durations: Vec<u32>,
    pub max_long_combined_ask: Decimal,
    pub min_short_combined_bid: Decimal,
    pub fee_buffer: Decimal,
    pub cooldown_secs: u64,
    pub pairs_per_trade: Decimal,
    pub max_trades_per_period: u32,
    pub long_enabled: bool,
    pub short_enabled: bool,
    pub merge_enabled: bool,
    pub split_enabled: bool,
}

impl CompleteSetAppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| BotError::Config(format!("Failed to read config: {e}")))?;
        toml::from_str(&content)
            .map_err(|e| BotError::Config(format!("Failed to parse config: {e}")))
    }

    pub fn validate(self) -> Result<CompleteSetConfig> {
        let mode = match self.general.mode.as_str() {
            "paper" => TradingMode::Paper,
            "live" => TradingMode::Live,
            "shadow" => TradingMode::Shadow,
            other => {
                return Err(BotError::Config(format!(
                    "Invalid mode '{other}', must be 'paper', 'live', or 'shadow'"
                )));
            }
        };
        let asset = match self.complete_set.asset.to_uppercase().as_str() {
            "BTC" => Asset::BTC,
            "ETH" => Asset::ETH,
            "SOL" => Asset::SOL,
            "XRP" => Asset::XRP,
            other => {
                return Err(BotError::Config(format!(
                    "Unsupported asset '{other}' for complete-set config"
                )));
            }
        };

        let max_long_combined_ask = Decimal::from_str(&self.complete_set.max_long_combined_ask)
            .map_err(|e| BotError::Config(format!("max_long_combined_ask invalid: {e}")))?;
        let min_short_combined_bid =
            Decimal::from_str(&self.complete_set.min_short_combined_bid)
                .map_err(|e| BotError::Config(format!("min_short_combined_bid invalid: {e}")))?;
        let fee_buffer = Decimal::from_str(&self.complete_set.fee_buffer)
            .map_err(|e| BotError::Config(format!("fee_buffer invalid: {e}")))?;
        let pairs_per_trade = Decimal::from_str(&self.complete_set.pairs_per_trade)
            .map_err(|e| BotError::Config(format!("pairs_per_trade invalid: {e}")))?;

        if self.complete_set.poll_interval_ms == 0 {
            return Err(BotError::Config("poll_interval_ms must be > 0".into()));
        }
        if !(0.0..1.0).contains(&self.complete_set.trading_window_start_pct) {
            return Err(BotError::Config(
                "trading_window_start_pct must be in [0, 1)".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.complete_set.trading_window_end_pct)
            || self.complete_set.trading_window_end_pct
                <= self.complete_set.trading_window_start_pct
        {
            return Err(BotError::Config(
                "trading_window_end_pct must be > start_pct and <= 1".into(),
            ));
        }
        if self.complete_set.allowed_durations.is_empty() {
            return Err(BotError::Config(
                "allowed_durations must contain at least one duration".into(),
            ));
        }
        if max_long_combined_ask >= Decimal::ONE {
            return Err(BotError::Config(
                "max_long_combined_ask must be < 1.0".into(),
            ));
        }
        if min_short_combined_bid <= Decimal::ONE {
            return Err(BotError::Config(
                "min_short_combined_bid must be > 1.0".into(),
            ));
        }
        if fee_buffer.is_sign_negative() {
            return Err(BotError::Config("fee_buffer must be >= 0".into()));
        }
        if pairs_per_trade <= Decimal::ZERO || !pairs_per_trade.fract().is_zero() {
            return Err(BotError::Config(
                "pairs_per_trade must be a positive whole number of shares".into(),
            ));
        }
        if self.complete_set.max_trades_per_period == 0 {
            return Err(BotError::Config("max_trades_per_period must be > 0".into()));
        }
        if !self.complete_set.long_enabled && !self.complete_set.short_enabled {
            return Err(BotError::Config(
                "At least one of long_enabled or short_enabled must be true".into(),
            ));
        }
        let eoa_mode = self.general.eoa_mode.unwrap_or(false);
        if mode.is_live() && !eoa_mode {
            return Err(BotError::Config(
                "complete-set live mode requires eoa_mode = true".into(),
            ));
        }
        if mode.is_live() && self.complete_set.long_enabled && !self.complete_set.merge_enabled {
            return Err(BotError::Config(
                "complete-set long live mode requires merge_enabled = true".into(),
            ));
        }
        if mode.is_live() && self.complete_set.short_enabled && !self.complete_set.split_enabled {
            return Err(BotError::Config(
                "complete-set short live mode requires split_enabled = true".into(),
            ));
        }

        Ok(CompleteSetConfig {
            mode,
            eoa_mode,
            wallet_address: self.general.wallet_address,
            onchain: self.onchain,
            asset,
            poll_interval_ms: self.complete_set.poll_interval_ms,
            market_discovery_interval_secs: self.complete_set.market_discovery_interval_secs,
            trading_window_start_pct: self.complete_set.trading_window_start_pct,
            trading_window_end_pct: self.complete_set.trading_window_end_pct,
            allowed_durations: self.complete_set.allowed_durations,
            max_long_combined_ask,
            min_short_combined_bid,
            fee_buffer,
            cooldown_secs: self.complete_set.cooldown_secs,
            pairs_per_trade,
            max_trades_per_period: self.complete_set.max_trades_per_period,
            long_enabled: self.complete_set.long_enabled,
            short_enabled: self.complete_set.short_enabled,
            merge_enabled: self.complete_set.merge_enabled,
            split_enabled: self.complete_set.split_enabled,
        })
    }
}

impl CompleteSetConfig {
    pub fn artifact_root(&self) -> String {
        let prefix = match self.mode {
            TradingMode::Live => "logs_complete_set",
            TradingMode::Paper => "logs_complete_set_paper",
            TradingMode::Shadow => "logs_complete_set_shadow",
        };
        format!(
            "{}_{}",
            prefix,
            self.asset.display_name().to_ascii_lowercase()
        )
    }

    fn run_profile(&self) -> CompleteSetRunProfile {
        CompleteSetRunProfile {
            asset: self.asset.display_name().to_string(),
            allowed_durations: self.allowed_durations.clone(),
            trading_window_start_pct: self.trading_window_start_pct,
            trading_window_end_pct: self.trading_window_end_pct,
            poll_interval_ms: self.poll_interval_ms,
            discovery_interval_secs: self.market_discovery_interval_secs,
            long_enabled: self.long_enabled,
            short_enabled: self.short_enabled,
            max_long_combined_ask: self.max_long_combined_ask.to_string(),
            min_short_combined_bid: self.min_short_combined_bid.to_string(),
            fee_buffer: self.fee_buffer.to_string(),
            cooldown_secs: self.cooldown_secs,
            pairs_per_trade: self.pairs_per_trade.to_string(),
            max_trades_per_period: self.max_trades_per_period,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum OpportunitySide {
    Long,
    Short,
}

impl OpportunitySide {
    fn as_str(self) -> &'static str {
        match self {
            Self::Long => "long_merge",
            Self::Short => "short_split_sell",
        }
    }
}

#[derive(Debug, Clone)]
struct Opportunity {
    side: OpportunitySide,
    combined_ask: Decimal,
    combined_bid: Decimal,
    edge_per_pair: Decimal,
}

#[derive(Debug, Clone)]
struct PeriodStats {
    period_name: String,
    condition_id: String,
    question: String,
    duration_mins: u32,
    trade_window_polls: u32,
    opportunities: u32,
    long_signals: u32,
    short_signals: u32,
    long_signal_polls: u32,
    short_signal_polls: u32,
    long_attempts: u32,
    short_attempts: u32,
    long_successes: u32,
    short_successes: u32,
    recoveries: u32,
    merge_pairs: Decimal,
    split_pairs: Decimal,
    expected_pnl: Decimal,
    trades_this_period: u32,
    last_trade_at: Option<DateTime<Utc>>,
    last_long_edge: Decimal,
    last_short_edge: Decimal,
    best_long_edge: Decimal,
    best_short_edge: Decimal,
    min_combined_ask: Option<Decimal>,
    max_combined_bid: Option<Decimal>,
    first_window_seen_at: Option<DateTime<Utc>>,
    last_window_seen_at: Option<DateTime<Utc>>,
}

impl PeriodStats {
    fn new(market: &TrackedMarket, duration_mins: u32) -> Self {
        Self {
            period_name: period_name_from_question(&market.question),
            condition_id: market.condition_id.clone(),
            question: market.question.clone(),
            duration_mins,
            trade_window_polls: 0,
            opportunities: 0,
            long_signals: 0,
            short_signals: 0,
            long_signal_polls: 0,
            short_signal_polls: 0,
            long_attempts: 0,
            short_attempts: 0,
            long_successes: 0,
            short_successes: 0,
            recoveries: 0,
            merge_pairs: Decimal::ZERO,
            split_pairs: Decimal::ZERO,
            expected_pnl: Decimal::ZERO,
            trades_this_period: 0,
            last_trade_at: None,
            last_long_edge: Decimal::ZERO,
            last_short_edge: Decimal::ZERO,
            best_long_edge: Decimal::ZERO,
            best_short_edge: Decimal::ZERO,
            min_combined_ask: None,
            max_combined_bid: None,
            first_window_seen_at: None,
            last_window_seen_at: None,
        }
    }
}

#[derive(Debug, Clone)]
struct ExecutionOutcome {
    status: String,
    detail: String,
    order_ids: Vec<String>,
    tx_hashes: Vec<String>,
    recovered: bool,
    success: bool,
    merge_pairs: Decimal,
    split_pairs: Decimal,
}

struct CompleteSetLogger {
    markets_file: File,
    scans_file: File,
    opportunities_file: File,
    executions_file: File,
    summary_file: File,
}

impl CompleteSetLogger {
    fn new<P: AsRef<Path>>(base_dir: P) -> anyhow::Result<Self> {
        fs::create_dir_all(base_dir.as_ref())?;
        let markets_path = base_dir.as_ref().join("markets.csv");
        let scans_path = base_dir.as_ref().join("window_scans.csv");
        let opportunities_path = base_dir.as_ref().join("opportunities.csv");
        let executions_path = base_dir.as_ref().join("executions.csv");
        let summary_path = base_dir.as_ref().join("session_summary.csv");

        let mut markets_file = open_csv(&markets_path)?;
        let mut scans_file = open_csv(&scans_path)?;
        let mut opportunities_file = open_csv(&opportunities_path)?;
        let mut executions_file = open_csv(&executions_path)?;
        let mut summary_file = open_csv(&summary_path)?;

        write_header_if_new(
            &markets_path,
            &mut markets_file,
            "timestamp,period_name,condition_id,question,duration_mins,start_date,end_date,tick_size,neg_risk",
        )?;
        write_header_if_new(
            &scans_path,
            &mut scans_file,
            "timestamp,period_name,condition_id,remaining_secs,elapsed_pct,combined_ask,combined_bid,long_edge,short_edge,long_signal,short_signal",
        )?;
        write_header_if_new(
            &opportunities_path,
            &mut opportunities_file,
            "timestamp,period_name,condition_id,side,decision,combined_ask,combined_bid,edge_per_pair,pairs,remaining_secs,elapsed_pct",
        )?;
        write_header_if_new(
            &executions_path,
            &mut executions_file,
            "timestamp,period_name,condition_id,side,status,detail,pairs,expected_pnl,order_ids,tx_hashes,recovered",
        )?;
        write_header_if_new(
            &summary_path,
            &mut summary_file,
            "period_name,condition_id,question,duration_mins,trade_window_polls,opportunities,long_signals,short_signals,long_signal_polls,short_signal_polls,long_attempts,short_attempts,long_successes,short_successes,recoveries,merge_pairs,split_pairs,expected_pnl,last_long_edge,last_short_edge,best_long_edge,best_short_edge,min_combined_ask,max_combined_bid,first_window_seen_at,last_window_seen_at,trades_this_period,finalized_at",
        )?;

        Ok(Self {
            markets_file,
            scans_file,
            opportunities_file,
            executions_file,
            summary_file,
        })
    }

    fn log_market(&mut self, market: &TrackedMarket, duration_mins: u32) {
        let now = Utc::now().to_rfc3339();
        let start = market
            .start_date
            .map(|d| d.to_rfc3339())
            .unwrap_or_default();
        let end = market.end_date.to_rfc3339();
        let _ = writeln!(
            self.markets_file,
            "{},{},{},{},{},{},{},{},{}",
            csv_field(&now),
            csv_field(&period_name_from_question(&market.question)),
            csv_field(&market.condition_id),
            csv_field(&market.question),
            duration_mins,
            csv_field(&start),
            csv_field(&end),
            market.tick_size,
            market.neg_risk,
        );
    }

    fn log_opportunity(
        &mut self,
        stats: &PeriodStats,
        opp: &Opportunity,
        decision: &str,
        pairs: Decimal,
        remaining_secs: f64,
        elapsed_pct: f64,
    ) {
        let _ = writeln!(
            self.opportunities_file,
            "{},{},{},{},{},{},{},{},{},{:.3},{:.4}",
            csv_field(&Utc::now().to_rfc3339()),
            csv_field(&stats.period_name),
            csv_field(&stats.condition_id),
            opp.side.as_str(),
            csv_field(decision),
            opp.combined_ask,
            opp.combined_bid,
            opp.edge_per_pair,
            pairs,
            remaining_secs,
            elapsed_pct,
        );
    }

    fn log_scan(
        &mut self,
        stats: &PeriodStats,
        remaining_secs: f64,
        elapsed_pct: f64,
        combined_ask: Decimal,
        combined_bid: Decimal,
        long_edge: Decimal,
        short_edge: Decimal,
        long_signal: bool,
        short_signal: bool,
    ) {
        let _ = writeln!(
            self.scans_file,
            "{},{},{},{:.3},{:.4},{},{},{},{},{},{}",
            csv_field(&Utc::now().to_rfc3339()),
            csv_field(&stats.period_name),
            csv_field(&stats.condition_id),
            remaining_secs,
            elapsed_pct,
            combined_ask,
            combined_bid,
            long_edge,
            short_edge,
            long_signal,
            short_signal,
        );
    }

    fn log_execution(
        &mut self,
        stats: &PeriodStats,
        side: OpportunitySide,
        pairs: Decimal,
        expected_pnl: Decimal,
        outcome: &ExecutionOutcome,
    ) {
        let order_ids = outcome.order_ids.join("|");
        let tx_hashes = outcome.tx_hashes.join("|");
        let _ = writeln!(
            self.executions_file,
            "{},{},{},{},{},{},{},{},{},{},{}",
            csv_field(&Utc::now().to_rfc3339()),
            csv_field(&stats.period_name),
            csv_field(&stats.condition_id),
            side.as_str(),
            csv_field(&outcome.status),
            csv_field(&outcome.detail),
            pairs,
            expected_pnl,
            csv_field(&order_ids),
            csv_field(&tx_hashes),
            outcome.recovered,
        );
    }

    fn log_summary(&mut self, stats: &PeriodStats) {
        if stats.trade_window_polls == 0 {
            return;
        }
        let _ = writeln!(
            self.summary_file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            csv_field(&stats.period_name),
            csv_field(&stats.condition_id),
            csv_field(&stats.question),
            stats.duration_mins,
            stats.trade_window_polls,
            stats.opportunities,
            stats.long_signals,
            stats.short_signals,
            stats.long_signal_polls,
            stats.short_signal_polls,
            stats.long_attempts,
            stats.short_attempts,
            stats.long_successes,
            stats.short_successes,
            stats.recoveries,
            stats.merge_pairs,
            stats.split_pairs,
            stats.expected_pnl,
            stats.last_long_edge,
            stats.last_short_edge,
            stats.best_long_edge,
            stats.best_short_edge,
            stats.min_combined_ask.unwrap_or(Decimal::ZERO),
            stats.max_combined_bid.unwrap_or(Decimal::ZERO),
            csv_field(
                &stats
                    .first_window_seen_at
                    .map(|ts| ts.to_rfc3339())
                    .unwrap_or_default(),
            ),
            csv_field(
                &stats
                    .last_window_seen_at
                    .map(|ts| ts.to_rfc3339())
                    .unwrap_or_default(),
            ),
            stats.trades_this_period,
            csv_field(&Utc::now().to_rfc3339()),
        );
    }
}

enum Clients {
    Live(Arc<SdkClients>),
    Public {
        gamma: gamma::Client,
        clob: clob::Client<Unauthenticated>,
    },
}

impl Clients {
    async fn discover_events(&self, tag: &str) -> Result<Vec<Event>> {
        match self {
            Self::Live(sdk) => sdk.discover_events(tag).await,
            Self::Public { gamma, .. } => {
                let now = Utc::now();
                let req = polymarket_client_sdk::gamma::types::request::EventsRequest::builder()
                    .limit(200)
                    .active(true)
                    .closed(false)
                    .end_date_min(now)
                    .tag_slug(tag.to_string())
                    .build();
                gamma
                    .events(&req)
                    .await
                    .map_err(|e| BotError::Sdk(format!("Gamma events query failed: {e}")))
            }
        }
    }

    async fn order_books(
        &self,
        requests: &[OrderBookSummaryRequest],
    ) -> Result<Vec<OrderBookSummaryResponse>> {
        match self {
            Self::Live(sdk) => sdk
                .clob
                .order_books(requests)
                .await
                .map_err(|e| BotError::Sdk(format!("Batch orderbook query failed: {e}"))),
            Self::Public { clob, .. } => clob
                .order_books(requests)
                .await
                .map_err(|e| BotError::Sdk(format!("Batch orderbook query failed: {e}"))),
        }
    }

    fn live_sdk(&self) -> Option<Arc<SdkClients>> {
        match self {
            Self::Live(sdk) => Some(sdk.clone()),
            Self::Public { .. } => None,
        }
    }
}

pub async fn run(config_path: &Path, runtime_limit: Option<Duration>) -> anyhow::Result<()> {
    install_tls()?;

    let raw = CompleteSetAppConfig::load(config_path)?;
    let config = raw.validate()?;
    let artifact_root = config.artifact_root();
    init_tracing(&artifact_root)?;

    info!(
        mode = config.mode.as_str(),
        asset = %config.asset,
        long_enabled = config.long_enabled,
        short_enabled = config.short_enabled,
        "Starting complete-set runner"
    );

    let clients = if config.mode.is_live() {
        let secrets = Secrets::from_env()?;
        let sdk = SdkClients::new(
            &secrets.private_key,
            &secrets.wallet_address,
            config.eoa_mode,
            &config.onchain.usdc_address,
            &secrets.builder_key,
            &secrets.builder_secret,
            &secrets.builder_passphrase,
        )
        .await?;
        Clients::Live(Arc::new(sdk))
    } else {
        Clients::Public {
            gamma: gamma::Client::default(),
            clob: clob::Client::default(),
        }
    };

    let wallet_address = if let Some(ref configured) = config.wallet_address {
        configured.clone()
    } else if config.mode.is_live() {
        std::env::var("WALLET_ADDRESS").unwrap_or_else(|_| "missing_wallet".to_string())
    } else {
        std::env::var("WALLET_ADDRESS").unwrap_or_else(|_| "shadow-observer".to_string())
    };

    let run_id = format!("{}_complete_set", Utc::now().format("%Y%m%dT%H%M%S%.3fZ"));
    let manifest = RunManifest::build_complete_set(
        run_id.clone(),
        config.mode,
        wallet_address,
        config_path,
        config.eoa_mode,
        config.run_profile(),
    )?;
    let manifest_path = manifest.persist(&artifact_root)?;
    info!(path = %manifest_path.display(), "Complete-set manifest written");

    let mut logger = CompleteSetLogger::new(&artifact_root)?;
    let mut active_markets: HashMap<String, TrackedMarket> = HashMap::new();
    let mut stats: HashMap<String, PeriodStats> = HashMap::new();
    let start = Instant::now();
    let mut next_discovery = Instant::now();

    loop {
        if let Some(limit) = runtime_limit {
            if start.elapsed() >= limit {
                info!("Runtime limit reached, stopping complete-set runner");
                break;
            }
        }

        if Instant::now() >= next_discovery {
            match clients.discover_events(config.asset.gamma_tag()).await {
                Ok(events) => {
                    let markets = filter_markets(config.asset, events, &config.allowed_durations);
                    let current_ids: HashSet<String> =
                        markets.iter().map(|m| m.condition_id.clone()).collect();

                    for market in markets {
                        let duration_mins =
                            market.duration_secs().unwrap_or(300).saturating_div(60) as u32;
                        active_markets
                            .entry(market.condition_id.clone())
                            .or_insert_with(|| {
                                logger.log_market(&market, duration_mins);
                                stats.insert(
                                    market.condition_id.clone(),
                                    PeriodStats::new(&market, duration_mins),
                                );
                                market
                            });
                    }

                    let stale: Vec<String> = active_markets
                        .keys()
                        .filter(|cid| !current_ids.contains(*cid))
                        .cloned()
                        .collect();
                    for cid in stale {
                        if let Some(period_stats) = stats.remove(&cid) {
                            logger.log_summary(&period_stats);
                        }
                        active_markets.remove(&cid);
                    }
                }
                Err(e) => warn!("Market discovery failed: {e}"),
            }

            next_discovery =
                Instant::now() + Duration::from_secs(config.market_discovery_interval_secs);
        }

        let now = Utc::now();
        let expired: Vec<String> = active_markets
            .iter()
            .filter(|(_, market)| market.end_date <= now)
            .map(|(cid, _)| cid.clone())
            .collect();
        for cid in expired {
            if let Some(period_stats) = stats.remove(&cid) {
                logger.log_summary(&period_stats);
            }
            active_markets.remove(&cid);
        }

        if active_markets.is_empty() {
            tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
            continue;
        }

        let requests = build_orderbook_requests(active_markets.values())?;
        let books = match clients.order_books(&requests).await {
            Ok(books) => books,
            Err(e) => {
                warn!("Orderbook fetch failed: {e}");
                tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
                continue;
            }
        };
        let book_map = map_books(books);

        for market in active_markets.values() {
            let Some(period_stats) = stats.get_mut(&market.condition_id) else {
                continue;
            };
            if let Err(e) = process_market(
                &config,
                &clients,
                &book_map,
                market,
                period_stats,
                &mut logger,
            )
            .await
            {
                warn!(condition_id = %market.condition_id, "Complete-set market processing failed: {e}");
            }
        }

        tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
    }

    for period_stats in stats.values() {
        logger.log_summary(period_stats);
    }

    Ok(())
}

pub async fn capture_live_snapshot(config_path: &Path) -> anyhow::Result<PathBuf> {
    install_tls()?;
    let raw = CompleteSetAppConfig::load(config_path)?;
    let config = raw.validate()?;
    let gamma = gamma::Client::default();
    let clob = clob::Client::default();

    let now = Utc::now();
    let req = polymarket_client_sdk::gamma::types::request::EventsRequest::builder()
        .limit(200)
        .active(true)
        .closed(false)
        .end_date_min(now)
        .tag_slug(config.asset.gamma_tag().to_string())
        .build();
    let events = gamma
        .events(&req)
        .await
        .map_err(|e| anyhow::anyhow!("Gamma snapshot query failed: {e}"))?;
    let markets = filter_markets(config.asset, events, &config.allowed_durations);
    let requests = build_orderbook_requests(markets.iter())?;
    let books = clob
        .order_books(&requests)
        .await
        .map_err(|e| anyhow::anyhow!("CLOB snapshot books query failed: {e}"))?;
    let book_map = map_books(books);

    let out_dir = Path::new("analysis").join("live");
    fs::create_dir_all(&out_dir)?;
    let duration_suffix = config
        .allowed_durations
        .iter()
        .map(|d| format!("{d}m"))
        .collect::<Vec<_>>()
        .join("_");
    let out_path = out_dir.join(format!(
        "{}_complete_set_snapshot_{}_{}.md",
        Utc::now().date_naive().format("%Y-%m-%d"),
        config.asset.display_name().to_ascii_lowercase(),
        duration_suffix
    ));
    let mut out = String::new();
    out.push_str(&format!(
        "# Complete-Set Live Snapshot ({})\n\n",
        config.asset.display_name()
    ));
    out.push_str(&format!(
        "- Captured at: `{}`\n",
        Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ")
    ));
    out.push_str(&format!("- Asset: `{}`\n", config.asset.display_name()));
    out.push_str(&format!(
        "- Durations: `{}`\n",
        config
            .allowed_durations
            .iter()
            .map(|d| format!("{d}m"))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    out.push_str(&format!("- Config: `{}`\n", config_path.display()));
    out.push_str(&format!(
        "- Thresholds: `long <= {}`, `short >= {}`, `fee_buffer = {}`, `pairs = {}`\n\n",
        config.max_long_combined_ask,
        config.min_short_combined_bid,
        config.fee_buffer,
        config.pairs_per_trade
    ));
    let mut rows = Vec::new();
    let mut long_signals = 0u32;
    let mut short_signals = 0u32;
    let mut valid_books = 0u32;
    let mut in_trade_window = 0u32;
    let mut closest_long_threshold: Option<Decimal> = None;
    let mut closest_short_threshold: Option<Decimal> = None;

    for market in markets {
        let Some(yes_book) = book_map.get(&market.token_id_yes) else {
            continue;
        };
        let Some(no_book) = book_map.get(&market.token_id_no) else {
            continue;
        };
        let start_date = market
            .start_date
            .unwrap_or_else(|| market.effective_start_date_15m_fallback());
        let duration_secs = market
            .duration_secs()
            .unwrap_or_else(|| market.effective_duration_secs_15m_fallback());
        let now = Utc::now();
        let elapsed_secs = ((now - start_date).num_milliseconds() as f64 / 1000.0).max(0.0);
        let elapsed_pct = (elapsed_secs / duration_secs.max(1) as f64).clamp(0.0, 1.0);
        let window_status = if now < start_date {
            "prestart"
        } else if now > market.end_date {
            "expired"
        } else if elapsed_pct < config.trading_window_start_pct {
            "observe"
        } else if elapsed_pct > config.trading_window_end_pct {
            "wind_down"
        } else {
            in_trade_window += 1;
            "trade_window"
        };
        let combined_ask = match (best_ask(yes_book), best_ask(no_book)) {
            (Some(y), Some(n)) => y + n,
            _ => Decimal::ZERO,
        };
        let combined_bid = match (best_bid(yes_book), best_bid(no_book)) {
            (Some(y), Some(n)) => y + n,
            _ => Decimal::ZERO,
        };
        if combined_ask > Decimal::ZERO || combined_bid > Decimal::ZERO {
            valid_books += 1;
        }
        let long_edge = if combined_ask > Decimal::ZERO {
            Decimal::ONE - combined_ask
        } else {
            Decimal::ZERO
        };
        let short_edge = if combined_bid > Decimal::ZERO {
            combined_bid - Decimal::ONE
        } else {
            Decimal::ZERO
        };
        let long_signal = combined_ask > Decimal::ZERO
            && combined_ask + config.fee_buffer <= config.max_long_combined_ask;
        let short_signal = combined_bid > Decimal::ZERO
            && combined_bid - config.fee_buffer >= config.min_short_combined_bid;
        if window_status == "trade_window" {
            if combined_ask > Decimal::ZERO {
                let required = combined_ask + config.fee_buffer;
                closest_long_threshold = Some(
                    closest_long_threshold
                        .map(|current| current.min(required))
                        .unwrap_or(required),
                );
            }
            if combined_bid > Decimal::ZERO {
                let feasible = combined_bid - config.fee_buffer;
                closest_short_threshold = Some(
                    closest_short_threshold
                        .map(|current| current.max(feasible))
                        .unwrap_or(feasible),
                );
            }
        }
        if long_signal {
            long_signals += 1;
        }
        if short_signal {
            short_signals += 1;
        }
        let remaining_secs =
            ((market.end_date - Utc::now()).num_milliseconds() as f64 / 1000.0).max(0.0);
        rows.push(format!(
            "| {} | {} | {:.1} | {} | {} | {} | {} | {} | {} |",
            period_name_from_question(&market.question),
            window_status,
            remaining_secs,
            combined_ask,
            combined_bid,
            long_edge,
            short_edge,
            if long_signal { "`YES`" } else { "`NO`" },
            if short_signal { "`YES`" } else { "`NO`" },
        ));
    }

    out.push_str(&format!("- Markets scanned: `{}`\n", rows.len()));
    out.push_str(&format!(
        "- Markets with visible book data: `{}`\n",
        valid_books
    ));
    out.push_str(&format!(
        "- Markets in configured trade window now: `{}`\n",
        in_trade_window
    ));
    out.push_str(&format!("- Long signals now: `{}`\n", long_signals));
    out.push_str(&format!("- Short signals now: `{}`\n\n", short_signals));
    match closest_long_threshold {
        Some(required) => out.push_str(&format!(
            "- Closest long threshold needed now: `{}` (current gap: `{}`)\n",
            required,
            required - config.max_long_combined_ask
        )),
        None => out.push_str("- Closest long threshold needed now: `n/a`\n"),
    }
    match closest_short_threshold {
        Some(feasible) => out.push_str(&format!(
            "- Closest short threshold achievable now: `{}` (current gap: `{}`)\n\n",
            feasible,
            config.min_short_combined_bid - feasible
        )),
        None => out.push_str("- Closest short threshold achievable now: `n/a`\n\n"),
    }
    out.push_str("| Period | Window Status | Remaining Secs | Combined Ask | Combined Bid | Long Edge | Short Edge | Long Signal | Short Signal |\n");
    out.push_str("|---|---|---:|---:|---:|---:|---:|---|---|\n");
    for row in rows {
        out.push_str(&row);
        out.push('\n');
    }

    fs::write(&out_path, out)?;
    Ok(out_path)
}

async fn process_market(
    config: &CompleteSetConfig,
    clients: &Clients,
    book_map: &HashMap<String, OrderBookSummaryResponse>,
    market: &TrackedMarket,
    stats: &mut PeriodStats,
    logger: &mut CompleteSetLogger,
) -> anyhow::Result<()> {
    let yes_book = match book_map.get(&market.token_id_yes) {
        Some(book) => book,
        None => return Ok(()),
    };
    let no_book = match book_map.get(&market.token_id_no) {
        Some(book) => book,
        None => return Ok(()),
    };

    let duration_secs = market
        .duration_secs()
        .unwrap_or_else(|| market.effective_duration_secs_15m_fallback());
    let start_date = market
        .start_date
        .unwrap_or_else(|| market.effective_start_date_15m_fallback());
    let now = Utc::now();
    if now <= start_date {
        return Ok(());
    }

    let elapsed_secs = (now - start_date).num_milliseconds() as f64 / 1000.0;
    let total_secs = duration_secs.max(1) as f64;
    let elapsed_pct = (elapsed_secs / total_secs).clamp(0.0, 1.0);
    if elapsed_pct < config.trading_window_start_pct || elapsed_pct > config.trading_window_end_pct
    {
        return Ok(());
    }

    let remaining_secs = ((market.end_date - now).num_milliseconds() as f64 / 1000.0).max(0.0);
    let yes_best_ask = best_ask(yes_book);
    let no_best_ask = best_ask(no_book);
    let yes_best_bid = best_bid(yes_book);
    let no_best_bid = best_bid(no_book);
    let combined_ask = match (yes_best_ask, no_best_ask) {
        (Some(y), Some(n)) => y + n,
        _ => Decimal::ZERO,
    };
    let combined_bid = match (yes_best_bid, no_best_bid) {
        (Some(y), Some(n)) => y + n,
        _ => Decimal::ZERO,
    };
    let long_edge_value = if combined_ask > Decimal::ZERO {
        Decimal::ONE - combined_ask
    } else {
        Decimal::ZERO
    };
    let short_edge_value = if combined_bid > Decimal::ZERO {
        combined_bid - Decimal::ONE
    } else {
        Decimal::ZERO
    };

    let long_edge = if config.long_enabled
        && combined_ask > Decimal::ZERO
        && combined_ask + config.fee_buffer <= config.max_long_combined_ask
    {
        Some(long_edge_value)
    } else {
        None
    };
    let short_edge = if config.short_enabled
        && combined_bid > Decimal::ZERO
        && combined_bid - config.fee_buffer >= config.min_short_combined_bid
    {
        Some(short_edge_value)
    } else {
        None
    };

    stats.trade_window_polls += 1;
    if stats.first_window_seen_at.is_none() {
        stats.first_window_seen_at = Some(now);
    }
    stats.last_window_seen_at = Some(now);
    stats.last_long_edge = long_edge_value;
    stats.last_short_edge = short_edge_value;
    stats.best_long_edge = stats.best_long_edge.max(long_edge_value);
    stats.best_short_edge = stats.best_short_edge.max(short_edge_value);
    stats.min_combined_ask = Some(
        stats
            .min_combined_ask
            .map(|current| current.min(combined_ask))
            .unwrap_or(combined_ask),
    );
    stats.max_combined_bid = Some(
        stats
            .max_combined_bid
            .map(|current| current.max(combined_bid))
            .unwrap_or(combined_bid),
    );
    if long_edge.is_some() {
        stats.long_signal_polls += 1;
    }
    if short_edge.is_some() {
        stats.short_signal_polls += 1;
    }
    logger.log_scan(
        stats,
        remaining_secs,
        elapsed_pct,
        combined_ask,
        combined_bid,
        long_edge_value,
        short_edge_value,
        long_edge.is_some(),
        short_edge.is_some(),
    );

    let opp = match (long_edge, short_edge) {
        (Some(long_edge), Some(short_edge)) if long_edge >= short_edge => Opportunity {
            side: OpportunitySide::Long,
            combined_ask,
            combined_bid,
            edge_per_pair: long_edge,
        },
        (Some(_), Some(short_edge)) => Opportunity {
            side: OpportunitySide::Short,
            combined_ask,
            combined_bid,
            edge_per_pair: short_edge,
        },
        (Some(long_edge), None) => Opportunity {
            side: OpportunitySide::Long,
            combined_ask,
            combined_bid,
            edge_per_pair: long_edge,
        },
        (None, Some(short_edge)) => Opportunity {
            side: OpportunitySide::Short,
            combined_ask,
            combined_bid,
            edge_per_pair: short_edge,
        },
        (None, None) => return Ok(()),
    };

    stats.opportunities += 1;
    match opp.side {
        OpportunitySide::Long => stats.long_signals += 1,
        OpportunitySide::Short => stats.short_signals += 1,
    }

    let pairs = config.pairs_per_trade;
    if stats.trades_this_period >= config.max_trades_per_period {
        logger.log_opportunity(
            stats,
            &opp,
            "period_trade_cap",
            pairs,
            remaining_secs,
            elapsed_pct,
        );
        return Ok(());
    }
    if let Some(last_trade_at) = stats.last_trade_at {
        if (Utc::now() - last_trade_at).num_seconds() < config.cooldown_secs as i64 {
            logger.log_opportunity(stats, &opp, "cooldown", pairs, remaining_secs, elapsed_pct);
            return Ok(());
        }
    }

    logger.log_opportunity(stats, &opp, "signal", pairs, remaining_secs, elapsed_pct);
    stats.last_trade_at = Some(Utc::now());
    let expected_pnl = opp.edge_per_pair * pairs;

    let outcome = if config.mode.is_live() {
        match opp.side {
            OpportunitySide::Long => {
                stats.long_attempts += 1;
                execute_long_complete_set(config, clients, market, pairs).await
            }
            OpportunitySide::Short => {
                stats.short_attempts += 1;
                execute_short_complete_set(config, clients, market, pairs).await
            }
        }
    } else {
        stats.trades_this_period += 1;
        ExecutionOutcome {
            status: "shadow_signal".to_string(),
            detail: "Signal detected in non-live mode".to_string(),
            order_ids: Vec::new(),
            tx_hashes: Vec::new(),
            recovered: false,
            success: true,
            merge_pairs: if matches!(opp.side, OpportunitySide::Long) {
                pairs
            } else {
                Decimal::ZERO
            },
            split_pairs: if matches!(opp.side, OpportunitySide::Short) {
                pairs
            } else {
                Decimal::ZERO
            },
        }
    };

    if outcome.success {
        stats.trades_this_period += 1;
        stats.expected_pnl += expected_pnl;
        stats.merge_pairs += outcome.merge_pairs;
        stats.split_pairs += outcome.split_pairs;
        if outcome.recovered {
            stats.recoveries += 1;
        }
        match opp.side {
            OpportunitySide::Long => stats.long_successes += 1,
            OpportunitySide::Short => stats.short_successes += 1,
        }
    }

    logger.log_execution(stats, opp.side, pairs, expected_pnl, &outcome);
    Ok(())
}

async fn execute_long_complete_set(
    config: &CompleteSetConfig,
    clients: &Clients,
    market: &TrackedMarket,
    pairs: Decimal,
) -> ExecutionOutcome {
    let Some(sdk) = clients.live_sdk() else {
        return ExecutionOutcome {
            status: "live_sdk_missing".to_string(),
            detail: "Live SDK unavailable".to_string(),
            order_ids: Vec::new(),
            tx_hashes: Vec::new(),
            recovered: false,
            success: false,
            merge_pairs: Decimal::ZERO,
            split_pairs: Decimal::ZERO,
        };
    };

    let batch = vec![
        MarketOrderSpec {
            token_id: market.token_id_yes.clone(),
            side: Side::Buy,
            shares: pairs,
        },
        MarketOrderSpec {
            token_id: market.token_id_no.clone(),
            side: Side::Buy,
            shares: pairs,
        },
    ];

    match sdk.place_batch_market_orders(batch).await {
        Ok(results) if results.iter().all(|r| r.success) => {
            let mut tx_hashes = Vec::new();
            if config.merge_enabled {
                let pairs_u64 = pairs.to_u64().unwrap_or(0);
                match sdk
                    .merge_positions(
                        &std::env::var("POLYGON_RPC_URL").unwrap_or_default(),
                        &market.condition_id,
                        pairs_u64,
                    )
                    .await
                {
                    Ok(tx) => tx_hashes.push(format!("{tx:#x}")),
                    Err(e) => {
                        return ExecutionOutcome {
                            status: "merge_failed".to_string(),
                            detail: e.to_string(),
                            order_ids: successful_order_ids(&results),
                            tx_hashes,
                            recovered: false,
                            success: false,
                            merge_pairs: Decimal::ZERO,
                            split_pairs: Decimal::ZERO,
                        };
                    }
                }
            }
            ExecutionOutcome {
                status: "long_complete_set_ok".to_string(),
                detail: "Paired market buys filled and merged".to_string(),
                order_ids: successful_order_ids(&results),
                tx_hashes,
                recovered: false,
                success: true,
                merge_pairs: pairs,
                split_pairs: Decimal::ZERO,
            }
        }
        Ok(results) => recover_long_partial(&sdk, market, pairs, results).await,
        Err(e) => ExecutionOutcome {
            status: "long_batch_failed".to_string(),
            detail: e.to_string(),
            order_ids: Vec::new(),
            tx_hashes: Vec::new(),
            recovered: false,
            success: false,
            merge_pairs: Decimal::ZERO,
            split_pairs: Decimal::ZERO,
        },
    }
}

async fn execute_short_complete_set(
    config: &CompleteSetConfig,
    clients: &Clients,
    market: &TrackedMarket,
    pairs: Decimal,
) -> ExecutionOutcome {
    let Some(sdk) = clients.live_sdk() else {
        return ExecutionOutcome {
            status: "live_sdk_missing".to_string(),
            detail: "Live SDK unavailable".to_string(),
            order_ids: Vec::new(),
            tx_hashes: Vec::new(),
            recovered: false,
            success: false,
            merge_pairs: Decimal::ZERO,
            split_pairs: Decimal::ZERO,
        };
    };
    if !config.split_enabled {
        return ExecutionOutcome {
            status: "split_disabled".to_string(),
            detail: "Short complete-set execution requires split_enabled".to_string(),
            order_ids: Vec::new(),
            tx_hashes: Vec::new(),
            recovered: false,
            success: false,
            merge_pairs: Decimal::ZERO,
            split_pairs: Decimal::ZERO,
        };
    }

    let Some(pairs_u64) = pairs.to_u64() else {
        return ExecutionOutcome {
            status: "invalid_pairs".to_string(),
            detail: "pairs_per_trade could not be represented as u64".to_string(),
            order_ids: Vec::new(),
            tx_hashes: Vec::new(),
            recovered: false,
            success: false,
            merge_pairs: Decimal::ZERO,
            split_pairs: Decimal::ZERO,
        };
    };
    let raw_amount = pairs_u64.saturating_mul(1_000_000u64);
    let rpc_url = std::env::var("POLYGON_RPC_URL").unwrap_or_default();
    let split_tx = match sdk
        .split_position(&rpc_url, &market.condition_id, raw_amount)
        .await
    {
        Ok(tx) => tx,
        Err(e) => {
            return ExecutionOutcome {
                status: "split_failed".to_string(),
                detail: e.to_string(),
                order_ids: Vec::new(),
                tx_hashes: Vec::new(),
                recovered: false,
                success: false,
                merge_pairs: Decimal::ZERO,
                split_pairs: Decimal::ZERO,
            };
        }
    };

    let batch = vec![
        MarketOrderSpec {
            token_id: market.token_id_yes.clone(),
            side: Side::Sell,
            shares: pairs,
        },
        MarketOrderSpec {
            token_id: market.token_id_no.clone(),
            side: Side::Sell,
            shares: pairs,
        },
    ];

    match sdk.place_batch_market_orders(batch).await {
        Ok(results) if results.iter().all(|r| r.success) => ExecutionOutcome {
            status: "short_complete_set_ok".to_string(),
            detail: "Split collateral and sold both legs".to_string(),
            order_ids: successful_order_ids(&results),
            tx_hashes: vec![format!("{split_tx:#x}")],
            recovered: false,
            success: true,
            merge_pairs: Decimal::ZERO,
            split_pairs: pairs,
        },
        Ok(results) => {
            let mut outcome = recover_short_partial(&sdk, market, pairs, results).await;
            outcome.tx_hashes.insert(0, format!("{split_tx:#x}"));
            outcome
        }
        Err(e) => ExecutionOutcome {
            status: "short_batch_failed".to_string(),
            detail: format!("Split succeeded, paired sells failed: {e}"),
            order_ids: Vec::new(),
            tx_hashes: vec![format!("{split_tx:#x}")],
            recovered: false,
            success: false,
            merge_pairs: Decimal::ZERO,
            split_pairs: Decimal::ZERO,
        },
    }
}

async fn recover_long_partial(
    sdk: &SdkClients,
    market: &TrackedMarket,
    pairs: Decimal,
    results: Vec<MarketOrderResult>,
) -> ExecutionOutcome {
    let mut held_yes = false;
    let mut held_no = false;
    let mut order_ids = successful_order_ids(&results);
    let mut detail = Vec::new();

    for result in &results {
        if result.success {
            if result.token_id == market.token_id_yes {
                held_yes = true;
            } else if result.token_id == market.token_id_no {
                held_no = true;
            }
        } else if let Some(msg) = &result.error_msg {
            detail.push(format!("{} failed: {}", result.token_id, msg));
        }
    }

    for (token_id, held) in [
        (market.token_id_yes.as_str(), &mut held_yes),
        (market.token_id_no.as_str(), &mut held_no),
    ] {
        if !*held {
            continue;
        }
        match sdk.place_market_sell_fok_shares(token_id, pairs).await {
            Ok(order_id) => {
                order_ids.push(order_id);
                *held = false;
            }
            Err(e) => detail.push(format!("recovery sell {} failed: {}", token_id, e)),
        }
    }

    let mut tx_hashes = Vec::new();
    if held_yes && held_no {
        if let Some(pairs_u64) = pairs.to_u64() {
            match sdk
                .merge_positions(
                    &std::env::var("POLYGON_RPC_URL").unwrap_or_default(),
                    &market.condition_id,
                    pairs_u64,
                )
                .await
            {
                Ok(tx) => {
                    tx_hashes.push(format!("{tx:#x}"));
                    held_yes = false;
                    held_no = false;
                    detail.push("recovered via merge".to_string());
                }
                Err(e) => detail.push(format!("recovery merge failed: {e}")),
            }
        }
    }

    let residual = held_yes || held_no;
    ExecutionOutcome {
        status: if residual {
            "long_partial_residual"
        } else {
            "long_partial_recovered"
        }
        .to_string(),
        detail: detail.join("; "),
        order_ids,
        tx_hashes,
        recovered: !residual,
        success: !residual,
        merge_pairs: if residual { Decimal::ZERO } else { pairs },
        split_pairs: Decimal::ZERO,
    }
}

async fn recover_short_partial(
    sdk: &SdkClients,
    market: &TrackedMarket,
    pairs: Decimal,
    results: Vec<MarketOrderResult>,
) -> ExecutionOutcome {
    let mut held_yes = true;
    let mut held_no = true;
    let mut order_ids = successful_order_ids(&results);
    let mut detail = Vec::new();

    for result in &results {
        if result.success {
            if result.token_id == market.token_id_yes {
                held_yes = false;
            } else if result.token_id == market.token_id_no {
                held_no = false;
            }
        } else if let Some(msg) = &result.error_msg {
            detail.push(format!("{} failed: {}", result.token_id, msg));
        }
    }

    for (token_id, held) in [
        (market.token_id_yes.as_str(), &mut held_yes),
        (market.token_id_no.as_str(), &mut held_no),
    ] {
        if *held {
            continue;
        }
        match sdk.place_market_buy_fok_shares(token_id, pairs).await {
            Ok(order_id) => {
                order_ids.push(order_id);
                *held = true;
            }
            Err(e) => detail.push(format!("buyback {} failed: {}", token_id, e)),
        }
    }

    let mut tx_hashes = Vec::new();
    if held_yes && held_no {
        if let Some(pairs_u64) = pairs.to_u64() {
            match sdk
                .merge_positions(
                    &std::env::var("POLYGON_RPC_URL").unwrap_or_default(),
                    &market.condition_id,
                    pairs_u64,
                )
                .await
            {
                Ok(tx) => {
                    tx_hashes.push(format!("{tx:#x}"));
                    detail.push("recovered via buyback + merge".to_string());
                    return ExecutionOutcome {
                        status: "short_partial_recovered".to_string(),
                        detail: detail.join("; "),
                        order_ids,
                        tx_hashes,
                        recovered: true,
                        success: true,
                        merge_pairs: pairs,
                        split_pairs: pairs,
                    };
                }
                Err(e) => detail.push(format!("merge after buyback failed: {e}")),
            }
        }
    }

    for (token_id, held) in [
        (market.token_id_yes.as_str(), &mut held_yes),
        (market.token_id_no.as_str(), &mut held_no),
    ] {
        if !*held {
            continue;
        }
        match sdk.place_market_sell_fok_shares(token_id, pairs).await {
            Ok(order_id) => {
                order_ids.push(order_id);
                *held = false;
            }
            Err(e) => detail.push(format!("flatten sell {} failed: {}", token_id, e)),
        }
    }

    let residual = held_yes || held_no;
    ExecutionOutcome {
        status: if residual {
            "short_partial_residual"
        } else {
            "short_partial_flattened"
        }
        .to_string(),
        detail: detail.join("; "),
        order_ids,
        tx_hashes,
        recovered: !residual,
        success: !residual,
        merge_pairs: Decimal::ZERO,
        split_pairs: if residual { Decimal::ZERO } else { pairs },
    }
}

fn build_orderbook_requests<'a>(
    markets: impl Iterator<Item = &'a TrackedMarket>,
) -> anyhow::Result<Vec<OrderBookSummaryRequest>> {
    let mut requests = Vec::new();
    for market in markets {
        for token_id in [&market.token_id_yes, &market.token_id_no] {
            let token_id = U256::from_str(token_id)
                .with_context(|| format!("Invalid token_id in tracked market: {token_id}"))?;
            requests.push(
                OrderBookSummaryRequest::builder()
                    .token_id(token_id)
                    .build(),
            );
        }
    }
    Ok(requests)
}

fn map_books(books: Vec<OrderBookSummaryResponse>) -> HashMap<String, OrderBookSummaryResponse> {
    books
        .into_iter()
        .map(|book| (book.asset_id.to_string(), book))
        .collect()
}

fn best_bid(book: &OrderBookSummaryResponse) -> Option<Decimal> {
    book.bids.first().map(|level| level.price)
}

fn best_ask(book: &OrderBookSummaryResponse) -> Option<Decimal> {
    book.asks.first().map(|level| level.price)
}

fn successful_order_ids(results: &[MarketOrderResult]) -> Vec<String> {
    results.iter().filter_map(|r| r.order_id.clone()).collect()
}

fn csv_field(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn open_csv(path: &Path) -> anyhow::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to open {}", path.display()))
}

fn write_header_if_new(path: &Path, file: &mut File, header: &str) -> anyhow::Result<()> {
    if path.metadata()?.len() == 0 {
        writeln!(file, "{header}")?;
    }
    Ok(())
}

fn default_true() -> bool {
    true
}

fn install_tls() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("Failed to install rustls crypto provider"))
}

fn init_tracing<P: AsRef<Path>>(artifact_root: P) -> anyhow::Result<()> {
    fs::create_dir_all(artifact_root.as_ref())?;
    let log_file = File::create(artifact_root.as_ref().join("bot.log"))?;
    let (non_blocking, _guard) = tracing_appender::non_blocking(log_file);
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(
                    "info,polymarket_client_sdk::serde_helpers=error",
                )
            }),
        )
        .with_target(true)
        .with_thread_ids(true)
        .with_writer(non_blocking)
        .json()
        .try_init()
        .map_err(|e| anyhow::anyhow!("Failed to initialize tracing: {e}"))?;
    Ok(())
}

fn period_name_from_question(question: &str) -> String {
    let time_part = if let Some(idx) = question.find(" - ") {
        &question[idx + 3..]
    } else {
        question
    };
    time_part
        .replace(':', "-")
        .replace(' ', "_")
        .replace(',', "")
}

fn filter_markets(
    asset: Asset,
    events: Vec<Event>,
    allowed_durations: &[u32],
) -> Vec<TrackedMarket> {
    let now = Utc::now();
    let max_future_start = now + chrono::Duration::minutes(10);
    let prefix_lower = format!("{} up or down", asset.market_prefix().to_lowercase());
    let mut results = Vec::new();

    for event in events {
        let Some(markets) = event.markets else {
            continue;
        };
        for market in markets {
            let Some(question) = market.question.clone() else {
                continue;
            };
            if !question.to_lowercase().contains(&prefix_lower) {
                continue;
            }
            if market.active != Some(true) {
                continue;
            }
            let end_date = match market.end_date {
                Some(date) if date > now => date,
                _ => continue,
            };
            let duration_mins = match parse_market_duration_minutes(&question) {
                Some(duration) if allowed_durations.contains(&duration) => duration,
                _ => continue,
            };
            let start_date = Some(end_date - chrono::Duration::minutes(duration_mins as i64));
            if let Some(start) = start_date {
                if start > max_future_start {
                    continue;
                }
            }
            let condition_id = match market.condition_id {
                Some(cid) => format!("{cid:?}"),
                None => continue,
            };
            let token_ids = match market.clob_token_ids {
                Some(ref ids) if ids.len() >= 2 => ids.clone(),
                _ => continue,
            };

            results.push(TrackedMarket {
                condition_id,
                token_id_yes: token_ids[0].to_string(),
                token_id_no: token_ids[1].to_string(),
                question,
                start_date,
                end_date,
                tick_size: market
                    .order_price_min_tick_size
                    .unwrap_or(Decimal::new(1, 2)),
                neg_risk: event.neg_risk.unwrap_or(false),
            });
        }
    }

    results
}

fn parse_market_duration_minutes(question: &str) -> Option<u32> {
    for (i, _) in question.match_indices(':') {
        if i < 1 || i + 7 > question.len() {
            continue;
        }
        let after_colon = &question[i + 1..];
        if after_colon.len() < 5 {
            continue;
        }
        let start_mins: u32 = after_colon[..2].parse().ok()?;
        let ampm_dash = &after_colon[2..];
        let (start_is_pm, rest) = if ampm_dash.starts_with("AM-") {
            (false, &ampm_dash[3..])
        } else if ampm_dash.starts_with("PM-") {
            (true, &ampm_dash[3..])
        } else {
            continue;
        };
        let before_colon = &question[..i];
        let start_hour_str: String = before_colon
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>()
            .chars()
            .rev()
            .collect();
        let start_hour: u32 = start_hour_str.parse().ok()?;
        let colon_pos = rest.find(':')?;
        let end_hour: u32 = rest[..colon_pos].parse().ok()?;
        let after_end_colon = &rest[colon_pos + 1..];
        if after_end_colon.len() < 4 {
            continue;
        }
        let end_mins: u32 = after_end_colon[..2].parse().ok()?;
        let end_is_pm = after_end_colon[2..].starts_with("PM");
        if !after_end_colon[2..].starts_with("AM") && !after_end_colon[2..].starts_with("PM") {
            continue;
        }
        let to_24h = |hour: u32, mins: u32, is_pm: bool| -> u32 {
            let h24 = if is_pm {
                if hour == 12 {
                    12
                } else {
                    hour + 12
                }
            } else if hour == 12 {
                0
            } else {
                hour
            };
            h24 * 60 + mins
        };
        let start_24h = to_24h(start_hour, start_mins, start_is_pm);
        let end_24h = to_24h(end_hour, end_mins, end_is_pm);
        let diff = if end_24h > start_24h {
            end_24h - start_24h
        } else {
            end_24h + 1440 - start_24h
        };
        if diff > 0 {
            return Some(diff);
        }
    }

    let q_upper = question.to_uppercase();
    if (q_upper.contains("AM ET") || q_upper.contains("PM ET")) && !q_upper.contains(':') {
        return Some(60);
    }
    None
}
