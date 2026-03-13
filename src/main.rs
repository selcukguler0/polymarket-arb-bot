// Types and methods defined here are used for SDK integration; suppress dead-code warnings
// until the SDK call-sites are wired up.

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod config;
mod dashboard;
mod error;
mod execution;
mod file_logger;
mod latency;
mod monitoring;
mod onchain;
mod orchestrator_v2;
mod paper_sim;
mod persistence;
mod relayer;
mod risk;
mod run_manifest;
mod sdk;
mod types;
mod vpin;
mod web;

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

/// Global panic flag — set by panic hook, checked by orchestrator health checks.
pub static PANIC_EMERGENCY: AtomicBool = AtomicBool::new(false);

use chrono::Utc;
use parking_lot::RwLock;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
#[allow(unused_imports)]
use rust_decimal_macros::dec;
use std::str::FromStr;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

use crate::config::{AppConfig, AssetRawConfig, Secrets};
use crate::dashboard::state::DashboardState;
use crate::monitoring::{alerting, AlertingService};
use crate::onchain::OnChainManager;
use crate::orchestrator_v2::{OrchestratorV2, V2Config};
use crate::persistence::Database;
use crate::risk::{EmergencyHandler, InventoryManager};
use crate::run_manifest::{AssetRunProfile, RunManifest};
use crate::sdk::SdkClients;
use crate::types::*;
use crate::web::{BotControl, SharedBotControl, SharedDashboard};
use alloy::signers::Signer as _;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let config_path_str = if args.len() > 1 {
        args[1].clone()
    } else if Path::new("config/v2.toml").exists() {
        "config/v2.toml".to_string()
    } else {
        "config/default.toml".to_string()
    };
    let config_path = Path::new(&config_path_str);
    let artifact_root = AppConfig::load(config_path)
        .ok()
        .and_then(|cfg| {
            cfg.validate()
                .ok()
                .map(|validated| validated.mode.artifact_root())
        })
        .unwrap_or("logs");

    // Install TLS crypto provider before any network calls
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Initialize tracing — non-blocking writer to avoid contention across orchestrator tasks.
    // The old std::sync::Mutex<File> writer was the single largest contention point:
    // all 4 asset orchestrators + WS tasks blocked on the same mutex for every log write.
    std::fs::create_dir_all(artifact_root).expect("Failed to create artifact dir");
    let log_file = std::fs::File::create(Path::new(artifact_root).join("bot.log"))
        .expect("Failed to create log file");
    let (non_blocking, _log_guard) = tracing_appender::non_blocking(log_file);
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
        .init();

    // Initialize Prometheus metrics recorder
    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("Failed to install Prometheus recorder");

    info!("Polymarket Complete-Set Arbitrage Bot starting...");

    // FIX 11 + FIX 22: Global panic handler — log and signal emergency via static flag
    std::panic::set_hook(Box::new(|info| {
        let msg = if let Some(s) = info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "Unknown panic".to_string()
        };
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "unknown".to_string());
        tracing::error!(
            panic_msg = %msg,
            location = %location,
            "PANIC in task — triggering emergency"
        );
        PANIC_EMERGENCY.store(true, Ordering::Relaxed);
    }));

    // ── Step 1: Load config and secrets ──
    // Support: cargo run --release -- config/live.toml
    info!("Loading config from {}", config_path.display());
    let raw_config = AppConfig::load(config_path)?;
    let v2_raw_section = raw_config.v2.clone();
    let btc_raw = raw_config.btc.clone().unwrap_or_default();
    let eth_raw = raw_config.eth.clone().unwrap_or_default();
    let sol_raw = raw_config.sol.clone().unwrap_or_default();
    let xrp_raw = raw_config.xrp.clone().unwrap_or_default();
    let config = raw_config.validate()?;
    let secrets = Secrets::from_env()?;

    info!(
        mode = ?config.mode,
        order_size = %config.order_size_usdc,
        min_profit = %config.min_profit_threshold,
        "Configuration loaded"
    );

    // ── Step 2: Initialize database ──
    let db = Arc::new(Database::open(&config.db_path).await?);
    info!("Database initialized at {}", config.db_path);

    // ── Step 3: Initialize on-chain manager ──
    let onchain = Arc::new(OnChainManager::new(
        &secrets.wallet_address,
        &secrets.polygon_rpc_url,
        &config.usdc_address,
        &config.ctf_exchange_address,
        &config.neg_risk_ctf_exchange_address,
    )?);

    // ── Step 4: Verify on-chain setup ──
    info!("Verifying on-chain setup...");
    if let Err(e) = onchain.verify_setup().await {
        if config.mode.is_live() {
            error!("On-chain verification failed in live mode: {e}");
            anyhow::bail!("Cannot start live trading without USDC approvals. Run approval first.");
        }
        warn!("On-chain verification failed: {e}");
        warn!(
            mode = config.mode.as_str(),
            "Continuing in non-live mode — some features may not work"
        );
    }

    // ── Step 4b: Initialize SDK clients (FIX 1) ──
    let sdk = if config.mode.is_live() {
        info!(
            eoa_mode = config.eoa_mode,
            "Authenticating SDK clients (live mode)..."
        );
        let mut sdk = SdkClients::new(
            &secrets.private_key,
            &secrets.wallet_address,
            config.eoa_mode,
            &config.usdc_address,
            &secrets.builder_key,
            &secrets.builder_secret,
            &secrets.builder_passphrase,
        )
        .await?;

        // ── Attach relayer for gasless on-chain operations (Safe wallet) ──
        // When eoa_mode is false, create a RelayerClient and attach it to the SDK.
        // All merge/redeem/split operations will then route through Polymarket's
        // relayer (gasless) instead of direct RPC (EOA pays POL gas).
        if !config.eoa_mode {
            let usdc_addr = alloy::primitives::Address::from_str(&config.usdc_address)
                .map_err(|e| anyhow::anyhow!("Invalid USDC address for relayer: {e}"))?;
            let relayer_signer =
                alloy::signers::local::PrivateKeySigner::from_str(&secrets.private_key)
                    .map_err(|e| anyhow::anyhow!("Invalid private key for relayer: {e}"))?
                    .with_chain_id(Some(polymarket_client_sdk::POLYGON));

            match relayer::RelayerClient::new(
                relayer_signer,
                usdc_addr,
                secrets.builder_key.clone(),
                secrets.builder_secret.clone(),
                secrets.builder_passphrase.clone(),
            ) {
                Ok(relayer_client) => {
                    info!(
                        safe = %relayer_client.safe_address(),
                        "Relayer client created — on-chain ops will be gasless"
                    );
                    sdk.set_relayer(relayer_client);
                }
                Err(e) => {
                    warn!("Failed to create relayer client, falling back to direct RPC: {e}");
                }
            }
        }

        // FIX 5: Cancel-all on startup (Rule #3 — start clean)
        info!("Cancelling all open orders on startup...");
        match sdk.cancel_all_orders().await {
            Ok(resp) => info!(
                cancelled = resp.canceled.len(),
                "Startup cancel-all complete"
            ),
            Err(e) => warn!("Startup cancel-all failed: {e}"),
        }

        Some(Arc::new(sdk))
    } else {
        info!(
            mode = config.mode.as_str(),
            "Non-live mode — skipping SDK authentication"
        );
        None
    };

    // ── Step 5: Initialize alerting ──
    let alert_tx = if config.alerting_enabled {
        match (&secrets.telegram_bot_token, &secrets.telegram_chat_id) {
            (Some(token), Some(chat_id)) => {
                let service = AlertingService::new(
                    token.clone(),
                    chat_id.clone(),
                    config.max_alerts_per_5min,
                );
                info!("Telegram alerting enabled");
                service.spawn()
            }
            _ => {
                warn!("Alerting enabled but Telegram credentials not set");
                alerting::noop_alert_sender()
            }
        }
    } else {
        alerting::noop_alert_sender()
    };

    // ── Step 6: Initialize inventory manager ──
    let mut inventory_mgr = InventoryManager::new(&config);

    // FIX 21: Attach USDC balance callback so can_place_order checks balance
    {
        let onchain_ref = onchain.clone();
        inventory_mgr
            .set_usdc_balance_fn(Arc::new(move || onchain_ref.cached_usdc_balance_value()));
    }

    let inventory = Arc::new(inventory_mgr);

    // Wire up SDK-level sell position guard (last line of defense against naked sells).
    // The guard callback uses InventoryManager's token registry to look up available position.
    if let Some(ref sdk) = sdk {
        let inv_for_guard = inventory.clone();
        sdk.set_sell_position_guard(Arc::new(move |token_id: &str, _size: Decimal| {
            inv_for_guard.available_to_sell_by_token(token_id)
        }));
        info!("SDK sell position guard installed");
    }

    // Load existing positions from DB for reconciliation.
    // In paper/shadow mode, stale positions from previous sessions are meaningless
    // and would block capacity (max_total_exposure), so start clean.
    if config.mode.starts_with_clean_positions() {
        info!(
            mode = config.mode.as_str(),
            "Starting with clean position state (ignoring stale DB positions)"
        );
        inventory.load_positions(Vec::new());
    } else {
        let db_positions = db.get_all_positions().await?;
        if !db_positions.is_empty() {
            info!(
                count = db_positions.len(),
                "Loading existing positions from database"
            );
            inventory.load_positions(db_positions);
        }
    }

    // FIX 8: Reconcile positions against Data API on startup
    if let Some(ref sdk) = sdk {
        info!("Reconciling positions against Data API...");
        match sdk.get_positions_from_api().await {
            Ok(api_positions) => {
                if api_positions.is_empty() {
                    info!("Data API returned zero positions — clearing stale DB positions");
                    inventory.load_positions(Vec::new());
                } else {
                    let today = Utc::now().date_naive();
                    info!(
                        count = api_positions.len(),
                        "Got positions from Data API for reconciliation"
                    );

                    // Filter out resolved/expired/empty positions to avoid inflating exposure
                    let mut skipped = 0usize;
                    let mut aggregated: std::collections::HashMap<String, crate::types::Position> =
                        std::collections::HashMap::new();
                    for ap in &api_positions {
                        // Skip zero-size positions (already redeemed/sold)
                        if ap.size <= Decimal::ZERO {
                            skipped += 1;
                            info!(
                                condition_id = %ap.condition_id,
                                outcome = %ap.outcome,
                                "Skipping zero-size position from Data API"
                            );
                            continue;
                        }
                        // Skip resolved markets (redeemable flag set by Polymarket)
                        if ap.redeemable {
                            skipped += 1;
                            info!(
                                condition_id = %ap.condition_id,
                                outcome = %ap.outcome,
                                size = %ap.size,
                                "Skipping resolved (redeemable) position from Data API"
                            );
                            continue;
                        }
                        // Skip expired markets (end_date in the past)
                        if ap.end_date < today {
                            skipped += 1;
                            info!(
                                condition_id = %ap.condition_id,
                                outcome = %ap.outcome,
                                end_date = %ap.end_date,
                                "Skipping expired position from Data API"
                            );
                            continue;
                        }

                        let cid = format!("{:?}", ap.condition_id);
                        let entry = aggregated.entry(cid.clone()).or_insert_with(|| {
                            crate::types::Position {
                                condition_id: cid,
                                ..Default::default()
                            }
                        });
                        match ap.outcome.as_str() {
                            "Yes" | "Up" => {
                                entry.yes_qty = ap.size;
                                entry.total_yes_spent = ap.initial_value;
                            }
                            "No" | "Down" => {
                                entry.no_qty = ap.size;
                                entry.total_no_spent = ap.initial_value;
                            }
                            other => {
                                warn!("Unknown outcome '{}' from Data API, skipping", other);
                            }
                        }
                    }
                    if skipped > 0 {
                        info!(
                            skipped,
                            total = api_positions.len(),
                            "Filtered out resolved/expired/empty positions"
                        );
                    }
                    // Verify each aggregated position's market is still active via CLOB
                    // This catches same-day expired 15-min markets that pass the NaiveDate filter
                    let mut verified: Vec<crate::types::Position> = Vec::new();
                    for pos in aggregated.into_values() {
                        // condition_id stored as "{:?}" of B256 — strip the 0x prefix format
                        let cid = &pos.condition_id;
                        match sdk.is_market_active(cid).await {
                            Ok(true) => {
                                verified.push(pos);
                            }
                            Ok(false) => {
                                skipped += 1;
                                info!(
                                    condition_id = %cid,
                                    yes_qty = %pos.yes_qty,
                                    no_qty = %pos.no_qty,
                                    "Skipping closed/resolved market position (CLOB says inactive)"
                                );
                            }
                            Err(e) => {
                                // On CLOB error, include the position to be safe (don't lose tracking)
                                warn!(
                                    condition_id = %cid,
                                    "CLOB market check failed ({e}), including position conservatively"
                                );
                                verified.push(pos);
                            }
                        }
                    }
                    if !verified.is_empty() {
                        let total_exposure: Decimal = verified
                            .iter()
                            .map(|p| p.total_yes_spent + p.total_no_spent)
                            .sum();
                        info!(
                            count = verified.len(),
                            total_exposure = %total_exposure,
                            "Reconciled active positions from Data API"
                        );
                    } else {
                        info!("No active positions after filtering — clearing stale DB positions");
                    }
                    // Always overwrite DB positions with Data API truth
                    // (load_positions clears first, so empty vec = fresh start)
                    inventory.load_positions(verified);
                }
            }
            Err(e) => {
                warn!("Data API reconciliation failed: {e}");
                warn!("Falling back to DB positions only");
            }
        }
    }

    // Load daily P&L (includes sell P&L if persisted from previous session)
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let daily_pnl = db.get_daily_pnl_full(&today).await?;
    if daily_pnl != Decimal::ZERO {
        info!(%daily_pnl, "Loaded daily P&L");
        inventory.set_daily_pnl(daily_pnl);
    }

    // ── Step 7: Initialize emergency handler ──
    let (strategy_heartbeat_tx, strategy_heartbeat_rx) = mpsc::channel(1);
    let (shutdown_tx, _) = broadcast::channel(1);
    let shutdown_flag = Arc::new(AtomicBool::new(false));

    // Shared aggregate daily P&L tracker (in cents, across all assets).
    // Each per-asset inventory updates this atomically on every sell/resolution.
    // The emergency handler reads it for the global daily loss limit check.
    let aggregate_daily_pnl_cents = Arc::new(AtomicI64::new(
        (daily_pnl * dec!(100)).to_i64().unwrap_or(0),
    ));

    let emergency = Arc::new(EmergencyHandler::new(
        inventory.clone(),
        alert_tx.clone(),
        config.health_check_interval_secs,
        aggregate_daily_pnl_cents.clone(),
        config.daily_loss_limit,
        config.session_loss_limit,
    ));

    // Spawn emergency monitor
    {
        let emergency_clone = emergency.clone();
        let onchain_clone = onchain.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        let is_paper = config.mode.uses_paper_sim();

        // In paper mode, return simulated balance so the emergency monitor
        // doesn't trigger on real on-chain state.
        let usdc_balance_fn: Arc<dyn Fn() -> Decimal + Send + Sync> = if is_paper {
            Arc::new(|| dec!(1000)) // Simulated $1000 USDC in paper mode
        } else {
            Arc::new(move || onchain_clone.cached_usdc_balance_value())
        };

        let sf = shutdown_flag.clone();
        tokio::spawn(async move {
            emergency_clone
                .run_monitor(
                    strategy_heartbeat_rx,
                    usdc_balance_fn,
                    shutdown_rx,
                    Some(sf),
                )
                .await;
        });
    }

    // ── Step 7b: Create latency tracker ──
    let latency_tracker = Arc::new(latency::LatencyTracker::new());

    // ── Step 8: Create per-asset dashboard states + bot control ──
    let bot_control: SharedBotControl = Arc::new(RwLock::new(BotControl::default()));
    let mut dashboard_map: HashMap<String, SharedDashboard> = HashMap::new();

    let asset_configs: Vec<(Asset, &AssetRawConfig)> = vec![
        (Asset::BTC, &btc_raw),
        (Asset::ETH, &eth_raw),
        (Asset::SOL, &sol_raw),
        (Asset::XRP, &xrp_raw),
    ];

    // Pre-create a SharedDashboard for each enabled asset
    for (asset, asset_raw) in &asset_configs {
        if asset_raw.enabled.unwrap_or(true) {
            let dash: SharedDashboard = Arc::new(RwLock::new(DashboardState {
                asset: asset.display_name().to_string(),
                ..DashboardState::default()
            }));
            dashboard_map.insert(asset.display_name().to_string(), dash);
        }
    }

    // ── Step 10a: Load persistent stats from DB into BTC dashboard ──
    // (All historical data is BTC — ETH starts fresh)
    {
        let today_str = Utc::now().format("%Y-%m-%d").to_string();
        if let Err(e) = db.reset_today_pnl_if_new_day(&today_str).await {
            warn!("Failed to reset today PnL: {e}");
        }

        // Load session stats into BTC dashboard (historical aggregate)
        if let Some(btc_dash) = dashboard_map.get("BTC") {
            match db.get_session_stats().await {
                Ok(stats) => {
                    let total_pnl = Decimal::from_str(&stats.total_pnl).unwrap_or_default();
                    let today_pnl = Decimal::from_str(&stats.today_pnl).unwrap_or_default();
                    let total_wl = stats.wins + stats.losses;
                    let win_rate = if total_wl > 0 {
                        stats.wins as f64 / total_wl as f64
                    } else {
                        0.0
                    };
                    let mut dash = btc_dash.write();
                    dash.total_pnl = total_pnl;
                    dash.today_pnl = today_pnl;
                    dash.wins = stats.wins as u32;
                    dash.losses = stats.losses as u32;
                    dash.total_periods = stats.total_periods as u32;
                    dash.total_fills = stats.total_fills as u32;
                    dash.win_rate = win_rate;
                    dash.avg_per_trade = if stats.total_periods > 0 {
                        total_pnl / Decimal::from(stats.total_periods)
                    } else {
                        Decimal::ZERO
                    };
                    info!(
                        %total_pnl, %today_pnl, wins = stats.wins, losses = stats.losses,
                        periods = stats.total_periods, fills = stats.total_fills,
                        "Loaded session stats from DB into BTC dashboard"
                    );
                }
                Err(e) => warn!("Failed to load session stats: {e}"),
            }

            // Load period history (has asset column — all assets included)
            match db.get_period_results().await {
                Ok(results) => {
                    let count = results.len();
                    btc_dash.write().period_history = results;
                    info!(count, "Loaded period history from DB");
                }
                Err(e) => warn!("Failed to load period history: {e}"),
            }

            // Load equity curve
            match db.get_equity_curve().await {
                Ok(points) => {
                    let count = points.len();
                    {
                        let mut dash = btc_dash.write();
                        for pt in &points {
                            let val: f64 = pt.cumulative_pnl.parse().unwrap_or(0.0);
                            dash.push_equity(val);
                            dash.push_pnl(val);
                        }
                        dash.equity_curve_db = points;
                    }
                    info!(count, "Loaded equity curve from DB");
                }
                Err(e) => warn!("Failed to load equity curve: {e}"),
            }
        }
    }

    // ── Step 10b: Set up Ctrl+C handler ──
    {
        let shutdown_tx = shutdown_tx.clone();
        let sf = shutdown_flag.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for Ctrl+C");
            info!("Ctrl+C received, initiating shutdown...");
            sf.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = shutdown_tx.send(());
        });
    }

    // ── Step 9: Spawn web dashboard server ──
    {
        let web_map = dashboard_map.clone();
        let web_ctrl = bot_control.clone();
        let web_sf = shutdown_flag.clone();
        let web_lt = latency_tracker.clone();
        tokio::spawn(async move {
            crate::web::start_web_server(
                web_map,
                web_ctrl,
                web_sf,
                4000,
                prometheus_handle,
                web_lt,
            )
            .await;
        });
    }

    // ── Step 10: Create and run per-asset orchestrators ──
    info!("All systems initialized. Starting main loop...");
    let _ = alert_tx.send(AlertMessage::System("Bot fully initialized".to_string()));

    let base_v2_config = match &v2_raw_section {
        Some(raw_v2) => V2Config::from_raw(raw_v2),
        None => V2Config::default(),
    };
    info!(?base_v2_config, "V2 base config loaded");

    let asset_profiles: Vec<AssetRunProfile> = asset_configs
        .iter()
        .filter(|(_, asset_raw)| asset_raw.enabled.unwrap_or(true))
        .map(|(asset, asset_raw)| {
            build_asset_run_profile(*asset, &config, &base_v2_config, asset_raw)
        })
        .collect();
    let run_id = format!(
        "{}_{}",
        Utc::now().format("%Y%m%dT%H%M%S%.3fZ"),
        config.mode.as_str()
    );
    let run_manifest = RunManifest::build(
        run_id.clone(),
        config.mode,
        secrets.wallet_address.clone(),
        config_path,
        config.eoa_mode,
        asset_profiles,
    )?;
    let manifest_path = run_manifest.persist(config.mode.artifact_root())?;
    info!(
        run_id = %run_id,
        manifest_path = %manifest_path.display(),
        "Run manifest written"
    );

    let mut handles = Vec::new();
    let mut per_asset_inventories: Vec<Arc<InventoryManager>> = Vec::new();

    for (asset, asset_raw) in &asset_configs {
        let enabled = asset_raw.enabled.unwrap_or(true);
        if !enabled {
            info!(%asset, "Asset disabled in config, skipping");
            continue;
        }

        // Per-asset channels
        let (market_tx, market_rx) = mpsc::channel::<TrackedMarket>(32);
        let (fill_tx, fill_rx) = mpsc::channel::<FillEvent>(500);
        let (order_update_tx, order_update_rx) = mpsc::channel::<(OrderId, String)>(500);

        // Per-asset inventory manager (independent budgets)
        let mut asset_inventory_mgr = InventoryManager::new(&config);
        {
            let onchain_ref = onchain.clone();
            asset_inventory_mgr
                .set_usdc_balance_fn(Arc::new(move || onchain_ref.cached_usdc_balance_value()));
        }
        // Wire aggregate P&L tracker so emergency handler sees cross-asset losses
        asset_inventory_mgr.set_aggregate_pnl(aggregate_daily_pnl_cents.clone());
        // Apply per-asset budget as max_position_per_market override
        if let Some(ref budget_str) = asset_raw.budget {
            if let Ok(budget) = Decimal::from_str(budget_str) {
                asset_inventory_mgr.set_max_position_per_market(budget);
                info!(%asset, %budget, "Per-asset budget override applied to max_position_per_market");
            }
        }
        // Apply canary budget to inventory manager so it actually restricts order placement.
        // The orchestrator also applies this to config.max_position_per_market, but the
        // InventoryManager has its own copy that must be set here before wrapping in Arc.
        if config.canary_mode {
            if let Some(budget) = config.canary_budget {
                asset_inventory_mgr.set_max_position_per_market(budget);
                info!(%asset, %budget, "Canary budget applied to inventory manager");
            }
        }
        // FIX: Seed per-asset inventory with today's daily P&L so the local
        // daily loss check in can_place_order() doesn't forget pre-restart losses.
        // The global aggregate atomic is already seeded (line ~398), but each
        // per-asset InventoryManager also has its own daily_pnl check.
        // NOTE: This seeds ALL per-asset inventories with the TOTAL daily PnL,
        // which is conservative — it may over-restrict individual assets but
        // ensures the safety limit is respected across restarts.
        if daily_pnl != Decimal::ZERO {
            asset_inventory_mgr.set_daily_pnl(daily_pnl);
        }
        let asset_inventory = Arc::new(asset_inventory_mgr);
        if config.mode.starts_with_clean_positions() {
            asset_inventory.load_positions(Vec::new());
        }
        per_asset_inventories.push(asset_inventory.clone());

        // Per-asset V2 config (apply asset-specific overrides)
        let v2_config = apply_asset_overrides(&base_v2_config, asset_raw);

        // Spawn market discovery for this asset
        {
            let market_tx = market_tx.clone();
            let discovery_interval = config.market_discovery_interval_secs;
            let sdk_clone = sdk.clone();
            let asset_copy = *asset;
            let durations = v2_config.allowed_durations.clone();

            tokio::spawn(async move {
                market_discovery_loop(
                    asset_copy,
                    market_tx,
                    discovery_interval,
                    sdk_clone,
                    durations,
                )
                .await;
            });
        }

        // Per-asset dashboard (already created above)
        let asset_dashboard = dashboard_map
            .get(asset.display_name())
            .expect("dashboard must exist for enabled asset")
            .clone();

        // Create orchestrator
        let mut orch = OrchestratorV2::new(
            *asset,
            config.clone(),
            v2_config,
            run_id.clone(),
            db.clone(),
            asset_inventory,
            emergency.clone(),
            onchain.clone(),
            sdk.clone(),
            alert_tx.clone(),
            fill_tx,
            order_update_tx,
            strategy_heartbeat_tx.clone(),
            shutdown_tx.clone(),
            asset_dashboard,
            bot_control.clone(),
            latency_tracker.clone(),
        );
        orch.start_binance_feed();
        orch.start_rtds_feed();

        let shutdown_rx = shutdown_tx.subscribe();
        let sf = shutdown_flag.clone();

        info!(%asset, "Spawning orchestrator");
        handles.push(tokio::spawn(async move {
            orch.run(market_rx, fill_rx, order_update_rx, shutdown_rx, sf)
                .await;
        }));
    }

    // Re-wire SDK sell guard to per-asset inventories (the per-asset inventories
    // are where tokens are registered and positions tracked — the global inventory
    // set up earlier doesn't have token registrations).
    if let Some(ref sdk) = sdk {
        if !per_asset_inventories.is_empty() {
            let invs = per_asset_inventories;
            sdk.set_sell_position_guard(Arc::new(move |token_id: &str, _size: Decimal| {
                for inv in &invs {
                    let available = inv.available_to_sell_by_token(token_id);
                    if available > Decimal::ZERO {
                        return available;
                    }
                }
                Decimal::ZERO
            }));
            info!("SDK sell position guard re-wired to per-asset inventories");
        }
    }

    // Wait for all orchestrators to finish
    for handle in handles {
        let _ = handle.await;
    }

    info!("Bot shut down cleanly");
    Ok(())
}

/// Market discovery loop: polls Gamma API for 15-minute crypto markets.
/// FIX 14: Real Gamma API implementation.
async fn market_discovery_loop(
    asset: Asset,
    market_tx: mpsc::Sender<TrackedMarket>,
    interval_secs: u64,
    sdk: Option<Arc<SdkClients>>,
    allowed_durations: Vec<u32>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));

    // Fallback Gamma client if SDK is None (paper mode without auth)
    let gamma = polymarket_client_sdk::gamma::Client::default();
    let tag = asset.gamma_tag().to_string();

    loop {
        interval.tick().await;

        let events = if let Some(ref sdk) = sdk {
            sdk.discover_events(&tag).await
        } else {
            // Paper mode: use standalone Gamma client
            let now = Utc::now();
            let req = polymarket_client_sdk::gamma::types::request::EventsRequest::builder()
                .limit(200)
                .active(true)
                .closed(false)
                .end_date_min(now)
                .tag_slug(tag.clone())
                .build();
            gamma
                .events(&req)
                .await
                .map_err(|e| crate::error::BotError::Sdk(format!("{e}")))
        };

        match events {
            Ok(events) => {
                let markets = filter_markets(asset, events, &allowed_durations);
                for market in markets {
                    if market_tx.send(market).await.is_err() {
                        info!("[{asset}] Market channel closed, stopping discovery");
                        return;
                    }
                }
            }
            Err(e) => {
                warn!("[{asset}] Market discovery failed: {e}");
            }
        }
    }
}

/// Filter Gamma events for Up/Down markets matching allowed durations for a given asset.
fn filter_markets(
    asset: Asset,
    events: Vec<polymarket_client_sdk::gamma::types::response::Event>,
    allowed_durations: &[u32],
) -> Vec<TrackedMarket> {
    let now = Utc::now();
    let mut results = Vec::new();
    let prefix_lower = format!("{} up or down", asset.market_prefix().to_lowercase());

    for event in events {
        // Check if event has markets
        let markets = match event.markets {
            Some(m) => m,
            None => continue,
        };

        for market in markets {
            // Must have a question
            let question = match &market.question {
                Some(q) => q.clone(),
                None => continue,
            };

            // Filter for Up/Down markets for this asset.
            // NOTE: Gamma API `start_date` is the market *creation* time, NOT
            // the period start.  Use title parsing to verify duration
            // and derive the real period start from `end_date - duration`.
            let q_lower = question.to_lowercase();
            let is_up_down = q_lower.contains(&prefix_lower);
            if !is_up_down {
                continue;
            }

            // Must be active
            if market.active != Some(true) {
                continue;
            }

            // Must have an end_date in the future
            let end_date = match market.end_date {
                Some(d) if d > now => d,
                _ => continue,
            };
            // Gamma `start_date` is creation time — derive real period start
            // from end_date once we confirm duration via title parsing.
            let duration_mins = match parse_market_duration_minutes(&question) {
                Some(d) if allowed_durations.contains(&d) => d,
                _ => continue,
            };
            let start_date = Some(end_date - chrono::Duration::minutes(duration_mins as i64));

            // Must have condition_id
            let condition_id = match market.condition_id {
                Some(cid) => format!("{cid:?}"),
                None => continue,
            };

            // Must have token IDs
            let token_ids = match market.clob_token_ids {
                Some(ref ids) if ids.len() >= 2 => ids.clone(),
                _ => continue,
            };

            let tick_size = market
                .order_price_min_tick_size
                .unwrap_or(rust_decimal_macros::dec!(0.01));

            // Read neg_risk from Gamma event (defaults to false for standard markets)
            let neg_risk = event.neg_risk.unwrap_or(false);

            results.push(TrackedMarket {
                condition_id,
                token_id_yes: token_ids[0].to_string(),
                token_id_no: token_ids[1].to_string(),
                question,
                start_date,
                end_date,
                tick_size,
                neg_risk,
            });
        }
    }

    if !results.is_empty() {
        info!(count = results.len(), asset = %asset, durations = ?allowed_durations, "Discovered markets");
    }

    results
}

/// Parse the duration in minutes from a market question's time range.
/// Matches patterns like "7:00AM-7:15AM" (15 min), "7:10AM-7:15AM" (5 min), etc.
/// Also matches hourly markets like "March 4, 9AM ET" (60 min).
/// Returns `Some(duration_minutes)` on success, `None` if no valid time range found.
fn parse_market_duration_minutes(question: &str) -> Option<u32> {
    let q = question;
    for (i, _) in q.match_indices(':') {
        if i < 1 || i + 7 > q.len() {
            continue;
        }
        let after_colon = &q[i + 1..];
        if after_colon.len() < 5 {
            continue;
        }
        let start_mins: u32 = match after_colon[..2].parse() {
            Ok(m) => m,
            Err(_) => continue,
        };
        let ampm_dash = &after_colon[2..];
        let (start_is_pm, rest) = if ampm_dash.starts_with("AM-") {
            (false, &ampm_dash[3..])
        } else if ampm_dash.starts_with("PM-") {
            (true, &ampm_dash[3..])
        } else {
            continue;
        };
        let before_colon = &q[..i];
        let start_hour_str: String = before_colon
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>()
            .chars()
            .rev()
            .collect();
        let start_hour: u32 = match start_hour_str.parse() {
            Ok(h) => h,
            Err(_) => continue,
        };
        let colon_pos = match rest.find(':') {
            Some(p) => p,
            None => continue,
        };
        let end_hour: u32 = match rest[..colon_pos].parse() {
            Ok(h) => h,
            Err(_) => continue,
        };
        let after_end_colon = &rest[colon_pos + 1..];
        if after_end_colon.len() < 4 {
            continue;
        }
        let end_mins: u32 = match after_end_colon[..2].parse() {
            Ok(m) => m,
            Err(_) => continue,
        };
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
            } else {
                if hour == 12 {
                    0
                } else {
                    hour
                }
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

    // Fallback: Hourly markets use "March 4, 9AM ET" format (no colons, single time).
    // 5m/15m markets always have colons in their time ranges.
    {
        let q_upper = q.to_uppercase();
        if (q_upper.contains("AM ET") || q_upper.contains("PM ET")) && !q_upper.contains(':') {
            return Some(60);
        }
    }

    None
}

/// Apply per-asset overrides from `[btc]`/`[eth]` config sections onto a base V2Config.
fn apply_asset_overrides(base: &V2Config, overrides: &AssetRawConfig) -> V2Config {
    let mut v2 = base.clone();
    if let Some(ref s) = overrides.base_order_shares {
        if let Ok(d) = Decimal::from_str(s) {
            v2.level_order_size = d;
        }
    }
    if let Some(ref s) = overrides.max_share_imbalance {
        if let Ok(d) = Decimal::from_str(s) {
            v2.max_abs_imbalance = d;
        }
    }
    if let Some(ref s) = overrides.imbalance_decay_floor_abs {
        if let Ok(d) = Decimal::from_str(s) {
            v2.imbalance_decay_floor_abs = d;
        }
    }
    if let Some(ref s) = overrides.one_sided_threshold {
        if let Ok(d) = Decimal::from_str(s) {
            v2.one_sided_threshold = d;
        }
    }
    if let Some(ref s) = overrides.target_combined {
        if let Ok(d) = Decimal::from_str(s) {
            v2.target_combined = d;
        }
    }
    if let Some(sigma) = overrides.max_sigma {
        v2.max_sigma = sigma;
    }
    if let Some(levels) = overrides.ladder_levels {
        v2.ladder_levels = levels;
    }
    if let Some(levels) = overrides.ladder_levels_5m {
        v2.ladder_levels_5m = Some(levels);
    }
    if let Some(levels) = overrides.ladder_levels_15m {
        v2.ladder_levels_15m = Some(levels);
    }
    v2
}

fn build_asset_run_profile(
    asset: Asset,
    config: &crate::config::ValidatedConfig,
    base_v2: &V2Config,
    overrides: &AssetRawConfig,
) -> AssetRunProfile {
    let v2 = apply_asset_overrides(base_v2, overrides);
    let mut max_position = config.max_position_per_market;
    if let Some(ref budget_str) = overrides.budget {
        if let Ok(budget) = Decimal::from_str(budget_str) {
            max_position = budget;
        }
    }
    if config.canary_mode {
        if let Some(budget) = config.canary_budget {
            max_position = budget;
        }
    }

    AssetRunProfile {
        asset: asset.display_name().to_string(),
        max_position_per_market: max_position.to_string(),
        base_order_shares: v2.level_order_size.to_string(),
        target_combined: v2.target_combined.to_string(),
        max_combined_avg_cost: v2.max_per_order_combined.to_string(),
        light_side_max_combined: v2.light_side_max_combined.to_string(),
        max_share_imbalance: v2.max_abs_imbalance.to_string(),
        one_sided_threshold: v2.one_sided_threshold.to_string(),
        trading_window_start_pct: v2.trading_window_start_pct,
        trading_window_end_pct: v2.trading_window_end_pct,
        allowed_durations: v2.allowed_durations.clone(),
        ladder_levels: v2.ladder_levels,
        ladder_levels_5m: v2.ladder_levels_5m,
        ladder_levels_15m: v2.ladder_levels_15m,
        ladder_levels_60m: v2.ladder_levels_60m,
        buy_level_activation_limit_5m: v2.buy_level_activation_limit_5m,
        merge_at_closing: v2.merge_at_closing,
        continuous_merge_enabled: v2.continuous_merge_enabled,
        period_gross_buy_cap_usdc: v2.period_gross_buy_cap_usdc.to_string(),
        single_order_notional_cap_usdc: v2.single_order_notional_cap_usdc.to_string(),
    }
}
