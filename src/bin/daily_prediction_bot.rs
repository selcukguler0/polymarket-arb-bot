//! Daily Prediction Market Pair-Arb Bot
//!
//! Buys both Yes+No tokens on daily BTC/ETH prediction markets when
//! combined ask < $1.00, then redeems at $1.00 per pair for risk-free profit.
//!
//! Run:
//!   cargo run --release --bin daily_prediction_bot                      # paper mode
//!   cargo run --release --bin daily_prediction_bot -- --live            # live mode
//!   cargo run --release --bin daily_prediction_bot -- --live --dry-run  # live book, no orders

use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use tracing::info;

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as _;
use polymarket_client_sdk::POLYGON;

use polymarket_arb::strategies::core::{Executor, RedeemContext};
use polymarket_arb::strategies::daily_prediction::{self, DailyPredictionConfig};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn parse_args() -> (bool, bool) {
    let args: Vec<String> = std::env::args().collect();
    let live = args.iter().any(|a| a == "--live");
    let dry_run = args.iter().any(|a| a == "--dry-run");
    (live, dry_run)
}

fn load_config_overrides(config: &mut DailyPredictionConfig) {
    if let Ok(v) = std::env::var("DP_MAX_COMBINED_COST") {
        if let Ok(d) = v.parse::<Decimal>() {
            config.max_combined_cost = d;
        }
    }
    if let Ok(v) = std::env::var("DP_MIN_MARGIN") {
        if let Ok(d) = v.parse::<Decimal>() {
            config.min_margin = d;
        }
    }
    if let Ok(v) = std::env::var("DP_ORDER_SIZE") {
        if let Ok(d) = v.parse::<Decimal>() {
            config.order_size = d;
        }
    }
    if let Ok(v) = std::env::var("DP_MAX_TOTAL_PAIRS") {
        if let Ok(d) = v.parse::<Decimal>() {
            config.max_total_pairs = d;
        }
    }
    if let Ok(v) = std::env::var("DP_POLL_MS") {
        if let Ok(ms) = v.parse::<u64>() {
            config.poll_interval_ms = ms;
        }
    }
    if let Ok(v) = std::env::var("DP_DISCOVERY_SECS") {
        if let Ok(s) = v.parse::<u64>() {
            config.market_discovery_secs = s;
        }
    }
    if let Ok(v) = std::env::var("DP_MIN_BOOK_SIZE") {
        if let Ok(d) = v.parse::<Decimal>() {
            config.min_book_size = d;
        }
    }
    if let Ok(v) = std::env::var("DP_MIN_HOURS") {
        if let Ok(h) = v.parse::<f64>() {
            config.min_hours_to_resolution = h;
        }
    }
    if let Ok(v) = std::env::var("DP_COINS") {
        config.coins = v.split(',').map(|s| s.trim().to_lowercase()).collect();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // TLS crypto provider
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Load .env
    dotenvy::dotenv().ok();

    // Logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,polymarket_arb=debug".parse().unwrap()),
        )
        .init();

    let (live, dry_run) = parse_args();

    let mut config = DailyPredictionConfig::default();
    config.live = live;
    config.dry_run = dry_run;
    load_config_overrides(&mut config);

    info!(
        "Daily Prediction Bot starting | live={} dry_run={} | max_cost={} margin={} size={} coins={:?}",
        config.live, config.dry_run, config.max_combined_cost, config.min_margin,
        config.order_size, config.coins
    );

    // Set up executor (live mode only)
    let executor = if config.live {
        let pk =
            std::env::var("POLYMARKET_PRIVATE_KEY").context("POLYMARKET_PRIVATE_KEY not set")?;
        let builder_key = std::env::var("POLY_BUILDER_KEY").context("POLY_BUILDER_KEY not set")?;
        let builder_secret =
            std::env::var("POLY_BUILDER_SECRET").context("POLY_BUILDER_SECRET not set")?;
        let builder_passphrase =
            std::env::var("POLY_BUILDER_PASSPHRASE").context("POLY_BUILDER_PASSPHRASE not set")?;

        let exec = Executor::new(
            &pk,
            &builder_key,
            &builder_secret,
            &builder_passphrase,
            dry_run,
        )
        .await
        .context("Failed to create executor")?;

        info!("Executor authenticated (Builder mode)");
        Some(exec)
    } else {
        None
    };

    // Set up redemption context (live mode, not dry-run)
    let redeem_ctx = if config.live && !config.dry_run {
        let pk = std::env::var("POLYMARKET_PRIVATE_KEY")?;
        let wallet = std::env::var("POLYMARKET_WALLET_ADDRESS")
            .context("POLYMARKET_WALLET_ADDRESS not set")?;
        let rpc = std::env::var("POLYGON_RPC_URL")
            .unwrap_or_else(|_| "https://polygon-rpc.com".to_string());

        let signer = PrivateKeySigner::from_str(&pk)
            .context("Invalid private key for redeem")?
            .with_chain_id(Some(POLYGON));

        Some(Arc::new(RedeemContext {
            signer,
            wallet_address: wallet,
            rpc_url: rpc,
        }))
    } else {
        None
    };

    daily_prediction::run(config, executor, redeem_ctx).await
}
