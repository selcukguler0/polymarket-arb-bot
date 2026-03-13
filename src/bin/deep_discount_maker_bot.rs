//! Deep Discount Maker Bot — 0xd0d605-style two-sided market making
//!
//! Places wide grid bids on BOTH Up and Down across $0.05-$0.50, accepts
//! directional risk, sells excess inventory, holds to resolution.
//!
//! Run:
//!   cargo run --release --bin deep_discount_maker_bot                      # paper mode
//!   cargo run --release --bin deep_discount_maker_bot -- --live            # live mode
//!   cargo run --release --bin deep_discount_maker_bot -- --live --dry-run  # live books, log only

use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use tracing::info;

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as _;
use polymarket_client_sdk::POLYGON;

use polymarket_arb::strategies::core::{Executor, RedeemContext};
use polymarket_arb::strategies::deep_discount_maker::{self, DeepDiscountConfig};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn parse_args() -> (bool, bool) {
    let args: Vec<String> = std::env::args().collect();
    let live = args.iter().any(|a| a == "--live");
    let dry_run = args.iter().any(|a| a == "--dry-run");
    (live, dry_run)
}

fn load_config_overrides(config: &mut DeepDiscountConfig) {
    if let Ok(v) = std::env::var("DDM_GRID_MIN") {
        if let Ok(d) = v.parse::<Decimal>() {
            config.grid_min_price = d;
        }
    }
    if let Ok(v) = std::env::var("DDM_GRID_MAX") {
        if let Ok(d) = v.parse::<Decimal>() {
            config.grid_max_price = d;
        }
    }
    if let Ok(v) = std::env::var("DDM_GRID_STEP") {
        if let Ok(d) = v.parse::<Decimal>() {
            config.grid_step = d;
        }
    }
    if let Ok(v) = std::env::var("DDM_SHARES_PER_LEVEL") {
        if let Ok(d) = v.parse::<Decimal>() {
            config.shares_per_level = d;
        }
    }
    if let Ok(v) = std::env::var("DDM_SELL_THRESHOLD") {
        if let Ok(d) = v.parse::<Decimal>() {
            config.sell_imbalance_threshold = d;
        }
    }
    if let Ok(v) = std::env::var("DDM_SELL_ENABLED") {
        config.sell_enabled = v == "true" || v == "1";
    }
    if let Ok(v) = std::env::var("DDM_MAX_POSITION") {
        if let Ok(d) = v.parse::<Decimal>() {
            config.max_position_per_side = d;
        }
    }
    if let Ok(v) = std::env::var("DDM_MAX_SPEND") {
        if let Ok(d) = v.parse::<Decimal>() {
            config.max_total_spend = d;
        }
    }
    if let Ok(v) = std::env::var("DDM_COINS") {
        config.coins = v.split(',').map(|s| s.trim().to_string()).collect();
    }
    if let Ok(v) = std::env::var("DDM_DURATIONS") {
        config.allowed_durations = v
            .split(',')
            .filter_map(|s| s.trim().parse::<u32>().ok())
            .collect();
    }
    if let Ok(v) = std::env::var("DDM_ENTRY_DELAY") {
        if let Ok(s) = v.parse::<u64>() {
            config.entry_delay_secs = s;
        }
    }
    if let Ok(v) = std::env::var("DDM_STOP_NEW_ORDERS") {
        if let Ok(s) = v.parse::<u64>() {
            config.stop_new_orders_secs = s;
        }
    }
    if let Ok(v) = std::env::var("DDM_CANCEL_ALL") {
        if let Ok(s) = v.parse::<u64>() {
            config.cancel_all_secs = s;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,polymarket_arb=debug".parse().unwrap()),
        )
        .init();

    let (live, dry_run) = parse_args();

    let mut config = DeepDiscountConfig::default();
    config.live = live;
    config.dry_run = dry_run;
    load_config_overrides(&mut config);

    let grid = config.grid_levels();
    let grid_str: Vec<String> = grid.iter().map(|l| l.to_string()).collect();
    info!(
        "Deep Discount Maker starting | live={} dry_run={} | grid=[{}] x {} shares/level | sell_threshold={}",
        config.live,
        config.dry_run,
        grid_str.join(","),
        config.shares_per_level,
        config.sell_imbalance_threshold,
    );

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

    deep_discount_maker::run(config, executor, redeem_ctx).await
}
