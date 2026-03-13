//! Post-Resolution Winner Token Buyer Bot
//!
//! Buys winner tokens at a discount to $1.00 after market resolution.
//! Zero directional risk — winner is already known.
//!
//! Run:
//!   cargo run --release --bin post_resolution_maker_bot                      # paper mode
//!   cargo run --release --bin post_resolution_maker_bot -- --live            # live mode
//!   cargo run --release --bin post_resolution_maker_bot -- --live --dry-run  # live book, log only

use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use tracing::info;

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as _;
use polymarket_client_sdk::POLYGON;

use polymarket_arb::strategies::core::{Executor, RedeemContext};
use polymarket_arb::strategies::post_resolution_maker::{self, PostResMakerConfig};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn parse_args() -> (bool, bool) {
    let args: Vec<String> = std::env::args().collect();
    let live = args.iter().any(|a| a == "--live");
    let dry_run = args.iter().any(|a| a == "--dry-run");
    (live, dry_run)
}

fn load_config_overrides(config: &mut PostResMakerConfig) {
    if let Ok(v) = std::env::var("PRM_BID_LEVELS") {
        // Comma-separated: "0.95,0.96,0.97,0.98,0.99"
        let levels: Vec<Decimal> = v
            .split(',')
            .filter_map(|s| s.trim().parse::<Decimal>().ok())
            .collect();
        if !levels.is_empty() {
            config.bid_levels = levels;
        }
    }
    if let Ok(v) = std::env::var("PRM_SHARES_PER_LEVEL") {
        if let Ok(d) = v.parse::<Decimal>() {
            config.shares_per_level = d;
        }
    }
    if let Ok(v) = std::env::var("PRM_MAX_ACTIVE_MARKETS") {
        if let Ok(n) = v.parse::<usize>() {
            config.max_active_markets = n;
        }
    }
    if let Ok(v) = std::env::var("PRM_POLL_MS") {
        if let Ok(ms) = v.parse::<u64>() {
            config.poll_interval_ms = ms;
        }
    }
    if let Ok(v) = std::env::var("PRM_BID_TIMEOUT_SECS") {
        if let Ok(s) = v.parse::<u64>() {
            config.bid_timeout_secs = s;
        }
    }
    if let Ok(v) = std::env::var("PRM_MIN_POST_RES_SECS") {
        if let Ok(s) = v.parse::<i64>() {
            config.min_post_res_secs = s;
        }
    }
    if let Ok(v) = std::env::var("PRM_MAX_POST_RES_SECS") {
        if let Ok(s) = v.parse::<i64>() {
            config.max_post_res_secs = s;
        }
    }
    if let Ok(v) = std::env::var("PRM_COINS") {
        config.coins = v.split(',').map(|s| s.trim().to_string()).collect();
    }
    if let Ok(v) = std::env::var("PRM_DURATIONS") {
        config.allowed_durations = v
            .split(',')
            .filter_map(|s| s.trim().parse::<u32>().ok())
            .collect();
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

    let mut config = PostResMakerConfig::default();
    config.live = live;
    config.dry_run = dry_run;
    load_config_overrides(&mut config);

    let levels_str: Vec<String> = config.bid_levels.iter().map(|l| l.to_string()).collect();
    info!(
        "Winner Token Bot starting | live={} dry_run={} | levels=[{}] x {} shares | timeout={}s",
        config.live,
        config.dry_run,
        levels_str.join(","),
        config.shares_per_level,
        config.bid_timeout_secs,
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
            .or_else(|_| std::env::var("WALLET_ADDRESS"))
            .context("POLYMARKET_WALLET_ADDRESS or WALLET_ADDRESS not set")?;
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

    post_resolution_maker::run(config, executor, redeem_ctx).await
}
