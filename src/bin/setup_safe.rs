//! One-time setup: deploy Gnosis Safe wallet and approve all contracts.
//!
//! Usage:
//!   cargo run --release --bin setup_safe
//!
//! This will:
//! 1. Derive the deterministic Safe address from your EOA
//! 2. Deploy the Safe wallet via Polymarket's relayer (gasless)
//! 3. Approve all exchange contracts to spend USDC and outcome tokens
//!
//! After running this, update config/v2.toml:
//!   - Set `eoa_mode = false`
//!   - Set `wallet_type = "safe"` (when implemented)
//!   - Transfer USDC from EOA to the Safe address printed below

use std::str::FromStr;

use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as _;
use polymarket_client_sdk::POLYGON;
use tracing::{error, info};

use polymarket_arb::config::Secrets;
use polymarket_arb::error::Result;
use polymarket_arb::relayer::RelayerClient;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt().with_env_filter("info").init();

    let secrets = Secrets::from_env()?;

    let signer = PrivateKeySigner::from_str(&secrets.private_key)
        .map_err(|e| polymarket_arb::error::BotError::Config(format!("Invalid private key: {e}")))?
        .with_chain_id(Some(POLYGON));

    let usdc_address =
        Address::from_str("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174").map_err(|e| {
            polymarket_arb::error::BotError::Config(format!("Invalid USDC address: {e}"))
        })?;

    let ctf_exchange =
        Address::from_str("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E").map_err(|e| {
            polymarket_arb::error::BotError::Config(format!("Invalid CTF exchange: {e}"))
        })?;

    let neg_risk_exchange = Address::from_str("0xC5d563A36AE78145C45a50134d48A1215220f80a")
        .map_err(|e| {
            polymarket_arb::error::BotError::Config(format!("Invalid NegRisk exchange: {e}"))
        })?;

    let relayer = RelayerClient::new(
        signer,
        usdc_address,
        secrets.builder_key,
        secrets.builder_secret,
        secrets.builder_passphrase,
    )?;

    info!("EOA address: {:?}", relayer.eoa_address());
    info!("Safe address (CREATE2): {:?}", relayer.safe_address());

    // Step 1: Check if already deployed
    let deployed = relayer.is_deployed().await?;
    if deployed {
        info!("Safe is already deployed!");
    } else {
        info!("Deploying Safe wallet...");
        match relayer.deploy_safe().await {
            Ok(tx) => info!("Safe deployed! TX: {tx}"),
            Err(e) => {
                error!("Safe deployment failed: {e}");
                return Err(e);
            }
        }
    }

    // Step 2: Approve all contracts
    info!("Setting up approvals for all exchange contracts...");
    match relayer
        .approve_all_contracts(ctf_exchange, neg_risk_exchange)
        .await
    {
        Ok(tx) => info!("All approvals set! TX: {tx}"),
        Err(e) => {
            error!("Approval setup failed: {e}");
            return Err(e);
        }
    }

    info!("");
    info!("=== SETUP COMPLETE ===");
    info!("Safe address: {:?}", relayer.safe_address());
    info!("");
    info!("Next steps:");
    info!(
        "  1. Transfer USDC from EOA to Safe: {:?}",
        relayer.safe_address()
    );
    info!("  2. Update config/v2.toml: eoa_mode = false");
    info!(
        "  3. Update .env: WALLET_ADDRESS={:?}",
        relayer.safe_address()
    );

    Ok(())
}
