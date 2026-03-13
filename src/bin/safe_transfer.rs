//! Transfer USDC from the Gnosis Safe wallet to any address (gasless).
//!
//! Usage:
//!   cargo run --release --bin safe_transfer -- <to_address> <amount_usdc>
//!
//! Examples:
//!   cargo run --release --bin safe_transfer -- <destination_address> 50.00
//!   cargo run --release --bin safe_transfer -- <destination_address> all

use std::str::FromStr;

use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as _;
use alloy::sol;
use alloy::sol_types::SolCall;
use polymarket_client_sdk::POLYGON;
use tracing::{error, info};

use polymarket_arb::config::Secrets;
use polymarket_arb::error::{BotError, Result};
use polymarket_arb::relayer::{RelayerClient, RelayerTransaction};

sol! {
    function transfer(address to, uint256 amount) returns (bool);
    function balanceOf(address account) returns (uint256);
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: safe_transfer <to_address> <amount_usdc|all>");
        eprintln!("Example: safe_transfer 0x9ddA...fEc9 50.00");
        eprintln!("Example: safe_transfer 0x9ddA...fEc9 all");
        std::process::exit(1);
    }

    let to_address = Address::from_str(&args[1])
        .map_err(|e| BotError::Config(format!("Invalid to address: {e}")))?;
    let amount_arg = &args[2];

    let secrets = Secrets::from_env()?;

    let signer = PrivateKeySigner::from_str(&secrets.private_key)
        .map_err(|e| BotError::Config(format!("Invalid private key: {e}")))?
        .with_chain_id(Some(POLYGON));

    let usdc_address = Address::from_str("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
        .map_err(|e| BotError::Config(format!("Invalid USDC address: {e}")))?;

    let relayer = RelayerClient::new(
        signer,
        usdc_address,
        secrets.builder_key,
        secrets.builder_secret,
        secrets.builder_passphrase,
    )?;

    info!("Safe address: {:?}", relayer.safe_address());
    info!("Transfer to:  {:?}", to_address);

    // Check current USDC balance in the Safe
    let rpc_url = secrets
        .polygon_rpc_url
        .parse()
        .map_err(|e| BotError::OnChain(format!("Invalid RPC URL: {e}")))?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let balance_call = balanceOfCall {
        account: relayer.safe_address(),
    };
    let call_data = balance_call.abi_encode();

    let result = provider
        .call(
            alloy::rpc::types::TransactionRequest::default()
                .to(usdc_address)
                .input(call_data.into()),
        )
        .await
        .map_err(|e| BotError::OnChain(format!("Balance check failed: {e}")))?;

    let balance_raw = U256::from_be_slice(&result);
    let balance_usdc = balance_raw.to::<u64>() as f64 / 1_000_000.0;

    info!("Safe USDC balance: ${:.2}", balance_usdc);

    if balance_usdc < 0.01 {
        error!("No USDC in Safe to transfer");
        return Err(BotError::OnChain("No USDC balance".into()));
    }

    // Determine amount to transfer
    let amount_raw = if amount_arg == "all" {
        info!("Transferring ALL: ${:.2}", balance_usdc);
        balance_raw
    } else {
        let amount: f64 = amount_arg
            .parse()
            .map_err(|e| BotError::Config(format!("Invalid amount: {e}")))?;
        if amount <= 0.0 {
            return Err(BotError::Config("Amount must be positive".into()));
        }
        let raw = (amount * 1_000_000.0) as u64;
        let raw_u256 = U256::from(raw);
        if raw_u256 > balance_raw {
            error!(
                "Requested ${:.2} but only ${:.2} available",
                amount, balance_usdc
            );
            return Err(BotError::OnChain("Insufficient balance".into()));
        }
        info!("Transferring ${:.2}", amount);
        raw_u256
    };

    // Build transfer transaction
    let call = transferCall {
        to: to_address,
        amount: amount_raw,
    };
    let tx = RelayerTransaction {
        to: usdc_address,
        data: alloy::primitives::Bytes::from(call.abi_encode()),
        value: U256::ZERO,
    };

    info!("Executing gasless USDC transfer via relayer...");

    match relayer.execute(&[tx], "USDC transfer").await {
        Ok(tx_hash) => {
            info!("Transfer successful! TX: {tx_hash}");
            info!(
                "Sent ${:.2} USDC to {:?}",
                amount_raw.to::<u64>() as f64 / 1_000_000.0,
                to_address
            );
        }
        Err(e) => {
            error!("Transfer failed: {e}");
            return Err(e);
        }
    }

    Ok(())
}
