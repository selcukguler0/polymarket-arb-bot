//! One-time setup: approve USDC and CTF tokens on all exchange contracts.
//!
//! Requires EOA mode — the EOA must hold USDC and have POL for gas.
//! Uses `polymarket_client_sdk::contract_config()` for contract addresses.
//!
//! Usage:
//!   cargo run --bin setup_approvals               # Execute approvals
//!   cargo run --bin setup_approvals -- --dry-run   # Preview only
//!   cargo run --bin setup_approvals -- --check-only # Verify existing approvals

use std::str::FromStr;

use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as _;
use alloy::sol;

use polymarket_client_sdk::{contract_config, POLYGON};

// ABI fragments for ERC-20 approval and ERC-1155 setApprovalForAll
sol! {
    #[sol(rpc)]
    interface IERC20 {
        function approve(address spender, uint256 amount) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);
    }
}

sol! {
    #[sol(rpc)]
    interface IERC1155 {
        function setApprovalForAll(address operator, bool approved) external;
        function isApprovedForAll(address account, address operator) external view returns (bool);
    }
}

fn separator(label: &str) {
    println!("\n{}", "=".repeat(60));
    println!("  {label}");
    println!("{}\n", "=".repeat(60));
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Install TLS crypto provider before any network calls
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Parse CLI args
    let args: Vec<String> = std::env::args().collect();
    let dry_run = args.iter().any(|a| a == "--dry-run");
    let check_only = args.iter().any(|a| a == "--check-only");

    if dry_run {
        println!("=== DRY RUN MODE — no transactions will be sent ===\n");
    } else if check_only {
        println!("=== CHECK-ONLY MODE — just verifying existing approvals ===\n");
    }

    // Load .env
    dotenvy::dotenv().ok();
    let private_key =
        std::env::var("POLYMARKET_PRIVATE_KEY").expect("POLYMARKET_PRIVATE_KEY not set in .env");
    let rpc_url = std::env::var("POLYGON_RPC_URL").expect("POLYGON_RPC_URL not set in .env");

    // Derive wallet address from private key (no need for separate WALLET_ADDRESS)
    let signer = PrivateKeySigner::from_str(&private_key)
        .expect("Invalid private key")
        .with_chain_id(Some(POLYGON));
    let wallet_address = signer.address();
    println!("EOA address: {wallet_address}");

    // Get contract addresses from SDK
    let config = contract_config(POLYGON, false).expect("No standard config for Polygon");
    let neg_risk_config = contract_config(POLYGON, true).expect("No neg-risk config for Polygon");

    println!("\nStandard contracts:");
    println!("  Exchange:           {:?}", config.exchange);
    println!("  Collateral (USDC):  {:?}", config.collateral);
    println!("  Conditional Tokens: {:?}", config.conditional_tokens);

    println!("\nNeg-Risk contracts:");
    println!("  Exchange:           {:?}", neg_risk_config.exchange);
    println!(
        "  Neg Risk Adapter:   {:?}",
        neg_risk_config.neg_risk_adapter
    );

    // Create provider (read-only for check, signing for write)
    let url = rpc_url.parse().expect("Invalid RPC URL");

    if check_only || dry_run {
        // Read-only provider
        let provider = ProviderBuilder::new().connect_http(url);
        check_all_approvals(&provider, wallet_address, config, neg_risk_config).await?;

        if dry_run {
            println!("\n--- DRY RUN: Would set the following approvals ---");
            print_planned_approvals(config, neg_risk_config);
        }
    } else {
        // Signing provider for transactions
        let provider = ProviderBuilder::new()
            .wallet(signer)
            .connect_http(url.clone());

        separator("BEFORE — Current Approvals");
        // Use a read-only provider for checks (the signing provider also implements Provider)
        check_all_approvals(&provider, wallet_address, config, neg_risk_config).await?;

        separator("Setting Approvals");
        set_all_approvals(&provider, wallet_address, config, neg_risk_config).await?;

        separator("AFTER — Verification");
        check_all_approvals(&provider, wallet_address, config, neg_risk_config).await?;
    }

    println!("\nDone.");
    Ok(())
}

async fn check_all_approvals<P: Provider>(
    provider: &P,
    owner: Address,
    config: &polymarket_client_sdk::ContractConfig,
    neg_risk_config: &polymarket_client_sdk::ContractConfig,
) -> anyhow::Result<()> {
    let usdc = IERC20::new(config.collateral, provider);
    let ctf = IERC1155::new(config.conditional_tokens, provider);

    // Contracts that need USDC approval
    let usdc_spenders = [
        ("CTF Exchange", config.exchange),
        ("NegRisk Exchange", neg_risk_config.exchange),
        ("Conditional Tokens", config.conditional_tokens),
    ];

    // Contracts that need ERC-1155 approval (setApprovalForAll)
    let mut ctf_operators = vec![
        ("CTF Exchange", config.exchange),
        ("NegRisk Exchange", neg_risk_config.exchange),
    ];
    if let Some(adapter) = neg_risk_config.neg_risk_adapter {
        ctf_operators.push(("NegRisk Adapter", adapter));
    }

    println!("USDC Allowances:");
    for (label, spender) in &usdc_spenders {
        let allowance = usdc.allowance(owner, *spender).call().await?;
        let status = if allowance >= U256::from(1u128 << 127) {
            "MAX (OK)"
        } else if allowance > U256::ZERO {
            "partial"
        } else {
            "ZERO (needs approval)"
        };
        println!("  {label:25} → {status}  (raw: {allowance})");
    }

    println!("\nERC-1155 Approvals:");
    for (label, operator) in &ctf_operators {
        let approved = ctf.isApprovedForAll(owner, *operator).call().await?;
        let status = if approved {
            "YES (OK)"
        } else {
            "NO (needs approval)"
        };
        println!("  {label:25} → {status}");
    }

    Ok(())
}

fn print_planned_approvals(
    config: &polymarket_client_sdk::ContractConfig,
    neg_risk_config: &polymarket_client_sdk::ContractConfig,
) {
    println!("\nUSDC approve(spender, U256::MAX):");
    println!("  • CTF Exchange:      {:?}", config.exchange);
    println!("  • NegRisk Exchange:  {:?}", neg_risk_config.exchange);
    println!("  • Conditional Tokens: {:?}", config.conditional_tokens);

    println!("\nCTF setApprovalForAll(operator, true):");
    println!("  • CTF Exchange:      {:?}", config.exchange);
    println!("  • NegRisk Exchange:  {:?}", neg_risk_config.exchange);
    if let Some(adapter) = neg_risk_config.neg_risk_adapter {
        println!("  • NegRisk Adapter:   {:?}", adapter);
    }
}

async fn set_all_approvals<P: Provider>(
    provider: &P,
    owner: Address,
    config: &polymarket_client_sdk::ContractConfig,
    neg_risk_config: &polymarket_client_sdk::ContractConfig,
) -> anyhow::Result<()> {
    let usdc = IERC20::new(config.collateral, provider);
    let ctf = IERC1155::new(config.conditional_tokens, provider);

    // USDC approvals
    let usdc_spenders = [
        ("CTF Exchange", config.exchange),
        ("NegRisk Exchange", neg_risk_config.exchange),
        ("Conditional Tokens", config.conditional_tokens),
    ];

    for (label, spender) in &usdc_spenders {
        // Check current allowance first
        let allowance = usdc.allowance(owner, *spender).call().await?;
        if allowance >= U256::from(1u128 << 127) {
            println!("  {label}: USDC already approved (skipping)");
            continue;
        }

        println!("  {label}: Approving USDC (U256::MAX)...");
        let tx_hash = usdc
            .approve(*spender, U256::MAX)
            .send()
            .await?
            .watch()
            .await?;
        println!("    tx: {tx_hash:?}");
    }

    // ERC-1155 approvals
    let mut ctf_operators = vec![
        ("CTF Exchange", config.exchange),
        ("NegRisk Exchange", neg_risk_config.exchange),
    ];
    if let Some(adapter) = neg_risk_config.neg_risk_adapter {
        ctf_operators.push(("NegRisk Adapter", adapter));
    }

    for (label, operator) in &ctf_operators {
        let approved = ctf.isApprovedForAll(owner, *operator).call().await?;
        if approved {
            println!("  {label}: CTF already approved (skipping)");
            continue;
        }

        println!("  {label}: Setting CTF approval...");
        let tx_hash = ctf
            .setApprovalForAll(*operator, true)
            .send()
            .await?
            .watch()
            .await?;
        println!("    tx: {tx_hash:?}");
    }

    Ok(())
}
