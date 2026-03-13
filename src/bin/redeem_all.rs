//! Scans all wallet positions via Data API and redeems resolved markets on-chain.
//!
//! Usage:
//!   cargo run --bin redeem_all            # Dry-run: list redeemable positions
//!   cargo run --bin redeem_all -- --execute  # Actually redeem on-chain

use std::collections::HashMap;
use std::str::FromStr;

use alloy::primitives::{Address, B256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as _;
use polymarket_client_sdk::ctf;
use polymarket_client_sdk::ctf::types::RedeemPositionsRequest;
use polymarket_client_sdk::data;
use polymarket_client_sdk::data::types::request::PositionsRequest;
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;

const USDC_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

struct RedeemableMarket {
    condition_id: B256,
    title: String,
    negative_risk: bool,
    outcomes: Vec<(String, Decimal)>, // (outcome_name, size)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Install rustls crypto provider FIRST
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // 2. Load .env
    dotenvy::dotenv().ok();
    let private_key =
        std::env::var("POLYMARKET_PRIVATE_KEY").expect("POLYMARKET_PRIVATE_KEY not set in .env");
    let wallet_address = std::env::var("WALLET_ADDRESS").expect("WALLET_ADDRESS not set in .env");
    let rpc_url = std::env::var("POLYGON_RPC_URL").expect("POLYGON_RPC_URL not set in .env");

    // 3. Parse --execute flag
    let execute = std::env::args().any(|a| a == "--execute");

    let signer = PrivateKeySigner::from_str(&private_key)
        .expect("Invalid private key")
        .with_chain_id(Some(POLYGON));

    let eoa_address = signer.address();
    let wallet_addr = Address::from_str(&wallet_address).expect("Invalid WALLET_ADDRESS");

    println!("=== Polymarket Position Redeemer ===");
    println!("EOA signer:     {eoa_address}");
    println!("Wallet (proxy): {wallet_addr}");
    println!(
        "Mode:           {}",
        if execute { "EXECUTE" } else { "DRY-RUN" }
    );
    println!();

    // 4. Query Data API for redeemable positions
    let data_client = data::Client::default();
    let req = PositionsRequest::builder()
        .user(wallet_addr)
        .redeemable(true)
        .size_threshold(Decimal::new(1, 2)) // 0.01 minimum
        .limit(500)
        .expect("invalid limit")
        .build();

    println!("Querying Data API for redeemable positions...");
    let positions = data_client
        .positions(&req)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to query positions: {e}"))?;

    if positions.is_empty() {
        println!("No redeemable positions found.");
        return Ok(());
    }

    println!("Found {} redeemable position entries.\n", positions.len());

    // 5. Group by condition_id
    let mut markets: HashMap<B256, RedeemableMarket> = HashMap::new();
    for pos in &positions {
        if pos.size <= Decimal::ZERO {
            continue;
        }
        let entry = markets
            .entry(pos.condition_id)
            .or_insert_with(|| RedeemableMarket {
                condition_id: pos.condition_id,
                title: pos.title.clone(),
                negative_risk: pos.negative_risk,
                outcomes: Vec::new(),
            });
        entry.outcomes.push((pos.outcome.clone(), pos.size));
    }

    // 6. Print summary
    println!("=== Redeemable Markets ({}) ===\n", markets.len());
    let mut sorted_markets: Vec<_> = markets.values().collect();
    sorted_markets.sort_by(|a, b| a.title.cmp(&b.title));

    for (i, market) in sorted_markets.iter().enumerate() {
        println!(
            "  {}. {} {}",
            i + 1,
            market.title,
            if market.negative_risk {
                "[neg-risk]"
            } else {
                ""
            }
        );
        println!("     Condition: {:?}", market.condition_id);
        for (outcome, size) in &market.outcomes {
            println!("     {outcome}: {size} tokens");
        }
        println!();
    }

    // 7. If not --execute, stop here
    if !execute {
        println!("Dry-run complete. Run with --execute to redeem on-chain.");
        return Ok(());
    }

    // 8. Execute redemptions
    println!("=== Executing Redemptions ===\n");

    let usdc_addr = Address::from_str(USDC_ADDRESS).unwrap();

    let mut success_count = 0u32;
    let mut fail_count = 0u32;

    for market in sorted_markets {
        print!("Redeeming: {} ... ", market.title);

        // Create provider + CTF client for each market (provider is cheap to create)
        let url = rpc_url.parse().expect("Invalid RPC URL");
        let provider = ProviderBuilder::new()
            .wallet(signer.clone())
            .connect_http(url);

        let ctf_client = if market.negative_risk {
            ctf::Client::with_neg_risk(provider, POLYGON)
        } else {
            ctf::Client::new(provider, POLYGON)
        }
        .map_err(|e| anyhow::anyhow!("CTF client creation failed: {e}"))?;

        let redeem_req = RedeemPositionsRequest::for_binary_market(usdc_addr, market.condition_id);

        match ctf_client.redeem_positions(&redeem_req).await {
            Ok(resp) => {
                println!("OK  tx={:?}", resp.transaction_hash);
                success_count += 1;
            }
            Err(e) => {
                println!("FAILED  {e}");
                fail_count += 1;
            }
        }
    }

    println!();
    println!("=== Summary ===");
    println!("  Redeemed: {success_count}");
    println!("  Failed:   {fail_count}");
    if fail_count > 0 {
        println!("  Note: Failures may occur if tokens are in the proxy wallet.");
        println!("  In that case, use the Polymarket UI to redeem.");
    }

    Ok(())
}
