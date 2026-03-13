//! Pre-launch Step 3: Single order round-trip test.
//!
//! Places one low-priced limit bid ($0.05) on YES token where it won't fill,
//! verifies the real order ID, checks it appears in the WS order stream,
//! cancels it, and verifies cancellation.
//!
//! Run: cargo run --bin order_roundtrip
//!
//! Prerequisites: Run smoke_test first to confirm connectivity.

use std::str::FromStr;
use std::time::Duration;

use chrono::Utc;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use polymarket_client_sdk::clob::types::{OrderType, Side};
use polymarket_client_sdk::clob::{self, Config as ClobConfig};
use polymarket_client_sdk::gamma;
use polymarket_client_sdk::gamma::types::request::EventsRequest;
use polymarket_client_sdk::types::Address as SdkAddress;
use polymarket_client_sdk::POLYGON;

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as _;

const CLOB_HOST: &str = "https://clob.polymarket.com";

/// Price low enough that it will never fill
const TEST_BID_PRICE: &str = "0.05";
/// Minimum order size
const TEST_BID_SIZE: &str = "5";

/// Check if a market question contains a 15-minute time range.
fn is_15_minute_range(question: &str) -> bool {
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
        if diff == 15 {
            return true;
        }
    }
    false
}

#[tokio::main]
async fn main() {
    // Install TLS crypto provider before any network calls
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    dotenvy::dotenv().ok();

    let private_key =
        std::env::var("POLYMARKET_PRIVATE_KEY").expect("POLYMARKET_PRIVATE_KEY not set");
    let wallet_address = std::env::var("WALLET_ADDRESS").expect("WALLET_ADDRESS not set");

    println!("=== STEP 3: Single Order Round-Trip Test ===\n");

    // ── Authenticate ──
    let signer = PrivateKeySigner::from_str(&private_key)
        .expect("Invalid private key")
        .with_chain_id(Some(POLYGON));

    let wallet_addr = SdkAddress::from_str(&wallet_address).expect("Invalid wallet address");

    let clob_config = ClobConfig::builder()
        .heartbeat_interval(Duration::from_secs(10))
        .build();
    let clob = clob::Client::new(CLOB_HOST, clob_config)
        .expect("Failed to create CLOB client")
        .authentication_builder(&signer)
        .signature_type(polymarket_client_sdk::clob::types::SignatureType::Proxy)
        .authenticate()
        .await
        .expect("CLOB authentication failed");

    println!("[OK] Authenticated");

    // WS client for order stream
    let credentials = clob.credentials().clone();
    let ws_unauth = polymarket_client_sdk::clob::ws::Client::default();
    let ws = ws_unauth
        .authenticate(credentials, wallet_addr)
        .expect("WS auth failed");

    // ── Find a market ──
    let gamma_client = gamma::Client::default();
    let now = Utc::now();
    let req = EventsRequest::builder()
        .limit(200)
        .active(true)
        .closed(false)
        .end_date_min(now)
        .tag_slug("bitcoin".to_string())
        .build();
    let events = gamma_client.events(&req).await.expect("Gamma query failed");
    let mut market_info = None;

    for event in &events {
        let markets = match &event.markets {
            Some(m) => m,
            None => continue,
        };
        for market in markets {
            let question = match &market.question {
                Some(q) => q,
                None => continue,
            };
            let q_lower = question.to_lowercase();

            // Match "Bitcoin Up or Down" markets with 15-min time ranges
            let is_btc_up_down = q_lower.contains("bitcoin up or down");
            let is_15min_range = is_btc_up_down && is_15_minute_range(question);

            if !is_15min_range {
                continue;
            }
            if market.active != Some(true) {
                continue;
            }
            if let Some(d) = market.end_date {
                if d <= now {
                    continue;
                }
            } else {
                continue;
            }
            let condition_id = match &market.condition_id {
                Some(cid) => cid,
                None => continue,
            };
            let token_ids = match &market.clob_token_ids {
                Some(ids) if ids.len() >= 2 => ids.clone(),
                _ => continue,
            };
            let tick_size = market.order_price_min_tick_size.unwrap_or(dec!(0.01));

            println!("[OK] Using market: {question}");
            println!("  Condition: {condition_id:?}");
            println!("  YES token: {}", token_ids[0]);
            println!("  Tick size: {tick_size}");

            market_info = Some((condition_id.clone(), token_ids, tick_size));
            break;
        }
        if market_info.is_some() {
            break;
        }
    }

    let (_condition_id, token_ids, tick_size) =
        market_info.expect("No active 15-min BTC market found");

    let yes_token = token_ids[0];
    let condition_b256 = _condition_id;

    // ── Subscribe to order WS stream ──
    println!("\nSubscribing to order stream...");
    let order_stream = ws
        .subscribe_orders(vec![condition_b256])
        .expect("Failed to subscribe to order stream");
    let mut order_stream = Box::pin(order_stream);

    // ── Place a low-priced limit bid ──
    let price = Decimal::from_str(TEST_BID_PRICE).unwrap();
    let size = Decimal::from_str(TEST_BID_SIZE).unwrap();

    // Round price to tick
    let rounded_price = (price / tick_size).floor() * tick_size;

    println!("\nPlacing limit bid: YES @ ${rounded_price} x {size} (should NOT fill)...");

    let signable = clob
        .limit_order()
        .token_id(yes_token)
        .side(Side::Buy)
        .price(rounded_price)
        .size(size)
        .order_type(OrderType::GTC)
        .post_only(true)
        .build()
        .await
        .expect("Order build failed");

    let signed = clob
        .sign(&signer, signable)
        .await
        .expect("Order signing failed");

    let response = clob.post_order(signed).await.expect("Post order failed");

    println!("  Response:");
    println!("    success:     {}", response.success);
    println!("    order_id:    {}", response.order_id);
    println!("    status:      {:?}", response.status);
    println!(
        "    error_msg:   {}",
        response.error_msg.as_deref().unwrap_or("(none)")
    );

    if !response.success {
        eprintln!("[FAIL] Order was rejected! Fix before proceeding.");
        return;
    }

    let order_id = response.order_id.clone();
    println!("\n[OK] Order placed — real CLOB order ID: {order_id}");

    // ── Wait for order to appear in WS stream ──
    println!("\nWaiting for order confirmation on WS stream (10s timeout)...");

    let ws_check = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(result) = order_stream.next().await {
            match result {
                Ok(msg) => {
                    println!(
                        "  WS order event: id={}, status={:?}, type={:?}",
                        msg.id, msg.status, msg.msg_type
                    );
                    if msg.id == order_id {
                        return true;
                    }
                }
                Err(e) => {
                    eprintln!("  WS error: {e}");
                    return false;
                }
            }
        }
        false
    })
    .await;

    match ws_check {
        Ok(true) => println!("[OK] Order confirmed on WS stream"),
        Ok(false) => println!("[WARN] WS stream ended without seeing our order"),
        Err(_) => {
            println!("[WARN] Timeout waiting for WS order event (may be normal if no state change)")
        }
    }

    // Drop stream before cancel
    drop(order_stream);

    // ── Cancel the order ──
    println!("\nCancelling order {order_id}...");

    match clob.cancel_order(&order_id).await {
        Ok(resp) => {
            println!("  Cancel response: {resp:?}");
            println!("[OK] Order cancelled successfully");
        }
        Err(e) => {
            eprintln!("[FAIL] Cancel failed: {e}");
            println!("  Trying cancel_all as fallback...");
            match clob.cancel_all_orders().await {
                Ok(resp) => println!("  cancel_all: {} cancelled", resp.canceled.len()),
                Err(e2) => eprintln!("  cancel_all also failed: {e2}"),
            }
        }
    }

    // ── Verify no open orders remain ──
    println!("\nVerifying no open orders remain...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    match clob.cancel_all_orders().await {
        Ok(resp) => {
            if resp.canceled.is_empty() {
                println!("[OK] No open orders remaining — clean state");
            } else {
                println!(
                    "[WARN] Found {} lingering orders, cancelled them",
                    resp.canceled.len()
                );
            }
        }
        Err(e) => println!("[WARN] Verification cancel_all error: {e}"),
    }

    // ── Summary ──
    println!("\n{}", "=".repeat(60));
    println!("  ORDER ROUND-TRIP TEST COMPLETE");
    println!("{}", "=".repeat(60));
    println!();
    println!("Validated:");
    println!("  - Real CLOB order ID returned (not synthetic)");
    println!("  - Order placement with postOnly=true");
    println!("  - Order cancellation with real ID");
    println!("  - WS order stream connectivity");
    println!();
    println!("If all steps show [OK], proceed to Step 4 (paper mode with real feeds).");
}
