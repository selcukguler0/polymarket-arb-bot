//! Pre-launch connectivity smoke test (Steps 1 + 2).
//!
//! Validates all external dependencies work with the bot's credentials:
//! 1. CLOB authentication
//! 2. Gamma API market discovery
//! 3. WebSocket orderbook subscription (prints 5 snapshots + level counts for delta detection)
//! 4. Authenticated cancel_all endpoint
//! 5. On-chain USDC and MATIC balance checks
//!
//! Run: cargo run --bin smoke_test

use std::str::FromStr;

use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::sol;
use chrono::Utc;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use polymarket_client_sdk::clob::{self, Config as ClobConfig};
use polymarket_client_sdk::gamma;
use polymarket_client_sdk::gamma::types::request::EventsRequest;
use polymarket_client_sdk::types::Address as SdkAddress;
use polymarket_client_sdk::POLYGON;

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as _;

const CLOB_HOST: &str = "https://clob.polymarket.com";
const USDC_E: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }
}

fn separator(label: &str) {
    println!("\n{}", "=".repeat(60));
    println!("  STEP: {label}");
    println!("{}\n", "=".repeat(60));
}

/// Check if a market question contains a 15-minute time range.
/// Matches patterns like "7:00AM-7:15AM", "1:15PM-1:30PM", etc.
fn is_15_minute_range(question: &str) -> bool {
    // Look for pattern: H:MMAM-H:MMAM or H:MMPM-H:MMPM
    // Parse both times and check if the difference is 15 minutes
    let q = question;
    // Find the time range pattern: "D:DDXM-D:DDXM"
    for (i, _) in q.match_indices(':') {
        // Try to parse start time ending at this colon
        // Pattern: digit(s) : 2digits AM/PM - digit(s) : 2digits AM/PM
        if i < 1 || i + 7 > q.len() {
            continue;
        }
        // Find the AM/PM-AM/PM pattern after this colon
        let after_colon = &q[i + 1..];
        // Look for MM followed by AM- or PM-
        if after_colon.len() < 5 {
            continue;
        }
        let minutes_str = &after_colon[..2];
        let start_mins: u32 = match minutes_str.parse() {
            Ok(m) => m,
            Err(_) => continue,
        };

        // Check for AM- or PM- after minutes
        let ampm_dash = &after_colon[2..];
        let (start_is_pm, rest) = if ampm_dash.starts_with("AM-") {
            (false, &ampm_dash[3..])
        } else if ampm_dash.starts_with("PM-") {
            (true, &ampm_dash[3..])
        } else {
            continue;
        };

        // Get start hour (1-2 digits before the colon)
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

        // Now parse end time from `rest`: "H:MMAM" or "H:MMPM"
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

        // Convert to 24h minutes-since-midnight
        let start_24h = to_minutes_24h(start_hour, start_mins, start_is_pm);
        let end_24h = to_minutes_24h(end_hour, end_mins, end_is_pm);

        let diff = if end_24h > start_24h {
            end_24h - start_24h
        } else {
            end_24h + 1440 - start_24h // across midnight
        };

        if diff == 15 {
            return true;
        }
    }
    false
}

fn to_minutes_24h(hour: u32, mins: u32, is_pm: bool) -> u32 {
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
}

#[tokio::main]
async fn main() {
    // Install TLS crypto provider before any network calls
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Load .env
    dotenvy::dotenv().ok();

    let private_key =
        std::env::var("POLYMARKET_PRIVATE_KEY").expect("POLYMARKET_PRIVATE_KEY not set in .env");
    let wallet_address = std::env::var("WALLET_ADDRESS").expect("WALLET_ADDRESS not set in .env");
    let rpc_url = std::env::var("POLYGON_RPC_URL").expect("POLYGON_RPC_URL not set in .env");

    println!("Wallet: {wallet_address}");
    println!("RPC:    {rpc_url}");
    println!();

    // ════════════════════════════════════════════════════════════
    // STEP 1: Authenticate CLOB client
    // ════════════════════════════════════════════════════════════
    separator("1 — CLOB Authentication");

    let signer = PrivateKeySigner::from_str(&private_key)
        .expect("Invalid private key")
        .with_chain_id(Some(POLYGON));

    let wallet_addr = SdkAddress::from_str(&wallet_address).expect("Invalid wallet address");

    println!("Creating CLOB client...");
    let clob_config = ClobConfig::builder()
        .heartbeat_interval(std::time::Duration::from_secs(10))
        .build();
    let clob = clob::Client::new(CLOB_HOST, clob_config).expect("Failed to create CLOB client");

    println!("Authenticating...");
    let clob = clob
        .authentication_builder(&signer)
        .signature_type(polymarket_client_sdk::clob::types::SignatureType::Proxy)
        .authenticate()
        .await
        .expect("CLOB authentication failed");

    println!("[OK] CLOB client authenticated successfully");

    // Create authenticated WS client
    let credentials = clob.credentials().clone();
    let ws_unauth = polymarket_client_sdk::clob::ws::Client::default();
    let ws = ws_unauth
        .authenticate(credentials, wallet_addr)
        .expect("WS authentication failed");

    println!("[OK] WebSocket client authenticated");

    // Gamma client
    let gamma_client = gamma::Client::default();
    println!("[OK] Gamma client created");

    // ════════════════════════════════════════════════════════════
    // STEP 2: Query Gamma API for a 15-minute BTC market
    // ════════════════════════════════════════════════════════════
    separator("2 — Gamma API Market Discovery");

    // Query active, non-closed events ending in the future
    // Try multiple strategies to find 15-min BTC markets
    let now = Utc::now();

    // Strategy 1: Try tag_slug "bitcoin" with large limit
    let strategies: Vec<(&str, EventsRequest)> = vec![
        (
            "tag_slug=bitcoin",
            EventsRequest::builder()
                .limit(200)
                .active(true)
                .closed(false)
                .end_date_min(now)
                .tag_slug("bitcoin".to_string())
                .build(),
        ),
        (
            "tag_slug=crypto",
            EventsRequest::builder()
                .limit(200)
                .active(true)
                .closed(false)
                .end_date_min(now)
                .tag_slug("crypto".to_string())
                .build(),
        ),
        (
            "broad (limit=500)",
            EventsRequest::builder()
                .limit(500)
                .active(true)
                .closed(false)
                .end_date_min(now)
                .build(),
        ),
    ];

    let mut events = Vec::new();
    for (label, req) in strategies {
        println!("Trying Gamma query: {label}...");
        match gamma_client.events(&req).await {
            Ok(ev) => {
                println!("  → Got {} events", ev.len());
                // Debug: show BTC-related titles
                let btc_count = ev
                    .iter()
                    .filter(|e| {
                        e.title
                            .as_deref()
                            .unwrap_or("")
                            .to_lowercase()
                            .contains("btc")
                            || e.title
                                .as_deref()
                                .unwrap_or("")
                                .to_lowercase()
                                .contains("bitcoin")
                    })
                    .count();
                println!("  → {btc_count} BTC-related events found");
                if !ev.is_empty() {
                    events = ev;
                    if btc_count > 0 {
                        break; // Found BTC events, use this set
                    }
                }
            }
            Err(e) => println!("  → Error: {e}"),
        }
    }

    println!("\nUsing {} events total", events.len());

    // Debug: print first 10 event titles
    println!("\n--- DEBUG: First 10 events ---");
    for (i, event) in events.iter().enumerate().take(10) {
        let title = event.title.as_deref().unwrap_or("(no title)");
        let slug = event.slug.as_deref().unwrap_or("(no slug)");
        println!("  Event {i}: title={title:?} slug={slug:?}");
        if let Some(markets) = &event.markets {
            for (j, market) in markets.iter().enumerate().take(3) {
                let q = market.question.as_deref().unwrap_or("(no question)");
                let active = market.active.unwrap_or(false);
                let end = market
                    .end_date
                    .map(|d| d.to_string())
                    .unwrap_or("(none)".into());
                println!("    Market {j}: q={q:?} active={active} end={end}");
            }
        } else {
            println!("    (no markets)");
        }
    }

    // Also specifically search for anything with "15" or "minute" in the question
    println!("\n--- DEBUG: Markets mentioning '15' or 'minute' ---");
    let mut minute_count = 0;
    for event in &events {
        if let Some(markets) = &event.markets {
            for market in markets {
                let q = market.question.as_deref().unwrap_or("");
                let q_lower = q.to_lowercase();
                if q_lower.contains("15") || q_lower.contains("minute") {
                    let active = market.active.unwrap_or(false);
                    let end = market
                        .end_date
                        .map(|d| d.to_string())
                        .unwrap_or("(none)".into());
                    println!("  q={q:?} active={active} end={end}");
                    minute_count += 1;
                }
            }
        }
    }
    if minute_count == 0 {
        println!("  (none found)");
    }
    println!("--- END DEBUG ---\n");

    let mut found_market = None;

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
            // Real format: "Bitcoin Up or Down - February 14, 7:00AM-7:15AM ET"
            let is_btc_up_down = q_lower.contains("bitcoin up or down");
            let is_15min_range = is_btc_up_down && is_15_minute_range(question);

            if !is_15min_range {
                continue;
            }

            if market.active != Some(true) {
                continue;
            }

            let end_date = match market.end_date {
                Some(d) if d > now => d,
                _ => continue,
            };

            let condition_id = match &market.condition_id {
                Some(cid) => format!("{cid:?}"),
                None => continue,
            };

            let token_ids = match &market.clob_token_ids {
                Some(ids) if ids.len() >= 2 => ids.clone(),
                _ => continue,
            };

            let tick_size = market.order_price_min_tick_size.unwrap_or(dec!(0.01));

            let neg_risk = market.neg_risk.unwrap_or(false);

            println!("[OK] Found 15-min BTC market:");
            println!("  Question:     {question}");
            println!("  Condition ID: {condition_id}");
            println!("  Token YES:    {}", token_ids[0]);
            println!("  Token NO:     {}", token_ids[1]);
            println!("  Tick size:    {tick_size}");
            println!("  Neg risk:     {neg_risk}");
            println!("  End date:     {end_date}");
            println!("  Remaining:    {} seconds", (end_date - now).num_seconds());

            found_market = Some((condition_id, token_ids, tick_size, neg_risk));
            break;
        }

        if found_market.is_some() {
            break;
        }
    }

    let (condition_id, token_ids, _tick_size, _neg_risk) = found_market
        .expect("No active 15-minute BTC market found! Try again when markets are active.");

    let yes_u256 = token_ids[0];
    let no_u256 = token_ids[1];

    // ════════════════════════════════════════════════════════════
    // STEP 3: Subscribe to orderbook WS, print 5 snapshots
    // ════════════════════════════════════════════════════════════
    separator("3 — WebSocket Orderbook (5 snapshots + delta detection)");

    println!("Subscribing to orderbook for tokens [{yes_u256}, {no_u256}]...");

    let stream = ws
        .subscribe_orderbook(vec![yes_u256, no_u256])
        .expect("Failed to subscribe to orderbook WS");

    let mut stream = Box::pin(stream);
    let mut count = 0;

    while let Some(result) = stream.next().await {
        match result {
            Ok(book_update) => {
                count += 1;
                let bid_count = book_update.bids.len();
                let ask_count = book_update.asks.len();

                let best_bid = book_update
                    .bids
                    .last()
                    .map(|l| format!("{} @ {}", l.size, l.price))
                    .unwrap_or_else(|| "none".into());
                let best_ask = book_update
                    .asks
                    .first()
                    .map(|l| format!("{} @ {}", l.size, l.price))
                    .unwrap_or_else(|| "none".into());

                let asset_label = if book_update.asset_id == yes_u256 {
                    "YES"
                } else if book_update.asset_id == no_u256 {
                    "NO"
                } else {
                    "???"
                };

                println!(
                    "  [{count}/5] {asset_label} | bids: {bid_count} levels, asks: {ask_count} levels | best_bid: {best_bid} | best_ask: {best_ask} | ts: {}",
                    book_update.timestamp
                );

                // Step 2 delta detection: if we consistently see 20+ levels, it's full snapshots.
                // If we see 1-2 levels, it's deltas and the bot's replace logic is wrong.
                if bid_count + ask_count <= 4 {
                    println!(
                        "  ⚠ WARNING: Only {} total levels — this looks like a DELTA update, not a full snapshot!",
                        bid_count + ask_count
                    );
                    println!(
                        "  ⚠ The bot's replace-entire-book logic will LOSE data. Needs delta fix."
                    );
                }

                if count >= 5 {
                    println!("\n[OK] Received 5 orderbook updates");
                    break;
                }
            }
            Err(e) => {
                eprintln!("  [ERROR] Orderbook WS error: {e}");
                break;
            }
        }
    }

    if count < 5 {
        eprintln!("[FAIL] Only received {count}/5 orderbook updates before stream ended");
    }

    // Drop stream to release borrow on ws
    drop(stream);

    // ════════════════════════════════════════════════════════════
    // STEP 4: Verify cancel_all (authenticated endpoint)
    // ════════════════════════════════════════════════════════════
    separator("4 — Cancel All Orders (auth endpoint test)");

    println!("Calling cancel_all_orders()...");
    match clob.cancel_all_orders().await {
        Ok(resp) => {
            println!(
                "[OK] cancel_all succeeded — {} orders cancelled",
                resp.canceled.len()
            );
            if !resp.canceled.is_empty() {
                for oid in &resp.canceled {
                    println!("  Cancelled: {oid}");
                }
            }
        }
        Err(e) => {
            // cancel_all on zero orders might return success or error depending on API
            println!("[WARN] cancel_all returned error: {e}");
            println!("  (This may be OK if there are no open orders)");
        }
    }

    // ════════════════════════════════════════════════════════════
    // STEP 5: On-chain balance checks
    // ════════════════════════════════════════════════════════════
    separator("5 — On-Chain Balances");

    let wallet_alloy = Address::from_str(&wallet_address).expect("Invalid wallet address");

    let url = rpc_url.parse().expect("Invalid RPC URL");
    let provider = ProviderBuilder::new().connect_http(url);

    // USDC.e balance
    let usdc_addr = Address::from_str(USDC_E).unwrap();
    let usdc_contract = IERC20::new(usdc_addr, &provider);
    match usdc_contract.balanceOf(wallet_alloy).call().await {
        Ok(raw) => {
            let usdc = Decimal::from_str(&raw.to_string()).unwrap_or_default() / dec!(1_000_000);
            println!("[OK] USDC.e balance: {usdc}");
            if usdc <= Decimal::ZERO {
                println!("  ⚠ WARNING: USDC balance is ZERO — cannot trade!");
            }
        }
        Err(e) => {
            eprintln!("[FAIL] USDC balance check failed: {e}");
        }
    }

    // MATIC balance (native)
    match provider.get_balance(wallet_alloy).await {
        Ok(raw) => {
            let matic = Decimal::from_str(&raw.to_string()).unwrap_or_default()
                / dec!(1_000_000_000_000_000_000);
            println!("[OK] MATIC balance: {matic}");
            if matic < dec!(0.1) {
                println!("  ⚠ WARNING: MATIC balance low — need at least 0.1 for gas");
            }
        }
        Err(e) => {
            eprintln!("[FAIL] MATIC balance check failed: {e}");
        }
    }

    // ════════════════════════════════════════════════════════════
    // SUMMARY
    // ════════════════════════════════════════════════════════════
    println!("\n{}", "=".repeat(60));
    println!("  SMOKE TEST COMPLETE");
    println!("{}", "=".repeat(60));
    println!();
    println!("If all steps show [OK], proceed to Step 3 (single order round-trip).");
    println!("If any step shows [FAIL], fix the issue before continuing.");
    println!();
    println!("Market found: {condition_id}");
    println!("This market can be used for the Step 3 order round-trip test.");
}
