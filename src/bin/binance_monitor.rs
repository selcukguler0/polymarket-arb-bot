//! Multi-source BTC price monitor.
//!
//! Streams real-time BTC prices from three sources:
//!   1. **Binance** — direct WebSocket (`btcusdt@trade`)
//!   2. **Chainlink** — on-chain oracle via RTDS (`subscribe_chainlink_prices`)
//!   3. **Polymarket CLOB** — 15-min Up/Down market midpoints (REST poll)
//!
//! Output:
//!   12:05:23 | BNB: 97500.23 | CHL: 97498.50 | Δ: +1.73 | Up: 0.80 | Dn: 0.20 | Σ: 1.00
//!
//! Usage:
//!   cargo run --bin binance_monitor

use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use parking_lot::RwLock;
use polymarket_client_sdk::{gamma, rtds};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio_tungstenite::connect_async;

const BINANCE_WS: &str = "wss://stream.binance.com:9443/ws/btcusdt@trade";
const CLOB_BOOK_URL: &str = "https://clob.polymarket.com/book";
const OUTPUT_DIR: &str = "data";
const OUTPUT_FILE: &str = "data/btc_price_monitor.csv";

const PRINT_INTERVAL_MS: u64 = 1000;
const BOOK_POLL_SECS: u64 = 2;
const DISCOVERY_SECS: u64 = 30;

// ── Shared state ────────────────────────────────────────────────────

#[derive(Default)]
struct MonitorState {
    /// BTC/USDT price from Binance (direct WS).
    binance_price: Option<Decimal>,

    /// BTC/USD price from Chainlink oracle (via RTDS).
    chainlink_price: Option<Decimal>,
    chainlink_ts: Option<i64>,

    /// Polymarket 15-min Up/Down market midpoints.
    up_mid: Option<f64>,
    down_mid: Option<f64>,

    /// Current tracked market token IDs.
    yes_token: Option<String>,
    no_token: Option<String>,
    question: Option<String>,
}

// ── Wire types (CLOB book polling) ──────────────────────────────────

#[derive(Deserialize)]
struct ClobBook {
    bids: Vec<ClobLevel>,
    asks: Vec<ClobLevel>,
}

#[derive(Deserialize)]
struct ClobLevel {
    price: String,
}

fn book_midpoint(book: &ClobBook) -> Option<f64> {
    let best_bid = book.bids.last()?.price.parse::<f64>().ok()?;
    let best_ask = book.asks.last()?.price.parse::<f64>().ok()?;
    Some((best_bid + best_ask) / 2.0)
}

// ── Main ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    eprintln!("=== BTC Price Monitor (Multi-Source) ===");
    eprintln!("Sources: Binance (WS) | Chainlink (RTDS) | Polymarket CLOB");
    eprintln!("Output:  {OUTPUT_FILE}");
    eprintln!("Press Ctrl+C to stop.\n");

    if !Path::new(OUTPUT_DIR).exists() {
        std::fs::create_dir_all(OUTPUT_DIR)?;
    }

    let file_is_new = !Path::new(OUTPUT_FILE).exists();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(OUTPUT_FILE)?;
    if file_is_new {
        writeln!(
            file,
            "timestamp_utc,binance_price,chainlink_price,price_delta,up_mid,down_mid,combined,market"
        )?;
    }

    let state = Arc::new(RwLock::new(MonitorState::default()));

    // Direct Binance WS for real-time BTC/USDT trades
    tokio::spawn(binance_ws_loop(state.clone()));

    // RTDS client for Chainlink oracle feed
    let rtds_client = rtds::Client::default();
    tokio::spawn(rtds_chainlink_loop(rtds_client, state.clone()));
    tokio::spawn(market_discovery_loop(state.clone()));
    tokio::spawn(polymarket_poll_loop(state.clone()));

    // Print + CSV loop
    let mut interval = tokio::time::interval(Duration::from_millis(PRINT_INTERVAL_MS));
    let mut flush_ctr = 0u32;

    loop {
        interval.tick().await;

        let (bnb, chl, up, down, question) = {
            let s = state.read();
            (
                s.binance_price,
                s.chainlink_price,
                s.up_mid,
                s.down_mid,
                s.question.clone(),
            )
        };

        let now = Utc::now();

        // Format prices
        let bnb_s = bnb.map_or("-".into(), |v| format!("{v:.2}"));
        let chl_s = chl.map_or("-".into(), |v| format!("{v:.2}"));
        let delta = match (bnb, chl) {
            (Some(b), Some(c)) => {
                let d = b - c;
                if d >= Decimal::ZERO {
                    format!("+{d:.2}")
                } else {
                    format!("{d:.2}")
                }
            }
            _ => "-".into(),
        };
        let up_s = up.map_or("-".into(), |v| format!("{v:.2}"));
        let dn_s = down.map_or("-".into(), |v| format!("{v:.2}"));
        let combined = match (up, down) {
            (Some(u), Some(d)) => format!("{:.2}", u + d),
            _ => "-".into(),
        };

        // Console output
        println!(
            "{} | BNB: {:>10} | CHL: {:>10} | \u{0394}: {:>7} | Up: {} | Dn: {} | \u{03a3}: {}",
            now.format("%H:%M:%S"),
            bnb_s,
            chl_s,
            delta,
            up_s,
            dn_s,
            combined,
        );

        // CSV output (only when at least one price source is live)
        if bnb.is_some() || chl.is_some() {
            let _ = writeln!(
                file,
                "{},{},{},{},{},{},{},\"{}\"",
                now.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                bnb.map_or(String::new(), |v| format!("{v}")),
                chl.map_or(String::new(), |v| format!("{v}")),
                match (bnb, chl) {
                    (Some(b), Some(c)) => format!("{}", b - c),
                    _ => String::new(),
                },
                up.map_or(String::new(), |v| format!("{v:.4}")),
                down.map_or(String::new(), |v| format!("{v:.4}")),
                match (up, down) {
                    (Some(u), Some(d)) => format!("{:.4}", u + d),
                    _ => String::new(),
                },
                question.as_deref().unwrap_or(""),
            );
            flush_ctr += 1;
            if flush_ctr % 10 == 0 {
                let _ = file.flush();
            }
        }
    }
}

// ── Binance direct WS task ──────────────────────────────────────────

#[derive(Deserialize)]
struct BinanceTrade {
    p: String,
}

async fn binance_ws_loop(state: Arc<RwLock<MonitorState>>) {
    let mut backoff = Duration::from_secs(2);
    let max_backoff = Duration::from_secs(30);

    loop {
        eprintln!("[binance] Connecting...");
        match connect_async(BINANCE_WS).await {
            Ok((ws, _)) => {
                eprintln!("[binance] Connected");
                backoff = Duration::from_secs(2);
                let (_w, mut r) = ws.split();

                while let Some(msg) = r.next().await {
                    let text = match msg {
                        Ok(m) => match m.into_text() {
                            Ok(t) => t,
                            Err(_) => continue,
                        },
                        Err(e) => {
                            eprintln!("[binance] WS error: {e}");
                            break;
                        }
                    };

                    if let Ok(trade) = serde_json::from_str::<BinanceTrade>(&text) {
                        if let Ok(price) = trade.p.parse::<f64>() {
                            let mut s = state.write();
                            s.binance_price = Decimal::from_f64(price);
                        }
                    }
                }
            }
            Err(e) => eprintln!("[binance] Connection failed: {e}"),
        }

        eprintln!("[binance] Reconnecting in {}s...", backoff.as_secs());
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

// ── RTDS Chainlink price task ───────────────────────────────────────

async fn rtds_chainlink_loop(client: rtds::Client, state: Arc<RwLock<MonitorState>>) {
    loop {
        eprintln!("[rtds-chainlink] Subscribing to BTC/USD oracle...");

        match client.subscribe_chainlink_prices(Some("btc/usd".to_owned())) {
            Ok(stream) => {
                eprintln!("[rtds-chainlink] Connected");
                tokio::pin!(stream);

                while let Some(result) = stream.next().await {
                    match result {
                        Ok(price) => {
                            let mut s = state.write();
                            s.chainlink_price = Some(price.value);
                            s.chainlink_ts = Some(price.timestamp);
                        }
                        Err(e) => {
                            eprintln!("[rtds-chainlink] Stream error: {e}");
                            break;
                        }
                    }
                }

                eprintln!("[rtds-chainlink] Stream ended");
            }
            Err(e) => eprintln!("[rtds-chainlink] Subscribe failed: {e}"),
        }

        eprintln!("[rtds-chainlink] Reconnecting in 3s...");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

// ── Polymarket market discovery task ────────────────────────────────

async fn market_discovery_loop(state: Arc<RwLock<MonitorState>>) {
    let gamma_client = gamma::Client::default();
    let mut interval = tokio::time::interval(Duration::from_secs(DISCOVERY_SECS));

    loop {
        interval.tick().await;
        let now = Utc::now();

        let req = gamma::types::request::EventsRequest::builder()
            .limit(200)
            .active(true)
            .closed(false)
            .end_date_min(now)
            .tag_slug("bitcoin".to_string())
            .build();

        match gamma_client.events(&req).await {
            Ok(events) => {
                if let Some((yes, no, q, end)) = find_nearest_15min_market(&events, now) {
                    let mut s = state.write();
                    let changed = s.yes_token.as_deref() != Some(&yes);
                    s.yes_token = Some(yes);
                    s.no_token = Some(no);
                    s.question = Some(q.clone());
                    if changed {
                        s.up_mid = None;
                        s.down_mid = None;
                        drop(s);
                        eprintln!(
                            "[gamma] Tracking: {q} (ends {})",
                            end.format("%H:%M:%S UTC")
                        );
                    }
                } else {
                    eprintln!("[gamma] No active 15-min BTC market found");
                }
            }
            Err(e) => eprintln!("[gamma] Discovery error: {e}"),
        }
    }
}

fn find_nearest_15min_market(
    events: &[gamma::types::response::Event],
    now: DateTime<Utc>,
) -> Option<(String, String, String, DateTime<Utc>)> {
    let mut best: Option<(String, String, String, DateTime<Utc>)> = None;

    for event in events {
        let Some(markets) = &event.markets else {
            continue;
        };
        for market in markets {
            let Some(question) = &market.question else {
                continue;
            };
            if !question.to_lowercase().contains("bitcoin up or down") {
                continue;
            }
            if !is_15_minute_range(question) {
                continue;
            }
            if market.active != Some(true) {
                continue;
            }
            let Some(end) = market.end_date else {
                continue;
            };
            if end <= now {
                continue;
            }
            let Some(ids) = &market.clob_token_ids else {
                continue;
            };
            if ids.len() < 2 {
                continue;
            }

            let yes = ids[0].to_string();
            let no = ids[1].to_string();

            if best.as_ref().is_none_or(|b| end < b.3) {
                best = Some((yes, no, question.clone(), end));
            }
        }
    }

    best
}

// ── Polymarket book poller task ─────────────────────────────────────

async fn polymarket_poll_loop(state: Arc<RwLock<MonitorState>>) {
    let mut interval = tokio::time::interval(Duration::from_secs(BOOK_POLL_SECS));

    loop {
        interval.tick().await;

        let (yes_tok, no_tok) = {
            let s = state.read();
            match (&s.yes_token, &s.no_token) {
                (Some(y), Some(n)) => (y.clone(), n.clone()),
                _ => continue,
            }
        };

        let yes_url = format!("{CLOB_BOOK_URL}?token_id={yes_tok}");
        let no_url = format!("{CLOB_BOOK_URL}?token_id={no_tok}");

        let (up, down) = tokio::task::spawn_blocking(move || {
            (fetch_midpoint(&yes_url), fetch_midpoint(&no_url))
        })
        .await
        .unwrap_or((None, None));

        let mut s = state.write();
        if up.is_some() {
            s.up_mid = up;
        }
        if down.is_some() {
            s.down_mid = down;
        }
    }
}

fn fetch_midpoint(url: &str) -> Option<f64> {
    let resp = ureq::get(url).call().ok()?;
    let body = resp.into_body().read_to_string().ok()?;
    let book: ClobBook = serde_json::from_str(&body).ok()?;
    book_midpoint(&book)
}

// ── 15-minute range detection ───────────────────────────────────────
// Matches "7:00AM-7:15AM", "1:15PM-1:30PM", etc. in market question text.

fn is_15_minute_range(question: &str) -> bool {
    for (i, _) in question.match_indices(':') {
        if i < 1 || i + 7 > question.len() {
            continue;
        }
        let after_colon = &question[i + 1..];
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
        let before_colon = &question[..i];
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
        if !after_end_colon[2..].starts_with("AM") && !after_end_colon[2..].starts_with("PM") {
            continue;
        }
        let end_is_pm = after_end_colon[2..].starts_with("PM");

        let to_24h = |hour: u32, mins: u32, is_pm: bool| -> u32 {
            let h24 = if is_pm {
                if hour == 12 {
                    12
                } else {
                    hour + 12
                }
            } else if hour == 12 {
                0
            } else {
                hour
            };
            h24 * 60 + mins
        };

        let start_total = to_24h(start_hour, start_mins, start_is_pm);
        let end_total = to_24h(end_hour, end_mins, end_is_pm);
        let diff = if end_total > start_total {
            end_total - start_total
        } else {
            end_total + 1440 - start_total
        };
        if diff == 15 {
            return true;
        }
    }
    false
}
