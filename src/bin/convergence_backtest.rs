//! Backtest the FV-Midpoint Convergence Strategy on historical 700-period data.
//!
//! Uses the EXACT same logic as convergence_bot.rs — same entry/exit conditions,
//! same guards, same thresholds. No Python approximations.
//!
//! Usage: cargo run --release --bin convergence_backtest [-- path/to/data/BTC]

use std::path::{Path, PathBuf};
use std::{env, fs};

use chrono::{DateTime, Utc};

// ── Config (exact copy from convergence_bot.rs defaults) ──

struct Config {
    min_divergence: f64,
    take_profit_div: f64,
    stop_loss_cents: f64,
    profit_take_cents: f64,
    order_size: f64,
    max_position: f64,
    max_cost: f64,
    min_remaining_secs: f64,
    force_sell_secs: f64,
    entry_cooldown_secs: f64,
    max_stoplosses: u32,
    min_sigma: f64,
    allowed_durations: Vec<u32>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            min_divergence: 0.06,
            take_profit_div: 0.01,
            stop_loss_cents: 0.10,
            profit_take_cents: 0.08,
            order_size: 60.0, // max_position = order_size in bot (buys full position at once)
            max_position: 60.0,
            max_cost: 0.55,
            min_remaining_secs: 30.0,
            force_sell_secs: 30.0,
            entry_cooldown_secs: 10.0,
            max_stoplosses: 1,
            min_sigma: 0.000020,
            allowed_durations: vec![5],
        }
    }
}

// ── FV Model (exact copy from convergence_bot.rs) ──

fn normal_cdf(x: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.2316419 * x.abs());
    let d = 0.3989422804014327_f64;
    let poly = t
        * (0.31938153 + t * (-0.35656378 + t * (1.78147794 + t * (-1.82125598 + t * 1.33027443))));
    let p = d * (-x * x / 2.0).exp() * poly;
    if x >= 0.0 {
        1.0 - p
    } else {
        p
    }
}

fn fair_value_up(btc_open: f64, btc_current: f64, sigma_per_sec: f64, remaining_secs: f64) -> f64 {
    if btc_open <= 0.0 || btc_current <= 0.0 || remaining_secs <= 0.0 {
        return 0.5;
    }
    let log_return = (btc_current / btc_open).ln();
    let remaining_vol = sigma_per_sec * remaining_secs.sqrt();
    if remaining_vol <= 1e-12 {
        return if log_return > 0.0 {
            0.95
        } else if log_return < 0.0 {
            0.05
        } else {
            0.5
        };
    }
    let z = log_return / remaining_vol;
    normal_cdf(z).clamp(0.02, 0.98)
}

// ── Tick Data ──

struct TickData {
    remaining_secs: f64,
    fv_up: f64,
    fv_down: f64,
    mid_up: f64,
    mid_down: f64,
    best_bid_up: f64,
    best_ask_up: f64,
    best_bid_down: f64,
    best_ask_down: f64,
    sigma: f64,
    btc: f64,
    btc_open: f64,
    // timestamp for cooldown timing (seconds from period start)
    elapsed_secs: f64,
}

// ── Position (mirrors convergence_bot.rs Position) ──

#[derive(Default)]
struct Position {
    up_qty: f64,
    up_cost: f64,
    down_qty: f64,
    down_cost: f64,
    up_entry_fv: f64,
    down_entry_fv: f64,
}

impl Position {
    fn avg_up(&self) -> f64 {
        if self.up_qty > 0.0 {
            self.up_cost / self.up_qty
        } else {
            0.0
        }
    }
    fn avg_down(&self) -> f64 {
        if self.down_qty > 0.0 {
            self.down_cost / self.down_qty
        } else {
            0.0
        }
    }
}

// ── Period Stats ──

#[derive(Default)]
struct PeriodStats {
    entries: u32,
    exits: u32,
    stoplosses: u32,
    sell_pnl: f64,
    last_stoploss_elapsed: f64, // elapsed_secs at last stoploss (-999 = none)
    last_entry_elapsed: f64,    // elapsed_secs at last entry (-999 = none)
}

// ── Period Result ──

struct PeriodResult {
    name: String,
    duration: u32,
    resolution: String,
    ticks: usize,
    entries: u32,
    exits: u32,
    stoplosses: u32,
    pnl: f64,
    sell_pnl: f64,
    settle_pnl: f64,
}

// ── Core Backtest Logic (mirrors run_convergence in convergence_bot.rs) ──

fn run_period(ticks: &[TickData], config: &Config, resolution: &str) -> PeriodResult {
    let mut position = Position::default();
    let mut stats = PeriodStats {
        last_stoploss_elapsed: -999.0,
        last_entry_elapsed: -999.0,
        ..Default::default()
    };

    // min_entry_remaining = force_sell_secs + 60.0 (exact match to bot line 948)
    let min_entry_remaining = config.force_sell_secs + 60.0;

    for tick in ticks {
        let remaining = tick.remaining_secs;
        let sigma = tick.sigma.max(config.min_sigma);
        let elapsed = tick.elapsed_secs;

        // Recompute FV from BTC data (using same model as live bot)
        // The CSV has pre-computed fv, but we use the raw BTC + sigma to match bot exactly
        let fv_up = fair_value_up(tick.btc_open, tick.btc, sigma, remaining);
        let fv_down = 1.0 - fv_up;

        let mid_up = tick.mid_up;
        let mid_down = tick.mid_down;

        if mid_up <= 0.0 || mid_down <= 0.0 {
            continue;
        }

        let div_up = fv_up - mid_up;
        let div_down = fv_down - mid_down;

        let force_sell = remaining < config.force_sell_secs;

        // ── Sell UP position (mirrors bot lines 846-890) ──
        if position.up_qty > 0.0 && tick.best_bid_up > 0.0 {
            let current_div = fv_up - mid_up;
            let sell_price = tick.best_bid_up;
            let avg = position.avg_up();
            let profit_per_share = sell_price - avg;

            let profit_take = profit_per_share >= config.profit_take_cents;
            let convergence = current_div <= config.take_profit_div && profit_per_share >= 0.0;
            let stop_loss = profit_per_share <= -config.stop_loss_cents;

            if force_sell || profit_take || convergence || stop_loss {
                let pnl_est = profit_per_share * position.up_qty;
                stats.sell_pnl += pnl_est;
                stats.exits += 1;
                if stop_loss && !force_sell && !profit_take && !convergence {
                    stats.stoplosses += 1;
                    stats.last_stoploss_elapsed = elapsed;
                }
                position.up_cost = 0.0;
                position.up_qty = 0.0;
                position.up_entry_fv = 0.0;
            }
        }

        // ── Sell DOWN position (mirrors bot lines 893-937) ──
        if position.down_qty > 0.0 && tick.best_bid_down > 0.0 {
            let current_div = fv_down - mid_down;
            let sell_price = tick.best_bid_down;
            let avg = position.avg_down();
            let profit_per_share = sell_price - avg;

            let profit_take = profit_per_share >= config.profit_take_cents;
            let convergence = current_div <= config.take_profit_div && profit_per_share >= 0.0;
            let stop_loss = profit_per_share <= -config.stop_loss_cents;

            if force_sell || profit_take || convergence || stop_loss {
                let pnl_est = profit_per_share * position.down_qty;
                stats.sell_pnl += pnl_est;
                stats.exits += 1;
                if stop_loss && !force_sell && !profit_take && !convergence {
                    stats.stoplosses += 1;
                    stats.last_stoploss_elapsed = elapsed;
                }
                position.down_cost = 0.0;
                position.down_qty = 0.0;
                position.down_entry_fv = 0.0;
            }
        }

        // ── Check Entries (mirrors bot lines 939-1027) ──
        let has_up = position.up_qty > 0.0;
        let has_down = position.down_qty > 0.0;
        let stoploss_cooldown = (elapsed - stats.last_stoploss_elapsed) < 30.0;
        let stoploss_maxed = stats.stoplosses >= config.max_stoplosses;

        // Buy UP
        if div_up >= config.min_divergence
            && remaining > min_entry_remaining
            && tick.best_ask_up > 0.0
            && tick.best_ask_up <= config.max_cost
            && position.up_qty < config.max_position
            && !has_down
            && !stoploss_cooldown
            && !stoploss_maxed
        {
            let buy_price = tick.best_ask_up;
            let buy_qty = config.max_position - position.up_qty;
            if position.up_qty == 0.0 {
                position.up_entry_fv = fv_up;
            }
            position.up_cost += buy_price * buy_qty;
            position.up_qty += buy_qty;
            stats.entries += 1;
            stats.last_entry_elapsed = elapsed;
        }

        // Buy DOWN
        if div_down >= config.min_divergence
            && remaining > min_entry_remaining
            && tick.best_ask_down > 0.0
            && tick.best_ask_down <= config.max_cost
            && position.down_qty < config.max_position
            && !has_up
            && !stoploss_cooldown
            && !stoploss_maxed
        {
            // Re-check has_up after potential UP buy above
            if position.up_qty == 0.0 {
                let buy_price = tick.best_ask_down;
                let buy_qty = config.max_position - position.down_qty;
                if position.down_qty == 0.0 {
                    position.down_entry_fv = fv_down;
                }
                position.down_cost += buy_price * buy_qty;
                position.down_qty += buy_qty;
                stats.entries += 1;
                stats.last_entry_elapsed = elapsed;
            }
        }
    }

    // ── Settlement: resolve unsold positions using actual outcome ──
    // On Polymarket, winning shares resolve to $1.00, losing shares to $0.00
    let settle_pnl = if position.up_qty > 0.0 {
        let resolve_price = if resolution == "UP" { 1.0 } else { 0.0 };
        (resolve_price - position.avg_up()) * position.up_qty
    } else if position.down_qty > 0.0 {
        let resolve_price = if resolution == "DOWN" { 1.0 } else { 0.0 };
        (resolve_price - position.avg_down()) * position.down_qty
    } else {
        0.0
    };

    let total_pnl = stats.sell_pnl + settle_pnl;

    PeriodResult {
        name: String::new(),
        duration: 0,
        resolution: String::new(),
        ticks: ticks.len(),
        entries: stats.entries,
        exits: stats.exits,
        stoplosses: stats.stoplosses,
        pnl: total_pnl,
        sell_pnl: stats.sell_pnl,
        settle_pnl,
    }
}

// ── Data Loading ──

fn parse_ticks(folder: &Path) -> Option<(Vec<TickData>, String)> {
    let prices_path = folder.join("prices.csv");
    if !prices_path.exists() {
        return None;
    }

    // Read period result
    let result_path = folder.join("period_result.csv");
    let mut resolution = String::from("?");
    if result_path.exists() {
        if let Ok(content) = fs::read_to_string(&result_path) {
            for line in content.lines().skip(1) {
                let fields: Vec<&str> = line.split(',').collect();
                if fields.len() >= 5 {
                    resolution = fields[4].to_string();
                }
            }
        }
    }

    // Parse prices.csv
    let content = fs::read_to_string(&prices_path).ok()?;
    let mut lines = content.lines();
    let header = lines.next()?;
    let headers: Vec<&str> = header.split(',').collect();

    // Find column indices
    let idx = |name: &str| headers.iter().position(|h| *h == name);
    let i_remaining = idx("remaining_secs")?;
    let i_fv_up = idx("fv_up")?;
    let i_fv_down = idx("fv_down")?;
    let i_mid_up = idx("mid_up")?;
    let i_mid_down = idx("mid_down")?;
    let i_bid_up = idx("best_bid_up")?;
    let i_ask_up = idx("best_ask_up")?;
    let i_bid_down = idx("best_bid_down")?;
    let i_ask_down = idx("best_ask_down")?;
    let i_sigma = idx("sigma")?;
    let i_btc = idx("binance_btc")?;
    let i_btc_open = idx("btc_open")?;
    let i_ts = idx("timestamp")?;

    let mut ticks = Vec::new();
    let mut first_remaining: Option<f64> = None;

    for line in lines {
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() <= i_btc_open.max(i_ts) {
            continue;
        }

        let remaining: f64 = match fields[i_remaining].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };

        if first_remaining.is_none() {
            first_remaining = Some(remaining);
        }

        // Compute elapsed_secs from first tick's remaining
        let elapsed = first_remaining.unwrap() - remaining;

        let tick = TickData {
            remaining_secs: remaining,
            fv_up: fields[i_fv_up].parse().unwrap_or(0.0),
            fv_down: fields[i_fv_down].parse().unwrap_or(0.0),
            mid_up: fields[i_mid_up].parse().unwrap_or(0.0),
            mid_down: fields[i_mid_down].parse().unwrap_or(0.0),
            best_bid_up: fields[i_bid_up].parse().unwrap_or(0.0),
            best_ask_up: fields[i_ask_up].parse().unwrap_or(0.0),
            best_bid_down: fields[i_bid_down].parse().unwrap_or(0.0),
            best_ask_down: fields[i_ask_down].parse().unwrap_or(0.0),
            sigma: fields[i_sigma].parse().unwrap_or(0.0),
            btc: fields[i_btc].parse().unwrap_or(0.0),
            btc_open: fields[i_btc_open].parse().unwrap_or(0.0),
            elapsed_secs: elapsed,
        };
        ticks.push(tick);
    }

    Some((ticks, resolution))
}

fn detect_duration(ticks: &[TickData]) -> u32 {
    if ticks.is_empty() {
        return 0;
    }
    if ticks[0].remaining_secs > 500.0 {
        15
    } else {
        5
    }
}

// ── Main ──

fn main() {
    let args: Vec<String> = env::args().collect();
    let data_dir = args.get(1).map(|s| s.as_str()).unwrap_or("700 periods/BTC");

    let mut config = Config::default();

    // Parse optional overrides
    for i in 0..args.len() {
        match args[i].as_str() {
            "--div" if i + 1 < args.len() => {
                config.min_divergence = args[i + 1].parse().unwrap_or(config.min_divergence);
            }
            "--profit" if i + 1 < args.len() => {
                config.profit_take_cents = args[i + 1].parse().unwrap_or(config.profit_take_cents);
            }
            "--stop" if i + 1 < args.len() => {
                config.stop_loss_cents = args[i + 1].parse().unwrap_or(config.stop_loss_cents);
            }
            "--max-sl" if i + 1 < args.len() => {
                config.max_stoplosses = args[i + 1].parse().unwrap_or(config.max_stoplosses);
            }
            "--max-pos" if i + 1 < args.len() => {
                config.max_position = args[i + 1].parse().unwrap_or(config.max_position);
            }
            "--max-cost" if i + 1 < args.len() => {
                config.max_cost = args[i + 1].parse().unwrap_or(config.max_cost);
            }
            "--dur" if i + 1 < args.len() => {
                config.allowed_durations = args[i + 1]
                    .split(',')
                    .filter_map(|s| s.parse().ok())
                    .collect();
            }
            _ => {}
        }
    }

    println!("Convergence Strategy Backtest (Rust)");
    println!("  data: {data_dir}");
    println!(
        "  min_div={:.2} profit={:.0}c stop={:.0}c take_profit_div={:.2} max_sl={}",
        config.min_divergence,
        config.profit_take_cents * 100.0,
        config.stop_loss_cents * 100.0,
        config.take_profit_div,
        config.max_stoplosses
    );
    println!(
        "  pos={:.0} max_cost={:.2} force_sell={:.0}s min_entry_remaining={:.0}s durations={:?}",
        config.max_position,
        config.max_cost,
        config.force_sell_secs,
        config.force_sell_secs + 60.0,
        config.allowed_durations
    );
    println!();

    // Collect all period folders
    let data_path = PathBuf::from(data_dir);
    let mut folders: Vec<PathBuf> = match fs::read_dir(&data_path) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .map(|e| e.path())
            .collect(),
        Err(e) => {
            eprintln!("Error reading {data_dir}: {e}");
            return;
        }
    };
    folders.sort();

    let mut results: Vec<PeriodResult> = Vec::new();
    let mut skipped = 0u32;

    println!(
        "{:<55} | {:>3} | {:>4} | {:>5} | {:>3} | {:>4} | {:>2} | {:>8}",
        "Period", "Dur", "Res", "Ticks", "Ent", "Exit", "SL", "PnL"
    );
    println!("{}", "-".repeat(100));

    for folder in &folders {
        let name = folder
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        let (ticks, resolution) = match parse_ticks(folder) {
            Some(v) => v,
            None => {
                skipped += 1;
                continue;
            }
        };

        if ticks.len() < 20 {
            skipped += 1;
            continue;
        }

        let duration = detect_duration(&ticks);
        if !config.allowed_durations.contains(&duration) {
            skipped += 1;
            continue;
        }

        let mut result = run_period(&ticks, &config, &resolution);
        result.name = name.clone();
        result.duration = duration;
        result.resolution = resolution.clone();

        println!(
            "{:<55} | {:>3} | {:>4} | {:>5} | {:>3} | {:>4} | {:>2} | ${:>+7.2}",
            name,
            duration,
            resolution,
            result.ticks,
            result.entries,
            result.exits,
            result.stoplosses,
            result.pnl
        );

        results.push(result);
    }

    // ── Summary ──
    println!();
    println!("{}", "=".repeat(100));

    let n = results.len();
    if n == 0 {
        println!("No periods processed!");
        return;
    }

    let total_pnl: f64 = results.iter().map(|r| r.pnl).sum();
    let avg_pnl = total_pnl / n as f64;
    let wins = results.iter().filter(|r| r.pnl > 0.0).count();
    let losses = results.iter().filter(|r| r.pnl < 0.0).count();
    let flats = results.iter().filter(|r| r.pnl == 0.0).count();
    let win_rate = wins as f64 / n as f64 * 100.0;

    let total_entries: u32 = results.iter().map(|r| r.entries).sum();
    let total_stoplosses: u32 = results.iter().map(|r| r.stoplosses).sum();
    let total_sell_pnl: f64 = results.iter().map(|r| r.sell_pnl).sum();
    let total_settle_pnl: f64 = results.iter().map(|r| r.settle_pnl).sum();

    let avg_win = if wins > 0 {
        results
            .iter()
            .filter(|r| r.pnl > 0.0)
            .map(|r| r.pnl)
            .sum::<f64>()
            / wins as f64
    } else {
        0.0
    };
    let avg_loss = if losses > 0 {
        results
            .iter()
            .filter(|r| r.pnl < 0.0)
            .map(|r| r.pnl.abs())
            .sum::<f64>()
            / losses as f64
    } else {
        0.0
    };
    let wl_ratio = if avg_loss > 0.0 {
        avg_win / avg_loss
    } else {
        f64::INFINITY
    };

    // Sharpe
    let sharpe = if n > 1 {
        let pnls: Vec<f64> = results.iter().map(|r| r.pnl).collect();
        let variance: f64 =
            pnls.iter().map(|p| (p - avg_pnl).powi(2)).sum::<f64>() / (n - 1) as f64;
        let std = variance.sqrt();
        if std > 0.0 {
            avg_pnl / std
        } else {
            0.0
        }
    } else {
        0.0
    };

    // Drawdown
    let mut cumulative = 0.0_f64;
    let mut peak = 0.0_f64;
    let mut max_dd = 0.0_f64;
    for r in &results {
        cumulative += r.pnl;
        if cumulative > peak {
            peak = cumulative;
        }
        let dd = peak - cumulative;
        if dd > max_dd {
            max_dd = dd;
        }
    }

    // Streaks
    let mut best_streak = 0u32;
    let mut worst_streak = 0u32;
    let mut current: i32 = 0;
    for r in &results {
        if r.pnl > 0.0 {
            current = if current > 0 { current + 1 } else { 1 };
            best_streak = best_streak.max(current as u32);
        } else if r.pnl < 0.0 {
            current = if current < 0 { current - 1 } else { -1 };
            worst_streak = worst_streak.max(current.unsigned_abs());
        } else {
            current = 0;
        }
    }

    println!("Periods: {n} (skipped {skipped})");
    println!("Total PnL: ${total_pnl:+.2} | Avg: ${avg_pnl:+.2}/period");
    println!("Sell PnL: ${total_sell_pnl:+.2} | Settle PnL: ${total_settle_pnl:+.2}");
    println!("Win/Loss/Flat: {wins}/{losses}/{flats} ({win_rate:.1}% win rate)");
    println!("Avg win: ${avg_win:.2} | Avg loss: ${avg_loss:.2} | W/L ratio: {wl_ratio:.2}x");
    println!("Sharpe: {sharpe:.3}");
    println!("Max drawdown: ${max_dd:.2}");
    println!("Best win streak: {best_streak} | Worst loss streak: {worst_streak}");
    println!("Total entries: {total_entries} | Total stoplosses: {total_stoplosses}");
    println!();

    // Top 5 best/worst
    let mut sorted: Vec<&PeriodResult> = results.iter().collect();
    sorted.sort_by(|a, b| a.pnl.partial_cmp(&b.pnl).unwrap());

    println!("Top 5 WORST periods:");
    for r in sorted.iter().take(5) {
        println!(
            "  {:<55} ${:+.2} ({} entries, {} SLs)",
            r.name, r.pnl, r.entries, r.stoplosses
        );
    }
    println!();
    println!("Top 5 BEST periods:");
    for r in sorted.iter().rev().take(5) {
        println!(
            "  {:<55} ${:+.2} ({} entries, {} SLs)",
            r.name, r.pnl, r.entries, r.stoplosses
        );
    }

    // By hour
    println!();
    println!("By hour (ET):");
    let mut hour_data: std::collections::BTreeMap<u32, Vec<f64>> =
        std::collections::BTreeMap::new();
    for r in &results {
        if let Some(hour) = extract_hour_et(&r.name) {
            hour_data.entry(hour).or_default().push(r.pnl);
        }
    }
    for (h, pnls) in &hour_data {
        let n_h = pnls.len();
        let avg_h: f64 = pnls.iter().sum::<f64>() / n_h as f64;
        let wins_h = pnls.iter().filter(|p| **p > 0.0).count();
        let wr = wins_h as f64 / n_h as f64 * 100.0;
        println!("  {h:2}:00 ET — {n_h:3} periods, avg ${avg_h:+.2}, win rate {wr:.0}%");
    }
}

fn extract_hour_et(name: &str) -> Option<u32> {
    // Folder format: 2026-02-28_February_28_3-05AM-3-10AM_ET
    // Find the time part like "3-05AM" or "10-00AM"
    let parts: Vec<&str> = name.split('_').collect();
    for part in &parts {
        if (part.contains("AM") || part.contains("PM")) && part.contains('-') {
            let time_str = part.split('-').next()?;
            let is_pm = part.contains("PM");
            let hour: u32 = time_str.parse().ok()?;
            let h24 = if is_pm && hour != 12 {
                hour + 12
            } else if !is_pm && hour == 12 {
                0
            } else {
                hour
            };
            return Some(h24);
        }
    }
    None
}
