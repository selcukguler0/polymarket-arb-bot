# How to Add a New Strategy

## File Structure

For a strategy called `my_strategy`:

```
src/strategies/my_strategy.rs       # Strategy logic (config, main loop, position tracking)
src/bin/my_strategy_bot.rs          # Entry point (arg parsing, env vars, executor setup)
strategies/my_strategy/             # Non-code assets (at project root)
  config/default.toml               # Strategy-specific config
  logs/                             # Trade CSV logs
  data/                             # Backtest data, analysis artifacts
```

## Steps

### 1. Create the strategy module

`src/strategies/my_strategy.rs` — contains:
- Config struct with `Default` impl
- Position tracking types
- `pub async fn run(config, executor, redeem_ctx) -> Result<()>` main loop
- Reuse `super::core::` for shared infra

### 2. Register in mod.rs

Add to `src/strategies/mod.rs`:
```rust
pub mod my_strategy;
```

### 3. Create the binary entry point

`src/bin/my_strategy_bot.rs` — pattern:
```rust
use polymarket_arb::strategies::core::{Executor, RedeemContext};
use polymarket_arb::strategies::my_strategy::{self, MyStrategyConfig};

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider().install_default().expect("TLS");
    dotenvy::dotenv().ok();
    // parse args, build config, create executor if --live, call my_strategy::run()
}
```

### 4. Create config and asset dirs

```bash
mkdir -p strategies/my_strategy/{config,logs,data}
```

Write `strategies/my_strategy/config/default.toml` with strategy parameters.

### 5. Build and test

```bash
cargo check --bin my_strategy_bot
cargo run --release --bin my_strategy_bot           # paper
cargo run --release --bin my_strategy_bot -- --live  # live
```

## Shared Core (`strategies::core`)

Reuse these from `super::core::`:

| Function | Purpose |
|----------|---------|
| `scan_clob_markets(cursor, durations)` | CLOB paginated market discovery |
| `estimate_clob_start_cursor()` | Starting cursor for CLOB scan |
| `fetch_book(token_id)` | Single token orderbook (best bid/ask + size) |
| `fetch_market_books(market)` | Both Up+Down books for a market |
| `Executor::new(pk, builder_key, secret, pass, dry_run)` | Authenticated CLOB client |
| `Executor::buy_fok(token_id, price, size, tick_size)` | FOK buy order |
| `Executor::place_fok(token_id, side, price, size, tick, label)` | Generic FOK |
| `redeem_sweep(ctx)` | Sweep all redeemable positions on-chain |
| `TradeLogger::new(path, header)` | CSV trade logger |
| `round_to_tick(price, tick_size)` | Price quantization |

## Key Types from Core

- `Market` — condition_id, token_id_up, token_id_down, start/end dates, tick_size
- `BookSnapshot` — best_bid, best_ask, bid_size, ask_size
- `Executor` — CLOB client with Builder auth + signer
- `RedeemContext` — signer, wallet_address, rpc_url for on-chain redemption

## Env Vars (shared across strategies)

Required for live mode:
- `POLYMARKET_PRIVATE_KEY`
- `POLYMARKET_WALLET_ADDRESS`
- `POLY_BUILDER_KEY`, `POLY_BUILDER_SECRET`, `POLY_BUILDER_PASSPHRASE`
- `POLYGON_RPC_URL` (optional, defaults to `https://polygon-rpc.com`)

## Existing Strategies

| Strategy | Binary | Module | Status |
|----------|--------|--------|--------|
| V2 Gabagool (maker arb) | `polymarket-arb` | `orchestrator_v2.rs` | Production |
| Convergence (taker FV) | `convergence_bot` | `src/bin/convergence_bot.rs` | Standalone |
| Complete Set | `complete_set_bot` | `src/complete_set.rs` | Standalone |
| Post-Resolution Pair (taker) | `post_resolution_bot` | `strategies/post_resolution.rs` | FAILED (no asks) |
| Post-Resolution Maker | `post_resolution_maker_bot` | `strategies/post_resolution_maker.rs` | New (uses core) |
| Daily Prediction Arb | `daily_prediction_bot` | `strategies/daily_prediction.rs` | FAILED (asks >$1) |

Note: convergence_bot and complete_set predate the `strategies/` module. They have their own inline market discovery and execution code. New strategies should use `strategies::core` instead.
