# Contributing to Polymarket Arb Bot

Thanks for your interest in contributing! This project is open-sourced primarily for educational purposes, but PRs are welcome.

## Getting Started

1. Fork the repo
2. Clone your fork
3. Copy `.env.example` to `.env` and fill in your credentials
4. Run `cargo build --release` to verify everything compiles
5. Run in paper mode first: set `mode = "paper"` in `config/v2.toml`

## Development

```bash
# Build
cargo build --release

# Run the main bot (paper mode)
cargo run --release --bin polymarket-arb

# Run a specific strategy
cargo run --release --bin convergence_bot
cargo run --release --bin deep_discount_maker_bot
```

## Project Structure

- `src/orchestrator_v2.rs` — Main bot logic (~9900 lines). This is the core.
- `src/sdk.rs` — Polymarket CLOB SDK wrapper
- `src/relayer.rs` — Gnosis Safe gasless relayer
- `src/strategies/` — Strategy implementations
- `config/v2.toml` — All configuration with extensive comments
- `learnings/` — Hard-won lessons from live trading. **Read these before making changes.**
- `docs/` — Research reports and validation results

## Before Submitting a PR

- [ ] `cargo build --release` passes
- [ ] `cargo clippy` has no warnings
- [ ] You've read the relevant `learnings/` files
- [ ] You haven't introduced any secrets or hardcoded wallet addresses
- [ ] You've tested in paper mode

## Key Rules

These rules exist because we learned them the hard way:

1. **NEVER place/cancel orders sequentially** — always use batch APIs
2. **NEVER remove ask-anchoring** — it's the primary fillability mechanism
3. **NEVER build taker strategies assuming combined asks < $1.00** — they're always above $1.00
4. **NEVER use live price as `btc_open`** — use `fetch_binance_kline_open()`
5. **Check `learnings/` before changing config** — many values were tuned through painful live experience

## Areas That Could Use Help

- **Latency optimization**: The biggest bottleneck. Sub-50ms to London CLOB would change everything.
- **Fill modeling**: Paper sim is still 2-3x optimistic vs live. Better adverse selection modeling needed.
- **New strategies**: The research data in `docs/` and `memory/` has unexplored angles.
- **Testing**: No test suite currently exists. Unit tests for the pricing model would be valuable.

## Code Style

- Follow existing patterns in the codebase
- Use `parking_lot::RwLock` (not `std::sync`), `tokio::sync::mpsc` for channels
- Batch all order operations (never sequential place/cancel)
- Keep the `NEVER Rules` in mind at all times

## Questions?

Open an issue. We're happy to explain the architecture or research findings.
