# Polymarket Arb Bot (Gabagool)

A Rust-based market-making and arbitrage bot for [Polymarket](https://polymarket.com) binary prediction markets (BTC/ETH/SOL/XRP Up/Down 5/15-minute markets).

> **Disclaimer**: This bot was a research/trading project that didn't achieve consistent profitability in production due to latency constraints and adverse selection. We're open-sourcing it for educational purposes. **Use at your own risk. You will likely lose money.**

## What This Is

A complete-set arbitrage bot that places maker bids on both sides of binary prediction markets. When both sides fill at a combined cost < $1.00, the pair locks in risk-free profit. The bot also includes several experimental strategies.

### Strategies

| Strategy | Binary | Status |
|----------|--------|--------|
| **Complete-set arb (main)** | `polymarket-arb` | Paper-profitable ($6.70/period), live performance gap |
| **Deep discount maker** | `deep_discount_maker_bot` | Experimental — wide grid inspired by top wallets |
| **Post-resolution maker** | `post_resolution_maker_bot` | Experimental — GTC bids after market settles |
| **Convergence (taker)** | `convergence_bot` | Experimental — FOK on FV-book divergence |
| **Daily prediction** | `daily_prediction_bot` | Failed — combined asks always > $1.00 |
| **Post-resolution taker** | `post_resolution_bot` | Failed — no asks on losing side |

### Key Features

- **Gnosis Safe integration**: Gasless trading via Polymarket relayer (no ETH needed for gas)
- **Black-Scholes fair value**: Real-time FV pricing from Binance BTC/ETH/SOL/XRP feeds
- **Deep ladder grid**: Non-uniform bid spacing ($0.01-$0.50) inspired by top wallet analysis
- **700K-row wallet analysis**: Research on top-20 Polymarket wallets and their strategies
- **Paper trading mode**: Full simulation with realistic fill modeling
- **Web dashboard**: Real-time SSE dashboard on port 4000
- **Batch order management**: Single HTTP call for up to 15 orders

## Architecture

```
src/
├── orchestrator_v2.rs  # Main bot logic (~9900 lines)
├── sdk.rs              # Polymarket CLOB SDK wrapper
├── relayer.rs          # Gnosis Safe gasless transaction relayer
├── config.rs           # Configuration parsing
├── strategies/         # Strategy implementations
│   ├── core.rs
│   ├── deep_discount_maker.rs
│   ├── post_resolution.rs
│   ├── post_resolution_maker.rs
│   └── daily_prediction.rs
├── bin/                # Binary entrypoints
│   ├── backtest.rs
│   ├── convergence_bot.rs
│   ├── complete_set_bot.rs
│   └── ...
├── web/                # Web dashboard (SSE)
└── onchain/            # On-chain operations (merge, redeem, split)
```

## Setup

### Prerequisites

- Rust 1.75+ (edition 2024)
- A Polygon wallet with USDC
- Polymarket CLOB API credentials ([docs](https://docs.polymarket.com))
- A VPS in Europe (London/Ireland preferred for lowest latency to Polymarket CLOB)

### Installation

```bash
git clone https://github.com/selcukguler0/polymarket-bot.git
cd polymarket-bot

# Copy and fill in your credentials
cp .env.example .env
# Edit .env with your keys

# Build
cargo build --release

# Run in paper mode first!
# Edit config/v2.toml and set mode = "paper"
cargo run --release --bin polymarket-arb
```

### Gnosis Safe Setup (Optional, Recommended)

The bot supports gasless trading via Gnosis Safe. This means you don't need ETH for gas — Polymarket's relayer pays for on-chain transactions.

```bash
# One-time: deploy Safe and approve contracts
cargo run --release --bin setup_safe

# Transfer USDC to your Safe
cargo run --release --bin safe_transfer
```

Set `eoa_mode = false` in `config/v2.toml` to use Safe mode.

### Configuration

All config is in `config/v2.toml`. Key settings:

- `mode`: `"paper"` or `"live"`
- `eoa_mode`: `true` for direct EOA, `false` for Gnosis Safe
- `target_combined`: Target combined bid cost (lower = wider margin, fewer fills)
- `base_order_shares`: Shares per order level
- `ladder_levels`: Number of bid levels per side
- `allowed_durations`: Which market durations to trade `[5, 15]`

See the config file for extensive documentation on every parameter.

### Cross-Compile for Linux VPS

```bash
# Install zigbuild
cargo install cargo-zigbuild

# Build for Linux
cargo zigbuild --release --target x86_64-unknown-linux-gnu --bin polymarket-arb

# Deploy
scp target/x86_64-unknown-linux-gnu/release/polymarket-arb user@your-vps:~/polymarket-bot/
```

## Why It Didn't Work

Honest post-mortem:

1. **Latency**: Our VPS in Helsinki had ~236ms round-trip to Polymarket CLOB (London). Top wallets likely have <10ms from co-located servers in Ireland/London. By the time our orders land, the market has moved.

2. **Adverse selection**: When BTC moves against our bids, counterparties dump losing shares into our resting orders. We fill disproportionately on the wrong side. The 500ms taker delay removal (Feb 2026) made this worse.

3. **Paper vs live gap**: Paper simulation was 4-7x too optimistic. We fixed the sim (Phase A), but the fundamental speed disadvantage remained.

4. **Market structure**: Combined asks are always > $1.00. Taker strategies are dead on arrival. Maker strategies require sub-50ms latency to avoid adverse selection.

## Research & Analysis

The `memory/`, `learnings/`, and `docs/` directories contain extensive research:

- **Wallet analysis**: How top-20 wallets profit (spoiler: deep maker bids, not taker)
- **Market structure**: Why combined asks > $1.00, post-resolution dynamics
- **Live trading reports**: What went wrong and what we learned
- **Validation results**: 688-period paper trading validation

## Important Notes

- **US IPs are blocked** on the international Polymarket CLOB API
- The Polymarket SDK (`polymarket-client-sdk`) is a third-party Rust crate
- This bot interacts with real money. Paper trade extensively before going live
- Dynamic taker fees (up to 3.15% at 50c) kill latency arbitrage strategies

## License

[MIT](LICENSE)
