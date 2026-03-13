# CLAUDE.md — Core Rules

## Market Structure Fundamentals
- **Combined ask (Yes+No or Up+Down) is ALWAYS > $1.00.** This is how binary markets work — market makers set asks above $1.00 to profit from spread. You CANNOT buy both sides at the ask and profit. NEVER build a taker strategy that assumes combined asks < $1.00.
- **Post-resolution, the losing side has ZERO asks on the book** but sellers DO hit resting bids during active hours. Overnight book snapshots (4,599) showed no asks, but 59K loser-token BUY trades prove counterparties sell into maker bids 6AM-5PM ET.
- **Top wallets profit by placing BIDS (maker), not lifting asks (taker).** 700K-row analysis: median combined cost $0.92-0.97 via limit bids. Best wallets achieve 18% ROI with sell mechanism for unmatched shares.
- Outcome strings: Up/Down markets use "Up"/"Down". Daily prediction markets use "Yes"/"No".

## Architecture
- Rust bot. SDK: `polymarket-client-sdk v0.4.2`. Config: `config/v2.toml`. Secrets: `.env`.
- Key files: `src/orchestrator_v2.rs` (~9900 lines), `src/sdk.rs`, `src/strategies/`, `src/relayer.rs`.
- `parking_lot::RwLock` for shared state, `tokio::sync::mpsc` for channels.
- Batch placement: `place_batch_orders()`. NEVER place/cancel orders sequentially.

## Wallet (Gnosis Safe — Gasless)
- Set your EOA and Safe addresses in `.env` (`POLYMARKET_PRIVATE_KEY`, `WALLET_ADDRESS`).
- `eoa_mode = false` in `config/v2.toml`. Orders signed with `SignatureType::GnosisSafe` (type 2).
- All on-chain ops (merge, redeem, split, approve) are gasless via Polymarket relayer (`src/relayer.rs`).
- Relayer uses HMAC-SHA256 Builder auth + EIP-712 SafeTx signing with eth_sign prefix (v+4/+31).
- MultiSend batching for multi-transaction operations (approvals, etc.).
- Utilities: `setup_safe` (one-time deploy+approve), `safe_transfer` (withdraw USDC from Safe).

## Deployment
- Cross-compile for Linux: `cargo zigbuild --release --target x86_64-unknown-linux-gnu --bin <binary_name>`
- Polymarket CLOB origin: **London (AWS eu-west-2)**, behind Cloudflare.
- Best non-blocked location: **Ireland (AWS eu-west-1)** — ~10-15ms to origin.
- US IPs are blocked on international CLOB API.

## NEVER Rules
1. ALL imbalance/threshold params MUST be > base_order_shares (15)
2. NEVER remove ask-anchoring — PRIMARY fillability mechanism
3. NEVER place/cancel orders sequentially — use batch APIs
4. NEVER set fv_stale_cancel_cents < 0.08 or ladder_reprice_threshold < 0.03
5. NEVER use live price as btc_open for hourly markets — use `fetch_binance_kline_open()`
6. NEVER build taker strategies assuming combined asks < $1.00

## Strategy Status
- **Main arb bot (orchestrator_v2)**: Maker-based complete-set arb on 5/15-min Up/Down markets. Validated 688 periods, $6.70/period avg in paper.
- **Post-resolution maker bot**: GTC limit bids on both sides post-resolution, sell unmatched. Binary: `post_resolution_maker_bot`.
- **Post-resolution bot (taker)**: FAILED — no asks on losing side post-resolution.
- **Daily prediction bot**: FAILED — combined asks always > $1.00. No taker opportunity exists.
- **Convergence bot**: Taker FOK on FV-book divergence. 5-min BTC only.

## Reference
- Learnings: `learnings/` — ALWAYS check before making changes
- Research docs: `docs/`
