# Hourly Market Research

## Discovery (2026-03-04)

Polymarket has hourly BTC Up/Down markets. They use a **different slug format** from 5m/15m markets.

### Slug Formats
- 5-min: `btc-updown-5m-{unix_timestamp}` (auto-generated, CLOB native)
- 15-min: `btc-updown-15m-{unix_timestamp}` (auto-generated, CLOB native)
- 1-hour: `bitcoin-up-or-down-march-4-9am-et` (Gamma-style, different naming)

### URL Format
- Polymarket page: `https://polymarket.com/event/bitcoin-up-or-down-march-4-9am-et`
- Gamma API: `https://gamma-api.polymarket.com/events?slug=bitcoin-up-or-down-march-4-9am-et`
- Crypto hourly hub: `https://polymarket.com/crypto/hourly`

### Market Properties
- Resolution: Binance BTC/USDT 1H candle, "Up" if close >= open
- Outcomes: "Up" / "Down" (same as 5m/15m)
- negRisk: false (same as 5m/15m)
- Created ~2 days before the period
- End time = period_start + 1 hour (UTC)

### Liquidity Comparison (2026-03-04)
| Duration | Typical Liquidity | Typical Volume |
|----------|------------------|----------------|
| 5-min | $1-5K | $500-2K |
| 15-min | $2-8K | $1-5K |
| **1-hour** | **$13-28K** | **$500-78K** |

Hourly markets have **4-10x more liquidity** than 5m/15m.

### Available Time Slots (March 4)
- 9am, 10am, 11am, 12pm, 1pm, 2pm ET (6 periods visible)
- Volume concentrates on earlier hours (9am: $78K, 2pm: $59)

### Implementation Considerations
1. **Discovery**: Cannot use the `btc-updown-{dur}-{timestamp}` pattern. Need Gamma API search with `slug=bitcoin-up-or-down-{month}-{day}-{time}-et`
2. **Date/time parsing**: Slug contains natural language date ("march-4") and time ("9am-et")
3. **Open price**: Must align with Binance 1H candle open. The open price is the first trade of the hourly candle on Binance.
4. **FV model**: Same formula works (Black-Scholes-like with btc_price vs open and sigma), but sigma needs recalibration for 1H timeframe (more time = more uncertainty = wider FV distribution near 0.50)
5. **Period management**: Only ~6 periods per day vs 96 for 15-min
6. **Slug prediction**: Need to know the pattern for future dates to auto-discover

### Advantages for Our Strategy
- **More time to build pairs** (60 min vs 15 min) — near-zero 0-pair risk
- **Higher liquidity** — better fills, less slippage
- **More counterparties** — harder for a single whale to adversely select us
- **FV model accuracy** — more price data, smoother FV evolution

### Disadvantages
- **Larger BTC moves** in 1H → wider FV swings → more repricing needed
- **Longer position hold** → more capital locked per period
- **Fewer periods** (6/day vs 96) → slower capital rotation
- **Adverse selection window** — counterparties have more time to dump losing side
- **Sigma calibration** needed for 1H (different from 5m/15m)

### Implementation (2026-03-04)
- Added to `parse_market_duration_minutes()`: fallback detects "AM ET"/"PM ET" without colons → returns 60
- Added `_60m` config fields: `ladder_levels_60m=15`, `trend_threshold_60m=800`, `price_shock_threshold_60m=100`
- Updated 3 match arms: ladder_levels, trend_threshold, price_shock to handle `8..=30` (15m) and `_` (60m)
- Config: `allowed_durations = [15, 60]`
- Verified: bot discovers hourly market and correctly applies late-entry filter (92% > 85%)
- Bot will trade the NEXT hourly period when it starts (current 9AM ET was already 92% elapsed)

### CRITICAL BUG: btc_open Captured Wrong (2026-03-04)

**Problem**: For hourly markets, `btc_open` was captured from the live Binance WS trade price at discovery time. But Polymarket resolves against the **Binance 1H kline open** (first trade at candle boundary). These can differ by **$100-300+**.

**Example**: 10AM ET market (15:00 UTC)
- Real Binance 1H kline open: **$71,621**
- Bot's captured btc_open: **$71,907** (live price at discovery, $286 too high!)
- Result: FV said 2% Up, market said 89% Up. **Completely wrong FV.**

**Why 5m/15m don't have this issue**: The bot discovers them at or very near period start, so live price ≈ candle open.

**Fix**: For markets with duration >= 60 min, fetch the real candle open from `GET https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h&startTime={ms}&limit=1`. Uses `spawn_blocking` + `ureq` (same pattern as CLOB book fetch). Falls back to live price if API call fails.

**Lesson**: Any time the bot enters a market that started more than a few seconds ago, the "live price = candle open" assumption breaks. For future durations (4h, daily), the same kline fetch pattern is needed.

### Open Questions
- What are the slug patterns for other coins (ETH, SOL, XRP)? → Confirmed: `ethereum-up-or-down-march-4-10am-et`, `solana-up-or-down-march-4-10am-et`, etc.
- Are markets created for all hours or just 9AM-2PM ET? → Needs observation
- Can we run hourly + 15-min simultaneously on different markets? → YES, implemented. Both run on the same orchestrator.
- What's the appropriate sigma floor for 1H? → Using same as 15m for now, may need tuning
