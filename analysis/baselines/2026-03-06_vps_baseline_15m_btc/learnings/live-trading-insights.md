# Live Trading Insights

## Session 1: March 4, 2026 (First Full Live Session)

### Overall Stats (as of period ~214)
- Total PnL: ~$15.23
- Periods: 214 (132W / 82L)
- Win rate: 61.7%
- Today PnL: ~-$5 (early 5-min losses dragged it down)

### Key Findings

#### 1. Paper Sim vs Live Gap
- Paper sim predicted $6.70/period avg. Live reality: ~$0.60/period (11x lower)
- Main cause: paper sim fills are instant and at bid price. Live fills are delayed and often at worse prices.
- Combined fill cost often exceeds $1.00 even though bids are placed at combined $0.95

#### 2. 5-min Periods Are Unprofitable Live
- 126 5-min periods: -$63.72 total (-$0.51/period)
- 46 15-min periods: +$70.90 total (+$1.54/period)
- 5-min doesn't give enough time to complete pairs in live order flow
- Switched to 15-min only: immediately improved to +$0.60/period

#### 3. PnL Per Pair is the Core Metric
- Target: >$0.03/pair (from combined cost < $0.97)
- Actual live: ~$0.02-$0.06/pair (varies by period)
- With 13-15 pairs/period at $15 budget: $0.26-$0.90/period
- With 8-10 pairs/period at $10 budget: $0.16-$0.20/period (breakeven)

#### 4. Loss Asymmetry
- Average win: ~$0.60-$0.90
- Average loss: ~$1.45
- Loss/win ratio: ~2x
- Need 67%+ win rate to be net positive
- Current: 61.7% → barely profitable
- The 0-pair catastrophic losses (-$7) destroy many periods of gains

#### 5. Time-of-Day Effects (Live)
- 3-4 AM ET: Best hours (confirmed in paper sim too)
- 8-9 AM ET: Volatile but still profitable (+$1.43 in 9:00-9:15 period)
- BTC trend matters: strong directional moves = high locked profit per pair

### Risk Events
- One 0-pair period at 3:30 AM: -$7.05 (wiped 10 periods of gains)
- Several negative locked profit periods (combined fill > $1.00)
- Today's PnL started negative from early 5-min losses

### What Works
1. 15-min only mode
2. $15 budget (NOT $10)
3. One-sided threshold at 16
4. Strong directional BTC moves (FV shifts → easy pair completion)

### What Doesn't Work
1. 5-min periods (not enough time for pair completion)
2. Budget reduction (reduces profit without reducing loss)
3. Thresholds much larger than order size (useless)
4. Relying on paper sim numbers for live expectations

## Monitoring Lessons (2026-03-04)

### Must-Monitor During Live Operation
1. **FV vs Market price divergence** — if FV is >20% off market mid, something is WRONG (btc_open, sigma, etc.)
2. **btc_open accuracy** — verify against Binance kline API, especially for hourly markets
3. **Pair size mismatches** — ladder decay causes unequal fill sizes (15 DOWN but 12 UP). Min pairs = min(up, down).
4. **Sigma sanity** — if sigma < 0.00010, FV becomes hypersensitive. If sigma > 0.001, FV is too wide.
5. **Fill latency** — if fills take >2s consistently, we're getting adversely selected
6. **0-pair detection** — if 3+ consecutive periods have 0 pairs, something systemic is wrong

### What to Automate
- Alert if |FV_up - market_mid_up| > 0.15 for more than 30 seconds
- Alert if btc_open differs from Binance kline open by >$50
- Alert if all fills are on one side only (100% UP or 100% DOWN) for >2 minutes

## Feature Ideas from Live Observation

### Pair Completion Over-Budget
**Problem**: Ladder decay means fills are unequal (e.g., 15 DOWN + 12 UP = only 12 pairs).
**Idea**: Allow bot to exceed budget by up to X% to complete pairing, then stop buying.
**Status**: Not yet implemented. Needs design — budget is per-period, excess only makes sense for completing the *current* imbalance.
