# Session 3 Config Changes (2026-03-05)

## Changes Made (chronological)

### 1. Disable hourly markets (commit 2e1ca62)
- `allowed_durations = [15]` (was [15, 60])
- Hourly: 0W/4L = -$13.78

### 2. Reduce ladder_levels_15m: 3 → 2 (commit c9b1814)
- Max burst 10 shares/side/cycle (was 15)
- With base=5 and imbalance guard at >8, 15-share burst outruns guard

### 3. Disable rebalance_size_multiplier: 2 → 1 (commit d1ea923)
- 2x creates 10-share orders that overshoot balance
- Example: 5.2 UP excess → 10+5 DOWN fill → 9.8 DOWN excess

### 4. Tighten max_sigma: 0.00020 → 0.00012 (commit 86408d9)
- All 10+ excess periods had sigma > 0.000140
- All profitable periods peaked below sigma 0.000103
- Zero false positives on winning periods

### 5. Tighten trend_threshold_15m: $150 → $100 (commit ac35c67)
- All periods with |move| > $100 AND excess >= 5 lost money
- Trend filter only suppresses LOSING-side buys
- Balanced winning periods unaffected

## Key Performance Data
- Low excess (<5 shares): +$0.53/period average (19 periods)
- High excess (>=5 shares): -$1.72/period average (10 periods)
- Excess shares on wrong side are the #1 source of losses
- In calm conditions: +$0.60-$1.05/period consistently

## Performance by Config Phase (103 periods)
- Pre-revert: -$18.80 / 64p (53% WR, -$0.29/p)
- Post-revert (3 levels): +$0.53 / 16p (69% WR)
- Post-2level: -$12.92 / 15p (dragged by $6.32 budget overshoot)
- Post-budget-fix: +$11.50 / 8p (88% WR, +$1.44/p)

Most impactful change: rebalance_max_extra_budget 25→5

## Root Cause Analysis
Excess shares accumulate because:
1. One side fills in a burst before guards react
2. Resting orders fill at stale prices during fast BTC moves
3. The 5-share CLOB minimum means even 1 extra fill = 5 excess
4. Sigma lags actual volatility during sudden crashes
5. Trend filter and vol breaker both have reaction delay
6. Rebalance extra budget ($25) allowed massive over-accumulation
