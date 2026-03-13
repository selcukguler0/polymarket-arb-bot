# Session 2 Analysis: March 5, 2026 (~04:44-07:58 UTC)

## Performance Summary
- 14 periods: 9W / 5L (64% win rate)
- Cumulative PnL: **-$4.24**
- Avg win: +$0.85, Avg loss: -$2.38
- Loss/Win ratio: 2.8x (need 74% WR to breakeven)

## By Duration
| Duration | Periods | PnL | Avg/Period |
|----------|---------|------|-----------|
| 15-min | 12 (8W/4L) | -$2.08 | -$0.17 |
| 60-min | 2 (1W/1L) | -$2.16 | -$1.08 |

## Critical Finding: Negative Locked Profit
**7 out of 14 periods (50%) had negative locked profit** — meaning the combined fill cost exceeded $1.00. This is the #1 issue.

### Root Cause
When BTC moves during a period:
1. Side A fills at price X (e.g., 0.48) when FV ≈ 0.50/0.50
2. BTC moves → FV shifts (e.g., 0.30/0.70)
3. Side B fills at price Y (e.g., 0.56) because that side is now more likely
4. Combined: X + Y = 1.04 → **negative locked profit**

The `target_combined = 0.95` setting controls individual ORDER pricing, but actual FILL PAIRS span different points in time when BTC has moved.

### Why Hourly is Worse Than 15-min
- 60-min BTC range is much larger (avg $282) than 15-min (avg $127)
- More time for BTC to move = more adverse fill timing = more negative locked profit
- The 12AM_ET hourly had BTC move of $404 → locked profit = -$1.68

## Config Changes Made (v2.toml)
1. `target_combined`: 0.95 → **0.93** — more margin for adverse fill timing
2. `allowed_durations`: [60] → **[15, 60]** — 15-min was historically +$1.54/period
3. `period_worst_case_loss_cap_usdc`: 6.0 → **4.0** — cap tail losses earlier

## What These Changes Should Do
- Lower target_combined → each pair has $0.07 margin instead of $0.05
- This means fills can differ by up to $0.07 before going negative (was $0.05)
- Trade-off: fewer fills (wider spread) but each fill is more profitable
- Fewer catastrophic periods from negative locked profit
- 15-min periods recover: more opportunities per hour, less BTC movement per period

## Additional Config Changes (mid-session)
5. `base_order_shares`: 10 → **7** (limit initial burst to ~7 shares/fill)
6. `max_share_imbalance`/`one_sided_threshold`: 12 → **10** (faster guard reaction)
7. `ladder_levels_15m`: 10 → **5** (fewer simultaneous orders)
8. `ladder_levels_60m`: 15 → **8**
9. `trend_threshold_15m`: 500 → **150** (catch BTC trends much earlier)
10. `trend_threshold_60m`: 800 → **300**
11. `light_side_max_combined`: 1.02 → **1.05** (allow moderate loss pair completion)

## Key Insight: Pair Completion Override is CORRECT
- Tested restricting pair completion (cap at light_side_max instead of skip)
- Result: -$6.59 loss vs +$0.03 with original unlimited pair completion
- Math proves: pairing at ANY cost is EV-neutral but VARIANCE-reducing
- Converting 96% chance of -$6.63 into guaranteed ~-$2 is better for arb
- The REAL fix for one-sided positions is smaller orders (base=7) and tighter thresholds

## Key Insight: Sigma ≠ Trend
- Sigma circuit breaker measures instantaneous volatility
- A STEADY trend has LOW sigma but causes the worst adverse selection
- Trend threshold (btc move from open) catches this better
- $500 threshold for 15-min was useless — lowered to $150

## Post-Config Period Results
- 4:15-4:30 (base=7, thresholds=10): **+$1.40**, 19 pairs, locked=+$0.80
- Combined cost: 0.958 (close to 0.93 target)
- Balanced fills: 20 UP / 19 DOWN — excellent pairing

## Monitoring Plan
- Track locked_profit per period (target: >60% positive)
- Track avg combined cost per pair (target: <$0.97)
- Compare periods WITH trend threshold trigger vs WITHOUT
- If combined costs still >0.97 often, try target_combined=0.91
