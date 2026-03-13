# Config Change Learnings

## 2026-03-04: Budget & Threshold Tuning (Live Canary)

### Change 1: Tighter Imbalance Thresholds (GOOD)
**What**: `one_sided_threshold`: 300 → 20, `max_share_imbalance`: 150 → 25, `imbalance_decay_floor_abs`: 100 → 20
**Why**: With canary_budget=$15 and base_order_shares=15, the old thresholds (300) never fired. The bot could accumulate up to 300 shares one-sided before the guard kicked in, but the budget only allowed ~15 shares. So the guard was useless.
**Result**: Reduced 0-pair exposure somewhat. Key insight: **thresholds must be proportional to budget/order_size, not set to arbitrary large values**.
**Lesson**: When budget is small (canary mode), all position guards need to scale down proportionally.

### Change 2: 15-min Only Mode (VERY GOOD)
**What**: `allowed_durations`: [5, 15] → [15]
**Why**: 5-min periods were net negative (-$63.72 over 126 periods = -$0.51/period). 15-min periods were net positive (+$70.90 over 46 periods = +$1.54/period). 5-min had 31% 0-pair rate vs 20% for 15-min.
**Result**: Practically eliminated 0-pair periods. More time to build pairs.
**Lesson**: **Shorter durations are WORSE for complete-set arbitrage** because there's less time to fill both sides. 15-min gives 3x more time to complete pairs.

### Change 3: Budget Reduction $15 → $10 (BAD - REVERTED)
**What**: `canary_budget`: $15 → $10
**Why**: Trying to cap max loss after a -$7.05 0-pair period.
**Result**: COUNTERPRODUCTIVE. The one-sided threshold (16) already caps max loss regardless of budget. Reducing budget only reduced pairs (from 13-15 to 8-10) and profit. PnL per pair dropped to $0.02 and avg period PnL went to -$0.14.
**Reverted to**: $15
**After revert**: 8W/1L in first 9 periods, avg +$0.65/period, pairs back to 13-15.
**Lesson**: **Budget caps profit, thresholds cap loss**. Never reduce budget to limit losses — use thresholds instead. The budget should be as high as risk appetite allows.

### Change 4: Further Threshold Tightening 20 → 16 (NEUTRAL)
**What**: All three thresholds lowered from 20 to 16 (just above base_order_shares=15).
**Why**: One more 0-pair period occurred even with threshold=20.
**Result**: Marginal effect. The 0-pair period was caused by FV extremes, not by one-sided accumulation.
**Lesson**: Thresholds help but can't prevent all 0-pair periods. FV-driven suppression is a different mechanism.

### Current Best Config (2026-03-04)
```toml
canary_budget = "15"
allowed_durations = [15]
one_sided_threshold = "16"
max_share_imbalance = "16"
imbalance_decay_floor_abs = "16"
```
Performance since this config: ~12 periods, 11W/1L, avg +$0.60/period.

## Key Principles Discovered
1. **Budget caps profit, thresholds cap loss** — never confuse the two
2. **Longer durations = more pair completion time = better for arb**
3. **Thresholds must be proportional to order size** — a threshold of 300 with 15-share orders is useless
4. **PnL per pair is the core metric** — target >$0.03/pair for sustainable profitability
5. **0-pair periods are catastrophic** — a single -$7 loss wipes out 10+ good periods
6. **The $15 budget revert was the most impactful change** — immediately went from -$0.14 to +$0.65/period
