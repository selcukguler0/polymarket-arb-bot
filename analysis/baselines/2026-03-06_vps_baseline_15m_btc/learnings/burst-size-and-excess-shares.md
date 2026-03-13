# Burst Size vs Excess Shares

## Problem
With `ladder_levels_15m = 3` and `base_order_shares = 5`, a single batch places
15 shares per side. If one side fills before the imbalance guard (>8 shares) reacts,
you get 15 excess shares on the wrong side — a pure directional bet.

## Evidence (Session 3, 2026-03-05)
Post-revert data (11 periods, 15-min only):
- Periods with excess >= 5: 3 out of 11
- Those 3 periods: +$2.01, +$2.05, -$2.49 (coin-flip directional)
- Periods with excess < 5: 8 out of 11, all profitable or near-breakeven
- The -$2.49 loss: DOWN filled 13.8 shares in 4 seconds, UP got only 10

## Root Cause
The imbalance guard fires when excess > 8 shares. With 3 levels × 5 shares = 15,
a full batch can fill on one side before the guard reacts. The guard prevents the
NEXT cycle but can't undo fills from the current batch.

## Fix: ladder_levels_15m = 3 → 2
- Max burst: 10 shares per side per cycle (was 15)
- After one full one-sided fill (10 shares), guard fires (10 > 8)
- Within $20 budget: more cycles with smaller batches = more gradual fills
- Expected: fewer excess shares, lower tail risk, similar fill capacity

## Key Insight
The optimal ladder_levels depends on base_order_shares and imbalance_threshold:
- levels * base_order_shares should be close to imbalance_threshold
- If burst > threshold: guard can't react fast enough
- If burst << threshold: unnecessarily slow filling
- Sweet spot: burst slightly above threshold → guard fires after 1 cycle
