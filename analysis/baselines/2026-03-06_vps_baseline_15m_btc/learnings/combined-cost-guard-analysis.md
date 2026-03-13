# Combined Cost Guard Analysis (Session 3, March 5 2026)

## The Three-Layer Cost Protection System

### Layer 1: target_combined (0.93) — Ladder Pricing
- Controls initial ladder CENTER price: `center = target_combined * FV`
- With FV_up=0.50, center_up = 0.465
- PROBLEM: PostOnly ask-anchoring overrides this, pushing bids near market ask
- PostOnly anchors bid to `ask - buffer_ticks` (3 ticks = $0.03)
- Result: effective center ≈ market_ask - 0.03, ignoring target_combined

### Layer 2: max_combined_avg_cost (was 1.00, now 0.97) — Hard Guard
- Per-order combined cost guard: `price + avg_opposite <= threshold`
- When position=0/0, estimates avg_opposite from opposite ladder top
- This is the REAL cap after PostOnly anchoring
- At 1.00: almost never triggered (useless kill switch)
- At 0.97: blocks top 2-3 levels after anchoring, ensures $0.03+ margin/pair

### Layer 3: light_side_max_combined (was 1.05, now 1.00) — Imbalance Threshold
- When one side is heavier, the lighter side gets this threshold instead
- Applied whenever imbalance > 0 (even 1 share difference!)
- At 1.05: allowed combined costs up to 1.05 on the light side = -$0.05/pair LOSS
- At 1.00: breakeven cap = no losing pairs when both sides have fills

### Pair Completion (separate, unlimited)
- When one side has ZERO shares and other has fills
- Combined cost guard is COMPLETELY SKIPPED
- Mathematically EV-neutral, variance-reducing
- NOT affected by any of the above thresholds

## The Kill Chain: How Losing Pairs Formed

### With old config (max=1.00, light=1.05):
1. Period starts, BTC below open (FV_down=0.64, FV_up=0.36)
2. Ladder centers: DOWN=0.60, UP=0.33
3. PostOnly anchors: DOWN→0.62 (near ask 0.63), UP→0.37 (near ask 0.38)
4. Guard check: 0.62 + 0.37 = 0.99 ≤ 1.00 → PASSES
5. DOWN fills at 0.60-0.61 (11 shares)
6. BTC reverses upward, FV flips
7. Pair completion: UP fills at 0.41+ (guard SKIPPED, one side was 0)
8. Now both sides have fills: yes=7, no=11, avg_no=0.605
9. Light side threshold: YES gets 1.05 (heavy NO)
10. New UP at 0.43: 0.43 + 0.605 = 1.035 ≤ 1.05 → PASSES! (LOSING)
11. Combined cost: 1.035 per pair → -$0.035/pair loss

### With new config (max=0.97, light=1.00):
- Step 4: 0.62 + 0.37 = 0.99 > 0.97 → BLOCKED!
- Only 0.58 + 0.33 = 0.91 would pass
- Step 10: 0.43 + 0.605 = 1.035 > 1.00 → BLOCKED!
- Only 0.39 + 0.605 = 0.995 ≤ 1.00 would pass

## Results After Changes

### max_combined_avg_cost = 0.97 (first fix):
- 5:45-6:00: +$5.61, 31 pairs, combined 0.952, locked +$1.48
- 6:00-6:15: +$1.31, 18 pairs, combined 0.955, locked +$0.81

### light_side_max_combined = 1.00 (second fix):
- Applied at restart 11:28 UTC. Monitor upcoming periods.

## Key Insight
The config parameter `target_combined` (0.93) is largely COSMETIC. PostOnly
ask-anchoring overrides it by re-centering bids near the market ask. The REAL
cost control comes from `max_combined_avg_cost` (the hard guard) and
`light_side_max_combined` (the imbalance guard). These were set to 1.00/1.05
= effectively disabled. Tightening them to 0.97/1.00 is the actual fix.
