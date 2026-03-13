# Pair Completion Bug Analysis (2026-03-04)

## The Problem
When BTC moves strongly in one direction during a period, one side fills cheap (the losing side) and the other side becomes too expensive to buy. The combined cost guard then blocks ALL orders on the expensive side, making pair completion impossible. The result: 0 pairs, full loss on the filled side.

## Example: 15-min market 10:15-10:30 AM ET
- BTC moved +$294 from open in first 2 minutes
- DOWN filled: 15 shares at $0.46 (=avg_no $0.46)
- Combined cost guard: `yes_price + avg_no <= 0.97` → YES price must be < $0.51
- But at FV=0.69, YES bids need to be ~$0.67 → **ALL YES levels blocked!**
- Result: 0 pairs, 15 excess DOWN, total loss = $6.90 (UP won at 97%)

## Root Cause: Combined Cost Guard Too Aggressive for Pair Completion
The guard at `guard_combined_cost()` (~line 2544) retains only levels where `level.price + avg_other <= target_combined (0.97)`. This makes sense for INITIAL pair building, but when we already have unpaired shares, the guard prevents recovery.

## Why Pair Completion Should Be Allowed Above $0.97 Combined
With 15 DOWN at $0.46 and FV Up = 0.69:
- **Without pairing**: Expected loss = 0.69 × $6.90 = $4.76 (UP wins, DOWN = $0)
- **Pairing at $0.55 UP**: Combined = $1.01, guaranteed loss = $0.15. But MUCH better than -$4.76!
- **Even at $0.60 UP**: Combined = $1.06, loss = $0.90. Still better than -$4.76.

The guard should use a RELAXED threshold when completing pairs, because the ALTERNATIVE (leaving shares unpaired) is usually far worse.

## Contributing Factors
1. **Budget constraint**: $15 budget limits total spend, so once DOWN uses $6.90, only $8.10 left for UP
2. **Ladder decay**: Different levels have different sizes, causing unequal fills
3. **Vol breaker**: After BTC moved $300+, vol breaker suppressed all orders for the rest of the period
4. **One-shot fill**: DOWN filled in one batch (15 shares) before UP could fill any

## Proposed Fix
Relax the combined cost guard when there are unpaired shares:
- If `excess_other > 0` (unpaired shares on opposite side), use `relaxed_threshold = 1.02` instead of `0.97`
- This allows pairing at up to 2c loss per pair, which is far better than the typical 50c+ loss from no pairing
- Alternatively: skip the combined cost guard entirely for the light side when in "pair recovery" mode

## Also Consider
- Vol breaker should NOT block pair completion on the light side
- Budget should not prevent pair completion (extend if needed for matching shares)
- Reduce `target_combined` sensitivity for the deficient side
