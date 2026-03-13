# Deep Discount Maker Strategy — How Top Wallets Really Work

> Source: Corrected 700K-row analysis, 2026-03-10
> See also: `memory/wallet-analysis-corrected.md`

## Core Mechanism

Top wallets place DEEP maker bids ($0.07-0.50) on BOTH sides DURING the active market.
When BTC swings, the losing side panics and dumps into resting bids at deep discounts.
Combined cost = cheap loser fill + expensive winner fill = still < $1.00.

**This is NOT prediction. It's catching panic sellers with deep resting liquidity.**

## 0xd0d605 Example ($54K/day)

```
Period opens. BTC at $80,000.
Bot places bids: Up @ $0.20, $0.30, $0.40 | Down @ $0.20, $0.30, $0.40

BTC jumps +$100 → Down holders panic, dump into resting bids:
  BUY Down 111 @ $0.32 (819s before end)
  BUY Down  74 @ $0.10 (629s before end)  ← catching the dump
  BUY Down 104 @ $0.11 (629s before end)
  BUY Up    17 @ $0.64 (843s before end)
  BUY Up    47 @ $0.83 (667s before end)

Result: Up avg $0.69 + Down avg $0.16 = combined $0.85 → $0.15/pair profit
```

## 0xd0d605 Statistics

- **857 markets** over 2 days (428/day)
- **81% both sides filled**, 70.3% profitable
- **Median combined cost: $0.92** (vs our bot $0.97)
- **Avg margin on winners: $0.164** (vs our $0.03)
- **95% maker** (exact-cent prices = limit orders)
- **33.7% of markets have sells** — selling unmatched → $25K/day revenue
- **22 trades per market**, 450 shares/market
- Timeframes: 5m (586) + 15m (271)
- Entry: right at period open (~295s before end)
- Last buy: ~177s before end (midway through period)

## Buy Price Distribution (profitable markets)

```
Up buys:  <10¢=9%  10-20¢=13%  20-30¢=12%  30-40¢=14%  40-50¢=19%  50-60¢=16%  60-80¢=14%  80¢+=3%
Dn buys:  <10¢=10% 10-20¢=12%  20-30¢=13%  30-40¢=14%  40-50¢=18%  50-60¢=15%  60-80¢=15%  80¢+=3%
```

22% of fills are at <$0.20 — these deep loser-side fills are where the profit comes from.

## Our Bot vs 0xd0d605

| Factor | Our Arb Bot | 0xd0d605 |
|--------|-------------|----------|
| Bid range | $0.47-0.49 (5 levels near 50¢) | $0.07-0.50+ (wide spread) |
| Combined target | $0.97 | $0.92 median |
| Margin on winners | $0.03 | $0.164 |
| Win rate | 76% | 70% |
| One-leg handling | Taker exit at loss | **Sell unmatched at partial recovery** |
| Markets/day | ~100 | ~430 |
| Shares/market | ~60 | ~450 |
| Fill pattern | Both sides fill near 50¢ → thin margin | Loser dumps to $0.10-0.20 → fat margin |

## Key Differences to Implement

### 1. Deeper Bid Ladder
Current: 5 levels starting near ask-0.03 ($0.47-0.49 range)
Needed: 10-15 levels from $0.10 to $0.50+ (full range)
The deep levels are where profit comes from. Level 0 is NEGATIVE EV (our own data confirms this).

### 2. Sell Mechanism for Unmatched
Current: taker exit when excess > threshold → often sells at loss
Needed: after period ends, sell unmatched shares at best bid (even partial recovery)
0xd0d605 makes $25K/day from sells alone. This is not optional.

### 3. More Markets, Bigger Size
Current: 4 coins × 5m = ~100 markets/day, 60 shares/market
Needed: 4 coins × (5m + 15m) = ~430 markets/day, 200-500 shares/market
Volume is how 70% win rate × $0.16 margin compounds into $54K/day.

### 4. Accept Lower Win Rate
Our bot optimizes for high win rate (76%). 0xd0d605 accepts 70% win rate
but makes 5x more per winning trade. Net PnL is 10-50x higher.

## Risk Warning

Deeper bids = more one-leg exposure. When both sides fill at $0.40+$0.40=$0.80,
profit is $0.20. But when only one side fills at $0.40 and the market resolves against
you, loss is $0.40. The sell mechanism is CRITICAL to limit this downside.

Without sells: expected PnL ≈ $0.164 × 70% - $0.40 × 30% = $0.115 - $0.12 = -$0.005 (breakeven)
With sells at $0.20 avg: expected PnL ≈ $0.164 × 70% - ($0.40 - $0.20) × 30% = $0.115 - $0.06 = +$0.055/market
