# Corrected Wallet Analysis (2026-03-10)

> **CRITICAL**: Previous analysis (`wallet-analysis-findings.md`) had a timing bug.
> The wallet collector stored market START time as `market_end_ts`.
> All "post-resolution" findings were wrong — 97% of trades classified as post-res
> were actually DURING the active market period.

## Collector Bugs Found

### Bug 1: Slug timestamp is START, not END
- Slug `btc-updown-5m-1773058800` → 1773058800 = market START time
- Collector stored this as `market_end_ts` → all `secs_to_expiry` off by duration
- For 5m markets: true post-res = `secs_to_expiry < -300`, not `< 0`
- **Impact**: 633K trades misclassified as post-resolution (97.2% false positive)

### Bug 2: up_price/down_price fetched at collection time
- `get_market_prices()` returns CURRENT price, not price at trade time
- For historical backfills, these are meaningless → 88% "unknown winner"
- Same issue with `binance_price`

## Corrected Findings

### Top wallets trade DURING the market, not after

| Wallet | True Post-Res | In-Market | Strategy |
|--------|:------------:|:---------:|----------|
| 0xd0d605 ($54K/day) | **0** | 41,781 | Deep maker bids during active period |
| 0x2d8b40 ($11K/day) | **0** | 44,096 | Tight maker bids, 100% completion |
| 0x63ce34 ($9K/day) | **0** | 16,648 | Deep bids + sells unmatched |
| 0x267cc5 ($500/day) | 247 | 63,811 | Speed taker, razor-thin margin |
| 0xd84c2b | 13,182 | 10,923 | Buys winners at $0.99 post-res (NOT pair arb) |

### How 0xd0d605 actually works (the $54K/day wallet)

**Strategy: Deep maker bids that catch panic sellers during price swings**

1. Places bids on BOTH sides at period OPEN (~295s before end)
2. Bids are DEEP: $0.07-0.50 range (not $0.48-0.49 like our bot)
3. When BTC moves, losing side tokens dump → resting deep bids catch the dump
4. 22 trades per market, 450 shares per market (gradual accumulation)
5. 95% exact-cent prices = maker fills

**Example profitable market (combined=$0.85, margin=$0.15):**
```
843s before end: BUY Up   17 @ $0.64   ← early Up bid
819s before end: BUY Down 111 @ $0.32  ← BTC moved Up, Down dumps
629s before end: BUY Down  74 @ $0.10  ← Down collapses further
629s before end: BUY Down 104 @ $0.11  ← catches deep dump
667s before end: BUY Up   47 @ $0.83   ← fills Up too
Result: Up avg $0.69 + Down avg $0.16 = combined $0.85 → $0.15/pair profit
```

**Key insight: This is NOT predicting winners. It's catching panic sellers.**
When BTC moves, one side dumps. Deep resting bids catch the dump cheaply.

**Price distribution in profitable markets:**
- 22% of fills are at <$0.20 (deep loser-side dumps)
- Up & Down distributions nearly identical → truly non-directional
- Profitable markets: median buy $0.41 each side
- Losing markets: median buy $0.50 each side (filled too close to 50/50)

**Sells are critical (33.7% of markets):**
- Sell prices: median $0.33 (selling excess loser-side tokens)
- Sell revenue: $50K over 2 days = $25K/day
- This converts one-leg risk into partial recovery

### What our arb bot does differently (and why it's worse)

| Factor | Our Arb Bot | 0xd0d605 |
|--------|-------------|----------|
| Bid depth | ~$0.48-0.49 (target $0.97 combined) | $0.07-0.50 (wide spread) |
| Fill type | Gets filled near 50/50 → thin margin | Catches dumps at deep discounts |
| One-leg risk | Taker exit at loss | Sells unmatched at partial recovery |
| Markets/day | ~100 | ~430 |
| Shares/market | ~60 | ~450 |
| Win rate | ~76% | 70% |
| Avg margin when profitable | $0.03 | $0.164 |

**The #1 difference: bid depth.** Our bot bids at $0.48, gets filled, and makes $0.03.
0xd0d605 bids at $0.20, gets filled 70% of the time, and makes $0.16.

### Post-resolution: NOT viable for pair completion

Only 2 wallets trade significantly post-resolution:
- **0xd84c2b**: Buys ONLY winner tokens at $0.991 (99.1¢). One side only. NOT pair arb.
- **0xba2643**: Same pattern — winner tokens at $0.991. One side only.

No wallet does post-resolution pair completion. The strategy premise was based on misclassified data.

**Post-res reality**: After resolution, people buy cheap winner tokens ($0.99) for guaranteed $0.01 profit at redemption. This requires massive volume and instant speed (co-located). Not a viable edge for us.

### Corrected Strategy Clusters

| Cluster | Wallets | Strategy | Daily PnL |
|---------|---------|----------|-----------|
| Deep discount maker | 0xd0d605, 0x63ce34, 0xe59433 | Deep bids, catch dumps, sell unmatched | $6K-54K |
| Volume maker | 0x1f0ebc, 0xd1ebe8, 0x2eb571 | Wide bids, high volume, no sells | $5K-28K |
| Tight maker | 0x2d8b40, 0xa1303d | Precise $0.97 bids, 100% completion | $2K-11K |
| Speed taker | 0x267cc5, 0xb0cc03, 0xd09007 | Sweep both sides instantly, $0.99 combined | $500 |
| Winner buyer | 0xd84c2b, 0xba2643 | Buy winners post-res at $0.99 | Unknown |

### Actionable Conclusions

1. **Post-res maker bot: LOW PRIORITY** — no evidence of post-res pair completion by top wallets
2. **Main arb bot improvement: HIGH PRIORITY** — our strategy is correct but bids too shallow
3. **Key changes needed**:
   - Deeper bid ladder ($0.20-0.50 range instead of $0.47-0.49)
   - Sell mechanism for unmatched positions (currently only taker exit)
   - More markets/day (add 15m, increase concurrent markets)
   - Larger position sizes per market
4. **Fix wallet collector**: start vs end timestamp bug, collection-time price enrichment
