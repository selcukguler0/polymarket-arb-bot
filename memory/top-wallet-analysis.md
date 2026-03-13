# Top-20 Wallet Trade Analysis (84K trades, 20 wallets)

> Source: `wallet_trades_ALL_ALL_all (1).csv` — March 9, 2026

## Key Finding: Post-Resolution Dominance

**19 of 20 wallets trade 100% post-resolution.** Only `0xd84c` does pre-resolution trading (38%).
These wallets do NOT predict price. They exploit settlement lag.

## Strategy Archetypes

### 1. Queue-Racing Post-Resolution Pair Arb (0xd0d6, 0x63ce)
- **Most profitable**: $0.14-0.21/share, 80-81% win rate
- Enter within **5-7 seconds** of period resolution
- Combined cost: $0.63-0.76
- 0xd0d6 also sells losing shares mid-period (746 sells, avg $0.30)
- **Barrier**: Co-located infrastructure required

### 2. Patient Post-Resolution Pair Arb (most wallets)
- 0x1f0ebc, 0x2d8b, 0xa1303d, 0x716445, 0x267cc5, etc.
- Enter 11-31s after resolution
- Combined costs: $0.83-0.94
- Profit: $0.03-0.05/share
- Accessible with standard REST API

### 3. Daily Prediction Markets (0xa42f12)
- "Will BTC be above $X?" markets (Yes/No outcomes)
- Active buyer AND seller (2329 buys, 2051 sells)
- Different market type entirely

### 4. High-Capital Directional (0xd84c) — WORST PERFORMER
- Only wallet doing pre-resolution trading
- avg_buy=$0.707, 84% BTC
- 28% profitable pairs, **-$0.18/share loss**
- Closest to our current strategy — and worst performing

## Per-Wallet Metrics

| Wallet | Trades | Pair% | Avg Buy | Profit/Share | Style |
|--------|--------|-------|---------|-------------|-------|
| 0x1f0ebc | 6200 | 93% | $0.353 | $0.048 | Cheap buyer, buys losers <$0.10 |
| 0xd1ebe8 | 5400 | 92% | $0.430 | $0.040 | Patient pair arb |
| 0xd0d605 | 5304 | 78% | $0.391 | $0.212 | Queue-racer, fastest |
| 0x2d8b40 | 5268 | 100% | $0.463 | $0.028 | BTC-only pure pair arb |
| 0xd84c2b | 5057 | 43% | $0.707 | -$0.177 | Directional (pre-resolution) |
| 0x63ce34 | 4662 | 92% | $0.448 | $0.142 | Queue-racer #2 |
| 0x818f21 | 4436 | 88% | $0.544 | -$0.040 | Mixed, slightly unprofitable |
| 0xa42f12 | 4380 | 4% | $0.551 | N/A | Daily prediction markets |
| 0xa1303d | 4375 | 98% | $0.483 | -$0.003 | Near breakeven pair arb |
| 0x267cc5 | 4260 | 100% | $0.482 | $0.005 | BTC-only, thin margin |

## Post-Resolution Price Discovery

After 5-min period ends but before settlement:
- **Winner outcome**: median $0.85 (range $0.50-$0.99)
- **Loser outcome**: median $0.12 (range $0.001-$0.50)
- Combined cost depends on speed: fast=$0.63-0.76, slow=$0.83-0.95

## Timing Patterns

- 98% of trading at 11:00-13:59 UTC (6-9 AM ET)
- Queue-racers need <10s after resolution
- Patient arbers enter at 11-31s — still profitable
- Most wallets spread buys across 0-300s post-resolution window

## Coin Preferences

- BTC dominates (70%+ for most wallets)
- 0x2d8b and 0x267cc5 are BTC-only (100%)
- 0x818f21 is most diversified (36% BTC, 21% each ETH/SOL, 19% XRP)
- 0xd0d6 heavily BTC (74%), 0x63ce even more (94%)

## Implications for Our Bot

1. Our Binance-signal approach matches 0xd84c (worst performer)
2. Post-resolution pair completion is the dominant edge
3. No Binance signal needed — pure structural arbitrage
4. Even at 30s delay, $0.03-0.05/share is achievable
5. Queue-racing ($0.14-0.21/share) needs VPS infrastructure
