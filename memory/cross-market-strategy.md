# Cross-Market Following Strategy — Research & Backtest (2026-03-11)

## Strategy Overview

**Concept**: When BTC's Polymarket book prices in a direction (Up or Down), ETH/SOL books lag by 30-60 seconds. Buy the predicted winner on ETH/SOL as a taker (FOK) before the price catches up.

**This is NOT about resolution timing.** All coins in a period share the same end time. Resolution happens 100-300s after market end, and by then books are fully priced. The exploitable lag is DURING the active 5m/15m period.

**No wallet to watch.** Signal comes from BTC's own order book mid-price, not from any trader.

## Data Source

- Collector: `/Volumes/KIOXIA/PROJECTS/PROJECTS/polymarket-data-collector/live_collector.py`
- Data: `memory/data/` — 1M trades, 700K book snapshots, 462 markets, 12.3 hours (Mar 10 7:48PM → Mar 11 8:04AM UTC)
- XRP added to collector on 2026-03-11 (was missing, only BTC/ETH/SOL)
- 93.8% of trades enriched with maker/taker addresses via on-chain Polygon events

## Top Trader Analysis (1M trades, 462 markets)

### Market-Wide Finding
- **Takers collectively beat makers** in this dataset (+$23,601 vs -$23,601)
- This contradicts the earlier wallet analysis which found makers winning (that data had a timestamp bug)

### Top 5 Profitable Traders
| Address | PnL | Strategy |
|---------|-----|----------|
| 0x4bfb41d5b3 | +$70,268 | Pure taker bot, all markets, directional + scalping |
| 0x48994587a2 | +$15,045 | Whale directional maker sells, 21 markets, $716/mkt |
| 0xb8cb25ed93 | +$12,938 | Large maker sells, 9 markets only |
| 0xd84c2b6d65 | +$11,293 | Post-resolution speed race, buys winner at $0.986 |
| 0x9ccc0ca985 | +$4,933 | Mixed MM, both sides |

### 0x4bfb41d5b3 — The Dominant Bot
- $12.1M volume, 521K trades, 100% taker
- Trades ALL 462 markets, both sides in every one
- Combined cost 0.99-1.38 (NOT sub-$1 arb)
- 92% of trades are buys, 62% market win rate
- Likely strategy: market-making with superior price discovery / directional betting
- Shows scalp-like patterns in 361/462 markets (~$47K profit from scalping)

### Deep Discount Buying = LOSING Strategy
- Buying at $0.05-0.30 has **negative PnL across ALL price buckets**
- Win rates of 2-24% insufficient to overcome cost basis
- Contradicts earlier wallet analysis (which had timestamp bug)
- Our 0xd0d605 copycat strategy would not work based on this data

## Cross-Market Correlation — The Signal

### BTC Predicts ETH/SOL Outcomes
| Pair | 5m Accuracy | 15m Accuracy |
|------|------------|-------------|
| BTC → ETH | 77.9% | 94.3% |
| BTC → SOL | 80.2% | 97.1% |

### Price Discovery Lag (BTC leads)
| Time Before End | BTC Ask | ETH Ask | SOL Ask |
|-----------------|---------|---------|---------|
| 5-10 min | 0.507 | 0.506 | 0.506 |
| 3-5 min | 0.606 | 0.617 | 0.594 |
| 2-3 min | 0.683 | 0.676 | 0.664 |
| 1-2 min | 0.727 | 0.746 | 0.727 |
| 30-45s | 0.777 | 0.827 | 0.802 |

Cross-market divergence > 5c occurs 60-80% of the time.

## Backtest Results (12.3 hours, threshold scan)

### Parameter Scan
| Threshold | Trades | Win% | Avg PnL | Total PnL | Max DD |
|-----------|--------|------|---------|-----------|--------|
| 0.52 | 175 | 61.1% | $1.61 | $282 | -$614 |
| 0.55 | 169 | 62.1% | $2.26 | $382 | -$443 |
| 0.60 | 148 | 64.2% | $2.94 | $436 | -$481 |
| 0.62 | 140 | 66.4% | $4.32 | $604 | -$445 |
| **0.65** | **128** | **67.2%** | **$4.93** | **$631** | **-$353** |
| **0.68** | **112** | **70.5%** | **$7.44** | **$833** | **-$306** |
| 0.70 | 106 | 69.8% | $6.52 | $691 | -$288 |

**Optimal threshold: 0.68** — best total PnL with strong win rate and controlled drawdown.

### Best Run (threshold=0.68, 100 shares/trade)
| Metric | Value |
|--------|-------|
| Total trades | 112 (in 12.3 hours) |
| Win rate | 70.5% |
| Avg PnL/trade | $7.44 |
| Total PnL | $833 |
| Max drawdown | -$306 |
| Max consec losses | 5 |
| Avg win | +$36.02 |
| Avg loss | -$60.98 |

### By Target Coin
| Coin | Trades | Win% | Avg PnL | Total PnL |
|------|--------|------|---------|-----------|
| **SOL** | **61** | **75.4%** | **$13.27** | **$810** |
| ETH | 51 | 64.7% | $0.46 | $23 |

**SOL is the primary target.** ETH barely breaks even.

### By Timeframe
| TF | Trades | Win% | Avg PnL |
|----|--------|------|---------|
| **15m** | **11** | **81.8%** | **$22.21** |
| 5m | 101 | 69.3% | $5.83 |

15m is safer but fewer opportunities.

### By Direction
| Direction | Win% | Avg PnL |
|-----------|------|---------|
| Down | 73.2% | $8.23 |
| Up | 67.9% | $6.64 |

Down signals slightly more reliable.

### Timing: First 60s After Signal = Best
| Secs Into Period | Win% | Avg PnL |
|------------------|------|---------|
| 0-60s | **93.8%** | **$23.81** |
| 60-120s | 69.2% | $4.81 |
| 120-180s | 59.1% | -$3.45 |
| 180-240s | 53.8% | -$6.14 |

Edge decays fast — enter early or not at all.

### Entry Price Sweet Spots
| Price Range | Win% | Avg PnL |
|-------------|------|---------|
| $0.00-0.45 | 63.6% | $21.76 |
| $0.45-0.55 | 61.1% | $8.41 |
| $0.55-0.65 | 62.5% | $0.03 |
| $0.65-0.75 | **80.4%** | **$8.65** |

Counter-intuitive: higher entry prices (0.65-0.75) have BEST win rate because the signal is stronger.

## Other Strategies Found (Not Pursued)

### 1. Late-Market Momentum
At 75% through 5m market, if Up > $0.65 → wins 91% of the time. But spread compression limits edge.

### 2. Post-Resolution Speed Race (0xd84c2b6d65's strategy)
Buy winner at $0.986 right after resolution → redeem at $1.00. $4,371 profit in data. Needs extreme low latency.

### 3. Early Directional MM (0x48994587a2's strategy)
Large maker sells on predicted loser. $716/market but only 21 markets, high risk.

## Competition Assessment
- **No bot currently exploits cross-market lag** in this dataset
- 0x4bfb is dominant taker but does complete-set arb, not cross-market
- First-mover advantage is real

## Implementation Plan

### Architecture (Rust live bot)
1. **Signal source**: Polymarket WebSocket for BTC book updates (real-time, not 2s polling)
2. **Entry execution**: FOK taker orders on ETH/SOL via CLOB API
3. **Per-period state**: Track which markets already entered, avoid re-entry
4. **Risk**: Max 1 entry per coin per period per direction

### Key Parameters (from backtest)
- `signal_threshold = 0.68` (BTC Up mid crosses this → buy Up on SOL/ETH)
- `position_size = 100` shares
- `max_entry_price = 0.75`
- `min_entry_price = 0.35`
- `warmup_secs = 30` (skip first 30s of period)
- `cutoff_before_end_secs = 15` (stop near expiry, no liquidity)
- Primary target: SOL. Secondary: ETH.
- Both Up and Down signals.

### Risks
1. **12.3 hours of data** — need 2-3 more days to validate (collector running)
2. **Slippage model is approximate** — real FOK fills may be worse
3. **Latency**: Helsinki VPS 236ms — may be fast enough since no competition yet
4. **Correlation breakdown**: BTC and SOL can diverge in crypto-specific events
5. **Fee erosion**: Polymarket dynamic taker fees 1-3%

## Backtest Code Location
`/Volumes/KIOXIA/PROJECTS/PROJECTS/cross-market-backtest/`
```
config.py      — All tunable parameters
loader.py      — Data loading + period grouping
strategy.py    — Signal detection + trade logic
analysis.py    — PnL reporting + parameter scan
run.py         — CLI entry point (--scan, --threshold, --coins, etc.)
results/       — Output CSVs
```

## Live Observer (Dry-Run) — DEPLOYED 2026-03-11

### VPS Setup
- **Location**: Helsinki VPS (YOUR_VPS_IP)
- **Tmux session**: `crossmarket`
- **Path**: `/root/cross-market-observer/`
- **Command**: `python3 observer.py --threshold 0.68 --targets SOL ETH XRP`
- **Logs**: `/root/cross-market-observer/logs/observer.log` + `signals_*.csv`
- **GitHub**: `github.com/selcukguler0/cross-market-observer` (private)
- **Local code**: `/Volumes/KIOXIA/PROJECTS/PROJECTS/cross-market-observer/`

### How to Check Status
```bash
# SSH to VPS
ssh -i ~/.ssh/id_vps root@YOUR_VPS_IP

# Attach to tmux
tmux attach -t crossmarket

# Check latest stats (without attaching)
grep 'STATUS\|WOULD_BUY\|RESULT' /root/cross-market-observer/logs/observer.log | tail -20

# Check signal CSV
tail -20 /root/cross-market-observer/logs/signals_*.csv

# Check if running
tmux ls
```

### What It Tracks
- BTC mid-price threshold crossings (0.68)
- What it WOULD buy on SOL/ETH/XRP (price, cost, hypothetical PnL)
- Actual market outcomes (win/loss tracking)
- Signal detection latency, book age at entry
- Skip reasons (price too high/low, no book, etc.)

### Early Observations (first 5 minutes, 2026-03-11 09:23-09:29 UTC)
- 3 WOULD_BUY signals in first 5 min
- SOL Up at ask=0.68, SOL Down at ask=0.52 — good entry prices
- ETH Up at ask=0.66 — entry would have been profitable
- XRP data too stale/extreme (0.95 ask or 0.04 ask) — may not be useful target
- Book age: 25ms (WS-fed) to 4.8s (REST-only) — WS-fed books much better
- Signal detect latency: <1ms (Python is fast enough for observation)

### What to Evaluate After 24-48h
1. **Total signals vs would-trade count** — are we getting enough opportunities?
2. **Win rate** — does it match backtest's 70.5%?
3. **Entry prices** — are they realistic vs what book showed?
4. **Book freshness** — how stale are target books at signal time?
5. **SOL vs ETH** — does SOL dominate like in backtest?
6. **XRP** — useful target or too thin?

## Next Steps
1. **Let observer run 2-3 days** (started 2026-03-11 09:23 UTC)
2. **Check results daily** with commands above
3. **Re-run backtest with collector's expanded dataset** (now includes XRP)
4. **If observer confirms edge** → design Rust live bot
5. **Consider SOL-only variant** for simplicity (75% of PnL comes from SOL)
