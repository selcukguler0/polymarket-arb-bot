# Top-20 Wallet Deep Analysis (2026-03-10)

> Data: 700K rows, 22 wallets, `memory/memory-data/wallet-10032026/`
> Previous summary: `memory/top-wallet-research.md` (superseded by this file)

## Critical Discovery: Post-Res Maker Bids, Not Taker Asks

**19 of 22 wallets trade post-resolution using MAKER BIDS.** Our previous post-res bot failed because it looked for asks (taker). The correct mechanism:

1. Place limit BIDS on both sides immediately after period ends (3-5s)
2. Counterparties SELL INTO bids — people dumping tokens for immediate liquidity
3. Winner tokens bought at median $0.74, loser tokens at median $0.27
4. Complete pairs redeemed at $1.00

**Why overnight book snapshots showed "zero asks"**: During overnight hours, nobody actively sells. During active trading hours (6 AM - 5 PM ET), 59K loser-token buy trades prove sellers exist. 80.9% of loser buys are at exact-cent prices = maker limit bids.

## Strategy Clusters

### Tier 1: Speed Pair Completers — 0x267cc5, 0xb0cc03, 0xd09007
- BTC 5-min only. Enter 3-5s after expiry. 0-2s between legs.
- Fractional prices (0% exact-cent) = taker sweeping resting limits.
- 100% completion rate. Median combined $0.99. ROI 0.4-1.1%.
- Likely same operator, 3 wallets. Profit through volume (~$0.01/pair × 200+ pairs).

### Tier 2: Patient Pair Completers — 0x2d8b40 (vidarx), 0xa1303d
- BTC (vidarx) / multi-coin 5-min. Enter 5-9s. 96% exact-cent = maker bids.
- 99.5% completion. Median combined $0.97. ROI 0.8-1.8%.
- Vidarx: 78.9% of pairs profitable, $0.044 avg margin on profitable pairs.

### Tier 3: Multi-Coin Completers — 0xd1ebe8, 0x1f0ebc, 0x2eb571, 0x716445, 0x732f18, 0xa45fe1, 0x52f878, 0xd111ce, 0x818f21
- BTC/ETH/SOL/XRP across 5m+15m+hourly. All buy-only (0 sells).
- 88-98% completion. Median combined $0.97-0.99. ROI 0.06-1.2%.
- 0x1f0ebc highest vol ($1.76M) but lowest ROI (0.06%) — scale doesn't help.

### Tier 4: Deep Discount Hunters — 0xd0d605, 0x63ce34
- **Highest ROI**: 0xd0d605 = 18%, 0x63ce34 = 9.6%.
- Lower completion (81-90%) but SELL unmatched positions (12.7% sells).
- Combined costs: 0xd0d605 median $0.92 (profitable pairs avg $0.84 margin = $0.16/pair).
- **Key differentiator**: Sell mechanism for unmatched shares turns risk into revenue.

### Tier 5: Pre-Expiry Directional — 0xd84c2b (BoneReader)
- ONLY wallet trading before resolution. Enters ~91s pre-expiry.
- Buys both sides at ~$0.50. Also does post-res (63% of trades).
- Negative matched-pair profit (-$43K), offset by winner redemption (+$51K).
- Highest volume ($2.3M) but near-zero ROI (0.03%).

### Tier 6: Daily Prediction Specialists — 0xde17f7, 0xa42f12
- 0xa42f12: **57.7% ROI** via market-making (47.5% sells). Spread capture, not pair completion.
- 0xde17f7: pair completion on dailies, median 70min between legs, 0.71% ROI.

### Tier 7: One-Sided Sweeper — 0xba2643
- Buys ONE side only (0.1% completion). Winner tokens at discount post-res.
- Taker (4.8% exact-cent). $1.24M volume, 0.45% ROI.

## Key Metrics

### Combined Costs (complete pairs, post-res only)
| Wallet | Median | % Profitable | Avg Margin (profitable) |
|--------|--------|-------------|------------------------|
| 0xe59433 | $0.8682 | 88.0% | $0.186 |
| 0xd0d605 | $0.9210 | 70.3% | $0.164 |
| 0x2d8b40 | $0.9720 | 78.9% | $0.044 |
| 0xd1ebe8 | $0.9792 | 57.9% | $0.104 |
| 0x1f0ebc | $0.9789 | 55.7% | $0.136 |
| 0x267cc5 | $0.9964 | 63.6% | $0.012 |
| 0xb0cc03 | $0.9923 | 67.3% | $0.020 |
| 0xd09007 | $0.9894 | 68.8% | $0.024 |

### Speed (first trade after market end)
| Wallet | Fastest | Median | p25 |
|--------|---------|--------|-----|
| 0x267cc5 | 3s | 5s | 5s |
| 0xb0cc03 | 3s | 5s | 5s |
| 0xd09007 | 3s | 5s | 5s |
| 0x2d8b40 | 5s | 9s | 7s |
| 0xd0d605 | 5s | 7s | 5s |
| 0x63ce34 | 5s | 9s | 5s |

### Post-Resolution Loser Token Buys (59K trades total)
- Median price: $0.27. P25: $0.10. P75: $0.45.
- 80.9% at exact-cent prices = maker limit bids.
- Median timing: 213s (3.5 min) after expiry.

### Share Imbalance (Up vs Down per market)
Even best wallets have significant imbalance:
- 0x267cc5: 93.9% balance ratio, 44.4% near-perfect (>0.95)
- 0x2d8b40: 82.2% balance ratio, 18.3% near-perfect
- 0xa1303d: 78.6% balance ratio, 12.7% near-perfect

## PnL Estimates (known-winner markets only)
| Wallet | Total Vol | Matched Profit | Winner Unmatched | Loser Loss | Sell Rev | Est PnL | ROI |
|--------|----------|---------------|-----------------|-----------|---------|---------|-----|
| 0xd0d605 | $423K | $29,111 | $11,319 | -$14,698 | $50,400 | **$76,133** | 18.0% |
| 0x63ce34 | $234K | $4,786 | $10,406 | -$9,781 | $17,202 | $22,613 | 9.6% |
| 0x2d8b40 | $536K | $14,426 | $5,772 | -$10,380 | $0 | $9,818 | 1.8% |
| 0xde17f7 | $1.21M | $5,814 | $18,712 | -$15,853 | $0 | $8,673 | 0.7% |
| 0x2eb571 | $632K | $8,618 | $6,973 | -$8,183 | $0 | $7,408 | 1.2% |

Note: Large "unknown winner" buckets (50-70% of markets) where up_price/down_price fields weren't populated. True PnL likely higher.

## Actionable Insights

### 1. Post-Res Maker Bot (NEW STRATEGY — highest priority)
Build a maker-based post-resolution pair completion bot:
- Place bids on BOTH sides 3-5s after period ends
- Winner side: bid $0.50-0.80 (median fill $0.74)
- Loser side: bid $0.01-0.30 (median fill $0.27)
- Expected combined $0.95-0.97 (conservative) or $0.85-0.92 (aggressive like 0xd0d605)
- **MUST include sell mechanism for unmatched shares** — this is the difference between 1% and 18% ROI

### 2. Speed Requirements
- Resolution detection: <1s after period end
- Bid placement: batch both sides within 2-3s
- Current VPS Helsinki (236ms avg) is adequate — top wallets enter at 3-5s, not sub-second

### 3. One-Leg Risk Management
- 20-30% of markets will have significant imbalance
- Sell unmatched shares rather than letting them expire
- Winner unmatched: hold for redemption ($1.00) or sell at $0.90+
- Loser unmatched: sell at any available bid, accept loss

### 4. Why Previous Post-Res Bot Failed
- Used TAKER orders (looking for asks). Should use MAKER bids.
- Tested overnight when no counterparties sell. Active hours have liquidity.
- Strategy is viable but mechanism was wrong.

### 5. Market-Making on Dailies (secondary opportunity)
0xa42f12 achieves 57.7% ROI market-making daily prediction markets. Different strategy (spread capture) worth investigating separately.
