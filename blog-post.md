# I Built a Polymarket Arbitrage Bot in Rust. Here's Why It Failed (and What I Learned).

After months of development, 700K rows of wallet analysis, 688 periods of paper trading validation, and a Hetzner VPS in Helsinki — I'm open-sourcing my Polymarket market-making bot. It's called **polymarket-arb-bot**, it's written in Rust, and it doesn't make money.

Here's the full story.

---

## The Idea

Polymarket runs binary prediction markets. For BTC 5-minute and 15-minute markets, you can bet "Up" or "Down". If you buy both sides for less than $1.00 combined, you lock in risk-free profit — a complete-set arbitrage.

The plan was simple: place maker bids on both sides, get filled below $1.00 combined, collect the spread.

Paper trading showed $6.70 profit per 15-minute period, 75% win rate, Sharpe ratio of 0.51. Looked great on paper.

## What I Built

A ~10,000-line Rust bot with:

- **Black-Scholes fair value pricing** fed by real-time Binance WebSocket data
- **Deep bid ladder** with non-uniform spacing ($0.01 to $0.50), inspired by top wallet analysis
- **Gnosis Safe integration** for gasless trading (no ETH needed)
- **Batch order management** — single HTTP call for up to 15 orders
- **Real-time web dashboard** via SSE
- **Multiple strategies**: complete-set arb, deep discount maker, convergence taker, post-resolution maker
- **Paper trading mode** with realistic fill simulation (proximity fills, queue position, PostOnly rejection)

The architecture is single-binary, async Rust with `tokio`. Shared state via `parking_lot::RwLock`, channel-based event flow, WebSocket feeds for both Binance prices and Polymarket orderbooks.

## The Research

Before writing a single line of trading logic, I analyzed 700K trade rows from the top 20 Polymarket wallets to understand how profitable traders actually operate.

**Key findings:**

- **Top wallets are makers, not takers.** They place deep limit bids and wait. The best wallet (`0xd0d6...`) makes $54K/day with median combined cost of $0.92 per pair.
- **Combined asks are ALWAYS > $1.00.** Market makers set the spread above $1.00 — that's their edge. You cannot profitably lift both asks. Every taker strategy we tried (daily prediction, post-resolution) failed for this reason.
- **Deep bids catch panic dumps.** When BTC moves 1%+ in minutes, the losing side collapses. Resting bids at $0.05-$0.15 get filled by panicking sellers. The top wallet bids at $0.07-$0.50, not $0.48-$0.49 like we initially tried.
- **Selling unmatched shares is essential.** The best wallet sells excess in 34% of markets, generating $25K/day in sell revenue alone.

## Why It Failed

### 1. Latency Kills

Polymarket's CLOB runs in London (AWS eu-west-2). Our VPS in Helsinki had **236ms average round-trip**. Top wallets likely operate from co-located servers in Ireland or London with <10ms latency.

At 236ms, by the time our orders land on the book, the market has already moved. We're always trading on stale information.

**The math is brutal**: At 236ms latency, a $50 BTC move during our order flight changes fair value by ~2-3 cents on a 5-minute market. Our entire profit margin per pair was ~3 cents. One adverse move wipes the edge.

### 2. Adverse Selection

This is the real killer. When BTC moves against our bids, counterparties dump their losing shares into our resting orders. We fill disproportionately on the wrong side.

From our live trading data: DOWN fills were **1.44x** more frequent than UP fills despite fewer DOWN orders. Counterparties were selling losing tokens into our bids during drawdowns.

Locked profit was $218, but excess (unmatched) risk was $334. The "arb" was underwater.

### 3. Paper Trading Lies

Our initial paper simulation was **4-7x too optimistic**. Three mechanisms were unrealistic:

- **Proximity fills**: We simulated 33% fill probability per tick within 5 cents of the ask. Reality: ~5% within 2 cents, requiring 5+ seconds of resting time.
- **PostOnly rejection**: We weren't modeling that fresh orders crossing the spread get rejected.
- **Queue position**: We assumed instant fills when price touched our level. Reality: you need to rest 3+ seconds minimum.

We fixed the simulation (Phase A), and profitability dropped from $15/period to $6.70/period. Still positive in paper. Still negative live.

### 4. Market Structure Works Against Small Players

Polymarket's February 2026 changes made things worse:
- The **500ms taker delay was removed**, meaning market makers lost their safety buffer
- **Dynamic taker fees up to 3.15%** at 50-cent prices killed any latency arbitrage
- Cancel/replace needs <100ms to be competitive. Our bot ran 150-4600ms.

## Strategies We Tried (and Their Outcomes)

| Strategy | Result |
|----------|--------|
| Complete-set arb (maker bids both sides) | Paper profitable, live underwater due to latency |
| Deep discount maker (bids at $0.01-$0.15) | Too slow to compete with co-located bots |
| Post-resolution taker (buy winning side cheap after settlement) | No asks exist on losing side post-resolution |
| Daily prediction arb (Yes+No < $1.00) | Combined asks always > $1.00. Impossible. |
| Convergence taker (FOK on FV divergence) | Marginal. 3.15% taker fees eat the edge. |
| Post-resolution maker (bid both sides after settlement) | Experimental, unvalidated |

## Hard-Won Lessons

Things I wish I knew before starting:

1. **Latency is everything in market making.** If you can't cancel and replace in <50ms, you will be adversely selected. This isn't a "nice to have" — it's the minimum viable infrastructure.

2. **Paper trading is necessary but not sufficient.** Even after fixing our simulation three times, paper was still 2-3x more optimistic than live. The gap comes from adverse selection, which is nearly impossible to simulate accurately.

3. **Binary markets have nasty microstructure.** Unlike continuous markets, binary outcomes create extreme information asymmetry. When BTC moves, one side's fair value goes to $0.95+ while the other crashes to $0.05. Everyone knows this simultaneously. Slow bots eat the losses.

4. **The Polymarket API is well-built.** Despite our failure, the CLOB API, WebSocket feeds, and Builder authentication are solid. The Gnosis Safe / relayer integration for gasless trading is elegant.

5. **Top wallets aren't doing magic.** They're doing exactly what you'd expect — deep maker bids, sub-10ms latency, smart position management. The "secret" is infrastructure, not strategy.

6. **Analyze before you build.** The 700K-row wallet analysis was the most valuable part of the project. It killed three bad strategies before we wasted time building them and revealed the actual mechanics of profitable trading.

## What's In The Repo

- Full Rust bot source (~10K lines of core logic)
- Gnosis Safe gasless trading integration
- Paper trading simulator with tuned fill model
- Multiple strategy implementations
- Complete configuration with extensive comments
- Research reports and validation results
- Wallet analysis findings
- Hard-won learnings from live trading

## Could This Be Made Profitable?

Maybe. You'd need:

- **Co-located infrastructure** in London or Ireland (<10ms to Polymarket CLOB)
- **Sub-50ms cancel/replace cycle** (our best was 150ms)
- **Better adverse selection modeling** to avoid filling on the wrong side of moves
- **Deeper pockets** — the top wallet runs ~$50K/day through the markets
- **Possibly a hybrid approach** — maker bids for accumulation + smart taker exits

The strategy logic and research are solid. The infrastructure gap is what killed us.

## Open Source

The full source code is available at: **[github.com/selcukguler0/polymarket-arb-bot](https://github.com/selcukguler0/polymarket-arb-bot)**

MIT licensed. Do what you want with it. If you make it work, I'd love to hear about it.

---

*Built with Rust, tears, and an unreasonable amount of Claude Code sessions.*
