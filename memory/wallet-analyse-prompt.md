## Task: Deep Strategy Analysis of Top Polymarket Wallets — ANALYSIS ONLY
                                                                                                                                            
You are analyzing trading data to understand EXACTLY how top wallets make money on Polymarket BTC Up/Down markets. DO NOT write any bot code.
DO NOT propose implementations. ONLY analyze data and present findings.

### Data File
`/memory/memory-data/wallet_trades_ALL_ALL_all (3).csv`

This CSV contains 77,509 corrected trades from 20 top wallets. Columns include: wallet, market_slug, side (BUY/SELL), outcome (Up/Down),
price, size, secs_to_expiry, timestamp_ms, market_end_ts_ms, duration_mins, maker_address, tx_hash.

secs_to_expiry = seconds until market END (positive = during market, negative = after market ended).

### What I Need

#### Part 1: Wallet Ranking by ACTUAL profitability
For each wallet, calculate:
- Total markets traded
- Total USDC spent (buys) and received (sells + winning resolutions at $1.00)
- Estimated PnL (assume: paired shares redeem at $1.00, excess winning side = $1.00, excess losing side = $0.00, use 50/50 win probability for
excess since we don't know actual outcomes)
- Risk profile: what % of their money is in "guaranteed pairs" vs "directional bets"?
- Rank wallets by: (a) estimated daily PnL, (b) risk-adjusted return (profit per dollar at risk), (c) simplicity of strategy

#### Part 2: Strategy Clustering
Group wallets into distinct strategy types. For EACH cluster:
- What exactly do they do? (timing, pricing, sizing, sell behavior)
- How much capital do they need?
- What's the skill/speed requirement? (can we replicate with 236ms latency from Helsinki?)
- What's the risk? (worst-case per market, worst-case per day)

#### Part 3: Per-Wallet Deep Dive (top 5 by PnL)
For each of the top 5 wallets, reconstruct 3 example markets trade-by-trade:
- 1 highly profitable market
- 1 losing market
- 1 average market

For each trade show: time_to_expiry, side, outcome, price, size, running_up_shares, running_down_shares, running_paired, running_imbalance

Answer: What EXACTLY triggered each trade? Was it:
- Placed at period open and filled by market movement? (check: was their bid price BELOW the ask at fill time?)
- Reactively placed after a price move? (check: do fills cluster after book changes?)
- Batch placed? (check: multiple fills at exact same timestamp)

#### Part 4: Book-Awareness Detection
CRITICAL QUESTION: Do top wallets use the current book price to decide WHERE to bid?

Evidence to check:
- Are their fill prices clustered near round numbers ($0.05 increments) → static grid
- Or clustered relative to the ask (ask-1c, ask-2c, ask-3c) → book-aware
- Do fill prices correlate with market mid/ask at the time? (we don't have book snapshots, but we can infer from price patterns)
- Within a single market, do they get fills at MANY different prices (suggesting repricing) or few prices (suggesting static placement)?

#### Part 5: Easiest Strategy to Copy
Based on all analysis, identify which strategy:
1. Has the highest profit per dollar of risk
2. Requires the LEAST speed advantage (we have 236ms latency)
3. Has the simplest logic (fewest moving parts)
4. Has the most predictable/consistent returns
5. Can work with $500-2000 starting capital

Don't just pick one — rank the top 3 with pros/cons for each.

#### Part 6: What We're Missing
List every piece of information we'd need to perfectly copy each strategy that we DON'T have in this CSV. Examples:
- Book state at time of each trade
- Whether orders were cancelled before filling
- Exact order placement time vs fill time
- etc.

### Rules
- Use Python for all analysis. Show your work.
- Print ACTUAL NUMBERS, not summaries. I want to see the data.
- If something is ambiguous, say so. Don't guess.
- DO NOT suggest code changes or bot implementations.
- DO NOT write Rust code.
- If you find something surprising or contradictory, highlight it.