#!/usr/bin/env python3
"""Comprehensive analysis of Polymarket BTC Up/Down collector data."""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

DATA = Path("memory/data")
OUT = Path(".")

# ============================================================
# SECTION A: DATA QUALITY CHECK
# ============================================================
print("=" * 80)
print("SECTION A: DATA QUALITY CHECK")
print("=" * 80)

# Load all files
trades = pd.read_csv(DATA / "live_trades.csv")
books = pd.read_csv(DATA / "book_snapshots.csv")
outcomes = pd.read_csv(DATA / "market_outcomes.csv")
markets = pd.read_csv(DATA / "active_markets.csv")

print(f"\n--- Row Counts ---")
print(f"live_trades.csv:     {len(trades):>10,}")
print(f"book_snapshots.csv:  {len(books):>10,}")
print(f"market_outcomes.csv: {len(outcomes):>10,}")
print(f"active_markets.csv:  {len(markets):>10,}")

# Time ranges
for name, df in [("live_trades", trades), ("book_snapshots", books)]:
    ts_col = "timestamp_ms"
    mn, mx = df[ts_col].min(), df[ts_col].max()
    dur_hrs = (mx - mn) / 3600000
    t_start = pd.to_datetime(mn, unit='ms', utc=True)
    t_end = pd.to_datetime(mx, unit='ms', utc=True)
    print(f"\n{name}:")
    print(f"  Time range: {t_start} → {t_end}")
    print(f"  Duration: {dur_hrs:.1f} hours ({dur_hrs/24:.1f} days)")

print(f"\n--- Coins & Timeframes ---")
print(f"Trades coins: {trades['coin'].value_counts().to_dict()}")
print(f"Trades timeframes: {trades['timeframe'].value_counts().to_dict()}")
print(f"Outcomes coins: {outcomes['coin'].value_counts().to_dict()}")

# Population rates
print(f"\n--- Population Rates (live_trades) ---")
for col in ['maker_address', 'taker_address', 'book_best_bid', 'book_best_ask', 'book_mid']:
    non_null = trades[col].notna().sum()
    non_empty = (trades[col].astype(str) != '').sum() if trades[col].dtype == object else non_null
    pct = non_null / len(trades) * 100
    print(f"  {col}: {non_null:,} / {len(trades):,} ({pct:.1f}%)")

# Check for the CTF proxy
ctf_proxy = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
maker_ctf = (trades['maker_address'] == ctf_proxy).sum()
taker_ctf = (trades['taker_address'] == ctf_proxy).sum()
print(f"\n  CTF proxy as maker: {maker_ctf:,} ({maker_ctf/len(trades)*100:.1f}%)")
print(f"  CTF proxy as taker: {taker_ctf:,} ({taker_ctf/len(trades)*100:.1f}%)")

# Book snapshot frequency
print(f"\n--- Book Snapshot Frequency ---")
snap_freq = books.groupby('condition_id')['timestamp_ms'].apply(lambda x: x.diff().median())
print(f"  Median interval per market: {snap_freq.median():.0f} ms ({snap_freq.median()/1000:.1f}s)")
print(f"  Mean interval per market: {snap_freq.mean():.0f} ms ({snap_freq.mean()/1000:.1f}s)")
print(f"  Min: {snap_freq.min():.0f} ms, Max: {snap_freq.max():.0f} ms")

# NaN/zero checks
print(f"\n--- NaN/Zero Issues ---")
for col in ['price', 'size']:
    nan_ct = trades[col].isna().sum()
    zero_ct = (trades[col] == 0).sum()
    print(f"  trades.{col}: {nan_ct} NaN, {zero_ct} zeros")
for col in ['best_bid', 'best_ask', 'mid_price']:
    nan_ct = books[col].isna().sum()
    zero_ct = (books[col] == 0).sum()
    print(f"  books.{col}: {nan_ct} NaN, {zero_ct} zeros")

# Outcomes check
print(f"\n--- Outcomes ---")
print(f"  Resolved markets: {len(outcomes)}")
print(f"  Outcome distribution: {outcomes['resolution_outcome'].value_counts().to_dict()}")
res_delay = (outcomes['resolution_timestamp_ms'] - outcomes['market_end_ts_ms']) / 1000
print(f"  Resolution delay: min={res_delay.min():.0f}s, median={res_delay.median():.0f}s, max={res_delay.max():.0f}s")

# ============================================================
# SECTION B: MAKER/TAKER ANALYSIS
# ============================================================
print("\n" + "=" * 80)
print("SECTION B: MAKER/TAKER ANALYSIS")
print("=" * 80)

KNOWN_WALLETS = {
    "0x1f0ebc543b2d411f66947041625c0aa1ce61cf86": ("0x1f0ebc", "$67,945 — aggressive cheap-side loader"),
    "0xe594336603f4fb5d3ba4125a67021ab3b4347052": ("0xe59433", "$24,189 — extreme deep discount"),
    "0xd0d6053c3c37e727402d84c14069780d360993aa": ("0xd0d605", "$9,422 — balanced + active seller"),
    "0xde17f7144fbd0eddb2679132c10ff5e74b120988": ("0xde17f7", "$9,376 — single-market whale"),
    "0xd1ebe815f921b3ebbd8d9e0a4192c6ab18360f5c": ("0xd1ebe8", "$6,771 — conservative broad spread"),
    "0x63ce342161250d705dc0b16df89036c8e5f9ba9a": ("0x63ce34", "$6,242 — deep discount + seller"),
    "0x2d8b401d2f0e6937afebf18e19e11ca568a5260a": ("0x2d8b40", "$5,299 — ultra-dense grid MM"),
    "0x2eb5714ff6f20f5f9f7662c556dbef5e1c9bf4d4": ("0x2eb571", "$5,027 — moderate spread pairs"),
}

# Build wallet stats: for each address, count maker and taker fills
# A "maker fill" = the address appears as maker_address
# A "taker fill" = the address appears as taker_address
maker_counts = trades['maker_address'].value_counts()
taker_counts = trades['taker_address'].value_counts()

all_wallets = set(maker_counts.index) | set(taker_counts.index)
# Remove NaN and CTF proxy
all_wallets.discard(np.nan)
all_wallets.discard(ctf_proxy)
all_wallets = {w for w in all_wallets if isinstance(w, str) and w.startswith('0x')}

wallet_stats = []
for w in all_wallets:
    mk = maker_counts.get(w, 0)
    tk = taker_counts.get(w, 0)
    total = mk + tk
    if total == 0:
        continue
    # Volume
    maker_vol = trades[trades['maker_address'] == w]['size'].sum() if mk > 0 else 0
    taker_vol = trades[trades['taker_address'] == w]['size'].sum() if tk > 0 else 0

    known = w in KNOWN_WALLETS
    label = KNOWN_WALLETS[w][0] if known else w[:10]
    desc = KNOWN_WALLETS[w][1] if known else ""

    wallet_stats.append({
        'address': w,
        'label': label,
        'known': known,
        'description': desc,
        'maker_fills': mk,
        'taker_fills': tk,
        'total_fills': total,
        'maker_pct': mk / total * 100,
        'maker_volume': maker_vol,
        'taker_volume': taker_vol,
        'total_volume': maker_vol + taker_vol,
    })

wdf = pd.DataFrame(wallet_stats).sort_values('total_fills', ascending=False)

print(f"\n--- All Wallets Summary ---")
print(f"  Total unique wallets (excl CTF proxy): {len(wdf)}")
print(f"  Known wallets found: {wdf['known'].sum()}")

print(f"\n--- Top 30 Wallets by Total Fills ---")
top30 = wdf.head(30)
for _, r in top30.iterrows():
    flag = " ★" if r['known'] else ""
    print(f"  {r['label']}{flag:3s} | maker={r['maker_fills']:>6,} taker={r['taker_fills']:>6,} total={r['total_fills']:>6,} | maker%={r['maker_pct']:5.1f}% | vol={r['total_volume']:>10,.0f}{' | ' + r['description'] if r['description'] else ''}")

print(f"\n--- Known Wallets Detail ---")
known_df = wdf[wdf['known']].sort_values('total_fills', ascending=False)
for _, r in known_df.iterrows():
    print(f"\n  {r['label']} ({r['description']})")
    print(f"    Maker fills: {r['maker_fills']:,}, Taker fills: {r['taker_fills']:,}")
    print(f"    Maker%: {r['maker_pct']:.1f}%")
    print(f"    Maker volume: {r['maker_volume']:,.0f}, Taker volume: {r['taker_volume']:,.0f}")

# NEW wallets with >50 fills
print(f"\n--- NEW Wallets (>50 fills, not in known list) ---")
new_wallets = wdf[(~wdf['known']) & (wdf['total_fills'] > 50)].sort_values('total_fills', ascending=False)
print(f"  Count: {len(new_wallets)}")
for _, r in new_wallets.head(20).iterrows():
    print(f"  {r['address']} | maker={r['maker_fills']:>5,} taker={r['taker_fills']:>5,} | maker%={r['maker_pct']:5.1f}% | vol={r['total_volume']:>8,.0f}")

# B.5: When known wallets TAKE — market state analysis
print(f"\n--- Known Wallet Taker Behavior ---")
# Merge trades with market end times
market_ends = markets[['condition_id', 'end_ts_ms']].drop_duplicates('condition_id')
trades_m = trades.merge(market_ends, on='condition_id', how='left')
trades_m['secs_to_expiry'] = (trades_m['end_ts_ms'] - trades_m['timestamp_ms']) / 1000

for addr, (label, desc) in KNOWN_WALLETS.items():
    taker_trades = trades_m[trades_m['taker_address'] == addr]
    if len(taker_trades) == 0:
        print(f"\n  {label}: NO taker trades")
        continue
    print(f"\n  {label} ({desc}):")
    print(f"    Total taker trades: {len(taker_trades):,}")
    print(f"    Side distribution: {taker_trades['side'].value_counts().to_dict()}")
    print(f"    Avg price: {taker_trades['price'].mean():.3f}")
    print(f"    Price distribution: p10={taker_trades['price'].quantile(.1):.3f}, p25={taker_trades['price'].quantile(.25):.3f}, median={taker_trades['price'].median():.3f}, p75={taker_trades['price'].quantile(.75):.3f}, p90={taker_trades['price'].quantile(.9):.3f}")

    # Book mid when taking
    has_mid = taker_trades['book_mid'].notna()
    if has_mid.sum() > 0:
        mids = taker_trades.loc[has_mid, 'book_mid']
        mid_dev = (mids - 0.5).abs()
        print(f"    Book mid when taking: avg={mids.mean():.3f}, avg |deviation from 0.50|={mid_dev.mean():.3f}")

    # Time to expiry
    has_exp = taker_trades['secs_to_expiry'].notna()
    if has_exp.sum() > 0:
        tte = taker_trades.loc[has_exp, 'secs_to_expiry']
        print(f"    Secs to expiry when taking: avg={tte.mean():.0f}, median={tte.median():.0f}, p10={tte.quantile(.1):.0f}, p90={tte.quantile(.9):.0f}")

    # Outcome (Up/Down) distribution
    print(f"    Outcome taken: {taker_trades['outcome'].value_counts().to_dict()}")

# Save wallet stats
wdf.to_csv(OUT / "wallet_stats.csv", index=False)
print(f"\n  [Saved wallet_stats.csv]")

# ============================================================
# SECTION C: TRADE-LEVEL BOOK ANALYSIS
# ============================================================
print("\n" + "=" * 80)
print("SECTION C: TRADE-LEVEL BOOK ANALYSIS")
print("=" * 80)

# Classify trades relative to book
has_book = trades['book_best_bid'].notna() & trades['book_best_ask'].notna()
tb = trades[has_book].copy()
print(f"\n  Trades with book data: {len(tb):,} / {len(trades):,} ({len(tb)/len(trades)*100:.1f}%)")

TOL = 0.005
def classify_trade(row):
    if row['side'] == 'BUY':
        if row['price'] <= row['book_best_bid'] + TOL:
            return 'at_bid'
        elif row['price'] >= row['book_best_ask'] - TOL:
            return 'at_ask'
        elif row['price'] > row['book_best_ask']:
            return 'above_ask'
        else:
            return 'between'
    else:  # SELL
        if row['price'] >= row['book_best_ask'] - TOL:
            return 'at_ask'
        elif row['price'] <= row['book_best_bid'] + TOL:
            return 'at_bid'
        elif row['price'] < row['book_best_bid']:
            return 'below_bid'
        else:
            return 'between'

tb['trade_class'] = tb.apply(classify_trade, axis=1)
print(f"\n--- Trade Classification ---")
tc_counts = tb['trade_class'].value_counts()
for cls, ct in tc_counts.items():
    print(f"  {cls}: {ct:,} ({ct/len(tb)*100:.1f}%)")

# For known wallets as makers: distance from best bid/ask
print(f"\n--- Known Wallet Maker Fill Depth ---")
for addr, (label, desc) in KNOWN_WALLETS.items():
    maker_trades = tb[tb['maker_address'] == addr]
    if len(maker_trades) == 0:
        print(f"\n  {label}: no maker trades with book data")
        continue

    buy_maker = maker_trades[maker_trades['side'] == 'BUY']
    sell_maker = maker_trades[maker_trades['side'] == 'SELL']

    print(f"\n  {label} ({len(maker_trades):,} maker fills with book):")

    if len(buy_maker) > 0:
        # When they're maker on BUY, their bid was filled. Distance = best_bid - fill_price
        # If fill_price < best_bid, they're deeper in the book
        depth = buy_maker['book_best_bid'] - buy_maker['price']
        at_top = (depth.abs() <= TOL).sum()
        deeper = (depth > TOL).sum()
        print(f"    BUY side: {len(buy_maker):,} fills")
        print(f"      At top of book (within 0.5¢): {at_top} ({at_top/len(buy_maker)*100:.1f}%)")
        print(f"      Deeper than best bid: {deeper} ({deeper/len(buy_maker)*100:.1f}%)")
        print(f"      Avg depth below best_bid: {depth[depth > TOL].mean():.3f}" if deeper > 0 else "")
        print(f"      Fill price distribution: p10={buy_maker['price'].quantile(.1):.3f}, median={buy_maker['price'].median():.3f}, p90={buy_maker['price'].quantile(.9):.3f}")

    if len(sell_maker) > 0:
        depth = sell_maker['price'] - sell_maker['book_best_ask']
        at_top = (depth.abs() <= TOL).sum()
        deeper = (depth > TOL).sum()
        print(f"    SELL side: {len(sell_maker):,} fills")
        print(f"      At top of book (within 0.5¢): {at_top} ({at_top/len(sell_maker)*100:.1f}%)")
        print(f"      Deeper than best ask: {deeper} ({deeper/len(sell_maker)*100:.1f}%)")
        print(f"      Avg depth above best_ask: {depth[depth > TOL].mean():.3f}" if deeper > 0 else "")
        print(f"      Fill price distribution: p10={sell_maker['price'].quantile(.1):.3f}, median={sell_maker['price'].median():.3f}, p90={sell_maker['price'].quantile(.9):.3f}")

# Average cents away from best bid/ask for each known wallet
print(f"\n--- Average Fill Distance from BBO (cents) ---")
for addr, (label, desc) in KNOWN_WALLETS.items():
    mt = tb[tb['maker_address'] == addr]
    if len(mt) == 0:
        continue
    buy_mt = mt[mt['side'] == 'BUY']
    sell_mt = mt[mt['side'] == 'SELL']

    distances = []
    if len(buy_mt) > 0:
        distances.extend((buy_mt['book_best_bid'] - buy_mt['price']).tolist())
    if len(sell_mt) > 0:
        distances.extend((sell_mt['price'] - sell_mt['book_best_ask']).tolist())

    if distances:
        d = np.array(distances)
        print(f"  {label}: avg={d.mean()*100:.1f}¢, median={np.median(d)*100:.1f}¢, p90={np.percentile(d, 90)*100:.1f}¢ away from BBO ({len(d)} fills)")


# ============================================================
# SECTION D: PnL WITH ACTUAL OUTCOMES
# ============================================================
print("\n" + "=" * 80)
print("SECTION D: PnL WITH ACTUAL OUTCOMES")
print("=" * 80)

# Build outcome lookup: condition_id → winning outcome
outcome_map = dict(zip(outcomes['condition_id'], outcomes['resolution_outcome']))
# Also map condition_id → token_id for Up and Down
up_token_map = dict(zip(outcomes['condition_id'], outcomes['token_id_up']))
down_token_map = dict(zip(outcomes['condition_id'], outcomes['token_id_down']))

# For each wallet in each resolved market, compute PnL
# PnL = shares_won * $1 - total_cost + (shares_sold * sell_price)
# A BUY = acquiring shares (cost = price * size)
# A SELL = disposing shares (revenue = price * size)

resolved_cids = set(outcomes['condition_id'])
resolved_trades = trades[trades['condition_id'].isin(resolved_cids)].copy()
print(f"\n  Trades in resolved markets: {len(resolved_trades):,}")
print(f"  Resolved markets: {len(resolved_cids)}")

# For each trade, determine which token (Up or Down) was traded
# The token_id column tells us
resolved_trades['is_up'] = resolved_trades.apply(
    lambda r: r['token_id'] == up_token_map.get(r['condition_id'], ''), axis=1
)

# For each wallet, in each market, compute:
# - shares_bought_up, cost_up (from BUY side where outcome=Up, or from being maker on BUY)
# - shares_sold_up, revenue_up
# etc.
# Simplification: we track by address appearing as maker or taker

def compute_wallet_market_pnl(wallet_trades, cid):
    """Compute PnL for a wallet in a single market."""
    winner = outcome_map.get(cid, None)
    if winner is None:
        return None

    up_trades = wallet_trades[wallet_trades['is_up']]
    down_trades = wallet_trades[~wallet_trades['is_up']]

    # Net position: BUY adds, SELL subtracts
    up_bought = up_trades[up_trades['_role_side'] == 'BUY']['size'].sum()
    up_sold = up_trades[up_trades['_role_side'] == 'SELL']['size'].sum()
    down_bought = down_trades[down_trades['_role_side'] == 'BUY']['size'].sum()
    down_sold = down_trades[down_trades['_role_side'] == 'SELL']['size'].sum()

    up_cost = (up_trades[up_trades['_role_side'] == 'BUY']['price'] * up_trades[up_trades['_role_side'] == 'BUY']['size']).sum()
    up_rev = (up_trades[up_trades['_role_side'] == 'SELL']['price'] * up_trades[up_trades['_role_side'] == 'SELL']['size']).sum()
    down_cost = (down_trades[down_trades['_role_side'] == 'BUY']['price'] * down_trades[down_trades['_role_side'] == 'BUY']['size']).sum()
    down_rev = (down_trades[down_trades['_role_side'] == 'SELL']['price'] * down_trades[down_trades['_role_side'] == 'SELL']['size']).sum()

    net_up = up_bought - up_sold
    net_down = down_bought - down_sold
    total_cost = up_cost + down_cost - up_rev - down_rev  # net spent

    # Resolution value
    if winner == 'Up':
        res_value = max(net_up, 0) * 1.0  # Up tokens pay $1, Down tokens pay $0
    else:
        res_value = max(net_down, 0) * 1.0

    # Also compute 50/50 estimate for comparison
    pairs = min(max(net_up, 0), max(net_down, 0))
    est_value_5050 = pairs * 1.0 + max(net_up - pairs, 0) * 0.5 + max(net_down - pairs, 0) * 0.5

    actual_pnl = res_value - total_cost
    est_pnl_5050 = est_value_5050 - total_cost

    return {
        'net_up': net_up,
        'net_down': net_down,
        'total_cost': total_cost,
        'pairs': pairs,
        'winner': winner,
        'res_value': res_value,
        'actual_pnl': actual_pnl,
        'est_pnl_5050': est_pnl_5050,
    }

# Process each known wallet
# For a wallet, they interact with trades as maker or taker
# When they are MAKER on a BUY trade → they are BUYING (their bid got hit)
# When they are MAKER on a SELL trade → they are SELLING (their ask got lifted)
# When they are TAKER on a BUY trade → they are BUYING (they lifted the ask)
# When they are TAKER on a SELL trade → they are SELLING (they hit the bid)

# Actually wait — the "side" in the trade is from the TAKER's perspective in Polymarket CLOB.
# side=BUY means the taker is buying, maker is selling
# side=SELL means the taker is selling, maker is buying
# So: maker on BUY trade → maker is SELLING. maker on SELL trade → maker is BUYING.
# taker on BUY trade → taker is BUYING. taker on SELL trade → taker is SELLING.

print(f"\n  NOTE: In CLOB trades, 'side' is the TAKER's side.")
print(f"  maker on BUY trade → maker SELLS. maker on SELL trade → maker BUYS.")
print(f"  taker on BUY trade → taker BUYS. taker on SELL trade → taker SELLS.")

pnl_results = []

for addr, (label, desc) in KNOWN_WALLETS.items():
    # Get all trades where this wallet is maker or taker, in resolved markets
    as_maker = resolved_trades[resolved_trades['maker_address'] == addr].copy()
    as_taker = resolved_trades[resolved_trades['taker_address'] == addr].copy()

    # For maker: their role_side is OPPOSITE of trade side
    as_maker['_role_side'] = as_maker['side'].map({'BUY': 'SELL', 'SELL': 'BUY'})
    # For taker: their role_side is SAME as trade side
    as_taker['_role_side'] = as_taker['side']

    all_wallet_trades = pd.concat([as_maker, as_taker])
    if len(all_wallet_trades) == 0:
        continue

    # Group by market
    wallet_pnl_by_market = []
    for cid, grp in all_wallet_trades.groupby('condition_id'):
        result = compute_wallet_market_pnl(grp, cid)
        if result is not None:
            result['condition_id'] = cid
            wallet_pnl_by_market.append(result)

    if not wallet_pnl_by_market:
        continue

    mdf = pd.DataFrame(wallet_pnl_by_market)
    total_actual = mdf['actual_pnl'].sum()
    total_est = mdf['est_pnl_5050'].sum()
    n_markets = len(mdf)
    n_profitable = (mdf['actual_pnl'] > 0).sum()

    # Maker vs taker PnL split
    maker_pnl_markets = []
    taker_pnl_markets = []
    for cid, grp in as_maker.groupby('condition_id'):
        grp = grp.copy()
        grp['_role_side'] = grp['side'].map({'BUY': 'SELL', 'SELL': 'BUY'})
        r = compute_wallet_market_pnl(grp, cid)
        if r: maker_pnl_markets.append(r['actual_pnl'])
    for cid, grp in as_taker.groupby('condition_id'):
        grp = grp.copy()
        grp['_role_side'] = grp['side']
        r = compute_wallet_market_pnl(grp, cid)
        if r: taker_pnl_markets.append(r['actual_pnl'])

    maker_pnl_total = sum(maker_pnl_markets)
    taker_pnl_total = sum(taker_pnl_markets)

    print(f"\n  {label} ({desc}):")
    print(f"    Markets traded: {n_markets}, Profitable: {n_profitable} ({n_profitable/n_markets*100:.0f}%)")
    print(f"    ACTUAL PnL: ${total_actual:,.2f}")
    print(f"    50/50 est PnL: ${total_est:,.2f}")
    print(f"    Difference: ${total_actual - total_est:,.2f} ({(total_actual - total_est)/abs(total_est)*100:+.0f}% if est≠0)" if total_est != 0 else f"    Difference: ${total_actual - total_est:,.2f}")
    print(f"    Maker PnL: ${maker_pnl_total:,.2f}, Taker PnL: ${taker_pnl_total:,.2f}")
    print(f"    Avg actual PnL/market: ${total_actual/n_markets:,.3f}")

    # Per-market stats
    print(f"    PnL distribution: p10=${mdf['actual_pnl'].quantile(.1):,.2f}, median=${mdf['actual_pnl'].median():,.2f}, p90=${mdf['actual_pnl'].quantile(.9):,.2f}")

    pnl_results.append({
        'label': label,
        'description': desc,
        'markets': n_markets,
        'actual_pnl': total_actual,
        'est_pnl_5050': total_est,
        'maker_pnl': maker_pnl_total,
        'taker_pnl': taker_pnl_total,
        'pnl_per_market': total_actual / n_markets,
        'win_rate': n_profitable / n_markets * 100,
    })

pnl_df = pd.DataFrame(pnl_results).sort_values('actual_pnl', ascending=False)
print(f"\n--- Wallet Ranking: Actual PnL vs 50/50 Estimate ---")
for _, r in pnl_df.iterrows():
    print(f"  {r['label']:10s} | actual=${r['actual_pnl']:>10,.2f} | est_5050=${r['est_pnl_5050']:>10,.2f} | maker=${r['maker_pnl']:>10,.2f} | taker=${r['taker_pnl']:>10,.2f} | $/mkt=${r['pnl_per_market']:>7,.3f} | win={r['win_rate']:.0f}%")

pnl_df.to_csv(OUT / "wallet_pnl_actual.csv", index=False)
print(f"\n  [Saved wallet_pnl_actual.csv]")


# ============================================================
# SECTION E: MARKET MICROSTRUCTURE
# ============================================================
print("\n" + "=" * 80)
print("SECTION E: MARKET MICROSTRUCTURE")
print("=" * 80)

# Merge book snapshots with market end times
books_m = books.merge(market_ends, on='condition_id', how='left')
books_m['secs_to_expiry'] = (books_m['end_ts_ms'] - books_m['timestamp_ms']) / 1000

# Time buckets
def time_bucket(secs):
    if secs > 240: return '300-240'
    elif secs > 180: return '240-180'
    elif secs > 120: return '180-120'
    elif secs > 60: return '120-60'
    elif secs > 0: return '60-0'
    else: return 'after_expiry'

books_m['time_bucket'] = books_m['secs_to_expiry'].apply(time_bucket)

bucket_order = ['300-240', '240-180', '180-120', '120-60', '60-0', 'after_expiry']

print(f"\n--- Book Evolution by Time Bucket ---")
for bucket in bucket_order:
    bk = books_m[books_m['time_bucket'] == bucket]
    if len(bk) == 0:
        continue
    print(f"\n  [{bucket}] ({len(bk):,} snapshots)")
    print(f"    Avg spread: {bk['spread'].mean():.4f} ({bk['spread'].mean()*100:.1f}¢)")
    print(f"    Avg mid: {bk['mid_price'].mean():.4f}")
    print(f"    Avg |mid - 0.50|: {(bk['mid_price'] - 0.5).abs().mean():.4f}")
    print(f"    Avg bid_depth_3: {bk['bid_depth_3'].mean():.0f}")
    print(f"    Avg ask_depth_3: {bk['ask_depth_3'].mean():.0f}")
    print(f"    Spread percentiles: p10={bk['spread'].quantile(.1):.3f}, p50={bk['spread'].median():.3f}, p90={bk['spread'].quantile(.9):.3f}")

# Volume profile by time bucket
trades_m['time_bucket'] = trades_m['secs_to_expiry'].apply(time_bucket)
print(f"\n--- Volume Profile by Time Bucket ---")
vol_by_bucket = trades_m.groupby('time_bucket').agg(
    trade_count=('size', 'count'),
    total_volume=('size', 'sum'),
    avg_size=('size', 'mean'),
).reindex(bucket_order)

total_vol = vol_by_bucket['total_volume'].sum()
for bucket in bucket_order:
    if bucket in vol_by_bucket.index:
        r = vol_by_bucket.loc[bucket]
        pct = r['total_volume'] / total_vol * 100
        print(f"  [{bucket}]: {r['trade_count']:>6,.0f} trades, {r['total_volume']:>10,.0f} shares ({pct:5.1f}%), avg size={r['avg_size']:.1f}")

# Big moves
print(f"\n--- Big Moves (>10¢ mid change between consecutive snapshots) ---")
books_sorted = books_m.sort_values(['condition_id', 'token_id', 'timestamp_ms'])
books_sorted['mid_diff'] = books_sorted.groupby(['condition_id', 'token_id'])['mid_price'].diff().abs()
big_moves = books_sorted[books_sorted['mid_diff'] > 0.10]
print(f"  Total big moves: {big_moves.shape[0]}")
if len(big_moves) > 0:
    big_moves_buckets = big_moves['time_bucket'].value_counts().reindex(bucket_order, fill_value=0)
    for bucket in bucket_order:
        print(f"    [{bucket}]: {big_moves_buckets.get(bucket, 0)} big moves")
    print(f"  Avg big move size: {big_moves['mid_diff'].mean():.3f}")
    print(f"  Max big move: {big_moves['mid_diff'].max():.3f}")

# Post-expiry trading
post_exp = trades_m[trades_m['secs_to_expiry'] < 0]
print(f"\n--- Post-Expiry Trading ---")
print(f"  Trades after market end: {len(post_exp):,} ({len(post_exp)/len(trades_m)*100:.1f}%)")
print(f"  Volume after market end: {post_exp['size'].sum():,.0f}")
if len(post_exp) > 0:
    print(f"  Time after expiry: avg={(-post_exp['secs_to_expiry']).mean():.0f}s, max={(-post_exp['secs_to_expiry']).max():.0f}s")
    print(f"  Price distribution: avg={post_exp['price'].mean():.3f}, p10={post_exp['price'].quantile(.1):.3f}, p90={post_exp['price'].quantile(.9):.3f}")


# ============================================================
# SECTION F: STRATEGY VIABILITY AT 236ms LATENCY
# ============================================================
print("\n" + "=" * 80)
print("SECTION F: STRATEGY VIABILITY AT 236ms LATENCY")
print("=" * 80)

# 100% maker wallets
pure_makers = wdf[(wdf['maker_pct'] > 95) & (wdf['total_fills'] > 50)]
print(f"\n--- Pure Maker Wallets (>95% maker, >50 fills) ---")
print(f"  Count: {len(pure_makers)}")
for _, r in pure_makers.head(10).iterrows():
    flag = " ★" if r['known'] else ""
    print(f"  {r['label']}{flag}: {r['total_fills']:,} fills, maker%={r['maker_pct']:.1f}%, vol={r['total_volume']:,.0f}")

# Compute fill rate at top of book vs deeper
print(f"\n--- Fill Location Analysis (all trades with book data) ---")
# For buys: if price >= best_bid - 0.005, it's a top-of-book fill
# For sells: if price <= best_ask + 0.005, it's a top-of-book fill
buys_with_book = tb[tb['side'] == 'SELL']  # side=SELL means maker is buying
sells_with_book = tb[tb['side'] == 'BUY']  # side=BUY means maker is selling

buy_at_top = buys_with_book[(buys_with_book['price'] - buys_with_book['book_best_bid']).abs() <= TOL]
buy_deeper = buys_with_book[(buys_with_book['book_best_bid'] - buys_with_book['price']) > TOL]
sell_at_top = sells_with_book[(sells_with_book['price'] - sells_with_book['book_best_ask']).abs() <= TOL]
sell_deeper = sells_with_book[(sells_with_book['price'] - sells_with_book['book_best_ask']) > TOL]

print(f"  Maker BUY fills at top of book: {len(buy_at_top):,} ({len(buy_at_top)/len(buys_with_book)*100:.1f}%)")
print(f"  Maker BUY fills deeper: {len(buy_deeper):,} ({len(buy_deeper)/len(buys_with_book)*100:.1f}%)")
print(f"  Maker SELL fills at top of book: {len(sell_at_top):,} ({len(sell_at_top)/len(sells_with_book)*100:.1f}%)")
print(f"  Maker SELL fills deeper: {len(sell_deeper):,} ({len(sell_deeper)/len(sells_with_book)*100:.1f}%)")

# Latency impact estimate
# At 236ms, we lose queue priority on top-of-book orders
# Deeper orders are less affected by latency (less competition)
print(f"\n--- Latency Impact Estimate ---")
print(f"  At 236ms round-trip, placement/cancel takes ~236ms vs ~50ms for colocated bots")
print(f"  Queue priority loss: ~186ms slower to place = worse queue position at same price")

# For known wallets: what % of their PnL comes from maker vs taker fills
print(f"\n--- Maker vs Taker PnL Split (from Section D) ---")
for _, r in pnl_df.iterrows():
    total = abs(r['actual_pnl']) if r['actual_pnl'] != 0 else 1
    mk_pct = r['maker_pnl'] / r['actual_pnl'] * 100 if r['actual_pnl'] != 0 else 0
    print(f"  {r['label']:10s} | total=${r['actual_pnl']:>8,.2f} | maker=${r['maker_pnl']:>8,.2f} ({mk_pct:+.0f}%) | taker=${r['taker_pnl']:>8,.2f}")

# Deeper maker analysis: how much volume sits deeper than L1?
print(f"\n--- Book Depth Distribution (how much rests deeper than L1) ---")
# From book snapshots, compare L1 size vs L2-L5
books_sample = books.copy()
books_sample['bid_l1_pct'] = books_sample['bid_1_size'] / (books_sample['bid_1_size'] + books_sample['bid_2_size'] + books_sample['bid_3_size'] + books_sample['bid_4_size'] + books_sample['bid_5_size'])
books_sample['ask_l1_pct'] = books_sample['ask_1_size'] / (books_sample['ask_1_size'] + books_sample['ask_2_size'] + books_sample['ask_3_size'] + books_sample['ask_4_size'] + books_sample['ask_5_size'])

print(f"  Bid L1 as % of L1-L5: avg={books_sample['bid_l1_pct'].mean()*100:.1f}%, median={books_sample['bid_l1_pct'].median()*100:.1f}%")
print(f"  Ask L1 as % of L1-L5: avg={books_sample['ask_l1_pct'].mean()*100:.1f}%, median={books_sample['ask_l1_pct'].median()*100:.1f}%")

# Pair cost at different levels
print(f"\n--- Combined (Up+Down) Cost at Book Levels ---")
# Group snapshots by condition_id and timestamp, pair Up and Down
# For each condition_id, we have Up and Down tokens
# We need to pair them by timestamp
up_books = books_m[books_m['outcome'] == 'Up' if 'outcome' in books_m.columns else True].copy()
# Actually, the outcome column is in the books data
if 'outcome' in books.columns:
    up_snaps = books[books['outcome'] == 'Up']
    dn_snaps = books[books['outcome'] == 'Down']
else:
    # Need to determine from token_id
    # Use the outcomes table: token_id_up tells us which token is Up
    up_tokens = set(outcomes['token_id_up'].astype(str))
    down_tokens = set(outcomes['token_id_down'].astype(str))
    books['_token_str'] = books['token_id'].astype(str)
    up_snaps = books[books['_token_str'].isin(up_tokens)]
    dn_snaps = books[books['_token_str'].isin(down_tokens)]

print(f"  Up snapshots: {len(up_snaps):,}, Down snapshots: {len(dn_snaps):,}")

# Merge on condition_id + timestamp
if len(up_snaps) > 0 and len(dn_snaps) > 0:
    paired = up_snaps.merge(dn_snaps, on=['condition_id', 'timestamp_ms'], suffixes=('_up', '_dn'), how='inner')
    if len(paired) > 0:
        paired['combined_ask_l1'] = paired['best_ask_up'] + paired['best_ask_dn']
        paired['combined_bid_l1'] = paired['best_bid_up'] + paired['best_bid_dn']
        paired['combined_mid'] = paired['mid_price_up'] + paired['mid_price_dn']

        print(f"  Paired snapshots: {len(paired):,}")
        print(f"  Combined ASK L1: avg={paired['combined_ask_l1'].mean():.4f}, median={paired['combined_ask_l1'].median():.4f}")
        print(f"  Combined BID L1: avg={paired['combined_bid_l1'].mean():.4f}, median={paired['combined_bid_l1'].median():.4f}")
        print(f"  Combined MID: avg={paired['combined_mid'].mean():.4f}, median={paired['combined_mid'].median():.4f}")

        # L5 combined
        if 'ask_5_price_up' in paired.columns:
            paired['combined_ask_l5'] = paired['ask_5_price_up'] + paired['ask_5_price_dn']
            paired['combined_bid_l5'] = paired['bid_5_price_up'] + paired['bid_5_price_dn']
            print(f"  Combined ASK L5: avg={paired['combined_ask_l5'].mean():.4f}, median={paired['combined_ask_l5'].median():.4f}")
            print(f"  Combined BID L5: avg={paired['combined_bid_l5'].mean():.4f}, median={paired['combined_bid_l5'].median():.4f}")

print(f"\n{'='*80}")
print("ANALYSIS COMPLETE")
print(f"{'='*80}")
print(f"\nOutput files saved:")
print(f"  - wallet_stats.csv")
print(f"  - wallet_pnl_actual.csv")
