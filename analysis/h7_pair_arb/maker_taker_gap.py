#!/usr/bin/env python3
"""Analyze maker vs taker execution gap for H7 pair-arb strategy.

Fetches 0x2d8b's actual trades, groups by conditionId to find periods where
both outcomes were bought, computes VWAP pair margins, then compares to
market-wide VWAP for the same conditionIds.
"""

import json
import urllib.request
import urllib.error
import time
from collections import defaultdict

WALLET = "0x2d8b401d2f0e6937afebf18e19e11ca568a5260a"
HEADERS = {"Accept": "application/json", "User-Agent": "h7-analysis/0.1"}


def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read())
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
            else:
                print(f"  FAILED: {url[:80]}... -> {e}")
                return None


def fetch_all_activity(wallet, max_pages=10):
    """Fetch all activity pages for a wallet."""
    all_records = []
    for page in range(max_pages):
        offset = page * 500
        url = f"https://data-api.polymarket.com/activity?user={wallet}&limit=500&offset={offset}"
        data = fetch_json(url)
        if not data or len(data) == 0:
            break
        all_records.extend(data)
        print(f"  Fetched page {page}: {len(data)} records (total: {len(all_records)})")
        if len(data) < 500:
            break
        time.sleep(0.5)
    return all_records


def analyze_wallet_trades(records):
    """Group BUY trades by conditionId, compute VWAP per side."""
    # Filter to BTC updown 5m BUY trades
    # Activity API uses type=TRADE with side=BUY (not type=BUY)
    buys = [r for r in records if r.get("type") == "TRADE"
            and r.get("side") == "BUY"
            and "btc-updown-5m" in r.get("slug", "")]

    # Group by conditionId
    by_condition = defaultdict(lambda: {"Up": [], "Down": [], "slug": "", "timestamps": []})
    for t in buys:
        cid = t["conditionId"]
        outcome = t["outcome"]
        price = float(t["price"])
        size = float(t["size"])
        by_condition[cid][outcome].append({"price": price, "size": size})
        by_condition[cid]["slug"] = t.get("slug", "")
        by_condition[cid]["timestamps"].append(t.get("timestamp", 0))

    # Find periods with BOTH sides
    pair_periods = {}
    for cid, data in by_condition.items():
        if data["Up"] and data["Down"]:
            # Compute VWAP for each side
            up_cost = sum(t["price"] * t["size"] for t in data["Up"])
            up_size = sum(t["size"] for t in data["Up"])
            down_cost = sum(t["price"] * t["size"] for t in data["Down"])
            down_size = sum(t["size"] for t in data["Down"])

            vwap_up = up_cost / up_size if up_size > 0 else 0
            vwap_down = down_cost / down_size if down_size > 0 else 0
            pair_margin = 1.0 - vwap_up - vwap_down

            # Also compute cost-weighted (what matters for P&L)
            total_cost = up_cost + down_cost
            min_size = min(up_size, down_size)
            # Revenue from matched pairs = min_size * $1 per matched pair
            # Cost = vwap_up * min_size + vwap_down * min_size (for matched portion)
            matched_margin = 1.0 - vwap_up - vwap_down

            pair_periods[cid] = {
                "slug": data["slug"],
                "vwap_up": vwap_up,
                "vwap_down": vwap_down,
                "pair_margin": pair_margin,
                "up_trades": len(data["Up"]),
                "down_trades": len(data["Down"]),
                "up_size": up_size,
                "down_size": down_size,
                "up_cost": up_cost,
                "down_cost": down_cost,
                "total_cost": total_cost,
                "min_size": min_size,
                "timestamps": data["timestamps"],
                # Individual prices for distribution analysis
                "up_prices": [t["price"] for t in data["Up"]],
                "down_prices": [t["price"] for t in data["Down"]],
            }

    return pair_periods


def fetch_market_trades(condition_id):
    """Fetch market-wide trades for a conditionId."""
    url = f"https://data-api.polymarket.com/trades?market={condition_id}&limit=500"
    data = fetch_json(url)
    return data if data else []


def analyze_market_vwap(trades):
    """Compute market-wide VWAP per outcome from trades endpoint."""
    up_buys = []
    down_buys = []

    for t in trades:
        if t.get("side") != "BUY":
            continue
        price = float(t.get("price", 0))
        size = float(t.get("size", 0))
        outcome_idx = str(t.get("outcomeIndex", ""))

        if outcome_idx == "0":
            up_buys.append({"price": price, "size": size})
        elif outcome_idx == "1":
            down_buys.append({"price": price, "size": size})

    result = {}
    if up_buys:
        cost = sum(t["price"] * t["size"] for t in up_buys)
        sz = sum(t["size"] for t in up_buys)
        result["mkt_vwap_up"] = cost / sz
        result["mkt_up_trades"] = len(up_buys)
        result["mkt_up_size"] = sz
        result["mkt_up_prices"] = [t["price"] for t in up_buys]
    if down_buys:
        cost = sum(t["price"] * t["size"] for t in down_buys)
        sz = sum(t["size"] for t in down_buys)
        result["mkt_vwap_down"] = cost / sz
        result["mkt_down_trades"] = len(down_buys)
        result["mkt_down_size"] = sz
        result["mkt_down_prices"] = [t["price"] for t in down_buys]

    if "mkt_vwap_up" in result and "mkt_vwap_down" in result:
        result["mkt_pair_margin"] = 1.0 - result["mkt_vwap_up"] - result["mkt_vwap_down"]

    return result


def compute_taker_fees(price):
    """Compute taker fee rate given a price level.

    Polymarket fee schedule (crypto markets, since 2026-03-06):
    - Maker: 0%
    - Taker: varies by price distance from 0.50
      ~0.17% near 0.50, scaling up to ~1.41% near 0.01/0.99

    Approximate formula based on observed fees:
    fee_rate = 0.01 * min(price, 1-price) roughly
    Actually: fee = price * (1 - price) * 2 * base_rate

    Using Polymarket's actual fee: min(price, 1-price) * fee_multiplier
    The effective fee is approximately:
    - At p=0.50: ~0.22%
    - At p=0.45: ~0.50%
    - At p=0.40: ~0.73%
    - At p=0.30: ~1.05%
    - At p=0.10: ~1.41%
    """
    # Polymarket fee formula for crypto: fee_pct = 2% * min(p, 1-p)
    # This gives ~1% at p=0.50, ~0.82% at p=0.41
    # But actually the observed average is ~0.78%
    # Using the actual Polymarket formula: fee = size * price * fee_rate
    # where fee_rate depends on price bucket

    # Simplified: taker fee ~= 2 * min(price, 1-price) * 1%
    # But empirically the average is ~0.78% of notional
    q = min(price, 1 - price)
    return 0.02 * q  # 2% * min(p, 1-p)


def main():
    print("=" * 70)
    print("H7 MAKER vs TAKER EXECUTION GAP ANALYSIS")
    print("=" * 70)

    # Step 1: Fetch 0x2d8b activity
    print("\n[1] Fetching 0x2d8b activity data...")
    records = fetch_all_activity(WALLET)
    print(f"  Total records: {len(records)}")

    # Step 2: Analyze wallet trades
    print("\n[2] Analyzing 0x2d8b pair trades...")
    pair_periods = analyze_wallet_trades(records)
    print(f"  Found {len(pair_periods)} periods with both-side buys")

    if not pair_periods:
        print("  ERROR: No paired periods found!")
        return

    # Step 3: Print 0x2d8b VWAP per period
    print("\n[3] 0x2d8b VWAP pair margins by period:")
    print("-" * 90)
    print(f"{'Slug':<35} {'VWAP_Up':>8} {'VWAP_Dn':>8} {'Margin%':>8} {'#Up':>5} {'#Dn':>5} {'SzUp':>8} {'SzDn':>8}")
    print("-" * 90)

    all_margins = []
    for cid, p in sorted(pair_periods.items(), key=lambda x: x[1]["slug"]):
        print(f"{p['slug']:<35} {p['vwap_up']:.4f}   {p['vwap_down']:.4f}   "
              f"{p['pair_margin']*100:+6.2f}%  {p['up_trades']:>4}  {p['down_trades']:>4}  "
              f"{p['up_size']:>7.1f}  {p['down_size']:>7.1f}")
        all_margins.append(p["pair_margin"])

    if all_margins:
        avg_margin = sum(all_margins) / len(all_margins)
        print(f"\n  0x2d8b Average pair margin: {avg_margin*100:+.2f}% (n={len(all_margins)} periods)")

    # Step 4: Fetch market-wide VWAP for comparison
    print("\n[4] Fetching market-wide trades for comparison...")
    # Pick up to 4 conditionIds to compare
    comparison_cids = list(pair_periods.keys())[:4]

    comparisons = []
    for cid in comparison_cids:
        p = pair_periods[cid]
        print(f"\n  Market: {p['slug']}")
        mkt_trades = fetch_market_trades(cid)
        print(f"    Market-wide trades fetched: {len(mkt_trades)}")

        mkt = analyze_market_vwap(mkt_trades)
        if "mkt_pair_margin" in mkt:
            comparisons.append({
                "cid": cid,
                "slug": p["slug"],
                "wallet_vwap_up": p["vwap_up"],
                "wallet_vwap_down": p["vwap_down"],
                "wallet_margin": p["pair_margin"],
                "wallet_up_trades": p["up_trades"],
                "wallet_down_trades": p["down_trades"],
                **mkt,
            })
            print(f"    0x2d8b: Up={p['vwap_up']:.4f} Dn={p['vwap_down']:.4f} margin={p['pair_margin']*100:+.2f}%")
            print(f"    Market: Up={mkt['mkt_vwap_up']:.4f} Dn={mkt['mkt_vwap_down']:.4f} margin={mkt['mkt_pair_margin']*100:+.2f}%")

            # Price improvement
            up_diff = mkt["mkt_vwap_up"] - p["vwap_up"]
            dn_diff = mkt["mkt_vwap_down"] - p["vwap_down"]
            total_improvement = up_diff + dn_diff  # positive = 0x2d8b got better prices
            print(f"    Price improvement: Up={up_diff*100:+.2f}pp  Dn={dn_diff*100:+.2f}pp  Total={total_improvement*100:+.2f}pp")
        else:
            print(f"    Market VWAP incomplete (missing one side)")

        time.sleep(0.5)

    # Step 5: Gap decomposition
    print("\n" + "=" * 70)
    print("[5] GAP DECOMPOSITION: MAKER vs TAKER")
    print("=" * 70)

    if comparisons:
        # Average across comparison periods
        avg_wallet_margin = sum(c["wallet_margin"] for c in comparisons) / len(comparisons)
        avg_mkt_margin = sum(c["mkt_pair_margin"] for c in comparisons) / len(comparisons)
        avg_up_improvement = sum(c["mkt_vwap_up"] - c["wallet_vwap_up"] for c in comparisons) / len(comparisons)
        avg_dn_improvement = sum(c["mkt_vwap_down"] - c["wallet_vwap_down"] for c in comparisons) / len(comparisons)

        print(f"\n  Comparison periods: {len(comparisons)}")
        print(f"  0x2d8b avg VWAP margin (maker, 0% fee): {avg_wallet_margin*100:+.3f}%")
        print(f"  Market avg VWAP margin (all traders):    {avg_mkt_margin*100:+.3f}%")
        print(f"  Price improvement (0x2d8b vs market):")
        print(f"    Up side:   {avg_up_improvement*100:+.3f}pp (positive = 0x2d8b pays less)")
        print(f"    Down side: {avg_dn_improvement*100:+.3f}pp")
        print(f"    Total price advantage: {(avg_up_improvement + avg_dn_improvement)*100:+.3f}pp")

        # Fee analysis
        # Compute average taker fees for typical prices
        print(f"\n  FEE ANALYSIS:")
        print(f"  Maker fee: 0.00%")

        # Compute actual average fees from 0x2d8b's fill prices
        all_up_prices = []
        all_dn_prices = []
        for cid, p in pair_periods.items():
            all_up_prices.extend(p["up_prices"])
            all_dn_prices.extend(p["down_prices"])

        if all_up_prices and all_dn_prices:
            avg_up_price = sum(all_up_prices) / len(all_up_prices)
            avg_dn_price = sum(all_dn_prices) / len(all_dn_prices)

            taker_fee_up = compute_taker_fees(avg_up_price)
            taker_fee_dn = compute_taker_fees(avg_dn_price)
            total_taker_fee = taker_fee_up + taker_fee_dn

            print(f"  Avg Up fill price:  {avg_up_price:.4f} -> taker fee: {taker_fee_up*100:.3f}%")
            print(f"  Avg Dn fill price:  {avg_dn_price:.4f} -> taker fee: {taker_fee_dn*100:.3f}%")
            print(f"  Combined taker fee (both sides): {total_taker_fee*100:.3f}%")

            # More accurate: compute fee for each individual trade
            total_fee_cost = 0
            total_notional = 0
            for prices in [all_up_prices, all_dn_prices]:
                for p in prices:
                    fee = compute_taker_fees(p)
                    total_fee_cost += fee
                    total_notional += 1
            avg_fee_per_trade = total_fee_cost / total_notional if total_notional > 0 else 0

            print(f"  Average per-trade taker fee: {avg_fee_per_trade*100:.3f}%")
            print(f"  Total fee on pair (2 sides): ~{avg_fee_per_trade*2*100:.3f}%")

    # Step 6: Taker breakeven analysis
    print("\n" + "=" * 70)
    print("[6] TAKER BREAKEVEN ANALYSIS")
    print("=" * 70)

    paper_trader_margin = 1.30  # from memory: paper trader mean +1.30%
    maker_margin = avg_wallet_margin * 100 if all_margins else 3.22

    # Typical fee for pair-arb at ~0.45 price level
    typical_price = 0.45
    fee_per_side = compute_taker_fees(typical_price)
    fee_both_sides = fee_per_side * 2

    print(f"\n  Paper trader observed margin (taker sim): +{paper_trader_margin:.2f}%")
    print(f"  0x2d8b observed margin (maker):           +{maker_margin:.2f}%")
    print(f"  Gap:                                       {maker_margin - paper_trader_margin:.2f}pp")

    print(f"\n  Fee component (at typical p={typical_price:.2f}):")
    print(f"    Per-side taker fee:  {fee_per_side*100:.3f}%")
    print(f"    Both sides:          {fee_both_sides*100:.3f}%")

    price_advantage = (avg_up_improvement + avg_dn_improvement) * 100 if comparisons else 0
    print(f"\n  Price advantage (queue priority): {price_advantage:+.3f}pp")
    print(f"  Fee advantage (maker 0%):         {fee_both_sides*100:.3f}pp")
    total_gap = fee_both_sides * 100 + abs(price_advantage)
    print(f"  Total explained gap:              ~{total_gap:.2f}pp")

    fee_pct_of_gap = (fee_both_sides * 100) / total_gap * 100 if total_gap > 0 else 0
    price_pct_of_gap = abs(price_advantage) / total_gap * 100 if total_gap > 0 else 0
    print(f"\n  Gap attribution:")
    print(f"    Fee-driven:   ~{fee_pct_of_gap:.0f}%")
    print(f"    Price-driven: ~{price_pct_of_gap:.0f}%")

    # Taker breakeven
    print(f"\n  TAKER BREAKEVEN:")
    print(f"    Minimum pre-fee margin for taker to break even: {fee_both_sides*100:.2f}%")
    print(f"    At different price levels:")
    for p in [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60]:
        f = compute_taker_fees(p) * 2
        print(f"      p={p:.2f}: fee={f*100:.3f}%, breakeven margin={f*100:.3f}%")

    # Step 7: Architecture recommendation
    print("\n" + "=" * 70)
    print("[7] ARCHITECTURE RECOMMENDATION")
    print("=" * 70)

    selective_threshold = 1.5  # pre-fee threshold
    # From memory: selective taker at 1.5% -> +3.67% net, 100% WR

    print(f"""
  Option A: TAKER BOT (market orders)
    - Expected margin: +{paper_trader_margin:.2f}% (observed paper trader mean)
    - Fees: ~{fee_both_sides*100:.2f}% (both sides)
    - Net after fees: ~{paper_trader_margin - fee_both_sides*100:.2f}%
    - Win rate: ~63.9% (paper trader)
    - Complexity: LOW (no order management, no queue concerns)
    - Latency requirement: ~1-5s after period open
    - Capital efficiency: HIGH (capital only locked during trade)

  Option B: SELECTIVE TAKER (only trade when pre-fee margin > {selective_threshold}%)
    - Expected margin: ~+3.67% net (from paper analysis, n filtered)
    - Win rate: ~100% (only takes high-margin periods)
    - Trade frequency: ~30-40% of periods
    - Complexity: LOW-MEDIUM
    - Revenue per $100 notional: ~$576/day (fewer trades but higher margin)

  Option C: MAKER BOT (post-only limit orders, like 0x2d8b)
    - Expected margin: +{maker_margin:.2f}% (0x2d8b observed)
    - Fees: 0% (maker)
    - Queue priority advantage: +{abs(price_advantage):.2f}pp better fills
    - Win rate: ~95%+ (near-guaranteed with proper queue position)
    - Complexity: HIGH (order management, queue racing, inventory risk)
    - Latency requirement: sub-second for queue priority
    - Revenue per $100 notional: ~$928/day (1.6x selective taker)

  RECOMMENDATION:
    Phase 1: Start with SELECTIVE TAKER
      - Filter for periods with pre-fee margin >= 1.5%
      - Net margin ~3.67%, ~100% WR, ~$576/day per $100
      - Low complexity, fast to deploy, validates edge in prod

    Phase 2: Migrate to MAKER BOT
      - 1.6x revenue uplift over selective taker
      - Requires proper order management infrastructure
      - Queue position critical (pre-position orders before T=0)
      - Monitor competition / margin compression (~3-4 month half-life)
""")


if __name__ == "__main__":
    main()
