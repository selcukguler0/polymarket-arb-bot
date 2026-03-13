#!/usr/bin/env python3
"""
H7 Maker vs Taker Execution Gap Analysis

1. Fetch 0x2d8b's activity (BTC 5-min BUY trades)
2. For each conditionId with both-side buys, compute 0x2d8b's VWAP per side
3. Pull all trades for those conditionIds, compute market VWAP
4. Compare maker vs taker execution quality
"""

import json
import time
import urllib.request
import sys
from collections import defaultdict
from datetime import datetime, timezone

WALLET = "0x2d8b401d2f0e6937afebf18e19e11ca568a5260a"
DATA_API = "https://data-api.polymarket.com"


def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "Mozilla/5.0")
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1 * (attempt + 1))
            else:
                print(f"  FAILED after {retries} attempts: {url[:100]}... -> {e}", file=sys.stderr)
                return None


def fetch_all_activity(wallet, limit=500):
    """Fetch all activity pages."""
    all_records = []
    offset = 0
    while True:
        url = f"{DATA_API}/activity?user={wallet}&limit={limit}&offset={offset}"
        data = fetch_json(url)
        if not data or len(data) == 0:
            break
        all_records.extend(data)
        print(f"  Fetched {len(data)} activity records (offset={offset}, total={len(all_records)})", file=sys.stderr)
        if len(data) < limit:
            break
        offset += limit
        time.sleep(0.5)
    return all_records


def fetch_all_trades(condition_id, limit=500):
    """Fetch all trades for a conditionId with pagination."""
    all_trades = []
    offset = 0
    while True:
        url = f"{DATA_API}/trades?market={condition_id}&limit={limit}&offset={offset}"
        data = fetch_json(url)
        if not data or len(data) == 0:
            break
        all_trades.extend(data)
        if len(data) < limit:
            break
        offset += limit
        time.sleep(0.3)
    return all_trades


def compute_vwap(trades):
    """Compute VWAP from list of (price, size) tuples."""
    total_cost = sum(p * s for p, s in trades)
    total_size = sum(s for _, s in trades)
    if total_size == 0:
        return None, 0
    return total_cost / total_size, total_size


def main():
    print("=" * 70, file=sys.stderr)
    print("H7 MAKER VS TAKER EXECUTION GAP ANALYSIS", file=sys.stderr)
    print("=" * 70, file=sys.stderr)

    # Step 1: Fetch 0x2d8b's activity
    print("\n[1] Fetching 0x2d8b activity...", file=sys.stderr)
    activity = fetch_all_activity(WALLET)
    print(f"  Total activity records: {len(activity)}", file=sys.stderr)

    # Step 2: Filter BTC 5-min BUY trades
    btc_buys = [
        r for r in activity
        if r.get("type") == "TRADE"
        and r.get("side") == "BUY"
        and "btc-updown-5m" in r.get("slug", "")
    ]
    print(f"  BTC 5-min BUY trades: {len(btc_buys)}", file=sys.stderr)

    # Group by conditionId
    by_condition = defaultdict(list)
    for t in btc_buys:
        by_condition[t["conditionId"]].append(t)

    # Find conditions with both-side buys
    both_side_conditions = {}
    for cid, trades in by_condition.items():
        outcomes = set(t["outcome"] for t in trades)
        if "Up" in outcomes and "Down" in outcomes:
            both_side_conditions[cid] = trades

    print(f"  ConditionIds with both-side buys: {len(both_side_conditions)}", file=sys.stderr)

    # Step 3: Compute 0x2d8b's VWAP per side per condition
    results = []

    for cid, trades in sorted(both_side_conditions.items(), key=lambda x: x[1][0].get("timestamp", 0)):
        slug = trades[0].get("slug", "")
        title = trades[0].get("title", "")
        timestamp = trades[0].get("timestamp", 0)

        up_trades = [(t["price"], t["size"]) for t in trades if t["outcome"] == "Up"]
        down_trades = [(t["price"], t["size"]) for t in trades if t["outcome"] == "Down"]

        up_vwap, up_size = compute_vwap(up_trades)
        down_vwap, down_size = compute_vwap(down_trades)

        if up_vwap is None or down_vwap is None:
            continue

        wallet_combined = up_vwap + down_vwap
        wallet_margin = 1.0 - wallet_combined

        results.append({
            "conditionId": cid,
            "slug": slug,
            "title": title,
            "timestamp": timestamp,
            "wallet_up_vwap": up_vwap,
            "wallet_down_vwap": down_vwap,
            "wallet_up_size": up_size,
            "wallet_down_size": down_size,
            "wallet_combined": wallet_combined,
            "wallet_margin": wallet_margin,
        })

    print(f"\n[2] Both-side pair trades to analyze: {len(results)}", file=sys.stderr)

    # Step 4: For each conditionId, fetch all market trades
    print("\n[3] Fetching market trades for each conditionId...", file=sys.stderr)
    for i, r in enumerate(results):
        cid = r["conditionId"]
        print(f"  [{i+1}/{len(results)}] {r['slug']}...", file=sys.stderr)

        all_trades = fetch_all_trades(cid)
        if not all_trades:
            r["market_trades_count"] = 0
            continue

        r["market_trades_count"] = len(all_trades)

        # Separate by outcome
        market_up = [(t["price"], float(t["size"])) for t in all_trades
                     if t.get("side") == "BUY" and t.get("outcome") == "Up"]
        market_down = [(t["price"], float(t["size"])) for t in all_trades
                       if t.get("side") == "BUY" and t.get("outcome") == "Down"]

        # Also get ALL trades (buy+sell) for true market VWAP
        # For pair-arb, we care about BUY prices (what buyers pay)
        market_up_vwap, market_up_size = compute_vwap(market_up)
        market_down_vwap, market_down_size = compute_vwap(market_down)

        r["market_up_vwap"] = market_up_vwap
        r["market_down_vwap"] = market_down_vwap
        r["market_up_size"] = market_up_size
        r["market_down_size"] = market_down_size

        if market_up_vwap and market_down_vwap:
            r["market_combined"] = market_up_vwap + market_down_vwap
            r["market_margin"] = 1.0 - r["market_combined"]
        else:
            r["market_combined"] = None
            r["market_margin"] = None

        time.sleep(0.3)

    # Step 5: Analysis
    print("\n[4] Computing analysis...", file=sys.stderr)

    valid = [r for r in results if r.get("market_margin") is not None and r.get("market_trades_count", 0) > 50]
    print(f"  Valid periods (both VWAPs + >50 trades): {len(valid)}", file=sys.stderr)

    # Output structured results
    output = {
        "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_activity_records": len(activity),
        "btc_5m_buys": len(btc_buys),
        "both_side_conditions": len(both_side_conditions),
        "valid_periods": len(valid),
        "periods": [],
    }

    wallet_margins = []
    market_margins = []
    price_advantages_up = []
    price_advantages_down = []

    for r in valid:
        period = {
            "slug": r["slug"],
            "timestamp": r["timestamp"],
            "wallet_up_vwap": round(r["wallet_up_vwap"], 6),
            "wallet_down_vwap": round(r["wallet_down_vwap"], 6),
            "wallet_combined": round(r["wallet_combined"], 6),
            "wallet_margin_pct": round(r["wallet_margin"] * 100, 4),
            "market_up_vwap": round(r["market_up_vwap"], 6),
            "market_down_vwap": round(r["market_down_vwap"], 6),
            "market_combined": round(r["market_combined"], 6),
            "market_margin_pct": round(r["market_margin"] * 100, 4),
            "wallet_trades": int(r["wallet_up_size"] + r["wallet_down_size"]),
            "market_trades": r["market_trades_count"],
        }

        # Price advantage: negative means 0x2d8b got BETTER (cheaper) prices
        if r["market_up_vwap"]:
            adv_up = r["wallet_up_vwap"] - r["market_up_vwap"]
            period["price_adv_up_pp"] = round(adv_up * 100, 4)
            price_advantages_up.append(adv_up)

        if r["market_down_vwap"]:
            adv_down = r["wallet_down_vwap"] - r["market_down_vwap"]
            period["price_adv_down_pp"] = round(adv_down * 100, 4)
            price_advantages_down.append(adv_down)

        wallet_margins.append(r["wallet_margin"])
        market_margins.append(r["market_margin"])

        output["periods"].append(period)

    # Summary stats
    if valid:
        avg_wallet_margin = sum(wallet_margins) / len(wallet_margins)
        avg_market_margin = sum(market_margins) / len(market_margins)
        avg_adv_up = sum(price_advantages_up) / len(price_advantages_up) if price_advantages_up else 0
        avg_adv_down = sum(price_advantages_down) / len(price_advantages_down) if price_advantages_down else 0

        # Taker fee impact: at p=0.50, taker fee = 1.56%. For pair (buy up + buy down),
        # fee applies to both legs. Fee = price * fee_rate for each leg.
        # Effective fee on combined cost: varies by price levels
        taker_fee_rate = 0.0156  # at p=0.50 midpoint approximation
        # More precise: fee = min(p, 1-p) * 0.0219 * 2 for the pair
        # At typical prices ~0.45-0.55, fee ~ 0.45 * 0.0219 * 2 = 1.97% of notional

        # Median
        wallet_margins_sorted = sorted(wallet_margins)
        market_margins_sorted = sorted(market_margins)
        n = len(wallet_margins_sorted)
        median_wallet = wallet_margins_sorted[n // 2] if n % 2 else (wallet_margins_sorted[n//2-1] + wallet_margins_sorted[n//2]) / 2
        median_market = market_margins_sorted[n // 2] if n % 2 else (market_margins_sorted[n//2-1] + market_margins_sorted[n//2]) / 2

        # Std dev
        import math
        std_wallet = math.sqrt(sum((m - avg_wallet_margin)**2 for m in wallet_margins) / len(wallet_margins))
        std_market = math.sqrt(sum((m - avg_market_margin)**2 for m in market_margins) / len(market_margins))

        # Count profitable at various fee levels
        taker_profitable_0 = sum(1 for m in market_margins if m > 0)
        taker_profitable_1 = sum(1 for m in market_margins if m > 0.01)
        taker_profitable_2 = sum(1 for m in market_margins if m > 0.02)
        taker_profitable_3 = sum(1 for m in market_margins if m > 0.03)

        # For taker bot: need combined < 1 - 2*fee
        # fee per leg at price p: min(p, 1-p) * 0.0219
        # For simplicity, estimate total taker fee on pair
        taker_fees_per_period = []
        for r in valid:
            up_fee = min(r["market_up_vwap"], 1 - r["market_up_vwap"]) * 0.0219
            down_fee = min(r["market_down_vwap"], 1 - r["market_down_vwap"]) * 0.0219
            total_fee = up_fee + down_fee
            taker_fees_per_period.append(total_fee)

        avg_taker_fee = sum(taker_fees_per_period) / len(taker_fees_per_period)
        taker_net_margins = [m - f for m, f in zip(market_margins, taker_fees_per_period)]
        avg_taker_net = sum(taker_net_margins) / len(taker_net_margins)
        taker_positive = sum(1 for m in taker_net_margins if m > 0)

        output["summary"] = {
            "n_periods": len(valid),
            "wallet_margin_mean_pct": round(avg_wallet_margin * 100, 4),
            "wallet_margin_median_pct": round(median_wallet * 100, 4),
            "wallet_margin_std_pct": round(std_wallet * 100, 4),
            "market_margin_mean_pct": round(avg_market_margin * 100, 4),
            "market_margin_median_pct": round(median_market * 100, 4),
            "market_margin_std_pct": round(std_market * 100, 4),
            "margin_gap_pp": round((avg_wallet_margin - avg_market_margin) * 100, 4),
            "avg_price_advantage_up_pp": round(avg_adv_up * 100, 4),
            "avg_price_advantage_down_pp": round(avg_adv_down * 100, 4),
            "avg_taker_fee_pct": round(avg_taker_fee * 100, 4),
            "taker_net_margin_mean_pct": round(avg_taker_net * 100, 4),
            "taker_profitable_count": taker_positive,
            "taker_profitable_pct": round(taker_positive / len(valid) * 100, 1),
            "market_positive_raw_count": taker_profitable_0,
            "market_positive_gt1pct": taker_profitable_1,
            "market_positive_gt2pct": taker_profitable_2,
            "market_positive_gt3pct": taker_profitable_3,
        }

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
