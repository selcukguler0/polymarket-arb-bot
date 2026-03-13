#!/usr/bin/env python3
"""
H7 Maker vs Taker Execution Gap Analysis v2

Key improvement: filter market trades to early-period window only (first 60s),
since pair-arb is only viable near period-open before prices diverge.

Also uses offset-based pagination to get >500 activity records.
"""

import json
import math
import time
import urllib.request
import sys
from collections import defaultdict
from datetime import datetime, timezone

WALLET = "0x2d8b401d2f0e6937afebf18e19e11ca568a5260a"
DATA_API = "https://data-api.polymarket.com"
PERIOD_DURATION = 300  # 5 minutes
EARLY_WINDOW_SEC = 120  # First 2 minutes = pair-arb window


def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "polymarket-h7-analysis/0.2")
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
            else:
                print(f"  FAILED: {url[:80]}... -> {e}", file=sys.stderr)
                return None


def fetch_all_activity(wallet):
    """Fetch all activity with pagination."""
    all_records = []
    offset = 0
    limit = 500
    while True:
        url = f"{DATA_API}/activity?user={wallet}&limit={limit}&offset={offset}"
        data = fetch_json(url)
        if not data or len(data) == 0:
            break
        all_records.extend(data)
        print(f"  Activity: offset={offset}, batch={len(data)}, total={len(all_records)}", file=sys.stderr)
        if len(data) < limit:
            break
        offset += limit
        time.sleep(0.5)
    return all_records


def fetch_market_trades(condition_id):
    """Fetch all trades for a market with full pagination."""
    all_trades = []
    offset = 0
    limit = 500
    max_pages = 20  # safety limit
    page = 0
    while page < max_pages:
        url = f"{DATA_API}/trades?market={condition_id}&limit={limit}&offset={offset}"
        data = fetch_json(url)
        if not data or len(data) == 0:
            break
        all_trades.extend(data)
        if len(data) < limit:
            break
        offset += limit
        page += 1
        time.sleep(0.3)
    return all_trades


def vwap(trades_list):
    """Compute VWAP from [(price, size), ...]"""
    if not trades_list:
        return None, 0
    total_cost = sum(p * s for p, s in trades_list)
    total_size = sum(s for _, s in trades_list)
    if total_size == 0:
        return None, 0
    return total_cost / total_size, total_size


def extract_epoch(slug):
    """Extract epoch timestamp from slug like btc-updown-5m-1772993700"""
    parts = slug.split("-")
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return None


def main():
    print("=" * 70, file=sys.stderr)
    print("H7 MAKER VS TAKER EXECUTION GAP ANALYSIS v2", file=sys.stderr)
    print("=" * 70, file=sys.stderr)

    # Step 1: Fetch all activity
    print("\n[1] Fetching 0x2d8b activity...", file=sys.stderr)
    activity = fetch_all_activity(WALLET)
    print(f"  Total records: {len(activity)}", file=sys.stderr)

    # Filter BTC 5-min BUY trades
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
    both_side = {}
    for cid, trades in by_condition.items():
        outcomes = set(t["outcome"] for t in trades)
        if "Up" in outcomes and "Down" in outcomes:
            both_side[cid] = trades

    print(f"  Conditions with both-side buys: {len(both_side)}", file=sys.stderr)

    # Step 2: Compute 0x2d8b's VWAP per side, and determine period epoch
    periods = []
    for cid, trades in sorted(both_side.items(), key=lambda x: x[1][0].get("timestamp", 0)):
        slug = trades[0].get("slug", "")
        epoch = extract_epoch(slug)
        if not epoch:
            continue

        up_trades = [(t["price"], t["size"]) for t in trades if t["outcome"] == "Up"]
        down_trades = [(t["price"], t["size"]) for t in trades if t["outcome"] == "Down"]

        up_vwap, up_size = vwap(up_trades)
        down_vwap, down_size = vwap(down_trades)

        if up_vwap is None or down_vwap is None:
            continue

        # 0x2d8b's trade timestamps
        wallet_timestamps = [t["timestamp"] for t in trades]
        wallet_first = min(wallet_timestamps)
        wallet_last = max(wallet_timestamps)
        wallet_offset_from_open = wallet_first - epoch

        periods.append({
            "conditionId": cid,
            "slug": slug,
            "epoch": epoch,
            "wallet_up_vwap": up_vwap,
            "wallet_down_vwap": down_vwap,
            "wallet_up_size": up_size,
            "wallet_down_size": down_size,
            "wallet_combined": up_vwap + down_vwap,
            "wallet_margin": 1.0 - (up_vwap + down_vwap),
            "wallet_first_ts": wallet_first,
            "wallet_last_ts": wallet_last,
            "wallet_offset_sec": wallet_offset_from_open,
            "wallet_n_fills": len(trades),
        })

    print(f"\n[2] Pair-trade periods: {len(periods)}", file=sys.stderr)

    # Step 3: Fetch market trades and compute early-window VWAP
    print("\n[3] Fetching market trades...", file=sys.stderr)
    for i, p in enumerate(periods):
        cid = p["conditionId"]
        epoch = p["epoch"]
        print(f"  [{i+1}/{len(periods)}] {p['slug']}...", file=sys.stderr)

        all_trades = fetch_market_trades(cid)
        if not all_trades:
            print(f"    No trades returned", file=sys.stderr)
            p["market_total_trades"] = 0
            continue

        p["market_total_trades"] = len(all_trades)

        # Filter to early window: epoch to epoch + EARLY_WINDOW_SEC
        early_cutoff = epoch + EARLY_WINDOW_SEC
        early_trades = [t for t in all_trades if t.get("timestamp", 0) <= early_cutoff]
        p["market_early_trades"] = len(early_trades)

        # Also: all BUY trades in early window (excluding 0x2d8b)
        early_buys_excl = [
            t for t in early_trades
            if t.get("side") == "BUY"
            and t.get("proxyWallet", "") != WALLET
        ]
        early_buys_all = [
            t for t in early_trades
            if t.get("side") == "BUY"
        ]

        # Market VWAP (all buyers, early window)
        mkt_up_all = [(t["price"], float(t["size"])) for t in early_buys_all if t.get("outcome") == "Up"]
        mkt_down_all = [(t["price"], float(t["size"])) for t in early_buys_all if t.get("outcome") == "Down"]

        p["mkt_up_vwap_all"], p["mkt_up_size_all"] = vwap(mkt_up_all)
        p["mkt_down_vwap_all"], p["mkt_down_size_all"] = vwap(mkt_down_all)

        # Market VWAP (excluding 0x2d8b, early window) = taker VWAP proxy
        mkt_up_excl = [(t["price"], float(t["size"])) for t in early_buys_excl if t.get("outcome") == "Up"]
        mkt_down_excl = [(t["price"], float(t["size"])) for t in early_buys_excl if t.get("outcome") == "Down"]

        p["mkt_up_vwap_excl"], p["mkt_up_size_excl"] = vwap(mkt_up_excl)
        p["mkt_down_vwap_excl"], p["mkt_down_size_excl"] = vwap(mkt_down_excl)

        # Combined VWAPs
        if p["mkt_up_vwap_all"] and p["mkt_down_vwap_all"]:
            p["mkt_combined_all"] = p["mkt_up_vwap_all"] + p["mkt_down_vwap_all"]
            p["mkt_margin_all"] = 1.0 - p["mkt_combined_all"]

        if p["mkt_up_vwap_excl"] and p["mkt_down_vwap_excl"]:
            p["mkt_combined_excl"] = p["mkt_up_vwap_excl"] + p["mkt_down_vwap_excl"]
            p["mkt_margin_excl"] = 1.0 - p["mkt_combined_excl"]

        # Wallet's trades from the /trades endpoint (not /activity) for apples-to-apples
        wallet_trades_in_market = [
            t for t in early_trades
            if t.get("proxyWallet", "") == WALLET and t.get("side") == "BUY"
        ]
        w_up = [(t["price"], float(t["size"])) for t in wallet_trades_in_market if t.get("outcome") == "Up"]
        w_dn = [(t["price"], float(t["size"])) for t in wallet_trades_in_market if t.get("outcome") == "Down"]
        p["wallet_trades_in_market_data"] = len(wallet_trades_in_market)
        p["wallet_up_vwap_mkt"], _ = vwap(w_up)
        p["wallet_down_vwap_mkt"], _ = vwap(w_dn)

        time.sleep(0.3)

    # Step 4: Compute analysis
    print("\n[4] Computing summary...", file=sys.stderr)

    # Filter to periods with valid market data
    valid = [p for p in periods
             if p.get("mkt_margin_excl") is not None
             and p.get("market_early_trades", 0) > 20]

    print(f"  Valid periods: {len(valid)}", file=sys.stderr)

    # Detailed per-period output
    output_periods = []
    wallet_margins = []
    mkt_margins_all = []
    mkt_margins_excl = []
    price_advs = []

    for p in valid:
        wallet_m = p["wallet_margin"]
        mkt_m_all = p.get("mkt_margin_all", 0)
        mkt_m_excl = p.get("mkt_margin_excl", 0)

        # Price advantage: wallet VWAP vs market VWAP (excl wallet)
        # Negative = wallet got cheaper prices (better for buyer)
        adv_up = (p["wallet_up_vwap"] - p.get("mkt_up_vwap_excl", p["wallet_up_vwap"]))
        adv_dn = (p["wallet_down_vwap"] - p.get("mkt_down_vwap_excl", p["wallet_down_vwap"]))
        combined_adv = adv_up + adv_dn  # negative = wallet pays less total

        # Taker fee estimate for the market VWAP prices
        up_price = p.get("mkt_up_vwap_excl", 0.5)
        dn_price = p.get("mkt_down_vwap_excl", 0.5)
        taker_fee = min(up_price, 1 - up_price) * 0.0219 + min(dn_price, 1 - dn_price) * 0.0219
        taker_net = mkt_m_excl - taker_fee

        row = {
            "slug": p["slug"],
            "epoch": p["epoch"],
            "wallet_margin_pct": round(wallet_m * 100, 3),
            "mkt_margin_all_pct": round(mkt_m_all * 100, 3),
            "mkt_margin_excl_pct": round(mkt_m_excl * 100, 3),
            "price_adv_combined_pp": round(combined_adv * 100, 3),
            "price_adv_up_pp": round(adv_up * 100, 3),
            "price_adv_dn_pp": round(adv_dn * 100, 3),
            "taker_fee_pct": round(taker_fee * 100, 3),
            "taker_net_pct": round(taker_net * 100, 3),
            "wallet_offset_sec": p["wallet_offset_sec"],
            "wallet_n_fills_activity": p["wallet_n_fills"],
            "wallet_n_fills_trades": p.get("wallet_trades_in_market_data", 0),
            "mkt_early_trades": p.get("market_early_trades", 0),
            "mkt_total_trades": p.get("market_total_trades", 0),
        }
        output_periods.append(row)

        wallet_margins.append(wallet_m)
        mkt_margins_all.append(mkt_m_all)
        mkt_margins_excl.append(mkt_m_excl)
        price_advs.append(combined_adv)

    # Summary statistics
    n = len(valid)
    if n == 0:
        print("No valid periods found!", file=sys.stderr)
        return

    def stats(arr):
        mean = sum(arr) / len(arr)
        med = sorted(arr)[len(arr) // 2]
        std = math.sqrt(sum((x - mean)**2 for x in arr) / len(arr))
        return mean, med, std

    wm_mean, wm_med, wm_std = stats(wallet_margins)
    mm_mean, mm_med, mm_std = stats(mkt_margins_excl)
    pa_mean, pa_med, pa_std = stats(price_advs)

    # Taker profitability analysis
    taker_nets = [p["taker_net_pct"] for p in output_periods]
    taker_positive = sum(1 for t in taker_nets if t > 0)

    # Fee decomposition: of the wallet-vs-market gap, how much is fee vs price?
    # Total gap = wallet_margin - market_margin_excl
    # This gap = price_advantage + fee_savings
    # price_advantage = -(combined_adv)  (negative adv = wallet pays less)
    # fee_savings = taker_fee (wallet pays 0, market pays taker_fee)
    avg_taker_fee = sum(p["taker_fee_pct"] for p in output_periods) / n

    gap = wm_mean - mm_mean
    price_component = -pa_mean  # positive means wallet gets better prices
    fee_component = gap - price_component  # remainder attributable to fees

    summary = {
        "n_periods": n,
        "early_window_sec": EARLY_WINDOW_SEC,
        "wallet_margin": {
            "mean_pct": round(wm_mean * 100, 3),
            "median_pct": round(wm_med * 100, 3),
            "std_pct": round(wm_std * 100, 3),
        },
        "market_vwap_margin_excl_wallet": {
            "mean_pct": round(mm_mean * 100, 3),
            "median_pct": round(mm_med * 100, 3),
            "std_pct": round(mm_std * 100, 3),
        },
        "total_gap_pp": round(gap * 100, 3),
        "price_advantage_pp": round(price_component * 100, 3),
        "fee_component_pp": round(fee_component * 100, 3),
        "avg_taker_fee_pct": round(avg_taker_fee, 3),
        "taker_net_margin_mean_pct": round(sum(taker_nets) / n, 3),
        "taker_profitable_count": taker_positive,
        "taker_profitable_pct": round(taker_positive / n * 100, 1),
    }

    output = {
        "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
        "wallet": WALLET,
        "total_activity": len(activity),
        "btc_5m_buys": len(btc_buys),
        "both_side_conditions": len(both_side),
        "summary": summary,
        "periods": output_periods,
    }

    print(json.dumps(output, indent=2))
    print(f"\nDone. {n} periods analyzed.", file=sys.stderr)


if __name__ == "__main__":
    main()
