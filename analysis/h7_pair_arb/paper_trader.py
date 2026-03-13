"""H7 pair-arb paper trader — observes BTC 5-min markets and logs simulated pair margins.

Strategy: simulate posting maker BUY orders on both Up and Down outcomes at period open.
After each period resolves, fetch actual trades and compute what pair margin a maker would
have achieved.

Run: python3 -m analysis.h7_pair_arb.paper_trader
"""

import csv
import json
import os
import time
import urllib.request
import urllib.error
from collections import defaultdict
from datetime import datetime, timezone

from .config import (
    COIN,
    DURATION_MIN,
    LOG_DIR,
    SAMPLE_OFFSETS_SEC,
    TARGET_MAX_COMBINED_ASK,
    REQUEST_TIMEOUT_SEC,
    MAX_RETRIES,
    RETRY_BACKOFF_SEC,
)
from .market_predictor import compute_slug, fetch_market, next_period_epoch, current_period_epoch
from .book_sampler import compute_pair_margin, sample_book


def _ensure_log_dir():
    os.makedirs(LOG_DIR, exist_ok=True)


def _csv_path() -> str:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return os.path.join(LOG_DIR, f"paper_trade_{date_str}.csv")


CSV_HEADERS = [
    "timestamp",
    "slug",
    "conditionId",
    "mode",
    "sample_offset_sec",
    "ask_up",
    "ask_down",
    "pair_margin_pct",
    "bid_depth_up",
    "ask_depth_up",
    "bid_depth_down",
    "ask_depth_down",
    "trade_count",
    "tradeable",
]


def _write_row(row: dict):
    path = _csv_path()
    file_exists = os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def _fetch_trades(condition_id: str) -> list:
    """Fetch recent trades for a market via data-api."""
    url = f"https://data-api.polymarket.com/trades?market={condition_id}&limit=500"
    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(url, headers={
                "Accept": "application/json",
                "User-Agent": "polymarket-h7-paper/0.1",
            })
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
                data = json.loads(resp.read())
            if isinstance(data, list):
                return data
            return []
        except (urllib.error.URLError, json.JSONDecodeError, TimeoutError):
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
    return []


def _analyze_trades(trades: list) -> dict | None:
    """Analyze trades to compute simulated maker pair margin.

    Uses matched-pair approach: sort each side's BUY prices ascending,
    match the cheapest N from each side (N = min of both sides).
    This simulates a maker who posts competitive bids and gets the best fills.
    """
    up_buys = sorted([float(t.get("price", 0)) for t in trades
                       if t.get("side") == "BUY" and str(t.get("outcomeIndex")) == "0"])
    down_buys = sorted([float(t.get("price", 0)) for t in trades
                         if t.get("side") == "BUY" and str(t.get("outcomeIndex")) == "1"])

    if not up_buys or not down_buys:
        return None

    # Matched pairs: cheapest N from each side
    n = min(len(up_buys), len(down_buys))
    matched_up = up_buys[:n]
    matched_down = down_buys[:n]
    avg_up = sum(matched_up) / n
    avg_down = sum(matched_down) / n
    matched_margin = 1.0 - avg_up - avg_down

    # Also compute market-wide average for comparison
    all_avg_up = sum(up_buys) / len(up_buys)
    all_avg_down = sum(down_buys) / len(down_buys)
    mkt_margin = 1.0 - all_avg_up - all_avg_down

    return {
        "avg_up": round(avg_up, 4),
        "avg_down": round(avg_down, 4),
        "margin_pct": round(matched_margin * 100, 2),
        "mkt_margin_pct": round(mkt_margin * 100, 2),
        "matched_pairs": n,
        "up_trades": len(up_buys),
        "down_trades": len(down_buys),
        "total_trades": len(trades),
    }


def _sample_book_and_log(market: dict, offset_sec: int):
    """Sample both books and log a row (book snapshot mode)."""
    book_up = sample_book(market["token_up"])
    book_down = sample_book(market["token_down"])

    if not book_up or not book_down:
        print(f"    [T+{offset_sec}s] Book fetch failed")
        return

    margin = compute_pair_margin(book_up["best_ask"], book_down["best_ask"])
    tradeable = margin > 0 and (book_up["best_ask"] + book_down["best_ask"]) < TARGET_MAX_COMBINED_ASK

    row = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "slug": market["slug"],
        "conditionId": market["conditionId"][:20],
        "mode": "book_snapshot",
        "sample_offset_sec": offset_sec,
        "ask_up": book_up["best_ask"],
        "ask_down": book_down["best_ask"],
        "pair_margin_pct": round(margin * 100, 4),
        "bid_depth_up": book_up["bid_depth"],
        "ask_depth_up": book_up["ask_depth"],
        "bid_depth_down": book_down["bid_depth"],
        "ask_depth_down": book_down["ask_depth"],
        "trade_count": 0,
        "tradeable": tradeable,
    }
    _write_row(row)

    symbol = "+" if margin > 0 else "-"
    trade_tag = " [TRADE]" if tradeable else ""
    print(
        f"    [T+{offset_sec:>2}s] ask_up={book_up['best_ask']:.2f} "
        f"ask_down={book_down['best_ask']:.2f} "
        f"margin={margin*100:+.2f}%{trade_tag}"
    )


def _log_trade_analysis(market: dict, analysis: dict):
    """Log retrospective trade analysis row."""
    row = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "slug": market["slug"],
        "conditionId": market["conditionId"][:20],
        "mode": "trade_retro",
        "sample_offset_sec": -1,
        "ask_up": analysis["avg_up"],
        "ask_down": analysis["avg_down"],
        "pair_margin_pct": analysis["margin_pct"],
        "bid_depth_up": 0,
        "ask_depth_up": 0,
        "bid_depth_down": 0,
        "ask_depth_down": 0,
        "trade_count": analysis["total_trades"],
        "tradeable": analysis["margin_pct"] > 0,
    }
    _write_row(row)


def run_one_period() -> dict | None:
    """Run one period cycle (retro-only). Returns trade analysis dict or None."""
    epoch = current_period_epoch(COIN, DURATION_MIN)
    slug = compute_slug(COIN, epoch, DURATION_MIN)
    period_time = datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%H:%M:%S")

    # Wait until period ends + 60s for trades to settle
    period_end = epoch + DURATION_MIN * 60
    wait_for_trades = period_end + 60
    now = time.time()
    if now < wait_for_trades:
        remaining = wait_for_trades - now
        print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
              f"Period {period_time}Z ({slug}). Waiting {remaining:.0f}s for trades to settle...")
        time.sleep(wait_for_trades - now)

    # Fetch market details
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Fetching market: {slug}")
    market = fetch_market(slug)
    if not market:
        print(f"  Market not found for {slug}, skipping period")
        return None

    print(f"  Found: {market['question']}")

    # Fetch and analyze trades retrospectively
    print(f"  Fetching trades for {slug}...")
    trades = _fetch_trades(market["conditionId"])
    if not trades:
        print(f"  No trades found for {slug}")
        return None

    analysis = _analyze_trades(trades)
    if not analysis:
        print(f"  Could not compute pair margin (missing up or down trades)")
        return None

    _log_trade_analysis(market, analysis)

    tradeable = analysis["margin_pct"] > 0
    tag = " [PROFITABLE]" if tradeable else " [LOSS]"
    print(
        f"  RETRO: avg_up={analysis['avg_up']:.4f} avg_down={analysis['avg_down']:.4f} "
        f"margin={analysis['margin_pct']:+.2f}% "
        f"({analysis['up_trades']}u/{analysis['down_trades']}d trades){tag}"
    )
    return analysis


def main():
    _ensure_log_dir()
    print("=" * 60)
    print("H7 Pair-Arb Paper Trader (Retrospective Mode)")
    print(f"Coin: {COIN.upper()}, Duration: {DURATION_MIN}min")
    print(f"Book sample offsets: {SAMPLE_OFFSETS_SEC}s after period start")
    print(f"Log dir: {LOG_DIR}")
    print("=" * 60)

    periods_run = 0
    margins = []

    try:
        while True:
            periods_run += 1
            analysis = run_one_period()

            if analysis:
                margins.append(analysis["margin_pct"])
                n = len(margins)
                avg = sum(margins) / n
                neg = sum(1 for m in margins if m < 0)
                sorted_m = sorted(margins)
                median = sorted_m[n // 2]
                print(
                    f"\n  Session: {n} periods | "
                    f"mean={avg:+.2f}% | median={median:+.2f}% | "
                    f"negative={neg}/{n} ({neg/n*100:.0f}%)"
                )

    except KeyboardInterrupt:
        print(f"\n\nStopped after {periods_run} periods ({len(margins)} with data)")
        if margins:
            n = len(margins)
            print(f"Final: mean={sum(margins)/n:+.2f}%, "
                  f"negative={sum(1 for m in margins if m < 0)}/{n}")
        print(f"Logs: {_csv_path()}")


if __name__ == "__main__":
    main()
