"""
HTTP client for Polymarket and Binance APIs.
Uses /positions (real P&L) and /closed-positions (resolved P&L) instead of
estimating from Binance prices. Uses /activity with time-windowing for
trade data beyond the /trades 4K hard cap.

Stdlib only (urllib, json, ssl). Python 3.10+.
"""
from __future__ import annotations

import json
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from typing import Optional

# SSL context (macOS certifi workaround)
try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()
    SSL_CTX.check_hostname = False
    SSL_CTX.verify_mode = ssl.CERT_NONE

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
BINANCE_API = "https://api.binance.com"

RATE_LIMIT_SLEEP = 0.1  # 100ms between requests


def api_get(url: str, params: dict | None = None) -> dict | list | None:
    """GET with rate limiting, 429 retry, graceful error handling."""
    if params:
        query = urllib.parse.urlencode(params, doseq=True)
        url = f"{url}?{query}"

    req = urllib.request.Request(url, headers={"User-Agent": "polymarket-analyzer/2.0"})
    try:
        with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as resp:
            data = json.loads(resp.read().decode())
            time.sleep(RATE_LIMIT_SLEEP)
            return data
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print("  Rate limited, waiting 5s...", file=sys.stderr)
            time.sleep(5)
            return api_get(url, params)
        # 400 at high offsets is expected — not an error
        if e.code != 400:
            print(f"  HTTP {e.code} for {url[:120]}", file=sys.stderr)
        return None
    except urllib.error.URLError as e:
        print(f"  URL error: {e}", file=sys.stderr)
        return None


# ── Wallet resolution ─────────────────────────────────────────────────

def resolve_proxy_wallet(address: str) -> tuple[str, str]:
    """Resolve EOA to proxy wallet. Returns (proxy_address, username)."""
    print(f"Resolving wallet for {address}...")
    try:
        profile = api_get(f"{GAMMA_API}/public-profile", {"address": address})
        if isinstance(profile, dict) and profile.get("proxyWallet"):
            proxy = profile["proxyWallet"]
            username = profile.get("pseudonym", "N/A")
            print(f"  Found proxy wallet: {proxy}")
            print(f"  Username: {username}")
            return proxy, username
    except Exception as e:
        print(f"  Could not resolve proxy wallet: {e}", file=sys.stderr)
    return address, "Unknown"


# ── Positions (REAL P&L) ──────────────────────────────────────────────
# These endpoints return actual P&L computed by Polymarket, not estimates.

def fetch_positions(user: str) -> list[dict]:
    """
    Fetch ALL current open positions with real P&L fields.
    Returns list with: cashPnl, realizedPnl, avgPrice, initialValue,
    currentValue, totalBought, size, curPrice, outcome, conditionId, title, etc.
    No pagination limit for typical wallets.
    """
    print("  Fetching open positions (with P&L)...")
    all_positions: list[dict] = []
    offset = 0
    limit = 500

    while True:
        params: dict = {
            "user": user, "limit": limit, "offset": offset,
            "sizeThreshold": 0,
        }
        positions = api_get(f"{DATA_API}/positions", params)
        if not positions:
            break
        all_positions.extend(positions)
        if len(positions) < limit:
            break
        offset += limit
        if offset > 10000:
            break

    print(f"    Got {len(all_positions)} open positions")
    return all_positions


def fetch_closed_positions(user: str, start_ts: int | None = None) -> list[dict]:
    """
    Fetch closed/resolved positions with realizedPnl.
    Offset limit: 100,000. Limit per page: 50.
    Each record has: realizedPnl, avgPrice, totalBought, curPrice, conditionId, etc.

    NOTE: The API's startTime parameter is IGNORED (returns identical results
    with or without it). We sort by TIMESTAMP DESC and filter client-side,
    stopping pagination early when all results on a page are before start_ts.
    """
    all_positions: list[dict] = []
    offset = 0
    limit = 50  # API max for closed-positions

    while True:
        params: dict = {
            "user": user,
            "limit": limit,
            "offset": offset,
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
        }

        print(f"  Fetching closed positions offset={offset}...")
        positions = api_get(f"{DATA_API}/closed-positions", params)

        if not positions:
            break

        # Client-side time filtering (API ignores startTime param)
        if start_ts:
            page_in_range = 0
            for p in positions:
                ts = p.get("timestamp")
                if ts is not None and int(ts) >= start_ts:
                    all_positions.append(p)
                    page_in_range += 1
            # Sorted DESC: if no results on this page are in range, we're done
            if page_in_range == 0:
                print(f"    All results on page below start_ts, stopping")
                break
        else:
            all_positions.extend(positions)

        if len(positions) < limit:
            break
        offset += limit
        if offset > 100000:
            print("  Hit closed-positions pagination limit (100K)", file=sys.stderr)
            break

    print(f"    Got {len(all_positions)} closed positions (after time filter)")
    return all_positions


# ── Trades (capped at ~4K) ────────────────────────────────────────────

def fetch_all_trades(user: str, start_ts: int | None = None,
                     end_ts: int | None = None) -> list[dict]:
    """
    Fetch trades via /trades endpoint.
    CAUTION: Hard-capped at ~4,000 results (offset max 3000, limit max 1000).
    For high-volume wallets this only covers ~15 hours of history.
    Use fetch_activity_trades() for time-windowed pagination beyond 4K.
    """
    all_trades: list[dict] = []
    offset = 0
    limit = 1000  # max per page

    while True:
        params: dict = {"user": user, "limit": limit, "offset": offset}
        print(f"  Fetching trades offset={offset}...")
        trades = api_get(f"{DATA_API}/trades", params)

        if not trades:
            break

        for t in trades:
            ts = _parse_ts(t.get("timestamp", ""))
            if ts is None:
                continue
            ts_epoch = int(ts.timestamp())
            if start_ts and ts_epoch < start_ts:
                continue
            if end_ts and ts_epoch > end_ts:
                continue
            all_trades.append(t)

        if len(trades) < limit:
            break
        offset += limit
        if offset > 3000:  # Hard API limit: offset > 3000 returns 400
            break

    return all_trades


# ── Activity (time-windowed, unlimited pagination) ────────────────────

def fetch_activity_trades(user: str, start_ts: int, end_ts: int) -> list[dict]:
    """
    Fetch TRADE activity using time-windowed pagination.
    Bypasses the offset limit by advancing the start timestamp.
    Returns activity records with: timestamp, side, outcome, size, price,
    usdcSize, conditionId, title, transactionHash, asset.
    """
    all_activity: list[dict] = []
    current_start = start_ts
    window_count = 0

    while current_start < end_ts:
        window_count += 1
        offset = 0
        limit = 500
        window_trades: list[dict] = []
        last_ts = current_start

        while True:
            params: dict = {
                "user": user,
                "limit": limit,
                "offset": offset,
                "type": "TRADE",
                "sortBy": "TIMESTAMP",
                "sortDirection": "ASC",
                "start": current_start,
                "end": end_ts,
            }

            print(f"  Fetching activity trades window={window_count} offset={offset} "
                  f"start={datetime.fromtimestamp(current_start, tz=timezone.utc).strftime('%m/%d %H:%M')}...")
            activity = api_get(f"{DATA_API}/activity", params)

            if not activity:
                break
            window_trades.extend(activity)

            # Track latest timestamp for next window
            for a in activity:
                ts = a.get("timestamp")
                if ts and isinstance(ts, (int, float)) and ts > last_ts:
                    last_ts = int(ts)

            if len(activity) < limit:
                break
            offset += limit
            if offset > 3000:  # Hit offset limit, advance time window
                break

        all_activity.extend(window_trades)

        if not window_trades:
            break

        # Advance to 1 second after last seen timestamp
        if last_ts <= current_start:
            break  # No progress, stop
        current_start = last_ts + 1

    return all_activity


def fetch_all_activity(user: str, activity_types: list[str],
                       start_ts: int | None = None,
                       end_ts: int | None = None) -> list[dict]:
    """Fetch activity for given types (MERGE, REDEEM, SPLIT, etc.)."""
    all_activity: list[dict] = []
    offset = 0
    limit = 500

    while True:
        params: dict = {
            "user": user,
            "limit": limit,
            "offset": offset,
            "sortBy": "TIMESTAMP",
            "sortDirection": "ASC",
            "activity_types[]": activity_types,
        }
        if start_ts:
            params["start"] = start_ts
        if end_ts:
            params["end"] = end_ts

        print(f"  Fetching activity ({','.join(activity_types)}) offset={offset}...")
        activity = api_get(f"{DATA_API}/activity", params)

        if not activity:
            break
        all_activity.extend(activity)

        if len(activity) < limit:
            break
        offset += limit
        if offset > 3000:
            break

    return all_activity


# ── Market info ───────────────────────────────────────────────────────

def fetch_market_info(condition_id: str) -> dict | None:
    """Fetch market metadata from Gamma API."""
    try:
        markets = api_get(f"{GAMMA_API}/markets", {"condition_id": condition_id})
        if isinstance(markets, list) and markets:
            return markets[0]
        if isinstance(markets, dict):
            return markets
    except Exception as e:
        print(f"  Could not fetch market {condition_id}: {e}", file=sys.stderr)
    return None


# ── Binance klines ────────────────────────────────────────────────────

def fetch_binance_klines(symbol: str, interval: str,
                         start_ms: int, end_ms: int) -> list[dict]:
    """Fetch Binance kline data, paginating if needed."""
    all_klines: list[dict] = []
    current_start = start_ms

    while current_start < end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current_start,
            "endTime": end_ms,
            "limit": 1000,
        }
        print(f"  Fetching Binance {symbol} klines from "
              f"{datetime.fromtimestamp(current_start / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}...")
        klines = api_get(f"{BINANCE_API}/api/v3/klines", params)

        if not klines:
            break

        for k in klines:
            all_klines.append({
                "open_time": k[0],
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": k[6],
            })

        last_close_time = klines[-1][6]
        current_start = last_close_time + 1

        if len(klines) < 1000:
            break

    return all_klines


def get_price_at(klines: list[dict], timestamp_ms: int) -> Optional[float]:
    """Find price at a given timestamp from kline data."""
    for k in klines:
        if k["open_time"] <= timestamp_ms <= k["close_time"]:
            return k["close"]
    if not klines:
        return None
    min_dist = float("inf")
    nearest = None
    for k in klines:
        mid = (k["open_time"] + k["close_time"]) / 2
        dist = abs(mid - timestamp_ms)
        if dist < min_dist:
            min_dist = dist
            nearest = k
    return nearest["close"] if nearest else None


# ── Internal helpers ──────────────────────────────────────────────────

def _parse_ts(ts_val) -> Optional[datetime]:
    """Parse various timestamp formats from Polymarket."""
    if ts_val is None or ts_val == "":
        return None
    try:
        if isinstance(ts_val, (int, float)):
            return datetime.fromtimestamp(ts_val, tz=timezone.utc)
        ts_str = str(ts_val)
        if "T" in ts_str:
            ts_str = ts_str.replace("Z", "+00:00")
            return datetime.fromisoformat(ts_str)
        return datetime.fromtimestamp(int(ts_str), tz=timezone.utc)
    except (ValueError, OSError):
        return None
