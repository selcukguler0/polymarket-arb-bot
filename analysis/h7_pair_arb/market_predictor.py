"""Predict next market slug and fetch market details from Gamma API."""

import time
import json
import urllib.request
import urllib.error

from .config import GAMMA_API, DURATION_MIN, REQUEST_TIMEOUT_SEC


def next_period_epoch(coin: str = "btc", duration_min: int = DURATION_MIN) -> int:
    """Return the unix epoch of the next period boundary."""
    now = int(time.time())
    interval = duration_min * 60
    return now - (now % interval) + interval


def current_period_epoch(coin: str = "btc", duration_min: int = DURATION_MIN) -> int:
    """Return the unix epoch of the current (most recently started) period."""
    now = int(time.time())
    interval = duration_min * 60
    return now - (now % interval)


def compute_slug(coin: str, epoch: int, duration_min: int = DURATION_MIN) -> str:
    """Compute deterministic market slug."""
    return f"{coin}-updown-{duration_min}m-{epoch}"


def fetch_market(slug: str) -> dict | None:
    """Fetch market details by slug. Returns dict with conditionId and token_ids, or None."""
    url = f"{GAMMA_API}/markets?slug={slug}"
    try:
        req = urllib.request.Request(url, headers={
            "Accept": "application/json",
            "User-Agent": "polymarket-h7-paper/0.1",
        })
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
            data = json.loads(resp.read())
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as e:
        print(f"  [market_predictor] fetch error for {slug}: {e}")
        return None

    if not isinstance(data, list) or not data:
        print(f"  [market_predictor] no market found for slug={slug}")
        return None

    m = data[0]
    condition_id = m.get("conditionId", "")
    clob_token_ids_raw = m.get("clobTokenIds", "[]")
    try:
        token_ids = json.loads(clob_token_ids_raw) if isinstance(clob_token_ids_raw, str) else clob_token_ids_raw
    except json.JSONDecodeError:
        token_ids = []

    if len(token_ids) < 2:
        print(f"  [market_predictor] insufficient tokens for {slug}: {token_ids}")
        return None

    return {
        "slug": slug,
        "conditionId": condition_id,
        "question": m.get("question", ""),
        "token_up": str(token_ids[0]),
        "token_down": str(token_ids[1]),
        "end_date": m.get("endDate", ""),
    }
