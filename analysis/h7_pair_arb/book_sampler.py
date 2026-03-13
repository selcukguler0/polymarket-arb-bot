"""Sample order books from Polymarket CLOB and compute pair margins."""

import json
import time
import urllib.request
import urllib.error

from .config import CLOB_API, REQUEST_TIMEOUT_SEC, MAX_RETRIES, RETRY_BACKOFF_SEC


def sample_book(token_id: str) -> dict | None:
    """Fetch order book for a token. Returns best bid/ask and depth, or None on failure."""
    url = f"{CLOB_API}/book?token_id={token_id}"

    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(url, headers={
                "Accept": "application/json",
                "User-Agent": "polymarket-h7-paper/0.1",
            })
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
                data = json.loads(resp.read())
            break
        except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
                continue
            print(f"  [book_sampler] failed after {MAX_RETRIES} attempts for {token_id[:20]}...: {e}")
            return None

    bids = data.get("bids", [])
    asks = data.get("asks", [])

    best_bid = float(bids[0]["price"]) if bids else 0.0
    best_ask = float(asks[0]["price"]) if asks else 1.0
    bid_depth = sum(float(b.get("size", 0)) for b in bids)
    ask_depth = sum(float(a.get("size", 0)) for a in asks)

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_depth": round(bid_depth, 2),
        "ask_depth": round(ask_depth, 2),
        "num_bid_levels": len(bids),
        "num_ask_levels": len(asks),
    }


def compute_pair_margin(ask_up: float, ask_down: float) -> float:
    """Compute pair margin: 1.0 - (ask_up + ask_down). Positive = profitable."""
    return 1.0 - ask_up - ask_down
