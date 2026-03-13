"""
Parse Polymarket binary market titles to extract asset, duration, period times.
Ported from scripts/analyze_account.py with multi-asset support.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# Known asset prefixes in market titles
ASSET_PREFIXES = {
    "Bitcoin": "BTC",
    "Ethereum": "ETH",
    "Solana": "SOL",
    "XRP": "XRP",
}

# Pattern: "Asset Up or Down - Month Day, StartTime-EndTime ET"
TITLE_PATTERN = re.compile(
    r"^(\w+)\s+Up or Down\s*[-\u2013]\s*"
    r"(\w+\s+\d+),?\s+"
    r"(\d+:\d+(?:AM|PM))\s*-\s*(\d+:\d+(?:AM|PM))\s*ET$"
)


def classify_market(title: str) -> Optional[str]:
    """Return asset code if this is a binary Up/Down market we care about, else None."""
    m = TITLE_PATTERN.match(title)
    if not m:
        return None
    prefix = m.group(1)
    return ASSET_PREFIXES.get(prefix)


def parse_asset(title: str) -> str:
    """Extract asset code from title. Returns 'UNKNOWN' if not recognized."""
    return classify_market(title) or "UNKNOWN"


def parse_market_period(title: str) -> tuple[Optional[datetime], Optional[datetime]]:
    """
    Parse period start/end from title.
    Returns (start_utc, end_utc) or (None, None).
    """
    m = TITLE_PATTERN.match(title)
    if not m:
        # Fallback: try looser pattern
        return _parse_loose(title)

    date_str = m.group(2)   # "February 14"
    start_time = m.group(3) # "7:00AM"
    end_time = m.group(4)   # "7:15AM"

    year = datetime.now().year

    try:
        start_dt = datetime.strptime(f"{date_str} {year} {start_time}", "%B %d %Y %I:%M%p")
        start_dt = start_dt.replace(tzinfo=ET)
        end_dt = datetime.strptime(f"{date_str} {year} {end_time}", "%B %d %Y %I:%M%p")
        end_dt = end_dt.replace(tzinfo=ET)
        return start_dt.astimezone(timezone.utc), end_dt.astimezone(timezone.utc)
    except Exception:
        return None, None


def parse_market_duration_minutes(title: str) -> Optional[int]:
    """Parse and return the market duration in minutes."""
    start, end = parse_market_period(title)
    if start and end:
        delta = (end - start).total_seconds()
        if delta > 0:
            return int(delta / 60)
    return None


def compute_period_elapsed(trade_ts: datetime,
                           period_start: datetime,
                           period_end: datetime) -> Optional[float]:
    """Compute how far into the period this trade occurred (0.0 to 1.0)."""
    total = (period_end - period_start).total_seconds()
    if total <= 0:
        return None
    elapsed = (trade_ts - period_start).total_seconds()
    return max(0.0, min(1.0, elapsed / total))


def _parse_loose(title: str) -> tuple[Optional[datetime], Optional[datetime]]:
    """Fallback parser for non-standard title formats."""
    pattern = r"(\w+ \d+),?\s+(\d+:\d+(?:AM|PM))\s*-\s*(\d+:\d+(?:AM|PM))\s*ET"
    m = re.search(pattern, title)
    if not m:
        return None, None

    date_str = m.group(1)
    start_time = m.group(2)
    end_time = m.group(3)
    year = datetime.now().year

    try:
        start_dt = datetime.strptime(f"{date_str} {year} {start_time}", "%B %d %Y %I:%M%p")
        start_dt = start_dt.replace(tzinfo=ET)
        end_dt = datetime.strptime(f"{date_str} {year} {end_time}", "%B %d %Y %I:%M%p")
        end_dt = end_dt.replace(tzinfo=ET)
        return start_dt.astimezone(timezone.utc), end_dt.astimezone(timezone.utc)
    except Exception:
        return None, None
