"""Market discovery and geopolitical filtering for Insider Finder v1."""
from __future__ import annotations

from pathlib import Path

from .api import GAMMA_API, api_get
from .insider_types import MarketMeta


def _as_float(value: object) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def load_keywords(path: str) -> list[str]:
    """Load non-empty lowercase keywords from file."""
    keywords: list[str] = []
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        kw = raw.strip().lower()
        if not kw or kw.startswith("#"):
            continue
        keywords.append(kw)
    return keywords


def _market_text(raw: dict) -> str:
    parts = [
        str(raw.get("question") or ""),
        str(raw.get("title") or ""),
        str(raw.get("slug") or ""),
        str(raw.get("eventSlug") or ""),
        str(raw.get("description") or ""),
    ]
    return " ".join(parts).lower()


def _is_geopolitical(raw: dict, keywords: list[str]) -> bool:
    text = _market_text(raw)
    return any(kw in text for kw in keywords)


def fetch_gamma_markets(limit: int = 500, closed: bool = False) -> list[dict]:
    """Fetch raw markets from Gamma API."""
    data = api_get(
        f"{GAMMA_API}/markets",
        {
            "limit": int(limit),
            "closed": str(closed).lower(),
        },
    )
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if isinstance(data.get("data"), list):
            return data["data"]
    return []


def normalize_market(raw: dict) -> MarketMeta | None:
    """Normalize one raw market dict into MarketMeta."""
    slug = str(raw.get("slug") or "").strip()
    condition_id = str(raw.get("conditionId") or raw.get("condition_id") or "").strip()
    question = str(raw.get("question") or raw.get("title") or slug or "").strip()

    if not slug or not condition_id:
        return None

    return MarketMeta(
        slug=slug,
        condition_id=condition_id,
        question=question,
        end_date=str(raw.get("endDate") or ""),
        liquidity=_as_float(raw.get("liquidity")),
        volume=_as_float(raw.get("volume")),
        active=_as_bool(raw.get("active")),
        closed=_as_bool(raw.get("closed")),
    )


def select_top_geopolitical_markets(
    top_markets: int,
    market_fetch_limit: int,
    keywords_path: str,
) -> list[MarketMeta]:
    """Fetch active markets, keyword-filter for geopolitics, sort by volume, keep top N."""
    keywords = load_keywords(keywords_path)
    raw_markets = fetch_gamma_markets(limit=market_fetch_limit, closed=False)

    selected: list[MarketMeta] = []
    for raw in raw_markets:
        if not isinstance(raw, dict):
            continue
        if not _is_geopolitical(raw, keywords):
            continue
        norm = normalize_market(raw)
        if norm is None:
            continue
        selected.append(norm)

    selected.sort(key=lambda m: m.volume, reverse=True)
    return selected[: max(0, int(top_markets))]
