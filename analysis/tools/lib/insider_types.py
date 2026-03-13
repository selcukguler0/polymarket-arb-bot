"""Typed domain models for Insider Finder v1."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MarketMeta:
    """Normalized market metadata used by the detector."""

    slug: str
    condition_id: str
    question: str
    end_date: str
    liquidity: float
    volume: float
    active: bool
    closed: bool


@dataclass
class TradeRow:
    """Normalized trade row persisted in local SQLite snapshots."""

    tx_hash: str
    wallet: str
    ts: int
    slug: str
    condition_id: str
    side: str
    outcome: str
    price: float
    size: float
    notional: float
    run_id: str = ""

    @property
    def trade_key(self) -> str:
        return (
            f"{self.tx_hash}|{self.wallet}|{self.side}|{self.price:.8f}|"
            f"{self.size:.8f}|{self.ts}"
        )


@dataclass
class FeatureRow:
    """Computed feature vector for a candidate suspicious trade."""

    trade_key: str
    market_slug: str
    condition_id: str
    question: str
    wallet: str
    side: str
    outcome: str
    trade_ts: int
    price: float
    size: float
    notional_usdc: float
    features: dict[str, float]


@dataclass
class AlertRow:
    """Final scored alert output record."""

    alert_id: str
    run_ts_utc: str
    market_slug: str
    condition_id: str
    question: str
    wallet: str
    side: str
    outcome: str
    trade_ts: int
    price: float
    size: float
    notional_usdc: float
    insider_score: float
    manipulation_score: float
    classification: str
    tier: str
    features: dict[str, float] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to JSON-safe dictionary."""
        return {
            "alert_id": self.alert_id,
            "run_ts_utc": self.run_ts_utc,
            "market_slug": self.market_slug,
            "condition_id": self.condition_id,
            "question": self.question,
            "wallet": self.wallet,
            "side": self.side,
            "outcome": self.outcome,
            "trade_ts": self.trade_ts,
            "price": self.price,
            "size": self.size,
            "notional_usdc": self.notional_usdc,
            "insider_score": self.insider_score,
            "manipulation_score": self.manipulation_score,
            "classification": self.classification,
            "tier": self.tier,
            "features": self.features,
            "reasons": self.reasons,
        }


@dataclass
class RunSummary:
    """Execution summary for snapshot/analyze commands."""

    run_id: str
    command: str
    run_ts_utc: str
    as_of_ts: int
    params: dict[str, Any] = field(default_factory=dict)

    selected_markets: int = 0
    trades_seen: int = 0
    trades_filtered: int = 0
    trades_inserted: int = 0
    trades_duplicates: int = 0

    candidates: int = 0
    alerts: int = 0

    output_json_path: str = ""
    output_md_path: str = ""
