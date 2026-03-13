from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.insider_scoring import determine_tier, score_feature_rows
from lib.insider_types import FeatureRow


def _mk_feature_row(
    trade_key: str,
    market_slug: str,
    wallet: str,
    trade_ts: int,
    features: dict[str, float],
) -> FeatureRow:
    return FeatureRow(
        trade_key=trade_key,
        market_slug=market_slug,
        condition_id="0xcond",
        question="Test market?",
        wallet=wallet,
        side="BUY",
        outcome="Yes",
        trade_ts=trade_ts,
        price=0.20,
        size=100.0,
        notional_usdc=20.0,
        features=features,
    )


def test_scoring_separates_insider_and_manipulation() -> None:
    insider_like = _mk_feature_row(
        "k1",
        "m1",
        "w1",
        1700000000,
        {
            "size_z": 8.0,
            "impact_30s": 0.18,
            "impact_5m": 0.09,
            "impact_15m": 0.17,
            "persistence_15m": 0.17,
            "reversal_15m": 0.01,
            "wallet_share_5m": 1.0,
            "wallet_flip_30m": 0.00,
            "wallet_edge_prior": 0.95,
            "regime_break": 1.0,
            "low_liquidity": 1.0,
            "burst_10s": 0.0,
        },
    )

    manip_like = _mk_feature_row(
        "k2",
        "m2",
        "w2",
        1700000100,
        {
            "size_z": 8.0,
            "impact_30s": 0.18,
            "impact_5m": 0.07,
            "impact_15m": -0.05,
            "persistence_15m": 0.0,
            "reversal_15m": 0.23,
            "wallet_share_5m": 0.60,
            "wallet_flip_30m": 0.80,
            "wallet_edge_prior": 0.20,
            "regime_break": 1.0,
            "low_liquidity": 1.0,
            "burst_10s": 6.0,
        },
    )

    alerts = score_feature_rows(
        [insider_like, manip_like],
        run_ts_utc="2026-03-02T12:00:00Z",
        min_confidence=0.0,
        top_k=10,
    )

    a1 = next(a for a in alerts if a.market_slug == "m1")
    a2 = next(a for a in alerts if a.market_slug == "m2")

    assert a1.classification == "INSIDER_LIKE"
    assert a2.classification == "MANIPULATION_LIKE"


def test_tier_boundaries() -> None:
    insider_raw = {
        "persistence_15m": 0.05,
        "reversal_15m": 0.02,
        "wallet_edge_prior": 0.62,
        "wallet_flip_30m": 0.10,
    }
    manip_raw = {
        "persistence_15m": 0.0,
        "reversal_15m": 0.06,
        "wallet_edge_prior": 0.40,
        "wallet_flip_30m": 0.55,
    }

    t3_insider = determine_tier("INSIDER_LIKE", insider_raw, insider_score=0.92, manipulation_score=0.30)
    t3_manip = determine_tier("MANIPULATION_LIKE", manip_raw, insider_score=0.20, manipulation_score=0.92)
    t1_fallback = determine_tier("INSIDER_LIKE", insider_raw, insider_score=0.80, manipulation_score=0.10)

    assert t3_insider == "T3"
    assert t3_manip == "T3"
    assert t1_fallback == "T1"
