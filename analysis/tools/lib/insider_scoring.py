"""Scoring logic for Insider Finder v1."""
from __future__ import annotations

import hashlib
import math
from datetime import datetime, timezone

from .insider_types import AlertRow, FeatureRow


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _sigmoid(value: float) -> float:
    return 1.0 / (1.0 + math.exp(-value))


def normalize_features(raw: dict[str, float]) -> dict[str, float]:
    """Map raw features to [0, 1] normalized features for scoring."""
    return {
        "size_norm": _clip01((raw.get("size_z", 0.0) - 3.0) / 4.0),
        "impact30_norm": _clip01(abs(raw.get("impact_30s", 0.0)) / 0.15),
        "persistence15_norm": _clip01(raw.get("persistence_15m", 0.0) / 0.15),
        "wallet_edge_norm": _clip01((raw.get("wallet_edge_prior", 0.5) - 0.5) / 0.4),
        "wallet_share_norm": _clip01(raw.get("wallet_share_5m", 0.0)),
        "reversal15_norm": _clip01(raw.get("reversal_15m", 0.0) / 0.15),
        "flip_norm": _clip01(raw.get("wallet_flip_30m", 0.0)),
        "burst_norm": _clip01(raw.get("burst_10s", 0.0) / 5.0),
        "low_liquidity": _clip01(raw.get("low_liquidity", 0.0)),
        "regime_break": _clip01(raw.get("regime_break", 0.0)),
    }


def compute_scores(raw: dict[str, float]) -> dict[str, float]:
    """Compute IFS/MCS/Context and final probabilities."""
    n = normalize_features(raw)

    ifs = (
        0.28 * n["size_norm"]
        + 0.24 * n["impact30_norm"]
        + 0.18 * n["persistence15_norm"]
        + 0.16 * n["wallet_edge_norm"]
        + 0.14 * n["wallet_share_norm"]
    )
    mcs = (
        0.32 * n["size_norm"]
        + 0.30 * n["reversal15_norm"]
        + 0.20 * n["flip_norm"]
        + 0.18 * n["burst_norm"]
    )
    context = 0.60 * n["low_liquidity"] + 0.40 * n["regime_break"]

    insider_score = _sigmoid(2.8 * ifs - 1.8 * mcs + 0.7 * context - 1.3)
    manipulation_score = _sigmoid(2.6 * mcs + 0.5 * context - 1.6)

    return {
        "IFS": ifs,
        "MCS": mcs,
        "Context": context,
        "insider_score": insider_score,
        "manipulation_score": manipulation_score,
        **n,
    }


def classify(insider_score: float, manipulation_score: float) -> str:
    """Binary class by higher score."""
    if insider_score >= manipulation_score:
        return "INSIDER_LIKE"
    return "MANIPULATION_LIKE"


def determine_tier(classification: str, raw: dict[str, float], insider_score: float, manipulation_score: float) -> str | None:
    """High-precision tiering logic from the plan."""
    persistence = raw.get("persistence_15m", 0.0)
    reversal = raw.get("reversal_15m", 0.0)
    edge = raw.get("wallet_edge_prior", 0.5)
    flip = raw.get("wallet_flip_30m", 0.0)

    if classification == "INSIDER_LIKE":
        if (
            insider_score >= 0.92
            and persistence >= 0.05
            and reversal <= 0.02
            and edge >= 0.62
        ):
            return "T3"
        if (
            insider_score >= 0.87
            and persistence >= 0.04
            and reversal <= 0.03
        ):
            return "T2"
    else:
        if (
            manipulation_score >= 0.92
            and reversal >= 0.06
            and flip >= 0.55
        ):
            return "T3"
        if (
            manipulation_score >= 0.87
            and reversal >= 0.05
        ):
            return "T2"

    if max(insider_score, manipulation_score) >= 0.80:
        return "T1"
    return None


def build_reasons(classification: str, raw: dict[str, float], scores: dict[str, float], tier: str | None) -> list[str]:
    """Human-readable rationale snippets."""
    reasons: list[str] = []

    size_z = raw.get("size_z", 0.0)
    if size_z >= 3.0:
        reasons.append(f"size_z={size_z:.2f} exceeds 3.0 threshold")

    impact_5m = raw.get("impact_5m", 0.0)
    if abs(impact_5m) >= 0.06:
        reasons.append(f"|impact_5m|={abs(impact_5m):.3f} indicates abnormal repricing")

    persistence = raw.get("persistence_15m", 0.0)
    reversal = raw.get("reversal_15m", 0.0)

    if classification == "INSIDER_LIKE":
        if persistence >= 0.04:
            reasons.append("15m persistence indicates non-reverting impact")
        if raw.get("wallet_edge_prior", 0.5) >= 0.62:
            reasons.append("wallet historical edge is elevated")
        if raw.get("wallet_share_5m", 0.0) >= 0.30:
            reasons.append("wallet captured a large share of local market flow")
    else:
        if reversal >= 0.05:
            reasons.append("price move strongly reverted by 15m")
        if raw.get("wallet_flip_30m", 0.0) >= 0.55:
            reasons.append("rapid side-flip behavior suggests churn")
        if raw.get("burst_10s", 0.0) >= 3.0:
            reasons.append("short burst of repeated prints from same wallet")

    if scores.get("Context", 0.0) >= 0.60:
        reasons.append("context risk elevated (low liquidity and/or regime break)")

    if tier == "T3":
        reasons.append("high-confidence tier gate satisfied")

    return reasons


def score_feature_rows(
    feature_rows: list[FeatureRow],
    run_ts_utc: str | None = None,
    min_confidence: float = 0.80,
    top_k: int = 50,
) -> list[AlertRow]:
    """Score feature rows and return top-K alerts at/above min confidence."""
    if run_ts_utc is None:
        run_ts_utc = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    alerts: list[AlertRow] = []
    for row in feature_rows:
        score_map = compute_scores(row.features)
        insider_score = score_map["insider_score"]
        manipulation_score = score_map["manipulation_score"]
        best = max(insider_score, manipulation_score)

        if best < min_confidence:
            continue

        classification = classify(insider_score, manipulation_score)
        tier = determine_tier(classification, row.features, insider_score, manipulation_score)
        if tier is None:
            continue

        merged_features = dict(row.features)
        merged_features.update(
            {
                "IFS": score_map["IFS"],
                "MCS": score_map["MCS"],
                "Context": score_map["Context"],
                "size_norm": score_map["size_norm"],
                "impact30_norm": score_map["impact30_norm"],
                "persistence15_norm": score_map["persistence15_norm"],
                "wallet_edge_norm": score_map["wallet_edge_norm"],
                "wallet_share_norm": score_map["wallet_share_norm"],
                "reversal15_norm": score_map["reversal15_norm"],
                "flip_norm": score_map["flip_norm"],
                "burst_norm": score_map["burst_norm"],
            }
        )

        reasons = build_reasons(classification, row.features, score_map, tier)

        h = hashlib.sha1(f"{row.trade_key}|{run_ts_utc}".encode("utf-8")).hexdigest()[:16]
        alert_id = f"if_{h}"

        alerts.append(
            AlertRow(
                alert_id=alert_id,
                run_ts_utc=run_ts_utc,
                market_slug=row.market_slug,
                condition_id=row.condition_id,
                question=row.question,
                wallet=row.wallet,
                side=row.side,
                outcome=row.outcome,
                trade_ts=row.trade_ts,
                price=row.price,
                size=row.size,
                notional_usdc=row.notional_usdc,
                insider_score=insider_score,
                manipulation_score=manipulation_score,
                classification=classification,
                tier=tier,
                features=merged_features,
                reasons=reasons,
            )
        )

    alerts.sort(key=lambda a: max(a.insider_score, a.manipulation_score), reverse=True)
    return alerts[: max(0, int(top_k))]
