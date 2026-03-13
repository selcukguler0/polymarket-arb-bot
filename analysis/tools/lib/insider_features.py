"""Feature engineering for Insider Finder v1."""
from __future__ import annotations

import bisect
import math
from collections import defaultdict

from .insider_types import FeatureRow, TradeRow


YES_OUTCOMES = {"yes", "up"}
NO_OUTCOMES = {"no", "down"}


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    if n % 2:
        return s[n // 2]
    return 0.5 * (s[n // 2 - 1] + s[n // 2])


def _mad(values: list[float], median_value: float) -> float:
    deviations = [abs(v - median_value) for v in values]
    return _median(deviations)


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(var)


def robust_zscore(value: float, baseline: list[float]) -> float:
    """Robust z-score using MAD with std fallback."""
    if len(baseline) < 5:
        return 0.0
    med = _median(baseline)
    mad = _mad(baseline, med)
    if mad > 1e-9:
        return (value - med) / (1.4826 * mad)
    stdev = _std(baseline)
    if stdev > 1e-9:
        return (value - med) / stdev
    return 0.0


def signed_direction(side: str, outcome: str) -> int:
    """Return +1 when upward move helps this trade, -1 otherwise."""
    side_l = (side or "").strip().lower()
    out_l = (outcome or "").strip().lower()

    if out_l in YES_OUTCOMES:
        return 1 if side_l == "buy" else -1
    if out_l in NO_OUTCOMES:
        return -1 if side_l == "buy" else 1

    # Conservative fallback: treat unknown outcome as YES-like
    return 1 if side_l == "buy" else -1


def nearest_price_at_horizon(
    trade_ts: int,
    target_ts: int,
    ts_list: list[int],
    price_list: list[float],
    max_abs_delta: int,
) -> float | None:
    """Return nearest available price around target timestamp, but not before trade_ts."""
    if not ts_list:
        return None

    idx = bisect.bisect_left(ts_list, target_ts)
    candidates: list[tuple[int, float]] = []

    for j in (idx - 1, idx, idx + 1):
        if j < 0 or j >= len(ts_list):
            continue
        t = ts_list[j]
        if t <= trade_ts:
            continue
        candidates.append((abs(t - target_ts), price_list[j]))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    delta, price = candidates[0]
    if delta > max_abs_delta:
        return None
    return price


def compute_markouts_for_trade(trade: TradeRow, ts_list: list[int], price_list: list[float]) -> dict[str, float | None]:
    """Compute signed markouts at 30s, 5m, 15m horizons."""
    direction = signed_direction(trade.side, trade.outcome)

    p30 = nearest_price_at_horizon(trade.ts, trade.ts + 30, ts_list, price_list, max_abs_delta=180)
    p5m = nearest_price_at_horizon(trade.ts, trade.ts + 300, ts_list, price_list, max_abs_delta=600)
    p15m = nearest_price_at_horizon(trade.ts, trade.ts + 900, ts_list, price_list, max_abs_delta=1800)

    def _signed(p_future: float | None) -> float | None:
        if p_future is None:
            return None
        return direction * (p_future - trade.price)

    return {
        "impact_30s": _signed(p30),
        "impact_5m": _signed(p5m),
        "impact_15m": _signed(p15m),
    }


def _build_regime_break_flags(trades: list[TradeRow]) -> dict[str, set[int]]:
    """CUSUM break flags on minute-aggregated price path, per market slug."""
    minute_prices_by_market: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for t in trades:
        minute_bucket = t.ts // 60
        minute_prices_by_market[t.slug][minute_bucket].append(t.price)

    out: dict[str, set[int]] = {}
    for slug, minute_map in minute_prices_by_market.items():
        minute_keys = sorted(minute_map.keys())
        if len(minute_keys) < 4:
            out[slug] = set()
            continue

        minute_series = [(m, sum(minute_map[m]) / len(minute_map[m])) for m in minute_keys]
        returns: list[float] = []
        for i in range(1, len(minute_series)):
            returns.append(minute_series[i][1] - minute_series[i - 1][1])

        sigma = _std(returns)
        if sigma <= 1e-9:
            out[slug] = set()
            continue

        k = 0.5 * sigma
        h = 3.0 * sigma
        s_pos = 0.0
        s_neg = 0.0
        flags: set[int] = set()

        for i, r in enumerate(returns, start=1):
            minute_bucket = minute_series[i][0]
            s_pos = max(0.0, s_pos + r - k)
            s_neg = min(0.0, s_neg + r + k)
            if s_pos > h or abs(s_neg) > h:
                flags.add(minute_bucket)
                s_pos = 0.0
                s_neg = 0.0

        out[slug] = flags

    return out


def _wallet_share_5m(trades: list[TradeRow], ts_vals: list[int], i: int) -> float:
    center = trades[i].ts
    left = bisect.bisect_left(ts_vals, center - 300)
    right = bisect.bisect_right(ts_vals, center + 300)
    window = trades[left:right]
    total = sum(t.notional for t in window)
    if total <= 0:
        return 0.0
    wallet = trades[i].wallet
    mine = sum(t.notional for t in window if t.wallet == wallet)
    return mine / total


def _wallet_flip_30m(trades: list[TradeRow], ts_vals: list[int], i: int) -> float:
    center = trades[i].ts
    left = bisect.bisect_left(ts_vals, center - 1800)
    right = bisect.bisect_right(ts_vals, center + 1800)
    wallet = trades[i].wallet
    mine = [t.side for t in trades[left:right] if t.wallet == wallet]
    if len(mine) < 2:
        return 0.0
    flips = 0
    for j in range(1, len(mine)):
        if mine[j] != mine[j - 1]:
            flips += 1
    return flips / (len(mine) - 1)


def _burst_10s(trades: list[TradeRow], ts_vals: list[int], i: int) -> float:
    center = trades[i].ts
    left = bisect.bisect_left(ts_vals, center - 10)
    right = bisect.bisect_right(ts_vals, center + 10)
    wallet = trades[i].wallet
    count = sum(1 for t in trades[left:right] if t.wallet == wallet)
    return max(0.0, float(count - 1))


def compute_feature_rows(
    trades: list[TradeRow],
    candidate_start_ts: int,
    as_of_ts: int,
    market_liquidity: dict[str, float],
    market_questions: dict[str, str] | None = None,
    market_conditions: dict[str, str] | None = None,
    apply_prefilter: bool = True,
) -> list[FeatureRow]:
    """
    Build candidate feature rows from normalized trades.

    Notes:
    - Trades missing a 15m markout are excluded.
    - If apply_prefilter=True, enforce size_z>=3 and |impact_5m|>=0.06.
    """
    if market_questions is None:
        market_questions = {}
    if market_conditions is None:
        market_conditions = {}

    if not trades:
        return []

    trades_sorted = sorted(trades, key=lambda t: t.ts)

    # Group by market for local window calculations.
    by_market: dict[str, list[TradeRow]] = defaultdict(list)
    for t in trades_sorted:
        by_market[t.slug].append(t)

    # Markouts for every trade keyed by trade_key.
    markout_by_key: dict[str, dict[str, float | None]] = {}
    for slug, m_trades in by_market.items():
        ts_list = [t.ts for t in m_trades]
        price_list = [t.price for t in m_trades]
        for t in m_trades:
            markout_by_key[t.trade_key] = compute_markouts_for_trade(t, ts_list, price_list)

    # Wallet prior edge (beta-smoothed) in chronological order.
    prior_edge_by_key: dict[str, float] = {}
    wallet_wins: dict[str, int] = defaultdict(int)
    wallet_total: dict[str, int] = defaultdict(int)

    for t in trades_sorted:
        wins = wallet_wins[t.wallet]
        total = wallet_total[t.wallet]
        prior_edge_by_key[t.trade_key] = (wins + 1.0) / (total + 2.0)

        impact_15m = markout_by_key[t.trade_key].get("impact_15m")
        if impact_15m is None:
            continue
        wallet_total[t.wallet] += 1
        if impact_15m > 0:
            wallet_wins[t.wallet] += 1

    # Regime breaks from per-market minute CUSUM.
    regime_breaks = _build_regime_break_flags(trades_sorted)

    # Market-level size baseline from trailing 7d window.
    trailing_start = as_of_ts - 7 * 86400
    notional_baseline: dict[str, list[float]] = defaultdict(list)
    for t in trades_sorted:
        if trailing_start <= t.ts <= as_of_ts:
            notional_baseline[t.slug].append(t.notional)

    feature_rows: list[FeatureRow] = []

    for slug, m_trades in by_market.items():
        ts_vals = [t.ts for t in m_trades]
        for i, t in enumerate(m_trades):
            if t.ts < candidate_start_ts or t.ts > as_of_ts:
                continue

            impacts = markout_by_key.get(t.trade_key, {})
            impact_30s = impacts.get("impact_30s")
            impact_5m = impacts.get("impact_5m")
            impact_15m = impacts.get("impact_15m")

            # Low-data protection: cannot score without 15m markout.
            if impact_15m is None or impact_30s is None or impact_5m is None:
                continue

            size_z = robust_zscore(t.notional, notional_baseline.get(slug, []))
            persistence_15m = max(0.0, float(impact_15m))
            reversal_15m = max(0.0, float(impact_30s) - float(impact_15m))
            wallet_share_5m = _wallet_share_5m(m_trades, ts_vals, i)
            wallet_flip_30m = _wallet_flip_30m(m_trades, ts_vals, i)
            burst_10s = _burst_10s(m_trades, ts_vals, i)
            wallet_edge_prior = prior_edge_by_key.get(t.trade_key, 0.5)

            minute_bucket = t.ts // 60
            flag_set = regime_breaks.get(slug, set())
            regime_break = 1.0 if (
                minute_bucket in flag_set
                or (minute_bucket - 1) in flag_set
                or (minute_bucket + 1) in flag_set
            ) else 0.0

            low_liquidity = 1.0 if market_liquidity.get(slug, 0.0) < 25000.0 else 0.0

            if apply_prefilter and not (size_z >= 3.0 and abs(float(impact_5m)) >= 0.06):
                continue

            feats = {
                "size_z": float(size_z),
                "impact_30s": float(impact_30s),
                "impact_5m": float(impact_5m),
                "impact_15m": float(impact_15m),
                "persistence_15m": float(persistence_15m),
                "reversal_15m": float(reversal_15m),
                "wallet_share_5m": float(wallet_share_5m),
                "wallet_flip_30m": float(wallet_flip_30m),
                "wallet_edge_prior": float(wallet_edge_prior),
                "regime_break": float(regime_break),
                "low_liquidity": float(low_liquidity),
                "burst_10s": float(burst_10s),
            }

            feature_rows.append(
                FeatureRow(
                    trade_key=t.trade_key,
                    market_slug=slug,
                    condition_id=market_conditions.get(slug, t.condition_id),
                    question=market_questions.get(slug, slug),
                    wallet=t.wallet,
                    side=t.side,
                    outcome=t.outcome,
                    trade_ts=t.ts,
                    price=t.price,
                    size=t.size,
                    notional_usdc=t.notional,
                    features=feats,
                )
            )

    feature_rows.sort(key=lambda r: r.trade_ts)
    return feature_rows
