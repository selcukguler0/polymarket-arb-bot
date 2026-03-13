"""
Core analysis engine: computes PeriodMetrics and WalletMetrics from raw data.
Every metric maps to a bot parameter in config/v2.toml.
"""
from __future__ import annotations

import math
from typing import Optional

from .models import Trade, Activity, MarketPeriod, PeriodMetrics, WalletMetrics
from .api import get_price_at


def compute_period_metrics(period: MarketPeriod,
                           klines: list[dict] | None = None) -> PeriodMetrics:
    """Compute all metrics for a single market period."""
    pm = PeriodMetrics(
        condition_id=period.condition_id,
        asset=period.asset,
        duration_minutes=period.duration_minutes,
        period_start=period.period_start,
        period_end=period.period_end,
    )

    buys = period.buys
    sells = period.sells

    if not buys:
        return pm

    # --- Entry/exit timing ---
    buy_pcts = [t.period_elapsed_pct for t in buys if t.period_elapsed_pct is not None]
    if buy_pcts:
        pm.first_buy_pct = min(buy_pcts)
        pm.last_buy_pct = max(buy_pcts)

    sell_pcts = [t.period_elapsed_pct for t in sells if t.period_elapsed_pct is not None]
    if sell_pcts:
        pm.first_sell_pct = min(sell_pcts)

    # --- Sizing ---
    pm.total_buy_trades = len(buys)
    pm.total_sell_trades = len(sells)

    up_buys = period.up_buys
    down_buys = period.down_buys

    pm.total_up_shares = sum(t.size for t in up_buys)
    pm.total_down_shares = sum(t.size for t in down_buys)

    buy_sizes = [t.size for t in buys]
    pm.avg_buy_size = sum(buy_sizes) / len(buy_sizes)
    pm.max_buy_size = max(buy_sizes)

    # --- Pricing ---
    if up_buys:
        total_up_cost = sum(t.price * t.size for t in up_buys)
        pm.avg_up_price = total_up_cost / pm.total_up_shares
    if down_buys:
        total_dn_cost = sum(t.price * t.size for t in down_buys)
        pm.avg_down_price = total_dn_cost / pm.total_down_shares

    # --- Pair completion ---
    pm.is_paired = bool(up_buys and down_buys)
    if pm.is_paired:
        pm.combined_cost = pm.avg_up_price + pm.avg_down_price
        max_shares = max(pm.total_up_shares, pm.total_down_shares)
        min_shares = min(pm.total_up_shares, pm.total_down_shares)
        pm.pair_ratio = min_shares / max_shares if max_shares > 0 else 0.0

    # --- Imbalance ---
    pm.share_imbalance = abs(pm.total_up_shares - pm.total_down_shares)

    # --- Ladder levels (unique price points per side) ---
    up_prices = set(round(t.price, 2) for t in up_buys)
    dn_prices = set(round(t.price, 2) for t in down_buys)
    pm.up_price_levels = len(up_prices)
    pm.down_price_levels = len(dn_prices)

    # --- Sells ---
    pm.num_sells = len(sells)
    if sells and buys:
        # Compute imbalance at the time of first sell
        first_sell_ts = min(t.timestamp for t in sells)
        pre_sell_buys = [t for t in buys if t.timestamp < first_sell_ts]
        if pre_sell_buys:
            up_before = sum(t.size for t in pre_sell_buys if t.outcome in ("Up", "Yes"))
            dn_before = sum(t.size for t in pre_sell_buys if t.outcome in ("Down", "No"))
            pm.sell_imbalance_at_first_sell = abs(up_before - dn_before)

    # --- Merges / Redeems ---
    pm.num_merges = len(period.merges)
    pm.merge_shares = sum(a.size for a in period.merges)
    pm.num_redeems = len(period.redeems)

    # --- P&L estimation (requires klines + resolved market) ---
    if klines and period.period_start and period.period_end:
        open_ms = int(period.period_start.timestamp() * 1000)
        close_ms = int(period.period_end.timestamp() * 1000)
        spot_open = get_price_at(klines, open_ms)
        spot_close = get_price_at(klines, close_ms)

        if spot_open is not None and spot_close is not None:
            went_up = spot_close > spot_open

            # Compute P&L from all buys and sells
            pnl = 0.0
            for t in buys:
                bought_up = t.outcome in ("Up", "Yes")
                won = (bought_up and went_up) or (not bought_up and not went_up)
                if won:
                    pnl += (1.0 - t.price) * t.size
                else:
                    pnl -= t.price * t.size
                pm.won = won if pm.won is None else pm.won

            # Sells: if you sold before resolution, you got the sell price back
            for t in sells:
                sold_up = t.outcome in ("Up", "Yes")
                # Selling removes exposure: you got t.price * t.size back
                # and no longer have the resolution payout
                won_if_held = (sold_up and went_up) or (not sold_up and not went_up)
                if won_if_held:
                    # Sold a winner: lost (1.0 - t.price) * t.size potential
                    pnl -= (1.0 - t.price) * t.size
                else:
                    # Sold a loser: saved t.price * t.size
                    pnl += t.price * t.size

            # Merges: complete pair realized at $1.00
            pnl += pm.merge_shares  # merge_shares pairs @ $1.00 each

            pm.estimated_pnl = pnl

    return pm


def compute_wallet_metrics(address: str, username: str, days: int,
                           period_metrics: list[PeriodMetrics]) -> WalletMetrics:
    """Aggregate period metrics into wallet-level summary."""
    wm = WalletMetrics(
        address=address,
        username=username,
        analysis_days=days,
        total_periods=len(period_metrics),
        period_metrics=period_metrics,
    )

    if not period_metrics:
        return wm

    # Total trades / volume
    wm.total_trades = sum(pm.total_buy_trades + pm.total_sell_trades for pm in period_metrics)
    wm.total_volume_usdc = sum(
        (pm.avg_up_price * pm.total_up_shares + pm.avg_down_price * pm.total_down_shares)
        for pm in period_metrics
    )

    # --- Entry timing ---
    first_pcts = [pm.first_buy_pct for pm in period_metrics if pm.first_buy_pct is not None]
    last_pcts = [pm.last_buy_pct for pm in period_metrics if pm.last_buy_pct is not None]
    if first_pcts:
        wm.avg_first_buy_pct = sum(first_pcts) / len(first_pcts)
        wm.median_first_buy_pct = _median(first_pcts)
    if last_pcts:
        wm.avg_last_buy_pct = sum(last_pcts) / len(last_pcts)

    # --- Sizing ---
    all_avg_sizes = [pm.avg_buy_size for pm in period_metrics if pm.avg_buy_size > 0]
    all_max_sizes = [pm.max_buy_size for pm in period_metrics if pm.max_buy_size > 0]
    if all_avg_sizes:
        wm.avg_buy_size = sum(all_avg_sizes) / len(all_avg_sizes)
        wm.median_buy_size = _median(all_avg_sizes)
    if all_max_sizes:
        wm.max_buy_size = max(all_max_sizes)

    # --- Ladder levels ---
    all_levels = [max(pm.up_price_levels, pm.down_price_levels)
                  for pm in period_metrics if pm.total_buy_trades > 0]
    if all_levels:
        wm.avg_price_levels = sum(all_levels) / len(all_levels)
        wm.max_price_levels = max(all_levels)

    # --- Combined cost ---
    paired = [pm for pm in period_metrics if pm.is_paired and pm.combined_cost is not None]
    if paired:
        costs = [pm.combined_cost for pm in paired]
        wm.avg_combined_cost = sum(costs) / len(costs)
        wm.median_combined_cost = _median(costs)

    # --- Pair completion ---
    periods_with_buys = [pm for pm in period_metrics if pm.total_buy_trades > 0]
    if periods_with_buys:
        wm.pair_rate = len(paired) / len(periods_with_buys)
        pair_ratios = [pm.pair_ratio for pm in paired if pm.pair_ratio > 0]
        if pair_ratios:
            wm.avg_pair_ratio = sum(pair_ratios) / len(pair_ratios)
        merged = [pm for pm in paired if pm.num_merges > 0]
        wm.merge_rate = len(merged) / len(paired) if paired else 0.0

    # --- Imbalance ---
    imbalances = [pm.share_imbalance for pm in period_metrics if pm.total_buy_trades > 0]
    if imbalances:
        wm.avg_imbalance = sum(imbalances) / len(imbalances)
        wm.max_imbalance = max(imbalances)
        wm.p75_imbalance = _percentile(imbalances, 75)

    # --- Sells ---
    periods_with_sells = [pm for pm in period_metrics if pm.num_sells > 0]
    if periods_with_buys:
        wm.sell_rate = len(periods_with_sells) / len(periods_with_buys)
    trigger_imbalances = [pm.sell_imbalance_at_first_sell
                          for pm in periods_with_sells
                          if pm.sell_imbalance_at_first_sell is not None]
    if trigger_imbalances:
        wm.avg_sell_trigger_imbalance = sum(trigger_imbalances) / len(trigger_imbalances)

    # --- P&L ---
    resolved = [pm for pm in period_metrics if pm.estimated_pnl is not None]
    if resolved:
        pnls = [pm.estimated_pnl for pm in resolved]
        wm.total_estimated_pnl = sum(pnls)
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        wm.win_rate = len(wins) / len(resolved) if resolved else None
        wm.avg_win_pnl = sum(wins) / len(wins) if wins else 0.0
        wm.avg_loss_pnl = sum(losses) / len(losses) if losses else 0.0
        if len(pnls) > 1:
            mean = sum(pnls) / len(pnls)
            variance = sum((p - mean) ** 2 for p in pnls) / (len(pnls) - 1)
            wm.pnl_std_dev = math.sqrt(variance)

    # --- Per-asset breakdown ---
    assets = set(pm.asset for pm in period_metrics if pm.asset)
    for asset in sorted(assets):
        asset_pms = [pm for pm in period_metrics if pm.asset == asset]
        asset_paired = [pm for pm in asset_pms if pm.is_paired and pm.combined_cost is not None]
        asset_resolved = [pm for pm in asset_pms if pm.estimated_pnl is not None]

        asset_info: dict = {
            "periods": len(asset_pms),
            "trades": sum(pm.total_buy_trades + pm.total_sell_trades for pm in asset_pms),
            "pair_rate": len(asset_paired) / len(asset_pms) if asset_pms else 0.0,
        }
        if asset_paired:
            costs = [pm.combined_cost for pm in asset_paired]
            asset_info["avg_combined_cost"] = sum(costs) / len(costs)
        if asset_resolved:
            pnls = [pm.estimated_pnl for pm in asset_resolved]
            asset_info["total_pnl"] = sum(pnls)
            asset_info["win_rate"] = len([p for p in pnls if p > 0]) / len(pnls)

        wm.per_asset[asset] = asset_info

    # --- Strategy classification ---
    if wm.pair_rate > 0.6:
        wm.strategy_type = "arb"
    elif wm.pair_rate < 0.3:
        wm.strategy_type = "directional"
    else:
        wm.strategy_type = "hybrid"

    return wm


def compute_real_pnl(open_positions: list[dict],
                     closed_positions: list[dict],
                     start_ts: int | None = None) -> dict:
    """
    Compute REAL P&L from Polymarket /positions and /closed-positions API data.

    closed_positions should already be time-filtered by fetch_closed_positions().
    open_positions cashPnl is ALL-TIME unrealized (API has no time filter).

    Returns dict with:
      - closed_pnl: realized P&L from positions resolved in the time window
      - open_pnl: ALL-TIME unrealized P&L (cannot be time-filtered)
      - total_pnl: closed_pnl only (the reliable time-windowed number)
    """
    from .market_parser import classify_market, parse_asset

    result = {
        "open_pnl": 0.0,        # ALL-TIME unrealized from open positions
        "open_realized": 0.0,   # realized portion of open positions
        "closed_pnl": 0.0,      # realized from positions resolved in time window
        "total_pnl": 0.0,       # closed_pnl only (time-windowed)
        "open_positions": 0,
        "closed_positions": 0,
        "by_market": {},         # condition_id -> {pnl, title, asset, ...}
        "by_asset": {},          # asset -> {pnl, count}
    }

    # Open positions: cashPnl = currentValue - initialValue (ALL-TIME unrealized)
    # NOTE: This is NOT time-filtered — the API doesn't support it.
    # We show it separately for reference but do NOT include in total_pnl.
    for p in open_positions:
        title = p.get("title", "")
        if not classify_market(title):
            continue

        cid = p.get("conditionId", "")
        cash_pnl = float(p.get("cashPnl", 0))
        realized = float(p.get("realizedPnl", 0))

        result["open_pnl"] += cash_pnl
        result["open_realized"] += realized
        result["open_positions"] += 1

        asset = parse_asset(title)
        if cid not in result["by_market"]:
            result["by_market"][cid] = {
                "title": title, "asset": asset,
                "open_pnl": 0, "closed_pnl": 0,
            }
        result["by_market"][cid]["open_pnl"] += cash_pnl

    # Closed positions: realizedPnl is ground truth
    # Already time-filtered by fetch_closed_positions() client-side filtering
    for p in closed_positions:
        title = p.get("title", "")
        if not classify_market(title):
            continue

        cid = p.get("conditionId", "")
        realized = float(p.get("realizedPnl", 0))

        result["closed_pnl"] += realized
        result["closed_positions"] += 1

        asset = parse_asset(title)
        if cid not in result["by_market"]:
            result["by_market"][cid] = {
                "title": title, "asset": asset,
                "open_pnl": 0, "closed_pnl": 0,
            }
        result["by_market"][cid]["closed_pnl"] += realized

        if asset not in result["by_asset"]:
            result["by_asset"][asset] = {"pnl": 0, "count": 0}
        result["by_asset"][asset]["pnl"] += realized
        result["by_asset"][asset]["count"] += 1

    # total_pnl = closed_pnl only (time-windowed, reliable)
    # open_pnl is all-time and shown separately
    result["total_pnl"] = result["closed_pnl"]

    return result


def _median(values: list[float]) -> float:
    """Compute median of a list."""
    s = sorted(values)
    n = len(s)
    if n == 0:
        return 0.0
    if n % 2 == 1:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2


def _percentile(values: list[float], pct: int) -> float:
    """Compute percentile (simple nearest-rank method)."""
    if not values:
        return 0.0
    s = sorted(values)
    idx = int(len(s) * pct / 100)
    idx = min(idx, len(s) - 1)
    return s[idx]
