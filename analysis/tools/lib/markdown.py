"""
Generate structured markdown reports from WalletMetrics.
Every metric row includes a "Bot Parameter" column showing which v2.toml param it maps to.
"""
from __future__ import annotations

from datetime import datetime, timezone

from .models import WalletMetrics, PeriodMetrics


def generate_wallet_report(wm: WalletMetrics,
                           real_pnl: dict | None = None) -> str:
    """Generate full per-wallet markdown report."""
    lines: list[str] = []
    _w = lines.append

    short_addr = f"{wm.address[:6]}...{wm.address[-4:]}"
    _w(f"# Wallet Analysis: {short_addr}")
    _w("")
    _w(f"- **Address**: `{wm.address}`")
    _w(f"- **Username**: {wm.username}")
    _w(f"- **Analysis period**: {wm.analysis_days} days")
    _w(f"- **Generated**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    _w(f"- **Strategy type**: **{wm.strategy_type}**")
    _w(f"- **P&L source**: {wm.pnl_source}")
    _w("")

    # ── Real P&L from API (if available) ──
    if real_pnl:
        _w(f"## P&L ({wm.analysis_days}-Day Window)")
        _w("")
        _w("> Closed P&L from `/closed-positions` (time-filtered client-side).")
        _w("> Open P&L is all-time unrealized (API has no time filter).")
        _w("")
        _w("| Metric | Value |")
        _w("|--------|-------|")
        _w(f"| **Closed P&L (realized, {wm.analysis_days}d)** | **${real_pnl['closed_pnl']:,.2f}** |")
        _w(f"| Closed positions resolved | {real_pnl['closed_positions']} |")
        _w(f"| Open P&L (unrealized, all-time) | ${real_pnl['open_pnl']:,.2f} |")
        _w(f"| Open positions count | {real_pnl['open_positions']} |")
        _w("")

        if real_pnl.get("by_asset"):
            _w(f"**Closed P&L by asset ({wm.analysis_days}d):**")
            _w("")
            _w("| Asset | Realized P&L | Positions |")
            _w("|-------|-------------|-----------|")
            for asset in sorted(real_pnl["by_asset"].keys()):
                info = real_pnl["by_asset"][asset]
                _w(f"| {asset} | ${info['pnl']:,.2f} | {info['count']} |")
            _w("")

    # ── Summary Table ──
    _w("## Summary")
    _w("")
    _w("| Metric | Value |")
    _w("|--------|-------|")
    _w(f"| Periods traded | {wm.total_periods} |")
    _w(f"| Total trades | {wm.total_trades} |")
    _w(f"| Total volume | ${wm.total_volume_usdc:,.2f} |")
    _w(f"| Pair rate | {wm.pair_rate:.1%} |")
    _w(f"| Win rate | {_fmt_pct(wm.win_rate)} |")
    if wm.total_real_pnl is not None:
        _w(f"| **Closed P&L ({wm.analysis_days}d, realized)** | **${wm.total_real_pnl:,.2f}** |")
    _w(f"| P&L (estimated from Binance) | ${wm.total_estimated_pnl:,.2f} |")
    _w("")

    # ── Entry Timing ──
    _w("## Entry Timing")
    _w("")
    _w("| Metric | Value | Bot Parameter |")
    _w("|--------|-------|---------------|")
    _w(f"| Avg first buy | {_fmt_pct(wm.avg_first_buy_pct)} into period | `trading_window_start_pct` |")
    _w(f"| Median first buy | {_fmt_pct(wm.median_first_buy_pct)} into period | `trading_window_start_pct` |")
    _w(f"| Avg last buy | {_fmt_pct(wm.avg_last_buy_pct)} into period | `trading_window_end_pct` |")
    _w("")

    # Timing distribution
    if wm.period_metrics:
        first_pcts = [pm.first_buy_pct for pm in wm.period_metrics if pm.first_buy_pct is not None]
        if first_pcts:
            q1 = len([p for p in first_pcts if p < 0.25])
            q2 = len([p for p in first_pcts if 0.25 <= p < 0.50])
            q3 = len([p for p in first_pcts if 0.50 <= p < 0.75])
            q4 = len([p for p in first_pcts if p >= 0.75])
            _w("**First buy timing distribution:**")
            _w("")
            _w(f"- 0-25%: {q1} periods")
            _w(f"- 25-50%: {q2} periods")
            _w(f"- 50-75%: {q3} periods")
            _w(f"- 75-100%: {q4} periods")
            _w("")

    # ── Order Sizing ──
    _w("## Order Sizing")
    _w("")
    _w("| Metric | Value | Bot Parameter |")
    _w("|--------|-------|---------------|")
    _w(f"| Avg order size | {wm.avg_buy_size:.1f} shares | `base_order_shares` |")
    _w(f"| Median order size | {wm.median_buy_size:.1f} shares | `base_order_shares` |")
    _w(f"| Max order size | {wm.max_buy_size:.1f} shares | — |")
    _w("")

    # ── Ladder Levels ──
    _w("## Ladder Shape")
    _w("")
    _w("| Metric | Value | Bot Parameter |")
    _w("|--------|-------|---------------|")
    _w(f"| Avg price levels/side | {wm.avg_price_levels:.1f} | `ladder_levels` |")
    _w(f"| Max price levels/side | {wm.max_price_levels} | `ladder_levels` |")
    _w("")

    # ── Pair Completion ──
    _w("## Pair Completion")
    _w("")
    _w("| Metric | Value | Bot Parameter |")
    _w("|--------|-------|---------------|")
    _w(f"| Pair rate | {wm.pair_rate:.1%} | — |")
    _w(f"| Avg pair ratio (min/max shares) | {wm.avg_pair_ratio:.2f} | — |")
    _w(f"| Merge rate (of paired) | {wm.merge_rate:.1%} | `continuous_merge_enabled` |")
    if wm.avg_combined_cost is not None:
        _w(f"| Avg combined cost | {wm.avg_combined_cost:.4f} | `target_combined` |")
        _w(f"| Median combined cost | {wm.median_combined_cost:.4f} | `target_combined` |")
        spread = 1.0 - wm.avg_combined_cost
        _w(f"| Avg spread profit | {spread:.4f} (${spread:.2f}/pair) | — |")
    _w("")

    # ── Imbalance ──
    _w("## Imbalance")
    _w("")
    _w("| Metric | Value | Bot Parameter |")
    _w("|--------|-------|---------------|")
    _w(f"| Avg imbalance | {wm.avg_imbalance:.1f} shares | `max_share_imbalance` |")
    _w(f"| Max imbalance | {wm.max_imbalance:.1f} shares | `max_share_imbalance` |")
    _w(f"| P75 imbalance | {wm.p75_imbalance:.1f} shares | `max_share_imbalance` |")
    _w("")

    # ── Sell-back ──
    _w("## Sell-back")
    _w("")
    _w("| Metric | Value | Bot Parameter |")
    _w("|--------|-------|---------------|")
    _w(f"| Sell rate (periods with sells) | {wm.sell_rate:.1%} | — |")
    if wm.avg_sell_trigger_imbalance is not None:
        _w(f"| Avg imbalance at first sell | {wm.avg_sell_trigger_imbalance:.1f} shares | `sellback_min_excess` |")
    _w("")

    # ── P&L Distribution ──
    _w("## P&L Distribution")
    _w("")
    _w("| Metric | Value | Bot Parameter |")
    _w("|--------|-------|---------------|")
    _w(f"| Total estimated P&L | ${wm.total_estimated_pnl:,.2f} | — |")
    _w(f"| Win rate | {_fmt_pct(wm.win_rate)} | — |")
    _w(f"| Avg winning period | ${wm.avg_win_pnl:,.2f} | — |")
    _w(f"| Avg losing period | ${wm.avg_loss_pnl:,.2f} | `daily_loss_limit` |")
    _w(f"| P&L std dev | ${wm.pnl_std_dev:,.2f} | — |")
    _w("")

    # ── Per-Asset Breakdown ──
    if wm.per_asset:
        _w("## Per-Asset Breakdown")
        _w("")
        _w("| Asset | Periods | Trades | Pair Rate | Avg Combined | Est. P&L | Win Rate |")
        _w("|-------|---------|--------|-----------|-------------|----------|----------|")
        for asset, info in sorted(wm.per_asset.items()):
            _w(f"| {asset} "
               f"| {info['periods']} "
               f"| {info['trades']} "
               f"| {info['pair_rate']:.1%} "
               f"| {info.get('avg_combined_cost', 0):.4f} "
               f"| ${info.get('total_pnl', 0):,.2f} "
               f"| {_fmt_pct(info.get('win_rate'))} |")
        _w("")

    # ── Last 20 Periods Detail ──
    recent = sorted(
        [pm for pm in wm.period_metrics if pm.period_start],
        key=lambda pm: pm.period_start,
        reverse=True,
    )[:20]

    if recent:
        _w("## Recent Periods (last 20)")
        _w("")
        _w("| Time (UTC) | Asset | Dur | Buys | Up$ | Dn$ | Comb | Imbal | Sells | Merges | P&L |")
        _w("|------------|-------|-----|------|-----|-----|------|-------|-------|--------|-----|")
        for pm in recent:
            ts = pm.period_start.strftime("%m/%d %H:%M") if pm.period_start else "?"
            comb = f"{pm.combined_cost:.3f}" if pm.combined_cost is not None else "—"
            pnl = f"${pm.estimated_pnl:+.2f}" if pm.estimated_pnl is not None else "?"
            _w(f"| {ts} | {pm.asset} | {pm.duration_minutes}m "
               f"| {pm.total_buy_trades} "
               f"| {pm.avg_up_price:.2f} | {pm.avg_down_price:.2f} "
               f"| {comb} | {pm.share_imbalance:.0f} "
               f"| {pm.num_sells} | {pm.num_merges} | {pnl} |")
        _w("")

    # ── Bot Parameter Mapping Summary ──
    _w("## Bot Parameter Mapping")
    _w("")
    _w("| Parameter | Current v2.toml | This Wallet | Notes |")
    _w("|-----------|----------------|-------------|-------|")
    _w(f"| `trading_window_start_pct` | 0.35 | {_fmt_pct(wm.median_first_buy_pct)} | When first buys occur |")
    _w(f"| `trading_window_end_pct` | 0.60 | {_fmt_pct(wm.avg_last_buy_pct)} | When last buys occur |")
    _w(f"| `base_order_shares` | 20 | {wm.median_buy_size:.0f} | Median trade size |")
    _w(f"| `ladder_levels` | 8 | {wm.avg_price_levels:.0f} | Avg unique price levels |")
    _w(f"| `target_combined` | 0.95 | {wm.avg_combined_cost or 0:.4f} | Avg YES+NO cost |")
    _w(f"| `max_share_imbalance` | 100 | {wm.p75_imbalance:.0f} | P75 imbalance observed |")
    if wm.avg_sell_trigger_imbalance is not None:
        _w(f"| `sellback_min_excess` | 15 | {wm.avg_sell_trigger_imbalance:.0f} | Imbalance when sells start |")
    _w("")

    return "\n".join(lines)


def _fmt_pct(val: float | None) -> str:
    """Format a 0-1 float as percentage, or 'N/A'."""
    if val is None:
        return "N/A"
    return f"{val:.1%}"
