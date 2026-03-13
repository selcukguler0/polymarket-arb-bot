"""Report writers for Insider Finder v1."""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from .insider_types import AlertRow, RunSummary


def _fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _best_score(alert: AlertRow) -> float:
    return max(alert.insider_score, alert.manipulation_score)


def write_json_report(
    alerts: list[AlertRow],
    output_path: str,
    run_summary: RunSummary | None = None,
) -> str:
    """Write machine-readable JSON report."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "run_summary": asdict(run_summary) if run_summary else {},
        "alerts": [a.to_dict() for a in alerts],
    }

    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
    return str(path)


def write_markdown_report(
    alerts: list[AlertRow],
    output_path: str,
    run_summary: RunSummary | None = None,
) -> str:
    """Write analyst-facing markdown report."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append("# Insider Finder Alerts")
    lines.append("")

    lines.append("## Run Summary")
    if run_summary is None:
        lines.append("- No run summary available")
    else:
        lines.append(f"- Run ID: `{run_summary.run_id}`")
        lines.append(f"- Command: `{run_summary.command}`")
        lines.append(f"- Run Time: {run_summary.run_ts_utc}")
        lines.append(f"- As Of (unix): `{run_summary.as_of_ts}`")
        lines.append(f"- Selected Markets: {run_summary.selected_markets}")
        lines.append(f"- Trades Seen: {run_summary.trades_seen}")
        lines.append(f"- Trades Filtered: {run_summary.trades_filtered}")
        lines.append(f"- Trades Inserted: {run_summary.trades_inserted}")
        lines.append(f"- Trade Duplicates: {run_summary.trades_duplicates}")
        lines.append(f"- Candidate Trades: {run_summary.candidates}")
        lines.append(f"- Alerts Emitted: {run_summary.alerts}")
    lines.append("")

    lines.append("## Top Alerts")
    if not alerts:
        lines.append("No alerts met confidence/tier thresholds.")
    else:
        lines.append("| Rank | Tier | Class | Score | Market | Wallet | Trade Time |")
        lines.append("|---:|---|---|---:|---|---|---|")
        for i, a in enumerate(alerts, start=1):
            score = _best_score(a)
            lines.append(
                f"| {i} | {a.tier} | {a.classification} | {score:.3f} | "
                f"`{a.market_slug}` | `{a.wallet}` | {_fmt_ts(a.trade_ts)} |"
            )
    lines.append("")

    lines.append("## Top Suspicious Wallets")
    if not alerts:
        lines.append("No wallets ranked (no alerts).")
    else:
        wallet_stats: dict[str, dict] = {}
        for a in alerts:
            stat = wallet_stats.setdefault(
                a.wallet,
                {
                    "count": 0,
                    "t3": 0,
                    "t2": 0,
                    "max_score": 0.0,
                    "markets": set(),
                },
            )
            stat["count"] += 1
            if a.tier == "T3":
                stat["t3"] += 1
            if a.tier == "T2":
                stat["t2"] += 1
            stat["max_score"] = max(stat["max_score"], _best_score(a))
            stat["markets"].add(a.market_slug)

        sorted_wallets = sorted(
            wallet_stats.items(),
            key=lambda kv: (kv[1]["max_score"], kv[1]["t3"], kv[1]["count"]),
            reverse=True,
        )

        lines.append("| Wallet | Alerts | T3 | T2 | Max Score | Markets |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for wallet, stat in sorted_wallets[:20]:
            lines.append(
                f"| `{wallet}` | {stat['count']} | {stat['t3']} | {stat['t2']} | "
                f"{stat['max_score']:.3f} | {len(stat['markets'])} |"
            )
    lines.append("")

    lines.append("## Alert Rationales")
    if not alerts:
        lines.append("No rationale entries (no alerts).")
    else:
        for a in alerts[:50]:
            score = _best_score(a)
            lines.append(
                f"- `{a.alert_id}` [{a.tier}/{a.classification}] {score:.3f} "
                f"`{a.market_slug}` `{a.wallet}` {_fmt_ts(a.trade_ts)}"
            )
            if a.reasons:
                for r in a.reasons:
                    lines.append(f"  - {r}")
            else:
                lines.append("  - No explicit rationale generated")
    lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")
    return str(path)
