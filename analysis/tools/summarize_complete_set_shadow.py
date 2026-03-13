#!/usr/bin/env python3
from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]


def d(value: str) -> Decimal:
    return Decimal(value or "0")


@dataclass
class PeriodAgg:
    trade_window_polls: int = 0
    long_signal_polls: int = 0
    short_signal_polls: int = 0
    min_combined_ask: Decimal = Decimal("999")
    max_combined_bid: Decimal = Decimal("-999")
    best_long_edge: Decimal = Decimal("0")
    best_short_edge: Decimal = Decimal("0")


def main() -> None:
    log_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else REPO_ROOT / "logs_complete_set_shadow"
    scans_path = log_dir / "window_scans.csv"
    summary_path = log_dir / "session_summary.csv"
    out_dir = REPO_ROOT / "analysis" / "shadow"
    out_dir.mkdir(parents=True, exist_ok=True)
    suffix = log_dir.name.replace("/", "_")
    out_path = out_dir / (
        f"{datetime.now(timezone.utc).date().isoformat()}_complete_set_shadow_report_{suffix}.md"
    )

    periods: dict[str, PeriodAgg] = defaultdict(PeriodAgg)
    total_scans = 0
    total_long_signal_scans = 0
    total_short_signal_scans = 0

    if scans_path.exists():
        with scans_path.open() as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                period = row["period_name"]
                agg = periods[period]
                agg.trade_window_polls += 1
                total_scans += 1
                long_signal = row["long_signal"].lower() == "true"
                short_signal = row["short_signal"].lower() == "true"
                if long_signal:
                    agg.long_signal_polls += 1
                    total_long_signal_scans += 1
                if short_signal:
                    agg.short_signal_polls += 1
                    total_short_signal_scans += 1
                agg.min_combined_ask = min(agg.min_combined_ask, d(row["combined_ask"]))
                agg.max_combined_bid = max(agg.max_combined_bid, d(row["combined_bid"]))
                agg.best_long_edge = max(agg.best_long_edge, d(row["long_edge"]))
                agg.best_short_edge = max(agg.best_short_edge, d(row["short_edge"]))

    session_rows = []
    if summary_path.exists():
        with summary_path.open() as fh:
            session_rows = list(csv.DictReader(fh))

    lines = [
        "# Complete-Set Shadow Report",
        "",
        f"- Generated at: `{datetime.now(timezone.utc).isoformat()}`",
        f"- Log dir: `{log_dir}`",
        f"- Trade-window scans: `{total_scans}`",
        f"- Long signal scans: `{total_long_signal_scans}`",
        f"- Short signal scans: `{total_short_signal_scans}`",
        f"- Long signal scan rate: `{(total_long_signal_scans / total_scans):.4f}`" if total_scans else "- Long signal scan rate: `0.0000`",
        f"- Short signal scan rate: `{(total_short_signal_scans / total_scans):.4f}`" if total_scans else "- Short signal scan rate: `0.0000`",
        "",
        "## Periods",
        "",
        "| Period | Polls | Long Signal Polls | Short Signal Polls | Min Combined Ask | Max Combined Bid | Best Long Edge | Best Short Edge |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for period, agg in sorted(
        periods.items(),
        key=lambda item: (
            item[1].long_signal_polls + item[1].short_signal_polls,
            item[1].best_long_edge + item[1].best_short_edge,
            item[0],
        ),
        reverse=True,
    ):
        lines.append(
            f"| `{period}` | {agg.trade_window_polls} | {agg.long_signal_polls} | {agg.short_signal_polls} | "
            f"{agg.min_combined_ask} | {agg.max_combined_bid} | {agg.best_long_edge} | {agg.best_short_edge} |"
        )

    if session_rows:
        lines.extend(
            [
                "",
                "## Session Summary Rows",
                "",
                "| Period | Trade Window Polls | Opportunities | Trades | Last Long Edge | Last Short Edge |",
                "|---|---:|---:|---:|---:|---:|",
            ]
        )
        for row in session_rows:
            lines.append(
                f"| `{row['period_name']}` | {row.get('trade_window_polls', '0')} | {row.get('opportunities', '0')} | "
                f"{row.get('trades_this_period', '0')} | {row.get('last_long_edge', '0')} | {row.get('last_short_edge', '0')} |"
            )

    out_path.write_text("\n".join(lines) + "\n")
    print(out_path)


if __name__ == "__main__":
    main()
