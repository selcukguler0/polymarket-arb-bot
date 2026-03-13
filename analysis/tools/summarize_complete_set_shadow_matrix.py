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
class AssetAgg:
    scans: int = 0
    long_signal_scans: int = 0
    short_signal_scans: int = 0
    min_combined_ask: Decimal = Decimal("999")
    max_combined_bid: Decimal = Decimal("-999")
    best_long_edge: Decimal = Decimal("0")
    best_short_edge: Decimal = Decimal("0")


def asset_name_from_dir(log_dir: Path) -> str:
    name = log_dir.name
    if name == "logs_complete_set_shadow":
        return "BTC_legacy"
    return name.removeprefix("logs_complete_set_shadow_").upper()


def summarize_log_dir(log_dir: Path) -> AssetAgg:
    agg = AssetAgg()
    scans_path = log_dir / "window_scans.csv"
    if not scans_path.exists():
        return agg

    with scans_path.open() as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            agg.scans += 1
            long_signal = row["long_signal"].lower() == "true"
            short_signal = row["short_signal"].lower() == "true"
            if long_signal:
                agg.long_signal_scans += 1
            if short_signal:
                agg.short_signal_scans += 1
            agg.min_combined_ask = min(agg.min_combined_ask, d(row["combined_ask"]))
            agg.max_combined_bid = max(agg.max_combined_bid, d(row["combined_bid"]))
            agg.best_long_edge = max(agg.best_long_edge, d(row["long_edge"]))
            agg.best_short_edge = max(agg.best_short_edge, d(row["short_edge"]))
    return agg


def main() -> None:
    log_dirs = [Path(arg) for arg in sys.argv[1:]]
    if not log_dirs:
        log_dirs = sorted(REPO_ROOT.glob("logs_complete_set_shadow*"))

    out_dir = REPO_ROOT / "analysis" / "shadow"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{datetime.now(timezone.utc).date().isoformat()}_complete_set_shadow_matrix.md"

    rows = []
    for log_dir in log_dirs:
        agg = summarize_log_dir(log_dir)
        asset = asset_name_from_dir(log_dir)
        rows.append(
            (
                asset,
                agg.scans,
                agg.long_signal_scans,
                agg.short_signal_scans,
                agg.min_combined_ask if agg.scans else Decimal("0"),
                agg.max_combined_bid if agg.scans else Decimal("0"),
                agg.best_long_edge,
                agg.best_short_edge,
                log_dir,
            )
        )

    lines = [
        "# Complete-Set Shadow Matrix",
        "",
        f"- Generated at: `{datetime.now(timezone.utc).isoformat()}`",
        "",
        "| Asset | Scans | Long Signal Scans | Short Signal Scans | Min Combined Ask | Max Combined Bid | Best Long Edge | Best Short Edge | Log Dir |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for asset, scans, long_scans, short_scans, min_ask, max_bid, best_long, best_short, log_dir in rows:
        lines.append(
            f"| `{asset}` | {scans} | {long_scans} | {short_scans} | {min_ask} | {max_bid} | {best_long} | {best_short} | `{log_dir}` |"
        )

    out_path.write_text("\n".join(lines) + "\n")
    print(out_path)


if __name__ == "__main__":
    main()
