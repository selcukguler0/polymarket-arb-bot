#!/usr/bin/env python3
from __future__ import annotations

import re
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
BASE_CONFIG = REPO_ROOT / "config" / "complete_set_shadow.toml"
SNAPSHOT_BIN = REPO_ROOT / "target" / "release" / "complete_set_snapshot"
ASSETS = ("BTC", "ETH", "SOL", "XRP")
DURATIONS = (5, 15, 60)


def duration_suffix(durations: tuple[int, ...]) -> str:
    return "_".join(f"{duration}m" for duration in durations)


def snapshot_report_path(asset: str, durations: tuple[int, ...]) -> Path:
    stamp = datetime.now(timezone.utc).date().isoformat()
    return (
        REPO_ROOT
        / "analysis"
        / "live"
        / f"{stamp}_complete_set_snapshot_{asset.lower()}_{duration_suffix(durations)}.md"
    )


def render_config(asset: str, durations: tuple[int, ...]) -> str:
    text = BASE_CONFIG.read_text()
    text = re.sub(r'^asset = "[A-Z]+"\s*$', f'asset = "{asset}"', text, count=1, flags=re.MULTILINE)
    duration_list = ", ".join(str(duration) for duration in durations)
    return re.sub(
        r"^allowed_durations = \[[0-9,\s]+\]\s*$",
        f"allowed_durations = [{duration_list}]",
        text,
        count=1,
        flags=re.MULTILINE,
    )


def run_snapshot(asset: str, durations: tuple[int, ...]) -> Path:
    rendered = render_config(asset, durations)
    with tempfile.NamedTemporaryFile("w", suffix=f"_{asset.lower()}_{duration_suffix(durations)}.toml", delete=False) as fh:
        fh.write(rendered)
        config_path = Path(fh.name)
    try:
        if SNAPSHOT_BIN.exists():
            subprocess.run([str(SNAPSHOT_BIN), str(config_path)], cwd=REPO_ROOT, check=True)
        else:
            subprocess.run(
                ["cargo", "run", "--release", "--bin", "complete_set_snapshot", "--", str(config_path)],
                cwd=REPO_ROOT,
                check=True,
            )
    finally:
        config_path.unlink(missing_ok=True)
    return snapshot_report_path(asset, durations)


def extract_metric(text: str, prefix: str) -> str:
    for line in text.splitlines():
        if line.startswith(prefix):
            return line.split("`", 2)[1]
    return "n/a"


def main() -> None:
    generated_at = datetime.now(timezone.utc).isoformat()
    out_dir = REPO_ROOT / "analysis" / "live"
    out_dir.mkdir(parents=True, exist_ok=True)
    sweep_path = out_dir / f"{datetime.now(timezone.utc).date().isoformat()}_complete_set_snapshot_matrix.md"

    rows: list[str] = []
    for asset in ASSETS:
        for duration in DURATIONS:
            durations = (duration,)
            report_path = run_snapshot(asset, durations)
            report = report_path.read_text() if report_path.exists() else ""
            rows.append(
                "| `{asset}` | `{duration}m` | {captured} | {scanned} | {visible} | {window} | {longs} | {shorts} | {path} |".format(
                    asset=asset,
                    duration=duration,
                    captured=extract_metric(report, "- Captured at:"),
                    scanned=extract_metric(report, "- Markets scanned:"),
                    visible=extract_metric(report, "- Markets with visible book data:"),
                    window=extract_metric(report, "- Markets in configured trade window now:"),
                    longs=extract_metric(report, "- Long signals now:"),
                    shorts=extract_metric(report, "- Short signals now:"),
                    path=report_path.relative_to(REPO_ROOT),
                )
            )

    lines = [
        "# Complete-Set Snapshot Matrix",
        "",
        f"- Generated at: `{generated_at}`",
        f"- Base config: `{BASE_CONFIG.relative_to(REPO_ROOT)}`",
        "",
        "| Asset | Duration | Captured At | Markets Scanned | Visible Books | In Window Now | Long Signals | Short Signals | Report |",
        "|---|---|---|---:|---:|---:|---:|---:|---|",
        *rows,
        "",
    ]
    sweep_path.write_text("\n".join(lines))
    print(sweep_path)


if __name__ == "__main__":
    main()
