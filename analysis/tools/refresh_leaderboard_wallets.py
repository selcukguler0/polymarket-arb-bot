#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


USER_AGENT = "Mozilla/5.0 (compatible; CodexResearchBot/1.0; +https://openai.com)"
TRACKED_WALLET = os.environ.get("WALLET_ADDRESS", "0xYOUR_WALLET_ADDRESS")
LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard"
PROFILE_URL = "https://gamma-api.polymarket.com/public-profile"
POLYMARKET_PROFILE = "https://polymarket.com/profile/{wallet}"


def fetch_json(url: str) -> object:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def leaderboard_rows(limit: int) -> List[Dict]:
    query = urllib.parse.urlencode(
        {
            "category": "CRYPTO",
            "timePeriod": "MONTH",
            "sortBy": "PROFIT",
            "limit": str(limit),
            "offset": "0",
        }
    )
    data = fetch_json(f"{LEADERBOARD_URL}?{query}")
    return data if isinstance(data, list) else []


def tracked_wallet_row(wallet: str) -> Optional[Dict]:
    query = urllib.parse.urlencode(
        {
            "category": "CRYPTO",
            "timePeriod": "MONTH",
            "sortBy": "PROFIT",
            "user": wallet.lower(),
        }
    )
    data = fetch_json(f"{LEADERBOARD_URL}?{query}")
    if isinstance(data, list) and data:
        return data[0]
    return None


def public_profile(wallet: str) -> Dict:
    query = urllib.parse.urlencode({"address": wallet.lower()})
    data = fetch_json(f"{PROFILE_URL}?{query}")
    return data if isinstance(data, dict) else {}


def row_wallet(row: Dict) -> str:
    return (
        row.get("walletAddress")
        or row.get("wallet")
        or row.get("proxyWallet")
        or row.get("address")
        or ""
    )


def row_username(row: Dict) -> str:
    return row.get("name") or row.get("username") or row_wallet(row) or "unknown"


def render_report(rows: List[Dict], tracked_row: Optional[Dict], tracked_profile: Dict, top_limit: int) -> str:
    generated_at = datetime.now(timezone.utc).isoformat()
    lines = [
        "# Leaderboard Wallet Refresh",
        "",
        f"- Generated at: `{generated_at}`",
        f"- Source: [{LEADERBOARD_URL}]({LEADERBOARD_URL})",
        f"- Tracked wallet: `{TRACKED_WALLET}`",
        f"- Top rows requested: `{top_limit}`",
        "",
        "## Top Leaderboard Snapshot",
        "",
        "| Rank | Username | Profit | Wallet | Profile |",
        "|---|---|---:|---|---|",
    ]

    for idx, row in enumerate(rows, start=1):
        wallet = row_wallet(row)
        profit = row.get("profit", row.get("profitUsd", row.get("pnl", "n/a")))
        username = row_username(row)
        profile_link = POLYMARKET_PROFILE.format(wallet=wallet)
        lines.append(
            f"| {idx} | `{username}` | {profit} | `{wallet}` | [profile]({profile_link}) |"
        )

    lines.extend(
        [
            "",
            "## Tracked Wallet",
            "",
            f"- Wallet: `{TRACKED_WALLET}`",
            f"- Polymarket profile: [link]({POLYMARKET_PROFILE.format(wallet=TRACKED_WALLET)})",
        ]
    )

    if tracked_row:
        lines.extend(
            [
                f"- Leaderboard username: `{row_username(tracked_row)}`",
                f"- Leaderboard profit: `{tracked_row.get('profit', tracked_row.get('profitUsd', 'n/a'))}`",
                f"- Leaderboard rank: `{tracked_row.get('rank', 'n/a')}`",
            ]
        )
    else:
        lines.append("- Leaderboard row: `not returned by API`")

    if tracked_profile:
        lines.extend(
            [
                f"- Gamma username: `{tracked_profile.get('username', 'n/a')}`",
                f"- Gamma proxy wallet: `{tracked_profile.get('proxyWallet', 'n/a')}`",
                f"- Gamma created at: `{tracked_profile.get('createdAt', 'n/a')}`",
            ]
        )

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This report is a lightweight hourly refresh, not a full forensic audit.",
            "- Public usernames can be missing from the leaderboard API; profile resolution stays separate.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-limit", type=int, default=20)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    rows = leaderboard_rows(args.top_limit)
    tracked_row = tracked_wallet_row(TRACKED_WALLET)
    tracked_profile = public_profile(TRACKED_WALLET)

    out_path = args.out
    if out_path is None:
        out_dir = Path(__file__).resolve().parents[2] / "analysis" / "live"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{datetime.now(timezone.utc).date().isoformat()}_leaderboard_wallet_refresh.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(render_report(rows, tracked_row, tracked_profile, args.top_limit) + "\n")
    print(out_path)


if __name__ == "__main__":
    main()
