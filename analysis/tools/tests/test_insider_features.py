from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.insider_features import compute_feature_rows
from lib.insider_types import TradeRow


def _load_fixture() -> list[TradeRow]:
    fixture = Path(__file__).resolve().parent / "fixtures" / "insider_fixture.jsonl"
    rows: list[TradeRow] = []
    for line in fixture.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        rows.append(
            TradeRow(
                tx_hash=raw["tx_hash"],
                wallet=raw["wallet"],
                ts=int(raw["ts"]),
                slug=raw["slug"],
                condition_id=raw["condition_id"],
                side=raw["side"],
                outcome=raw["outcome"],
                price=float(raw["price"]),
                size=float(raw["size"]),
                notional=float(raw["notional"]),
            )
        )
    return rows


def test_persistent_vs_reversal_markouts() -> None:
    trades = _load_fixture()
    market_liq = {
        "us-strikes-iran-by-february-28-2026": 10000.0,
        "missile-strike-middle-east-march-2026": 20000.0,
    }
    market_q = {
        "us-strikes-iran-by-february-28-2026": "US strikes Iran by Feb 28, 2026?",
        "missile-strike-middle-east-march-2026": "Missile strike in Middle East by March 2026?",
    }
    market_c = {
        "us-strikes-iran-by-february-28-2026": "0xcond_iran",
        "missile-strike-middle-east-march-2026": "0xcond_missile",
    }

    rows = compute_feature_rows(
        trades=trades,
        candidate_start_ts=1699999000,
        as_of_ts=1700005000,
        market_liquidity=market_liq,
        market_questions=market_q,
        market_conditions=market_c,
        apply_prefilter=False,
    )

    persistent = next(r for r in rows if r.wallet == "0xaaa111" and r.trade_ts == 1700000000)
    reversal = next(r for r in rows if r.wallet == "0xaaa222" and r.trade_ts == 1700003600)

    assert persistent.features["persistence_15m"] > persistent.features["reversal_15m"]
    assert reversal.features["reversal_15m"] >= 0.05


def test_low_data_excludes_missing_15m_markout() -> None:
    trades = [
        TradeRow(
            tx_hash="0x1",
            wallet="0xwallet",
            ts=1700000000,
            slug="test-market",
            condition_id="0xcond",
            side="BUY",
            outcome="Yes",
            price=0.20,
            size=100.0,
            notional=20.0,
        ),
        TradeRow(
            tx_hash="0x2",
            wallet="0xother",
            ts=1700000300,
            slug="test-market",
            condition_id="0xcond",
            side="BUY",
            outcome="Yes",
            price=0.25,
            size=10.0,
            notional=2.5,
        ),
    ]

    rows = compute_feature_rows(
        trades=trades,
        candidate_start_ts=1699999900,
        as_of_ts=1700000400,
        market_liquidity={"test-market": 10000.0},
        market_questions={"test-market": "Test market?"},
        market_conditions={"test-market": "0xcond"},
        apply_prefilter=False,
    )

    assert rows == []
