from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.insider_store import InsiderStore
from lib.insider_types import MarketMeta, TradeRow


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


def test_storage_idempotency(tmp_path: Path) -> None:
    db_path = tmp_path / "insider_test.db"
    store = InsiderStore(str(db_path))
    try:
        markets = [
            MarketMeta(
                slug="us-strikes-iran-by-february-28-2026",
                condition_id="0xcond_iran",
                question="US strikes Iran by Feb 28, 2026?",
                end_date="2026-02-28T23:59:59Z",
                liquidity=10000.0,
                volume=50000.0,
                active=True,
                closed=False,
            ),
            MarketMeta(
                slug="missile-strike-middle-east-march-2026",
                condition_id="0xcond_missile",
                question="Missile strike in Middle East by March 2026?",
                end_date="2026-03-31T23:59:59Z",
                liquidity=20000.0,
                volume=45000.0,
                active=True,
                closed=False,
            ),
        ]
        trades = _load_fixture()

        run1, _ = store.begin_run("snapshot", as_of_ts=1700005000, params={"test": 1})
        store.insert_markets(run1, markets)
        inserted1, duplicates1 = store.insert_trades(run1, trades)

        assert inserted1 == len(trades)
        assert duplicates1 == 0
        assert store.count_trades() == len(trades)

        run2, _ = store.begin_run("snapshot", as_of_ts=1700005000, params={"test": 2})
        inserted2, duplicates2 = store.insert_trades(run2, trades)

        assert inserted2 == 0
        assert duplicates2 == len(trades)
        assert store.count_trades() == len(trades)
    finally:
        store.close()
