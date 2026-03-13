"""SQLite persistence for Insider Finder v1."""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .insider_types import AlertRow, FeatureRow, MarketMeta, TradeRow


def _utc_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class InsiderStore:
    """SQLite-backed store for runs, snapshots, features, and alerts."""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.init_schema()

    def close(self) -> None:
        self.conn.close()

    def init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                run_ts INTEGER NOT NULL,
                as_of_ts INTEGER NOT NULL,
                params_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS markets (
                run_id TEXT NOT NULL,
                slug TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                question TEXT NOT NULL,
                end_date TEXT,
                liquidity REAL NOT NULL,
                volume REAL NOT NULL,
                active INTEGER NOT NULL,
                closed INTEGER NOT NULL,
                FOREIGN KEY(run_id) REFERENCES runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS trades (
                tx_hash TEXT NOT NULL,
                wallet TEXT NOT NULL,
                ts INTEGER NOT NULL,
                slug TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                side TEXT NOT NULL,
                outcome TEXT NOT NULL,
                price REAL NOT NULL,
                size REAL NOT NULL,
                notional REAL NOT NULL,
                run_id TEXT NOT NULL,
                PRIMARY KEY (tx_hash, wallet, side, price, size, ts),
                FOREIGN KEY(run_id) REFERENCES runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS features (
                run_id TEXT NOT NULL,
                trade_key TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                wallet TEXT NOT NULL,
                feature_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS alerts (
                run_id TEXT NOT NULL,
                alert_id TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                wallet TEXT NOT NULL,
                classification TEXT NOT NULL,
                tier TEXT NOT NULL,
                insider_score REAL NOT NULL,
                manipulation_score REAL NOT NULL,
                alert_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES runs(run_id)
            );

            CREATE INDEX IF NOT EXISTS idx_runs_ts ON runs(run_ts);
            CREATE INDEX IF NOT EXISTS idx_markets_run ON markets(run_id);
            CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts);
            CREATE INDEX IF NOT EXISTS idx_trades_slug_ts ON trades(slug, ts);
            CREATE INDEX IF NOT EXISTS idx_trades_wallet_ts ON trades(wallet, ts);
            CREATE INDEX IF NOT EXISTS idx_features_run ON features(run_id);
            CREATE INDEX IF NOT EXISTS idx_alerts_run ON alerts(run_id);
            """
        )
        self.conn.commit()

    def begin_run(
        self,
        command: str,
        as_of_ts: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> tuple[str, int]:
        run_id = uuid.uuid4().hex
        now_ts = int(time.time())
        if as_of_ts is None:
            as_of_ts = now_ts
        if params is None:
            params = {}
        self.conn.execute(
            """
            INSERT INTO runs(run_id, command, run_ts, as_of_ts, params_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, command, now_ts, int(as_of_ts), json.dumps(params, sort_keys=True)),
        )
        self.conn.commit()
        return run_id, now_ts

    def insert_markets(self, run_id: str, markets: list[MarketMeta]) -> int:
        rows = [
            (
                run_id,
                m.slug,
                m.condition_id,
                m.question,
                m.end_date,
                float(m.liquidity),
                float(m.volume),
                int(bool(m.active)),
                int(bool(m.closed)),
            )
            for m in markets
        ]
        self.conn.executemany(
            """
            INSERT INTO markets(
                run_id, slug, condition_id, question, end_date, liquidity, volume, active, closed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()
        return len(rows)

    def insert_trades(self, run_id: str, trades: list[TradeRow]) -> tuple[int, int]:
        if not trades:
            return 0, 0

        before = self.conn.total_changes
        for t in trades:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO trades(
                    tx_hash, wallet, ts, slug, condition_id, side, outcome,
                    price, size, notional, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    t.tx_hash,
                    t.wallet,
                    int(t.ts),
                    t.slug,
                    t.condition_id,
                    t.side,
                    t.outcome,
                    float(t.price),
                    float(t.size),
                    float(t.notional),
                    run_id,
                ),
            )
        self.conn.commit()
        inserted = self.conn.total_changes - before
        duplicates = len(trades) - inserted
        return inserted, duplicates

    def insert_features(self, run_id: str, feature_rows: list[FeatureRow]) -> int:
        if not feature_rows:
            return 0
        self.conn.executemany(
            """
            INSERT INTO features(run_id, trade_key, market_slug, wallet, feature_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    row.trade_key,
                    row.market_slug,
                    row.wallet,
                    json.dumps(row.features, sort_keys=True),
                )
                for row in feature_rows
            ],
        )
        self.conn.commit()
        return len(feature_rows)

    def insert_alerts(self, run_id: str, alerts: list[AlertRow]) -> int:
        if not alerts:
            return 0
        self.conn.executemany(
            """
            INSERT INTO alerts(
                run_id, alert_id, market_slug, wallet, classification,
                tier, insider_score, manipulation_score, alert_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    a.alert_id,
                    a.market_slug,
                    a.wallet,
                    a.classification,
                    a.tier,
                    float(a.insider_score),
                    float(a.manipulation_score),
                    json.dumps(a.to_dict(), sort_keys=True),
                )
                for a in alerts
            ],
        )
        self.conn.commit()
        return len(alerts)

    def latest_markets(self, as_of_ts: int | None = None) -> list[MarketMeta]:
        if as_of_ts is None:
            row = self.conn.execute(
                """
                SELECT run_id FROM runs
                WHERE run_id IN (SELECT DISTINCT run_id FROM markets)
                ORDER BY run_ts DESC
                LIMIT 1
                """
            ).fetchone()
        else:
            row = self.conn.execute(
                """
                SELECT run_id FROM runs
                WHERE run_id IN (SELECT DISTINCT run_id FROM markets)
                  AND as_of_ts <= ?
                ORDER BY as_of_ts DESC, run_ts DESC
                LIMIT 1
                """,
                (int(as_of_ts),),
            ).fetchone()

        if row is None:
            return []

        run_id = row["run_id"]
        rows = self.conn.execute(
            """
            SELECT slug, condition_id, question, end_date, liquidity, volume, active, closed
            FROM markets
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchall()

        markets: list[MarketMeta] = []
        for r in rows:
            markets.append(
                MarketMeta(
                    slug=r["slug"],
                    condition_id=r["condition_id"],
                    question=r["question"],
                    end_date=r["end_date"] or "",
                    liquidity=float(r["liquidity"] or 0.0),
                    volume=float(r["volume"] or 0.0),
                    active=bool(r["active"]),
                    closed=bool(r["closed"]),
                )
            )
        return markets

    def load_trades(
        self,
        slugs: list[str],
        start_ts: int,
        end_ts: int,
    ) -> list[TradeRow]:
        if not slugs:
            return []
        placeholders = ",".join(["?"] * len(slugs))
        q = (
            "SELECT tx_hash, wallet, ts, slug, condition_id, side, outcome, "
            "price, size, notional, run_id FROM trades "
            f"WHERE ts >= ? AND ts <= ? AND slug IN ({placeholders}) "
            "ORDER BY ts ASC"
        )
        rows = self.conn.execute(q, [int(start_ts), int(end_ts), *slugs]).fetchall()
        trades: list[TradeRow] = []
        for r in rows:
            trades.append(
                TradeRow(
                    tx_hash=r["tx_hash"],
                    wallet=r["wallet"],
                    ts=int(r["ts"]),
                    slug=r["slug"],
                    condition_id=r["condition_id"],
                    side=r["side"],
                    outcome=r["outcome"],
                    price=float(r["price"]),
                    size=float(r["size"]),
                    notional=float(r["notional"]),
                    run_id=r["run_id"],
                )
            )
        return trades

    def count_trades(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS c FROM trades").fetchone()
        return int(row["c"] if row else 0)

    def market_maps(self, as_of_ts: int | None = None) -> tuple[dict[str, float], dict[str, str], dict[str, str]]:
        """Return (liquidity_by_slug, question_by_slug, condition_by_slug)."""
        markets = self.latest_markets(as_of_ts=as_of_ts)
        liquidity: dict[str, float] = {}
        question: dict[str, str] = {}
        condition: dict[str, str] = {}
        for m in markets:
            liquidity[m.slug] = float(m.liquidity)
            question[m.slug] = m.question
            condition[m.slug] = m.condition_id
        return liquidity, question, condition

    def run_record(self, run_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT run_id, command, run_ts, as_of_ts, params_json FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "run_id": row["run_id"],
            "command": row["command"],
            "run_ts": int(row["run_ts"]),
            "run_ts_utc": _utc_iso(int(row["run_ts"])),
            "as_of_ts": int(row["as_of_ts"]),
            "params": json.loads(row["params_json"]),
        }
