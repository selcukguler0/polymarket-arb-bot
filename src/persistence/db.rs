use rusqlite::{params, Connection};
use rust_decimal::Decimal;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::error::{BotError, Result};
use crate::types::{Outcome, Position, TrackedMarket};

/// Row from period_results table
#[derive(Debug, Clone, serde::Serialize)]
pub struct PeriodResultRow {
    pub id: i64,
    pub run_id: Option<String>,
    pub period_name: String,
    pub condition_id: String,
    pub result: String,
    pub won: bool,
    pub pairs: i64,
    pub excess: i64,
    pub locked_profit: String,
    pub sell_pnl: String,
    pub merge_pnl: String,
    pub merge_pairs: i64,
    pub period_pnl: String,
    pub btc_open: Option<f64>,
    pub btc_close: Option<f64>,
    pub created_at: String,
    pub asset: String,
}

/// Row from session_stats table
#[derive(Debug, Clone)]
pub struct SessionStatsRow {
    pub total_pnl: String,
    pub today_pnl: String,
    pub today_date: Option<String>,
    pub wins: i64,
    pub losses: i64,
    pub total_periods: i64,
    pub total_fills: i64,
    pub total_merged_pairs: i64,
    pub total_rebates: String,
}

/// Row from equity_curve table
#[derive(Debug, Clone, serde::Serialize)]
pub struct EquityCurvePoint {
    pub timestamp: String,
    pub cumulative_pnl: String,
    pub event_type: Option<String>,
    pub run_id: Option<String>,
}

/// Thread-safe SQLite database wrapper.
/// All operations go through `spawn_blocking` to avoid blocking the tokio runtime.
pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

impl Database {
    /// Open (or create) the database at `path` and run migrations.
    pub async fn open(path: &str) -> Result<Self> {
        let path = path.to_string();
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection> {
            // Ensure parent directory exists
            if let Some(parent) = Path::new(&path).parent() {
                std::fs::create_dir_all(parent)?;
            }
            let conn = Connection::open(&path)?;
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA busy_timeout=5000;
                 PRAGMA synchronous=NORMAL;
                 PRAGMA foreign_keys=ON;",
            )?;
            Ok(conn)
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))??;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.run_migrations().await?;
        Ok(db)
    }

    async fn run_migrations(&self) -> Result<()> {
        let sql_001 = include_str!("../../migrations/001_init.sql");
        let sql_002 = include_str!("../../migrations/002_add_asset.sql");
        let sql_003 = include_str!("../../migrations/003_add_run_metadata.sql");
        let conn = self.conn.clone();
        let sql_001 = sql_001.to_string();
        let sql_002 = sql_002.to_string();
        let sql_003 = sql_003.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute_batch(&sql_001)?;
            // Migration 002: add asset column (safe to re-run — ALTER TABLE is idempotent if column exists)
            // Check if column already exists before adding
            let has_asset_col: bool = conn
                .prepare("SELECT asset FROM period_results LIMIT 0")
                .is_ok();
            if !has_asset_col {
                conn.execute_batch(&sql_002)?;
            }
            let has_run_id_col: bool = conn
                .prepare("SELECT run_id FROM period_results LIMIT 0")
                .is_ok();
            if !has_run_id_col {
                conn.execute_batch(&sql_003)?;
            }
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    // ── Markets ──

    pub async fn upsert_market(&self, market: &TrackedMarket) -> Result<()> {
        let conn = self.conn.clone();
        let m = market.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute(
                "INSERT INTO markets (condition_id, token_id_yes, token_id_no, question, end_date, tick_size, neg_risk, status)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'active')
                 ON CONFLICT(condition_id) DO UPDATE SET
                     status = 'active',
                     end_date = excluded.end_date",
                params![
                    m.condition_id,
                    m.token_id_yes,
                    m.token_id_no,
                    m.question,
                    m.end_date.to_rfc3339(),
                    m.tick_size.to_string(),
                    m.neg_risk as i32,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    pub async fn mark_market_resolved(&self, condition_id: &str) -> Result<()> {
        let conn = self.conn.clone();
        let cid = condition_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute(
                "UPDATE markets SET status = 'resolved' WHERE condition_id = ?1",
                params![cid],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    // ── Orders ──

    pub async fn insert_order(
        &self,
        order_id: &str,
        condition_id: &str,
        outcome: Outcome,
        price: Decimal,
        size: Decimal,
        side: &str,
    ) -> Result<()> {
        let conn = self.conn.clone();
        let oid = order_id.to_string();
        let cid = condition_id.to_string();
        let out = outcome.to_string();
        let p = price.to_string();
        let s = size.to_string();
        let sd = side.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute(
                "INSERT OR REPLACE INTO orders (order_id, condition_id, outcome, price, size, side, status)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'open')",
                params![oid, cid, out, p, s, sd],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    pub async fn update_order_status(&self, order_id: &str, status: &str) -> Result<()> {
        let conn = self.conn.clone();
        let oid = order_id.to_string();
        let st = status.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute(
                "UPDATE orders SET status = ?2, updated_at = datetime('now') WHERE order_id = ?1",
                params![oid, st],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    // ── Fills ──

    pub async fn insert_fill(
        &self,
        order_id: &str,
        condition_id: &str,
        outcome: Outcome,
        price: Decimal,
        size: Decimal,
        side: &str,
    ) -> Result<()> {
        let conn = self.conn.clone();
        let oid = order_id.to_string();
        let cid = condition_id.to_string();
        let out = outcome.to_string();
        let p = price.to_string();
        let s = size.to_string();
        let sd = side.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute(
                "INSERT INTO fills (order_id, condition_id, outcome, price, size, side)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![oid, cid, out, p, s, sd],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    /// BUG #9 fix: Write fill and position in a single SQLite transaction.
    /// If either fails, both are rolled back for crash consistency.
    pub async fn write_fill_and_position(
        &self,
        fill_order_id: &str,
        fill_condition_id: &str,
        fill_outcome: Outcome,
        fill_price: Decimal,
        fill_size: Decimal,
        fill_side: &str,
        position: &Position,
    ) -> Result<()> {
        let conn = self.conn.clone();
        let oid = fill_order_id.to_string();
        let cid = fill_condition_id.to_string();
        let out = fill_outcome.to_string();
        let p = fill_price.to_string();
        let s = fill_size.to_string();
        let sd = fill_side.to_string();
        let pos = position.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            let tx = conn.unchecked_transaction()
                .map_err(BotError::Database)?;

            tx.execute(
                "INSERT INTO fills (order_id, condition_id, outcome, price, size, side)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![oid, cid, out, p, s, sd],
            )
            .map_err(BotError::Database)?;

            tx.execute(
                "INSERT INTO positions (condition_id, yes_qty, no_qty, total_yes_spent, total_no_spent, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))
                 ON CONFLICT(condition_id) DO UPDATE SET
                     yes_qty = excluded.yes_qty,
                     no_qty = excluded.no_qty,
                     total_yes_spent = excluded.total_yes_spent,
                     total_no_spent = excluded.total_no_spent,
                     updated_at = datetime('now')",
                params![
                    pos.condition_id,
                    pos.yes_qty.to_string(),
                    pos.no_qty.to_string(),
                    pos.total_yes_spent.to_string(),
                    pos.total_no_spent.to_string(),
                ],
            )
            .map_err(BotError::Database)?;

            tx.commit().map_err(BotError::Database)?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    // ── Positions ──

    pub async fn upsert_position(&self, pos: &Position) -> Result<()> {
        let conn = self.conn.clone();
        let p = pos.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute(
                "INSERT INTO positions (condition_id, yes_qty, no_qty, total_yes_spent, total_no_spent, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))
                 ON CONFLICT(condition_id) DO UPDATE SET
                     yes_qty = excluded.yes_qty,
                     no_qty = excluded.no_qty,
                     total_yes_spent = excluded.total_yes_spent,
                     total_no_spent = excluded.total_no_spent,
                     updated_at = datetime('now')",
                params![
                    p.condition_id,
                    p.yes_qty.to_string(),
                    p.no_qty.to_string(),
                    p.total_yes_spent.to_string(),
                    p.total_no_spent.to_string(),
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    pub async fn get_position(&self, condition_id: &str) -> Result<Option<Position>> {
        let conn = self.conn.clone();
        let cid = condition_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<Position>> {
            let conn = conn.blocking_lock();
            let mut stmt = conn.prepare(
                "SELECT condition_id, yes_qty, no_qty, total_yes_spent, total_no_spent
                 FROM positions WHERE condition_id = ?1",
            )?;
            let mut rows = stmt.query_map(params![cid], |row| {
                Ok(Position {
                    condition_id: row.get(0)?,
                    yes_qty: Decimal::from_str(&row.get::<_, String>(1)?).unwrap_or_default(),
                    no_qty: Decimal::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
                    total_yes_spent: Decimal::from_str(&row.get::<_, String>(3)?)
                        .unwrap_or_default(),
                    total_no_spent: Decimal::from_str(&row.get::<_, String>(4)?)
                        .unwrap_or_default(),
                })
            })?;
            match rows.next() {
                Some(Ok(p)) => Ok(Some(p)),
                Some(Err(e)) => Err(BotError::Database(e)),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    pub async fn get_all_positions(&self) -> Result<Vec<Position>> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<Position>> {
            let conn = conn.blocking_lock();
            let mut stmt = conn.prepare(
                "SELECT condition_id, yes_qty, no_qty, total_yes_spent, total_no_spent
                 FROM positions",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(Position {
                    condition_id: row.get(0)?,
                    yes_qty: Decimal::from_str(&row.get::<_, String>(1)?).unwrap_or_default(),
                    no_qty: Decimal::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
                    total_yes_spent: Decimal::from_str(&row.get::<_, String>(3)?)
                        .unwrap_or_default(),
                    total_no_spent: Decimal::from_str(&row.get::<_, String>(4)?)
                        .unwrap_or_default(),
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(BotError::Database)
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    pub async fn delete_position(&self, condition_id: &str) -> Result<()> {
        let conn = self.conn.clone();
        let cid = condition_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute(
                "DELETE FROM positions WHERE condition_id = ?1",
                params![cid],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    // ── P&L Log ──

    pub async fn insert_pnl(
        &self,
        condition_id: &str,
        gross_pnl: Decimal,
        fees: Decimal,
        net_pnl: Decimal,
        winning_outcome: Option<Outcome>,
    ) -> Result<()> {
        let conn = self.conn.clone();
        let cid = condition_id.to_string();
        let gp = gross_pnl.to_string();
        let f = fees.to_string();
        let np = net_pnl.to_string();
        let wo = winning_outcome.map(|o| o.to_string());
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute(
                "INSERT INTO pnl_log (condition_id, gross_pnl, fees, net_pnl, winning_outcome)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![cid, gp, f, np, wo],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    /// Get total net P&L for a given date (resolved periods only — excludes in-flight sell P&L).
    /// For a complete picture including sell P&L, use `get_daily_pnl_full()`.
    pub async fn get_daily_pnl(&self, date: &str) -> Result<Decimal> {
        let conn = self.conn.clone();
        let d = date.to_string();
        tokio::task::spawn_blocking(move || -> Result<Decimal> {
            let conn = conn.blocking_lock();
            let mut stmt =
                conn.prepare("SELECT net_pnl FROM pnl_log WHERE date(resolved_at) = ?1")?;
            let rows = stmt.query_map(params![d], |row| row.get::<_, String>(0))?;
            let mut total = Decimal::ZERO;
            for row in rows {
                let s = row.map_err(BotError::Database)?;
                total += Decimal::from_str(&s).unwrap_or_default();
            }
            Ok(total)
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    /// Get the full daily P&L including in-flight sell P&L.
    /// Checks the `state` KV table for a persisted aggregate counter (which includes
    /// both resolved and sell P&L), falling back to `get_daily_pnl()` if not found.
    /// The aggregate counter is persisted at period resolution and graceful shutdown.
    pub async fn get_daily_pnl_full(&self, date: &str) -> Result<Decimal> {
        // First try the persisted aggregate (includes sell P&L)
        let key = format!("daily_pnl_cents_{date}");
        if let Some(cents_str) = self.get_state(&key).await? {
            if let Ok(cents) = cents_str.parse::<i64>() {
                return Ok(Decimal::from(cents) / Decimal::from(100));
            }
        }
        // Fallback: resolved-only P&L from pnl_log
        self.get_daily_pnl(date).await
    }

    /// Persist the aggregate daily P&L counter (in cents) to the state KV table.
    /// Called at period resolution and graceful shutdown so restarts include sell P&L.
    ///
    /// FIX: Uses monotonic update — only writes if the absolute magnitude of the
    /// new value is >= the stored value (i.e., losses get deeper, gains get higher).
    /// This prevents a stale read from one orchestrator overwriting a more recent
    /// value written by another orchestrator.
    pub async fn persist_daily_pnl_cents(&self, date: &str, cents: i64) -> Result<()> {
        let key = format!("daily_pnl_cents_{date}");
        // Read current stored value; only overwrite if new value has larger absolute magnitude.
        // This is safe because the shared counter only ever moves away from zero during a day
        // (sells add loss, resolutions add gain/loss), so |new| >= |stored| when new is fresher.
        if let Some(stored_str) = self.get_state(&key).await? {
            if let Ok(stored) = stored_str.parse::<i64>() {
                if cents.abs() < stored.abs() {
                    return Ok(()); // Stale: new value is closer to zero than stored
                }
            }
        }
        self.set_state(&key, &cents.to_string()).await
    }

    // ── State KV Store ──

    pub async fn set_state(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.conn.clone();
        let k = key.to_string();
        let v = value.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute(
                "INSERT INTO state (key, value, updated_at) VALUES (?1, ?2, datetime('now'))
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')",
                params![k, v],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    pub async fn get_state(&self, key: &str) -> Result<Option<String>> {
        let conn = self.conn.clone();
        let k = key.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<String>> {
            let conn = conn.blocking_lock();
            let result = conn.query_row(
                "SELECT value FROM state WHERE key = ?1",
                params![k],
                |row| row.get(0),
            );
            match result {
                Ok(v) => Ok(Some(v)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(BotError::Database(e)),
            }
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    // ── Period Results ──

    pub async fn insert_period_result(
        &self,
        period_name: &str,
        condition_id: &str,
        result: &str,
        won: bool,
        pairs: i64,
        excess: i64,
        locked_profit: Decimal,
        sell_pnl: Decimal,
        merge_pnl: Decimal,
        merge_pairs: i64,
        period_pnl: Decimal,
        btc_open: f64,
        btc_close: f64,
        run_id: &str,
        asset: &str,
    ) -> Result<()> {
        let conn = self.conn.clone();
        let pn = period_name.to_string();
        let cid = condition_id.to_string();
        let res = result.to_string();
        let lp = locked_profit.to_string();
        let sp = sell_pnl.to_string();
        let mp = merge_pnl.to_string();
        let pp = period_pnl.to_string();
        let run_id = run_id.to_string();
        let asset = asset.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute(
                "INSERT INTO period_results (period_name, condition_id, result, won, pairs, excess, locked_profit, sell_pnl, merge_pnl, merge_pairs, period_pnl, btc_open, btc_close, run_id, asset)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                params![pn, cid, res, won as i32, pairs, excess, lp, sp, mp, merge_pairs, pp, btc_open, btc_close, run_id, asset],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    pub async fn get_period_results(&self) -> Result<Vec<PeriodResultRow>> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<PeriodResultRow>> {
            let conn = conn.blocking_lock();
            let mut stmt = conn.prepare(
                "SELECT id, run_id, period_name, condition_id, result, won, pairs, excess, locked_profit, sell_pnl, merge_pnl, merge_pairs, period_pnl, btc_open, btc_close, created_at, asset
                 FROM period_results ORDER BY id ASC",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(PeriodResultRow {
                    id: row.get(0)?,
                    run_id: row.get(1)?,
                    period_name: row.get(2)?,
                    condition_id: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                    result: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                    won: row.get::<_, i32>(5)? != 0,
                    pairs: row.get(6)?,
                    excess: row.get(7)?,
                    locked_profit: row.get::<_, String>(8)?,
                    sell_pnl: row.get::<_, String>(9)?,
                    merge_pnl: row.get::<_, String>(10)?,
                    merge_pairs: row.get(11)?,
                    period_pnl: row.get::<_, String>(12)?,
                    btc_open: row.get(13)?,
                    btc_close: row.get(14)?,
                    created_at: row.get(15)?,
                    asset: row.get::<_, Option<String>>(16)?.unwrap_or_else(|| "BTC".to_string()),
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(BotError::Database)
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    // ── Session Stats ──

    pub async fn get_session_stats(&self) -> Result<SessionStatsRow> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<SessionStatsRow> {
            let conn = conn.blocking_lock();
            // Ensure row exists
            conn.execute(
                "INSERT OR IGNORE INTO session_stats (id) VALUES (1)",
                [],
            )?;
            conn.query_row(
                "SELECT total_pnl, today_pnl, today_date, wins, losses, total_periods, total_fills, total_merged_pairs, total_rebates
                 FROM session_stats WHERE id = 1",
                [],
                |row| {
                    Ok(SessionStatsRow {
                        total_pnl: row.get(0)?,
                        today_pnl: row.get(1)?,
                        today_date: row.get(2)?,
                        wins: row.get(3)?,
                        losses: row.get(4)?,
                        total_periods: row.get(5)?,
                        total_fills: row.get(6)?,
                        total_merged_pairs: row.get(7)?,
                        total_rebates: row.get(8)?,
                    })
                },
            )
            .map_err(BotError::Database)
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    pub async fn record_period_in_session_stats(
        &self,
        period_pnl: Decimal,
        won: bool,
        fills: i64,
        merged_pairs: i64,
        today_date: &str,
    ) -> Result<()> {
        let conn = self.conn.clone();
        let pp = period_pnl.to_string();
        let td = today_date.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            // Ensure row exists
            conn.execute("INSERT OR IGNORE INTO session_stats (id) VALUES (1)", [])?;

            // Check if today_date changed (midnight reset)
            let stored_date: Option<String> = conn
                .query_row(
                    "SELECT today_date FROM session_stats WHERE id = 1",
                    [],
                    |row| row.get(0),
                )
                .map_err(BotError::Database)?;

            if stored_date.as_deref() != Some(&td) {
                // New day: reset today_pnl
                conn.execute(
                    "UPDATE session_stats SET today_pnl = '0', today_date = ?1 WHERE id = 1",
                    params![td],
                )
                .map_err(BotError::Database)?;
            }

            let win_inc = if won { 1 } else { 0 };
            let loss_inc = if won { 0 } else { 1 };

            // Atomic update: read current values, add new, write back
            // Using SQL arithmetic on TEXT requires CAST
            let tx = conn.unchecked_transaction().map_err(BotError::Database)?;

            // Read current totals
            let (current_total_pnl_str, current_today_pnl_str): (String, String) = tx
                .query_row(
                    "SELECT total_pnl, today_pnl FROM session_stats WHERE id = 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(BotError::Database)?;

            let current_total = Decimal::from_str(&current_total_pnl_str).unwrap_or_default();
            let current_today = Decimal::from_str(&current_today_pnl_str).unwrap_or_default();
            let pnl_dec = Decimal::from_str(&pp).unwrap_or_default();
            let new_total = (current_total + pnl_dec).to_string();
            let new_today = (current_today + pnl_dec).to_string();

            tx.execute(
                "UPDATE session_stats SET
                    total_pnl = ?1,
                    today_pnl = ?2,
                    wins = wins + ?3,
                    losses = losses + ?4,
                    total_periods = total_periods + 1,
                    total_fills = total_fills + ?5,
                    total_merged_pairs = total_merged_pairs + ?6
                 WHERE id = 1",
                params![new_total, new_today, win_inc, loss_inc, fills, merged_pairs],
            )
            .map_err(BotError::Database)?;

            tx.commit().map_err(BotError::Database)?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    pub async fn reset_today_pnl_if_new_day(&self, today_date: &str) -> Result<()> {
        let conn = self.conn.clone();
        let td = today_date.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute("INSERT OR IGNORE INTO session_stats (id) VALUES (1)", [])?;
            let stored_date: Option<String> = conn
                .query_row(
                    "SELECT today_date FROM session_stats WHERE id = 1",
                    [],
                    |row| row.get(0),
                )
                .map_err(BotError::Database)?;

            if stored_date.as_deref() != Some(&td) {
                conn.execute(
                    "UPDATE session_stats SET today_pnl = '0', today_date = ?1 WHERE id = 1",
                    params![td],
                )
                .map_err(BotError::Database)?;
            }
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    pub async fn increment_session_fills(&self, count: i64) -> Result<()> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute("INSERT OR IGNORE INTO session_stats (id) VALUES (1)", [])?;
            conn.execute(
                "UPDATE session_stats SET total_fills = total_fills + ?1 WHERE id = 1",
                params![count],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    // ── Equity Curve ──

    pub async fn insert_equity_point(
        &self,
        cumulative_pnl: Decimal,
        event_type: &str,
        run_id: &str,
        asset: &str,
    ) -> Result<()> {
        let conn = self.conn.clone();
        let cp = cumulative_pnl.to_string();
        let et = event_type.to_string();
        let run_id = run_id.to_string();
        let asset = asset.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute(
                "INSERT INTO equity_curve (cumulative_pnl, event_type, run_id, asset)
                 VALUES (?1, ?2, ?3, ?4)",
                params![cp, et, run_id, asset],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    pub async fn get_equity_curve(&self) -> Result<Vec<EquityCurvePoint>> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<EquityCurvePoint>> {
            let conn = conn.blocking_lock();
            let mut stmt = conn.prepare(
                "SELECT timestamp, cumulative_pnl, event_type, run_id FROM equity_curve ORDER BY id ASC",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(EquityCurvePoint {
                    timestamp: row.get(0)?,
                    cumulative_pnl: row.get(1)?,
                    event_type: row.get(2)?,
                    run_id: row.get(3)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(BotError::Database)
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    /// Get today's fill count from the fills table
    pub async fn get_today_fill_count(&self, today_date: &str) -> Result<i64> {
        let conn = self.conn.clone();
        let td = today_date.to_string();
        tokio::task::spawn_blocking(move || -> Result<i64> {
            let conn = conn.blocking_lock();
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM fills WHERE date(filled_at) = ?1",
                    params![td],
                    |row| row.get(0),
                )
                .map_err(BotError::Database)?;
            Ok(count)
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    /// Get open order IDs for a market, grouped by outcome
    pub async fn get_open_orders(&self, condition_id: &str) -> Result<(Vec<String>, Vec<String>)> {
        let conn = self.conn.clone();
        let cid = condition_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<(Vec<String>, Vec<String>)> {
            let conn = conn.blocking_lock();
            let mut stmt = conn.prepare(
                "SELECT order_id, outcome FROM orders WHERE condition_id = ?1 AND status IN ('open', 'Live', 'delayed', 'unmatched')",
            )?;
            let mut yes_ids = Vec::new();
            let mut no_ids = Vec::new();
            let rows = stmt.query_map(params![cid], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                let (oid, outcome) = row?;
                match outcome.as_str() {
                    "yes" => yes_ids.push(oid),
                    "no" => no_ids.push(oid),
                    _ => {}
                }
            }
            Ok((yes_ids, no_ids))
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }

    /// Cancel all open orders for a market in the DB
    pub async fn cancel_all_orders_for_market(&self, condition_id: &str) -> Result<()> {
        let conn = self.conn.clone();
        let cid = condition_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn.blocking_lock();
            conn.execute(
                "UPDATE orders SET status = 'cancelled', updated_at = datetime('now')
                 WHERE condition_id = ?1 AND status IN ('open', 'Live', 'delayed', 'unmatched')",
                params![cid],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| BotError::Database(rusqlite::Error::InvalidParameterName(e.to_string())))?
    }
}
