-- Markets table
CREATE TABLE IF NOT EXISTS markets (
    condition_id TEXT PRIMARY KEY,
    token_id_yes TEXT NOT NULL,
    token_id_no  TEXT NOT NULL,
    question     TEXT NOT NULL,
    end_date     TEXT NOT NULL,
    tick_size    TEXT NOT NULL,
    neg_risk     INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL DEFAULT 'active'
);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id     TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL REFERENCES markets(condition_id),
    outcome      TEXT NOT NULL CHECK (outcome IN ('yes', 'no')),
    price        TEXT NOT NULL,
    size         TEXT NOT NULL,
    side         TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    status       TEXT NOT NULL DEFAULT 'open',
    filled_size  TEXT NOT NULL DEFAULT '0',
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_orders_condition_status ON orders(condition_id, status);

-- Fills table
CREATE TABLE IF NOT EXISTS fills (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id     TEXT NOT NULL REFERENCES orders(order_id),
    condition_id TEXT NOT NULL,
    outcome      TEXT NOT NULL CHECK (outcome IN ('yes', 'no')),
    price        TEXT NOT NULL,
    size         TEXT NOT NULL,
    side         TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    filled_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_fills_condition ON fills(condition_id);

-- Positions table
CREATE TABLE IF NOT EXISTS positions (
    condition_id    TEXT PRIMARY KEY,
    yes_qty         TEXT NOT NULL DEFAULT '0',
    no_qty          TEXT NOT NULL DEFAULT '0',
    total_yes_spent TEXT NOT NULL DEFAULT '0',
    total_no_spent  TEXT NOT NULL DEFAULT '0',
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

-- P&L log
CREATE TABLE IF NOT EXISTS pnl_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id    TEXT NOT NULL,
    gross_pnl       TEXT NOT NULL,
    fees            TEXT NOT NULL DEFAULT '0',
    net_pnl         TEXT NOT NULL,
    winning_outcome TEXT,
    resolved_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_pnl_resolved ON pnl_log(resolved_at);

-- Key-value state store
CREATE TABLE IF NOT EXISTS state (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Per-period results (one row per completed period)
CREATE TABLE IF NOT EXISTS period_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    period_name     TEXT NOT NULL,
    condition_id    TEXT,
    result          TEXT,
    won             INTEGER NOT NULL DEFAULT 0,
    pairs           INTEGER NOT NULL DEFAULT 0,
    excess          INTEGER NOT NULL DEFAULT 0,
    locked_profit   TEXT NOT NULL DEFAULT '0',
    sell_pnl        TEXT NOT NULL DEFAULT '0',
    merge_pnl       TEXT NOT NULL DEFAULT '0',
    merge_pairs     INTEGER NOT NULL DEFAULT 0,
    period_pnl      TEXT NOT NULL DEFAULT '0',
    btc_open        REAL,
    btc_close       REAL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Running session stats (single row, updated in place)
CREATE TABLE IF NOT EXISTS session_stats (
    id                  INTEGER PRIMARY KEY DEFAULT 1,
    total_pnl           TEXT NOT NULL DEFAULT '0',
    today_pnl           TEXT NOT NULL DEFAULT '0',
    today_date          TEXT,
    wins                INTEGER NOT NULL DEFAULT 0,
    losses              INTEGER NOT NULL DEFAULT 0,
    total_periods       INTEGER NOT NULL DEFAULT 0,
    total_fills         INTEGER NOT NULL DEFAULT 0,
    total_merged_pairs  INTEGER NOT NULL DEFAULT 0,
    total_rebates       TEXT NOT NULL DEFAULT '0'
);

-- Equity curve data points
CREATE TABLE IF NOT EXISTS equity_curve (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       TEXT NOT NULL DEFAULT (datetime('now')),
    cumulative_pnl  TEXT NOT NULL,
    event_type      TEXT
);
