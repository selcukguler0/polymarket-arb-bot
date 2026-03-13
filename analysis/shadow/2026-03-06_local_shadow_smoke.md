# Local Shadow Smoke

- Smoke date: `2026-03-06`
- Config: `config/rebuild_5m_shadow.toml`
- Verified run ID: `20260306T050055.096Z_shadow`
- Artifact root: `logs_shadow/`
- Shadow DB: `data/polymarket-arb-shadow.db`

## What was verified

- Shadow artifacts are isolated from live/baseline outputs:
  - `logs_shadow/manifests/20260306T050055.096Z_shadow.json`
  - `logs_shadow/BTC/session_summary.csv`
  - `data/polymarket-arb-shadow.db`
- The run manifest links the runtime to a concrete config hash, git SHA, wallet, enabled assets, enabled durations, and key risk settings.
- A zero-fill shutdown period now persists correctly on graceful termination:
  - `logs_shadow/BTC/2026-03-06_March_6_12-00AM-12-05AM_ET/period_result.csv`
  - `logs_shadow/BTC/session_summary.csv`
  - `period_results.run_id = 20260306T050055.096Z_shadow`
  - `equity_curve.event_type = shutdown`
- The 5m target ladder depth and activation throttle show up in telemetry:
  - `quote_levels_yes = 12`
  - `quote_levels_no = 12`
  - `suppression_reason_counts = buy_activation_throttle_5m:3`
- Shadow remained non-executing in the captured shutdown summary:
  - `orders_placed = 0`
  - `orders_filled = 0`
  - `cancel_all_count = 0`
  - `settlement_mode = none`

## Runtime notes

- A direct `Ctrl-C` through `cargo run` did not produce reliable shutdown artifacts during early smoke attempts.
- Graceful termination through `POST /api/terminate` produced the verified shutdown-period summary above.
- `resolution_safety_margin_secs` validation was relaxed for non-live modes to allow the `5m` shadow profile (`>= 20` for `paper`/`shadow`, `>= 120` for `live`).
