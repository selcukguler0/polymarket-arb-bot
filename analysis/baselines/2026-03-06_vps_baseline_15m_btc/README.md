# VPS baseline: 15m BTC control

Imported on `2026-03-06` from VPS.

Frozen evidence in this package:
- `vps_config_v2.toml`: deployed config used by `target/release/polymarket-arb config/v2.toml`
- `session_summary.csv`: raw BTC period history from VPS logs
- `learnings/`: operator notes written on the VPS during live tuning

Ground-truth context captured during import:
- Remote git commit: `6713d650ac3306eb6bbcdb1482ef9f87c41b1401`
- `session_summary.csv` rows: `103`
- Distinct `session_start` values: `21`
- Aggregate logged period PnL across the CSV: about `-19.70`
- The last positive segment was dominated by one outlier win, so it is not treated as proof that the strategy was fixed

Use this package as the control baseline for replay, paper, and shadow comparisons.
Do not live-tune against these files in place.
