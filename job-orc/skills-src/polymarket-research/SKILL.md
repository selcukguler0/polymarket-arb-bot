---
name: polymarket-research
description: Continuous research workflow for Polymarket crypto strategy work. Use when the task is to gather live evidence, compare top wallets, refresh leaderboard or tracked-wallet reports, or synthesize non-code findings into dated reports and rolling memory without mutating runtime code.
---

# Polymarket Research

Use this skill for evidence collection and synthesis only.

## Rules

- Treat live data and primary evidence as the source of truth.
- Assume obvious complete-set mispricing is only one hypothesis, not the thesis.
- Do not mutate source code, configs, systemd units, or runtime services.
- Write outputs under `job-orc/reports/YYYY-MM-DD/` and `job-orc/memory/`.
- Separate direct evidence from inference.

## Active Research Surface

- Live reports: `analysis/live/`
- Shadow reports: `analysis/shadow/`, `analysis/vps_shadow/`
- Baseline evidence: `analysis/baselines/2026-03-06_vps_baseline_15m_btc/`
- Snapshot tools:
  - `analysis/tools/run_complete_set_snapshot_sweep.py`
  - `analysis/tools/run_complete_set_snapshot_matrix.py`
  - `analysis/tools/summarize_complete_set_shadow.py`
  - `analysis/tools/summarize_complete_set_shadow_matrix.py`
  - `analysis/tools/refresh_leaderboard_wallets.py`

## Research Priorities

1. Top-wallet microstructure and timing
2. Pair-completion, merge, redeem, and settlement behavior
3. Warehouse or inventory-cycling behavior
4. Leaderboard artifact, routing, or multi-wallet effects
5. Complete-set edge frequency across assets and durations

## Output Standard

- Keep reports concise and dated.
- Include timestamps and source paths.
- If evidence is weak, say so explicitly.
