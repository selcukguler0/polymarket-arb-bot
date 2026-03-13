---
name: polymarket-shadow-ops
description: Shadow runtime operations for the Polymarket research system. Use when the task is to run, inspect, summarize, or operate shadow observers and related reports without placing live trades or mutating strategy code.
---

# Polymarket Shadow Ops

Use this skill for runtime-safe observation only.

## Rules

- Shadow only. No live order placement, no merge, no redeem, no wallet mutation.
- Prefer the dedicated runtime repo or runtime workspace when operating observers.
- Do not change strategy code from the runtime workspace.
- Keep outputs in logs, `analysis/shadow/`, `analysis/vps_shadow/`, and `job-orc/reports/YYYY-MM-DD/`.

## Commands And Files

- Shadow session wrapper: `scripts/run_complete_set_shadow_session.sh`
- Runtime worker wrapper: `scripts/run_complete_set_shadow_worker.sh`
- Shadow summary tools:
  - `analysis/tools/summarize_complete_set_shadow.py`
  - `analysis/tools/summarize_complete_set_shadow_matrix.py`
- Shadow configs:
  - `config/complete_set_shadow.toml`
  - `config/complete_set_shadow_eth.toml`
  - `config/complete_set_shadow_sol.toml`
  - `config/complete_set_shadow_xrp.toml`

## Required Checks

- Confirm the service or process is shadow-only.
- Confirm logs are asset-scoped.
- Confirm reports are refreshed without restarting unrelated services.
