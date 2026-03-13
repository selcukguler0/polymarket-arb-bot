---
name: polymarket-controller
description: Controller workflow for the Polymarket Codex queue. Use when the task is to evaluate current evidence, maintain the task board, prioritize hypotheses, queue follow-up research or build work, and keep the autonomous loop aligned with safety boundaries.
---

# Polymarket Controller

Use this skill to steer the queue, not to write production code.

## Rules

- Prefer evidence-backed prioritization over intuition.
- Treat complete-set mispricing as one hypothesis among several.
- Queue build work only when evidence exists in at least two source artifacts.
- Queue research work when the current thesis is weak, sparse, or contradicted.
- Write controller outputs to `job-orc/reports/YYYY-MM-DD/` and `job-orc/memory/`.

## Inputs

- `job-orc/tasks.json`
- latest reports under `job-orc/reports/`
- active live/shadow/baseline artifacts

## Required Outputs

- `hypothesis_scoreboard.md`
- `build_candidates.md`
- task upserts with the new task schema

## Controller Heuristics

- If complete-set signals stay zero across assets and durations, deprioritize that branch and queue wallet-behavior and microstructure research.
- If a repo or runtime bug is directly evidenced, queue a build task with branch mutation mode.
- Keep tasks small, explicit, and source-linked.
