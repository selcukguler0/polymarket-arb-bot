---
name: polymarket-build
description: Evidence-backed implementation workflow for the Polymarket research system. Use when the task is to make code changes from research evidence, run checks, commit on a codex/auto branch, and write a dated branch summary report.
---

# Polymarket Build

Use this skill only for implementation tasks backed by research evidence.

## Rules

- Never mutate `main`.
- Work only on `codex/auto/*` branches.
- Consume only tasks with at least two source artifacts and `evidence_score >= 2`.
- Do not touch runtime observer services.
- Keep changes scoped to the task.

## Workflow

1. Read the task and its source artifacts.
2. Verify the active branch starts with `codex/auto/`.
3. Implement the change.
4. Run the narrowest checks that prove the change.
5. Commit to the current branch.
6. Push the branch to `origin`.
7. Write a short branch summary report under `job-orc/reports/YYYY-MM-DD/`.

## Reporting

Include:

- what changed
- checks run
- remaining risks
- branch name and commit hash
