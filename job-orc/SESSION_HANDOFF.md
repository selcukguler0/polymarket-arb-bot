# Session Handoff

Last updated: `2026-03-06`

## Current Branch

- Local working branch: `codex/research-reorg-20260306`
- Commit: `0e65ad6`
- Remote branch: `origin/codex/research-reorg-20260306`
- `main` is intentionally not updated yet

## Current VPS Layout

- Runtime repo: `/home/botuser/polymarket-bot-runtime`
- Agent repo: `/home/botuser/polymarket-bot-agent`
- Codex home: `/home/botuser/.codex`

## Current VPS Services

- `complete-set-shadow@btc.service`
- `complete-set-shadow@eth.service`
- `complete-set-shadow@sol.service`
- `complete-set-shadow@xrp.service`
- `codex-controller.service`
- `codex-research.service`
- `codex-builder.service`

Old service:

- `polymarket-bot.service` must stay `disabled` and `inactive`

## Operator Commands

From local:

```bash
./job-orc/run.sh vps-status
./job-orc/run.sh vps-orc-status
./job-orc/run.sh vps-watch
./job-orc/run.sh vps-stop
./job-orc/run.sh vps-start
./job-orc/run.sh vps-restart
./job-orc/run.sh vps-inventory
```

## Current Strategy State

- The old `15m BTC` quoting bot is rejected as the main strategy path.
- The active VPS runtime is shadow-only.
- `complete-set-shadow@*.service` does not trade live.
- It observes crypto markets, scans order books, and logs complete-set style signal frequency.
- Current working assumption: obvious live complete-set mispricing is not the main edge, but it is still being measured as one hypothesis.

## Current Research Direction

Active hypotheses:

- top-wallet microstructure and timing
- pair completion / merge / redeem behavior
- warehouse / inventory cycling
- leaderboard routing / wallet artifact effects
- live opportunity frequency

## Important Files

- Runbook: `/Volumes/KIOXIA/PROJECTS/PROJECTS/polymarket-bot/job-orc/VPS_OPERATIONS.md`
- VPS inventory snapshot: `/Volumes/KIOXIA/PROJECTS/PROJECTS/polymarket-bot/job-orc/reports/2026-03-06/vps_runtime_inventory.md`
- Curated research index: `/Volumes/KIOXIA/PROJECTS/PROJECTS/polymarket-bot/job-orc/knowledge/README.md`
- Queue state: `/Volumes/KIOXIA/PROJECTS/PROJECTS/polymarket-bot/job-orc/tasks.json`
- Controller prompts: `/Volumes/KIOXIA/PROJECTS/PROJECTS/polymarket-bot/job-orc/prompts/`
- Research reports: `/Volumes/KIOXIA/PROJECTS/PROJECTS/polymarket-bot/job-orc/reports/2026-03-06/`

## How To Brief A New Agent

Give the new agent these facts first:

1. Work from branch `codex/research-reorg-20260306`, not `main`.
2. VPS stack is already running shadow observers and Codex services under systemd.
3. Use `./job-orc/run.sh vps-watch` for remote queue state, not `./job-orc/run.sh watch`.
4. Treat the old quoting strategy as baseline/control only.
5. Read this file, `AGENTS.md`, and `job-orc/VPS_OPERATIONS.md` before changing anything.
