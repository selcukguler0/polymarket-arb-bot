# VPS Operations

Current VPS layout:

- runtime repo: `/home/botuser/polymarket-bot-runtime`
- agent repo: `/home/botuser/polymarket-bot-agent`
- Codex home: `/home/botuser/.codex`

Managed services:

- `complete-set-shadow@btc.service`
- `complete-set-shadow@eth.service`
- `complete-set-shadow@sol.service`
- `complete-set-shadow@xrp.service`
- `codex-controller.service`
- `codex-research.service`
- `codex-builder.service`

Old service:

- `polymarket-bot.service` should stay `disabled` and `inactive`

## On VPS

Run the helper from either repo:

```bash
sudo /home/botuser/polymarket-bot-agent/scripts/vps_stack_ctl.sh status
sudo /home/botuser/polymarket-bot-agent/scripts/vps_stack_ctl.sh stop
sudo /home/botuser/polymarket-bot-agent/scripts/vps_stack_ctl.sh start
sudo /home/botuser/polymarket-bot-agent/scripts/vps_stack_ctl.sh restart
cd /home/botuser/polymarket-bot-agent && POLYMARKET_ORC_HOST=vps POLYMARKET_ORC_RUNTIME_WORKSPACE=/home/botuser/polymarket-bot-runtime POLYMARKET_ORC_AGENT_WORKSPACE=/home/botuser/polymarket-bot-agent python3 job-orc/orchestrator.py status
cd /home/botuser/polymarket-bot-agent && POLYMARKET_ORC_HOST=vps POLYMARKET_ORC_RUNTIME_WORKSPACE=/home/botuser/polymarket-bot-runtime POLYMARKET_ORC_AGENT_WORKSPACE=/home/botuser/polymarket-bot-agent python3 job-orc/orchestrator.py watch
```

## From Local

Use:

```bash
./job-orc/run.sh vps-status
./job-orc/run.sh vps-orc-status
./job-orc/run.sh vps-stop
./job-orc/run.sh vps-start
./job-orc/run.sh vps-restart
./job-orc/run.sh vps-watch
./job-orc/run.sh vps-inventory
```

## Notes

- Stopping the stack stops both Codex workers and all four shadow observers.
- Starting the stack starts all seven managed services.
- `vps-inventory` writes a dated markdown snapshot under `job-orc/reports/YYYY-MM-DD/`.
- `./job-orc/run.sh watch` only shows the local workspace state.
- Use `vps-orc-status` or `vps-watch` to inspect the remote VPS queue.
