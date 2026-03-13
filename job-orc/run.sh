#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ORC="${ROOT_DIR}/job-orc/orchestrator.py"
BOOTSTRAP="${ROOT_DIR}/job-orc/bootstrap_codex.py"

VPS_HOST="${POLYMARKET_VPS_HOST:-root@YOUR_VPS_IP}"
VPS_USER="botuser"
VPS_HOME="/home/${VPS_USER}"
VPS_RUNTIME_DIR="${POLYMARKET_VPS_RUNTIME_DIR:-${VPS_HOME}/polymarket-bot-runtime}"
VPS_AGENT_DIR="${POLYMARKET_VPS_AGENT_DIR:-${VPS_HOME}/polymarket-bot-agent}"
VPS_CODEX_HOME="${POLYMARKET_VPS_CODEX_HOME:-${VPS_HOME}/.codex}"
VPS_STACK_SCRIPT="${VPS_AGENT_DIR}/scripts/vps_stack_ctl.sh"

bootstrap_local() {
  local codex_home="${CODEX_HOME:-$HOME/.codex}"
  python3 "${BOOTSTRAP}" --repo-root "${ROOT_DIR}" --codex-home "${codex_home}" --config "${codex_home}/config.toml"
  python3 "${ORC}" init
}

bootstrap_vps() {
  bootstrap_local

  ssh -i ~/.ssh/id_vps "${VPS_HOST}" "set -euo pipefail
    apt-get update
    apt-get install -y rsync git curl python3 python3-venv build-essential pkg-config libssl-dev nodejs npm
    id -u ${VPS_USER} >/dev/null 2>&1 || useradd -m -s /bin/bash ${VPS_USER}
    mkdir -p ${VPS_RUNTIME_DIR} ${VPS_AGENT_DIR} ${VPS_CODEX_HOME}
    chown -R ${VPS_USER}:${VPS_USER} ${VPS_HOME}
  "

  if ! ssh -i ~/.ssh/id_vps "${VPS_HOST}" "node --version | grep -Eq '^v(1[6-9]|[2-9][0-9])'"; then
    ssh -i ~/.ssh/id_vps "${VPS_HOST}" "set -euo pipefail
      curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
      apt-get install -y nodejs
    "
  fi

  ssh -i ~/.ssh/id_vps "${VPS_HOST}" "npm install -g @openai/codex"

  rsync -az --delete \
    --exclude ".git" \
    --exclude "target" \
    --exclude "archive" \
    --exclude "data" \
    --exclude "logs" \
    --exclude "oldlogs" \
    -e "ssh -i ~/.ssh/id_vps" \
    "${ROOT_DIR}/" "${VPS_HOST}:${VPS_RUNTIME_DIR}/"

  rsync -az --delete \
    --exclude "target" \
    --exclude "archive" \
    --exclude "data" \
    --exclude "logs" \
    --exclude "oldlogs" \
    -e "ssh -i ~/.ssh/id_vps" \
    "${ROOT_DIR}/" "${VPS_HOST}:${VPS_AGENT_DIR}/"

  scp -i ~/.ssh/id_vps "${HOME}/.codex/auth.json" "${VPS_HOST}:/tmp/polymarket_codex_auth.json"
  ssh -i ~/.ssh/id_vps "${VPS_HOST}" "set -euo pipefail
    chown -R ${VPS_USER}:${VPS_USER} ${VPS_RUNTIME_DIR} ${VPS_AGENT_DIR}
    install -d -o ${VPS_USER} -g ${VPS_USER} ${VPS_CODEX_HOME}
    mv /tmp/polymarket_codex_auth.json ${VPS_CODEX_HOME}/auth.json
    chown ${VPS_USER}:${VPS_USER} ${VPS_CODEX_HOME}/auth.json
    sudo -u ${VPS_USER} env CODEX_HOME='${VPS_CODEX_HOME}' python3 '${VPS_AGENT_DIR}/job-orc/bootstrap_codex.py' --repo-root '${VPS_AGENT_DIR}' --codex-home '${VPS_CODEX_HOME}' --config '${VPS_CODEX_HOME}/config.toml'
    sudo -u ${VPS_USER} bash -lc 'cd ${VPS_RUNTIME_DIR} && cargo build --release --bin complete_set_bot --bin complete_set_snapshot'
    sudo -u ${VPS_USER} bash -lc 'cd ${VPS_AGENT_DIR} && cargo build --release --bin complete_set_bot --bin complete_set_snapshot'
    systemctl disable --now polymarket-bot.service || true
  "

  scp -i ~/.ssh/id_vps "${ROOT_DIR}/deploy/systemd/complete-set-shadow@.service" "${VPS_HOST}:/etc/systemd/system/complete-set-shadow@.service"
  scp -i ~/.ssh/id_vps "${ROOT_DIR}/deploy/systemd/codex-controller.service" "${VPS_HOST}:/etc/systemd/system/codex-controller.service"
  scp -i ~/.ssh/id_vps "${ROOT_DIR}/deploy/systemd/codex-research.service" "${VPS_HOST}:/etc/systemd/system/codex-research.service"
  scp -i ~/.ssh/id_vps "${ROOT_DIR}/deploy/systemd/codex-builder.service" "${VPS_HOST}:/etc/systemd/system/codex-builder.service"

  ssh -i ~/.ssh/id_vps "${VPS_HOST}" "set -euo pipefail
    systemctl daemon-reload
    systemctl enable --now complete-set-shadow@btc.service
    systemctl enable --now complete-set-shadow@eth.service
    systemctl enable --now complete-set-shadow@sol.service
    systemctl enable --now complete-set-shadow@xrp.service
    systemctl enable --now codex-controller.service
    systemctl enable --now codex-research.service
    systemctl enable --now codex-builder.service
  "
}

run_vps_stack_action() {
  local action="$1"
  ssh -i ~/.ssh/id_vps "${VPS_HOST}" "bash '${VPS_STACK_SCRIPT}' '${action}'"
}

run_vps_orc_action() {
  local action="$1"
  ssh -i ~/.ssh/id_vps "${VPS_HOST}" "cd '${VPS_AGENT_DIR}' && POLYMARKET_ORC_HOST='vps' POLYMARKET_ORC_RUNTIME_WORKSPACE='${VPS_RUNTIME_DIR}' POLYMARKET_ORC_AGENT_WORKSPACE='${VPS_AGENT_DIR}' python3 '${VPS_AGENT_DIR}/job-orc/orchestrator.py' '${action}'"
}

write_vps_inventory() {
  local date_dir
  date_dir="$(date -u +%F)"
  local out_file="${ROOT_DIR}/job-orc/reports/${date_dir}/vps_runtime_inventory.md"
  mkdir -p "${ROOT_DIR}/job-orc/reports/${date_dir}"
  {
    echo "# VPS Runtime Inventory"
    echo
    echo "- Generated at: \`$(date -u +%Y-%m-%dT%H:%M:%SZ)\`"
    echo "- Host: \`${VPS_HOST}\`"
    echo
    echo "## Managed Services"
    echo
    ssh -i ~/.ssh/id_vps "${VPS_HOST}" "systemctl list-units --type=service --no-pager --no-legend | egrep 'complete-set-shadow@|codex-(controller|research|builder)|polymarket-bot.service' || true"
    echo
    echo "## Control"
    echo
    echo "- Start all: \`./job-orc/run.sh vps-start\`"
    echo "- Stop all: \`./job-orc/run.sh vps-stop\`"
    echo "- Restart all: \`./job-orc/run.sh vps-restart\`"
    echo "- Service status: \`./job-orc/run.sh vps-status\`"
    echo "- Orchestrator status: \`./job-orc/run.sh vps-orc-status\`"
    echo "- Orchestrator watch: \`./job-orc/run.sh vps-watch\`"
    echo "- On VPS directly: \`sudo ${VPS_STACK_SCRIPT} {start|stop|restart|status}\`"
  } > "${out_file}"
  echo "${out_file}"
}

case "${1:-}" in
  bootstrap-local)
    bootstrap_local
    ;;
  bootstrap-vps)
    bootstrap_vps
    ;;
  controller)
    shift
    python3 "${ORC}" controller "$@"
    ;;
  research-loop)
    shift
    python3 "${ORC}" research-loop "$@"
    ;;
  build-loop)
    shift
    python3 "${ORC}" build-loop "$@"
    ;;
  sync-research)
    shift
    python3 "${ORC}" sync-research "$@"
    ;;
  status)
    python3 "${ORC}" status
    ;;
  watch)
    python3 "${ORC}" watch
    ;;
  vps-status)
    run_vps_stack_action status
    ;;
  vps-orc-status)
    run_vps_orc_action status
    ;;
  vps-start)
    run_vps_stack_action start
    ;;
  vps-stop)
    run_vps_stack_action stop
    ;;
  vps-restart)
    run_vps_stack_action restart
    ;;
  vps-watch)
    run_vps_orc_action watch
    ;;
  vps-inventory)
    write_vps_inventory
    ;;
  *)
    echo "Usage: $0 {bootstrap-local|bootstrap-vps|controller|research-loop|build-loop|sync-research|status|watch|vps-status|vps-orc-status|vps-start|vps-stop|vps-restart|vps-watch|vps-inventory}" >&2
    exit 1
    ;;
esac
