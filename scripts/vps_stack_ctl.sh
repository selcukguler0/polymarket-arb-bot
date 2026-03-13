#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-status}"

SERVICES=(
  "complete-set-shadow@btc.service"
  "complete-set-shadow@eth.service"
  "complete-set-shadow@sol.service"
  "complete-set-shadow@xrp.service"
  "codex-controller.service"
  "codex-research.service"
  "codex-builder.service"
)

case "${ACTION}" in
  start|stop|restart)
    systemctl "${ACTION}" "${SERVICES[@]}"
    ;;
  status)
    systemctl --no-pager --full status "${SERVICES[@]}"
    ;;
  enable)
    systemctl enable "${SERVICES[@]}"
    ;;
  disable)
    systemctl disable "${SERVICES[@]}"
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|enable|disable}" >&2
    exit 1
    ;;
esac
