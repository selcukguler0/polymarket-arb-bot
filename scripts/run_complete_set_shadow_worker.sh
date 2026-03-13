#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ASSET="${1:-btc}"

case "${ASSET,,}" in
  btc) CONFIG_PATH="config/complete_set_shadow.toml" ;;
  eth) CONFIG_PATH="config/complete_set_shadow_eth.toml" ;;
  sol) CONFIG_PATH="config/complete_set_shadow_sol.toml" ;;
  xrp) CONFIG_PATH="config/complete_set_shadow_xrp.toml" ;;
  *)
    echo "Unknown asset: ${ASSET}" >&2
    exit 1
    ;;
esac

cd "${ROOT_DIR}"

if [[ -x target/release/complete_set_bot ]]; then
  exec target/release/complete_set_bot "${CONFIG_PATH}"
else
  exec cargo run --release --bin complete_set_bot -- "${CONFIG_PATH}"
fi
