#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${1:-config/complete_set_shadow.toml}"
DURATION_SECS="${2:-86400}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ASSET_SUFFIX="$(awk -F'"' '/^asset = "/ { print tolower($2); exit }' "${ROOT_DIR}/${CONFIG_PATH}")"
if [[ -z "${ASSET_SUFFIX}" ]]; then
  echo "Failed to read asset from ${CONFIG_PATH}" >&2
  exit 1
fi
LOG_DIR="${ROOT_DIR}/logs_complete_set_shadow_${ASSET_SUFFIX}"
SNAPSHOT_BIN="${ROOT_DIR}/target/release/complete_set_snapshot"
BOT_BIN="${ROOT_DIR}/target/release/complete_set_bot"

cd "${ROOT_DIR}"

if [[ -d "${LOG_DIR}" ]]; then
  mv "${LOG_DIR}" "${ROOT_DIR}/logs_complete_set_shadow_${STAMP}"
fi

run_release_bin() {
  local bin_path="$1"
  local bin_name="$2"
  shift 2

  if [[ -x "${bin_path}" ]]; then
    "${bin_path}" "$@"
  else
    cargo run --release --bin "${bin_name}" -- "$@"
  fi
}

run_release_bin "${SNAPSHOT_BIN}" "complete_set_snapshot" "${CONFIG_PATH}"
run_release_bin "${BOT_BIN}" "complete_set_bot" "${CONFIG_PATH}" --runtime-secs "${DURATION_SECS}"
python3 analysis/tools/summarize_complete_set_shadow.py "${LOG_DIR}"

echo "Complete-set shadow session finished."
echo "Log dir: ${LOG_DIR}"
