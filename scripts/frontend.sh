#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
node_ready() { command -v node >/dev/null && node -e 'process.exit(Number(process.versions.node.split(".")[0]) >= 20 ? 0 : 1)'; }
if [[ -n "${DOMEYE_NODE_BIN:-}" ]]; then
  export PATH="$DOMEYE_NODE_BIN:$PATH"
fi
if ! node_ready; then
  bundled_node='/home/bgpdata/.local/node-v22.23.1-linux-x64/bin'
  if [[ -x "$bundled_node/node" ]]; then
    export PATH="$bundled_node:$PATH"
  fi
fi
if ! node_ready || ! command -v npm >/dev/null; then
  printf '%s\n' '需要 Node.js 20+ 与 npm，可通过 DOMEYE_NODE_BIN 指定已有工具目录。' >&2
  exit 1
fi
cd "$project_root/frontend"
export VITE_API_URL='/api/v1/'
unset VITE_DATA_WINDOW_START VITE_DATA_WINDOW_END
command_name="${1:-dev}"
if [[ $# -gt 0 ]]; then shift; fi
case "$command_name" in
  install) exec npm ci "$@" ;;
  test|build|api:types|typecheck) exec npm run "$command_name" -- "$@" ;;
  dev|preview)
    export VITE_API_PROXY_TARGET="${DOMEYE_API_TARGET:-http://127.0.0.1:28473}"
    export VITE_API_V2_PROXY_TARGET="$VITE_API_PROXY_TARGET"
    exec npm run "$command_name" -- --host "${DOMEYE_WEB_HOST:-127.0.0.1}" --port "${DOMEYE_WEB_PORT:-28471}" --strictPort "$@"
    ;;
  *) printf '%s\n' '允许命令：dev、preview、install、test、build、api:types、typecheck。' >&2; exit 2 ;;
esac
