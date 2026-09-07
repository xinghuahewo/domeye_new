#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ ! -x "$project_root/backend/.venv/bin/python" ]]; then
  printf '%s\n' '缺少本项目 Python 环境，请先运行 make setup。' >&2
  exit 1
fi
exec "$project_root/backend/.venv/bin/python" "$project_root/scripts/run_backend.py"
