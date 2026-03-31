#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

ENDPOINT_URL="${ENDPOINT_URL:-}"
INTERVAL_MINUTES="${INTERVAL_MINUTES:-60}"
MAX_CYCLES="${MAX_CYCLES:-0}"
RUN_METEO_EACH_CYCLE="${RUN_METEO_EACH_CYCLE:-false}"
POLLUTION_INPUT_CSV="${POLLUTION_INPUT_CSV:-}"

ARGS=(
  --interval-minutes "$INTERVAL_MINUTES"
  --endpoint-url "$ENDPOINT_URL"
  --max-cycles "$MAX_CYCLES"
  --pollution-input-csv "$POLLUTION_INPUT_CSV"
)

if [[ "$RUN_METEO_EACH_CYCLE" == "true" ]]; then
  ARGS+=(--run-meteo-each-cycle)
fi

"$PYTHON_BIN" scripts/hourly_pipeline_worker.py "${ARGS[@]}"
