#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

INPUT_CSV="${1:-}"

if [[ -n "$INPUT_CSV" ]]; then
  if [[ ! -f "$INPUT_CSV" ]]; then
    echo "Input CSV not found: $INPUT_CSV" >&2
    exit 1
  fi
  cp "$INPUT_CSV" data/raw/pollution/latest_pollution.csv
else
  "$PYTHON_BIN" scripts/download_pollution_data.py \
    --config config/pollution_sources.json \
    --output data/raw/pollution/latest_pollution.csv \
    --metadata-output data/raw/pollution/latest_pollution_meta.json
fi

"$PYTHON_BIN" scripts/transform_pollution_data.py \
  --input data/raw/pollution/latest_pollution.csv \
  --output data/processed/pollution_normalized.jsonl \
  --quality-report data/processed/pollution_quality_report.md
