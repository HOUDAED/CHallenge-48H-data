#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

"$PYTHON_BIN" scripts/join_meteo_pollution.py \
  --meteo-input data/processed/meteo_normalized.jsonl \
  --pollution-input data/processed/pollution_normalized.jsonl \
  --stations-geojson data/raw/postes_synop.geojson \
  --output data/processed/station_snapshots.jsonl \
  --quality-report data/processed/join_quality_report.md \
  --max-distance-km 50 \
  --max-time-diff-hours 6
