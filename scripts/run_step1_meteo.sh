#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

"$PYTHON_BIN" scripts/download_meteo_data.py \
  --config config/meteo_sources.json \
  --output-dir data/raw

YEAR=$(
"$PYTHON_BIN" - << 'PY'
import json
from pathlib import Path
cfg = json.loads(Path('config/meteo_sources.json').read_text(encoding='utf-8'))
print(int(cfg['synop_year']))
PY
)

"$PYTHON_BIN" scripts/transform_meteo_data.py \
  --input "data/raw/synop_${YEAR}.csv.gz" \
  --output data/processed/meteo_normalized.jsonl \
  --quality-report data/processed/meteo_quality_report.md
