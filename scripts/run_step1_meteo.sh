#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

"$PYTHON_BIN" scripts/download_meteo_data.py --config config/meteo_sources.json --output-dir data/raw

YEAR=$("$PYTHON_BIN" - <<'PY'
import json
from pathlib import Path
config = json.loads(Path("config/meteo_sources.json").read_text(encoding="utf-8"))
print(config["synop_year"])
PY
)

"$PYTHON_BIN" scripts/transform_meteo_data.py \
  --input "data/raw/synop_${YEAR}.csv.gz" \
  --output "data/processed/meteo_normalized.jsonl" \
  --quality-report "data/processed/meteo_quality_report.md"

echo "Step 1 complete: meteorological data fetched and normalized"
