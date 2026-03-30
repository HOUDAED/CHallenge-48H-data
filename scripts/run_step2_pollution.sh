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
LATEST_FILE="data/raw/pollution/latest_pollution.csv"
BACKUP_FILE="data/raw/pollution/latest_pollution_last_good.csv"
TMP_FILE="data/raw/pollution/latest_pollution.tmp.csv"

mkdir -p data/raw/pollution

if [[ -s "$LATEST_FILE" ]]; then
  cp "$LATEST_FILE" "$BACKUP_FILE"
fi

if [[ -n "$INPUT_CSV" ]]; then
  if [[ ! -f "$INPUT_CSV" ]]; then
    echo "Input CSV not found: $INPUT_CSV" >&2
    exit 1
  fi

  if [[ ! -s "$INPUT_CSV" ]]; then
    if [[ -s "$BACKUP_FILE" ]]; then
      cp "$BACKUP_FILE" "$LATEST_FILE"
      echo "Input CSV is empty, reusing local fallback: $BACKUP_FILE"
    else
      echo "Input CSV is empty" >&2
      exit 1
    fi
  else
  INPUT_ABS="$(realpath "$INPUT_CSV")"
  LATEST_ABS="$(realpath -m "$LATEST_FILE")"
  if [[ "$INPUT_ABS" != "$LATEST_ABS" ]]; then
    cp "$INPUT_CSV" "$LATEST_FILE"
  else
    echo "Input CSV already points to latest file: $LATEST_FILE"
  fi
  fi
else
  if ! "$PYTHON_BIN" scripts/download_pollution_data.py \
    --config config/pollution_sources.json \
    --output "$TMP_FILE" \
    --metadata-output data/raw/pollution/latest_pollution_meta.json; then
    if [[ -s "$BACKUP_FILE" ]]; then
      cp "$BACKUP_FILE" "$LATEST_FILE"
      echo "Download failed, reusing local fallback: $BACKUP_FILE"
    else
      echo "Download failed and no local fallback found" >&2
      exit 1
    fi
  elif [[ -s "$TMP_FILE" ]]; then
    mv "$TMP_FILE" "$LATEST_FILE"
  elif [[ -s "$BACKUP_FILE" ]]; then
    cp "$BACKUP_FILE" "$LATEST_FILE"
    echo "Downloaded file is empty, reusing local fallback: $BACKUP_FILE"
  else
    echo "Downloaded file is empty and no local fallback found" >&2
    exit 1
  fi
fi

"$PYTHON_BIN" scripts/transform_pollution_data.py \
  --input "$LATEST_FILE" \
  --output data/processed/pollution_normalized.jsonl \
  --quality-report data/processed/pollution_quality_report.md
