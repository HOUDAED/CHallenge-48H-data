#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python3 scripts/forecast_indices.py \
  --input data/processed/meteo_normalized.jsonl \
  --output data/processed/forecast.jsonl \
  --quality-report data/processed/forecast_quality_report.md \
  --horizon-hours 24 \
  --horizon-days 7
