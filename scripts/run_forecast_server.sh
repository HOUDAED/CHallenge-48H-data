#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python3 scripts/serve_forecast_api.py \
  --data data/processed/forecast.jsonl \
  --host 0.0.0.0 \
  --port 8001
