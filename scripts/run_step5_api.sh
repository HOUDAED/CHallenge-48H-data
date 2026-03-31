#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python3 scripts/serve_indices_api.py \
  --data data/processed/indices_composite.jsonl \
  --stations-geojson data/raw/postes_synop.geojson \
  --host 0.0.0.0 \
  --port 8000
