#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python3 scripts/calculate_indices.py \
  --input data/processed/station_snapshots.jsonl \
  --output data/processed/indices_composite.jsonl \
  --quality-report data/processed/indices_quality_report.md
