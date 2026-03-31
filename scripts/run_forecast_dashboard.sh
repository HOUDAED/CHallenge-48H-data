#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

streamlit run scripts/forecast_dashboard.py \
  --server.port 8501 \
  --server.address 0.0.0.0 \
  --server.headless true
