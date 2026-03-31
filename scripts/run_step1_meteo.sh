#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

# 1. TÉLÉCHARGEMENT ET EXTRACTION DES 10 DERNIERS JOURS
# Le script python va télécharger les .csv.gz manquants ou mis à jour, 
# puis générer le fichier allégé 'latest_meteo_10_days.csv'
"$PYTHON_BIN" scripts/download_meteo_data.py \
  --config config/meteo_sources.json \
  --output-dir data/raw \
  --extract-days 10

# 2. TRANSFORMATION (Uniquement sur les données récentes)
OUTPUT_FILE="data/processed/meteo_normalized.jsonl"
REPORT_FILE="data/processed/meteo_quality_report.md"

# On vide le fichier jsonl de sortie pour ne pas empiler les données à chaque heure
: > "$OUTPUT_FILE"

echo "Transformation des données météo récentes..."

"$PYTHON_BIN" scripts/transform_meteo_data.py \
  --input "data/raw/latest_meteo_10_days.csv" \
  --output "$OUTPUT_FILE" \
  --quality-report "$REPORT_FILE"

echo "Étape 1 (Météo) terminée avec succès."