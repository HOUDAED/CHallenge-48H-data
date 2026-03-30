# CHallenge-48H-data

## Step 1 - Recuperer les donnees meteorologiques

Ce scope couvre:
- telechargement des observations SYNOP
- telechargement du referentiel des stations meteo
- normalisation des mesures vers le contrat MeteoData
- generation d'un rapport qualite

### Prerequis

- Python 3.10+
- dependances Python:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Execution rapide

```bash
bash scripts/run_step1_meteo.sh
```

### Execution manuelle

```bash
python3 scripts/download_meteo_data.py --config config/meteo_sources.json --output-dir data/raw
python3 scripts/transform_meteo_data.py --input data/raw/synop_2025.csv.gz --output data/processed/meteo_normalized.jsonl --quality-report data/processed/meteo_quality_report.md
```

### Sorties

- `data/raw/synop_YYYY.csv.gz`
- `data/raw/postes_synop.geojson`
- `data/processed/meteo_normalized.jsonl`
- `data/processed/meteo_quality_report.md`

### Mapping MeteoData

- temperature: champ `t` converti de Kelvin vers degres C
- humidity: champ `u` (pourcentage)
- pressure: champ `pres` (hPa)
- windSpeed: champ `ff` converti de m/s vers km/h
- windDirection: champ `dd` converti en cardinal (N, NNE, ...)
- rainfall: champ `rr1` (fallback `rr3`)
