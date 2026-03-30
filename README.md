# CHallenge-48H-data

## Step 1 - Recuperer les donnees meteorologiques

Ce scope couvre:

- telechargement des observations SYNOP
- telechargement du referentiel des stations meteo
- normalisation des mesures vers le contrat MeteoData
- generation d'un rapport qualite

### Execution rapide Step 1

```bash
bash scripts/run_step1_meteo.sh
```

### Execution manuelle Step 1

```bash
python3 scripts/download_meteo_data.py --config config/meteo_sources.json --output-dir data/raw
python3 scripts/transform_meteo_data.py --input data/raw/synop_2025.csv.gz --output data/processed/meteo_normalized.jsonl --quality-report data/processed/meteo_quality_report.md
```

### Sorties

- data/raw/synop_YYYY.csv.gz
- data/raw/postes_synop.geojson
- data/processed/meteo_normalized.jsonl
- data/processed/meteo_quality_report.md

### Mapping MeteoData

- temperature: champ t converti de Kelvin vers degres C
- humidity: champ u (pourcentage)
- pressure: champ pres (hPa)
- windSpeed: champ ff converti de m/s vers km/h
- windDirection: champ dd converti en cardinal (N, NNE, ...)
- rainfall: champ rr1 (fallback rr3)

## Step 2 - Recuperer les donnees de concentrations des polluants

Ce scope couvre:

- telechargement du dernier fichier journalier FR_E2_YYYY-MM-DD.csv disponible
- transformation des concentrations polluants vers un format normalise
- generation d'un rapport qualite

### Execution rapide Step 2

```bash
bash scripts/run_step2_pollution.sh
```

Le script essaie d'abord les URLs journalieres par date, puis une ressource CSV data.gouv de repli.

Si vous avez deja un fichier CSV pollution (par exemple fourni par un collaborateur):

```bash
bash scripts/run_step2_pollution.sh /chemin/vers/FR_E2_YYYY-MM-DD.csv
```

### Execution manuelle Step 2

```bash
python3 scripts/download_pollution_data.py --config config/pollution_sources.json --output data/raw/pollution/latest_pollution.csv --metadata-output data/raw/pollution/latest_pollution_meta.json
python3 scripts/transform_pollution_data.py --input data/raw/pollution/latest_pollution.csv --output data/processed/pollution_normalized.jsonl --quality-report data/processed/pollution_quality_report.md
```

Ou en forcant une URL directe connue:

```bash
python3 scripts/download_pollution_data.py --url "https://.../FR_E2_YYYY-MM-DD.csv" --output data/raw/pollution/latest_pollution.csv --metadata-output data/raw/pollution/latest_pollution_meta.json
```

### Sorties Step 2

- data/raw/pollution/latest_pollution.csv
- data/raw/pollution/latest_pollution_meta.json
- data/processed/pollution_normalized.jsonl
- data/processed/pollution_quality_report.md

### Mapping PollutantData

- stationId: identifiant station pollution
- timestamp: horodatage normalise en UTC
- coordinates.latitude / coordinates.longitude: coordonnees GPS (si presentes)
- pollution.pm25: PM2.5
- pollution.pm10: PM10
- pollution.no2: NO2
- pollution.o3: O3
- pollution.co: CO
- pollution.so2: SO2
