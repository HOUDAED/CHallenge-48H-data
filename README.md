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

## Step 3 - Faire une jointure geospatiale des stations

Ce scope couvre:

- jointure par stationId quand l'identifiant est commun
- fallback geospatial vers la station meteo la plus proche (rayon max 50 km)
- appariement temporel vers l'observation meteo la plus proche (delta max 6 h)
- generation d'un rapport qualite de jointure

### Execution rapide Step 3

```bash
bash scripts/run_step3_join.sh
```

### Execution manuelle Step 3

```bash
python3 scripts/join_meteo_pollution.py --meteo-input data/processed/meteo_normalized.jsonl --pollution-input data/processed/pollution_normalized.jsonl --stations-geojson data/raw/postes_synop.geojson --output data/processed/station_snapshots.jsonl --quality-report data/processed/join_quality_report.md --max-distance-km 50 --max-time-diff-hours 6
```

### Sorties Step 3

- data/processed/station_snapshots.jsonl
- data/processed/join_quality_report.md

## Execution en boucle toutes les 60 minutes

Le worker suivant execute le pipeline et publie un payload JSON a chaque cycle.

```bash
bash scripts/run_hourly_worker.sh
```

Variables d'environnement utiles:

- `INTERVAL_MINUTES` (defaut: `60`)
- `ENDPOINT_URL` (URL de POST cible, si vide les payloads sont ecrits dans `data/processed/outbox`)
- `MAX_CYCLES` (defaut: `0` = infini)
- `RUN_METEO_EACH_CYCLE` (`true`/`false`)
- `POLLUTION_INPUT_CSV` (fichier CSV local pollution a reutiliser sur chaque cycle)

Exemple (1 cycle de test):

```bash
MAX_CYCLES=1 INTERVAL_MINUTES=60 RUN_METEO_EACH_CYCLE=true ENDPOINT_URL='' bash scripts/run_hourly_worker.sh
```
