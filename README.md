# CHallenge-48H-data

Air quality + meteorological data integration platform for France's major cities.



## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Pipeline

```bash
# 1. Fetch raw data (if not already done)
## Downloads raw LCSQA + SYNOP   data → data/processed

python3 fetch_data.py

# 2. Clean, normalize and join
## Clean each dataset (handle nulls, remove bad values, drop useless columns)
## Normalize and join them via geospatial proximity (LCSQA station ↔ nearest SYNOP station)

##LCSQA 
### Key issue: no lat/lng — need to fetch station metadata separately
### 3 completely empty columns: taux de saisie, couverture temporelle, couverture de données
### null value rows
### Columns to keep: timestamp, station_id, station_name, pollutant, value, unit, validité

##SYNOP
###Has lat, lng — good for geospatial join
###Timestamps are tz-aware (UTC)
###Columns to keep: station_id, station_name, lat, lng, timestamp, temperature, humidity, pressure, wind_speed, ###wind_direction, rainfall
###Some nulls in meteo fields (~0.5–20% depending on column)


python3 clean_normalize.py
```

## Geospatial Join (normalizer.py)
### 1. Merge LCSQA station coordinates into lcsqa_df on station_id
### 2. For each unique LCSQA station, find nearest SYNOP station using haversine distance:
 # haversine formula (no extra lib needed)
### def haversine(lat1, lon1, lat2, lon2) -> float  # returns km
### 3. Accept match only if distance ≤ 50 km
### 4. Build mapping table: lcsqa_station_id → synop_station_id
### 5. Join SYNOP meteo onto LCSQA rows via this mapping

##  Temporal Alignment
### Round both timestamps to the nearest hour
### 2. SYNOP is 3-hourly → after rounding, forward fill to hourly within each station
### 3. Merge LCSQA + SYNOP on (synop_station_id, timestamp_hour)


`clean_normalize.py` cleans both datasets, joins them geospatially and saves 6 files to `data/processed/`.


## API Server:  THIS PART IS REPLACED WITH HTTP REQUEST (STEP "ENDPOINT")

```bash
### Starts FastAPI server on port 8000
python3 main.py
```

Server starts at `http://localhost:8000`. Interactive docs at `http://localhost:8000/docs`.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/health` | Health check + station count |
| GET | `/api/v1/station-ids` | List all station IDs |
| GET | `/api/v1/stations` | All stations with latest snapshot |
| GET | `/api/v1/stations/{id}` | Single station (latest data) |
| GET | `/api/v1/stations/{id}/history` | Time-series for a station |
| GET | `/api/v1/indices/{id}` | Composite indices (ATMO, IQA) |

History query parameters: `start`, `end` (ISO 8601), `pollutants` (comma-separated, e.g. `PM2.5,NO2`).

### Response format

Responses match the required TypeScript interfaces (`Station`, `PollutantData`, `MeteoData`, `CompositeIndex`).

## Orchestrator

```python
from src.orchestrator import Pipeline

pipeline = Pipeline()
pipeline.run_month(2026, 3)       # fetch + clean + join a single month
pipeline.run_historical(2026)     # full year
pipeline.run_realtime()           # yesterday's data (most recent available)
```

The orchestrator fetches, cleans, normalizes, persists processed files, and reloads the API store automatically.

## Endpoint

```bash

 python3 test_receiver.py



 Terminal 1 — start the receiver

  python3 test_receiver.py
  # Listening on http://localhost:9000 — waiting for POST requests...

  Terminal 2 — send the data
   # Full send (423 stations, batches of 50)
  python3 send_data.py --url http://localhost:9000 --batch 50

    # Check the data before sending (no network call)
  python3 send_data.py --url http://localhost:9000 --dry-run

  When you have a real target app

  python3 send_data.py --url https://your-app.com/api/stations

  The receiver just needs to accept POST with a JSON array of Station objects.
```

## Tests

```bash

pytest tests/test_api.py -v
```

## Indices

 Le projet génère 2 indices
 1. Indice ATMO (id: "ATMO")
  Interprétation :

  ┌─────────┬───────────────┐
  │ Valeur  │    Qualité    │
  ├─────────┼───────────────┤
  │ 0–20    │ Très bonne    │
  ├─────────┼───────────────┤
  │ 20–40   │ Bonne         │
  ├─────────┼───────────────┤
  │ 40–60   │ Moyenne       │
  ├─────────┼───────────────┤
  │ 60–80   │ Mauvaise      │
  ├─────────┼───────────────┤
  │ 80–100+ │ Très mauvaise │
  └─────────┴───────────────┘

    2. Indice IQA (id: "IQA")
 
 Lien avec la météo

  Les indices sont calculés à partir de la pollution uniquement, mais chaque réponse Station associe
  systématiquement pollutants + meteo + indices pour le même horodatage. Cela permet côté visualisation de
  corréler :

  - Vent fort (windSpeed élevé) → dispersion des polluants → ATMO/IQA bas
  - Pression haute + humidité forte → inversion thermique → polluants concentrés → ATMO/IQA haut
  - Pluie (rainfall > 0) → lessivage des PM → PM2.5/PM10 en baisse

  Un indice météo-pollution combiné n'est pas encore calculé côté serveur — les données sont exposées
  ensemble pour que la couche de visualisation puisse construire cette corrélation.

