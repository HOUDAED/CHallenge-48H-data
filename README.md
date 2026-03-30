# CHallenge-48H-data

Air quality + meteorological data integration platform for France's major cities.

## Setup

```bash
pip install -r requirements.txt
```

## Pipeline

```bash
# 1. Fetch raw data (if not already done)
python3 fetch_data.py

# 2. Clean, normalize and join
## Clean each dataset (handle nulls, remove bad values, drop useless columns)
## Normalize and join them via geospatial proximity (LCSQA station ↔ nearest SYNOP station)

##LCSQA 
### Key issue: no lat/lng — need to fetch station metadata separately
### 3 completely empty columns: taux de saisie, couverture temporelle, couverture de données
### null value rows
### Columns to keep: timestamp, station_id, station_name, pollutant, value, unit, validité

python3 clean_normalize.py
```

`clean_normalize.py` cleans both datasets, joins them geospatially and saves 6 files to `data/processed/`.


## API Server

```bash
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
