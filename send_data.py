"""
Push all stations data (pollutants + meteo + indices) to an external web application.

Usage:
    python3 send_data.py --url https://your-app.com/api/stations
    python3 send_data.py --url https://your-app.com/api/stations --batch 50
"""
import argparse
import json
import logging
import math
import sys
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")

# Pollutant column → JSON field
POLLUTANT_MAP = {"PM2.5": "pm25", "PM10": "pm10", "NO2": "no2", "O3": "o3", "CO": "co", "SO2": "so2"}

# ATMO breakpoints per pollutant
ATMO_BREAKPOINTS = {
    "pm25": [(5,10),(10,20),(15,30),(25,40),(35,50),(50,60),(75,70),(100,80),(150,90),(math.inf,100)],
    "pm10": [(10,10),(20,20),(25,30),(50,40),(65,50),(90,60),(107,70),(200,80),(250,90),(math.inf,100)],
    "no2":  [(10,10),(20,20),(45,30),(80,40),(110,50),(150,60),(200,70),(270,80),(400,90),(math.inf,100)],
    "o3":   [(20,10),(40,20),(70,30),(90,40),(110,50),(130,60),(150,70),(180,80),(240,90),(math.inf,100)],
    "so2":  [(10,10),(20,20),(60,30),(100,40),(150,50),(200,60),(275,70),(400,80),(500,90),(math.inf,100)],
}
ATMO_WEIGHTS = {"pm25": 0.3, "pm10": 0.2, "no2": 0.25, "o3": 0.15, "so2": 0.1}


def sub_index(pollutant: str, value: float) -> float:
    for threshold, idx in ATMO_BREAKPOINTS.get(pollutant, []):
        if value <= threshold:
            return idx
    return 100.0


def build_indices(pollutants: dict) -> list[dict]:
    available = {k: v for k, v in pollutants.items() if v is not None and k in ATMO_WEIGHTS}
    if not available:
        return []

    total_w = sum(ATMO_WEIGHTS[k] for k in available)
    atmo_score = round(sum(sub_index(k, v) * ATMO_WEIGHTS[k] for k, v in available.items()) / total_w, 1)
    used_weights = {k: round(ATMO_WEIGHTS[k] / total_w, 3) for k in available}

    sub_indices = {k: round(sub_index(k, v), 1) for k, v in available.items()}

    return [
        {"id": "ATMO", "label": "Indice ATMO", "value": atmo_score, "weights": used_weights},
        {"id": "IQA",  "label": "Indice Qualité Air", "value": max(sub_indices.values()), "weights": sub_indices},
    ]


def build_stations(df: pd.DataFrame) -> list[dict]:
    stations = []

    for station_id, group in df.groupby("lcsqa_station_id"):
        with_meteo = group[group["temperature"].notna()]
        rows = with_meteo if not with_meteo.empty else group
        latest_ts = rows["timestamp"].max()
        rows = rows[rows["timestamp"] == latest_ts]
        first = rows.iloc[0]

        # Pollutants
        pollutants = {}
        for _, row in rows.iterrows():
            field = POLLUTANT_MAP.get(row["pollutant"])
            if field and pd.notna(row["value"]):
                pollutants[field] = round(float(row["value"]), 4)

        # Skip stations with no meteo
        if pd.isna(first.get("temperature")):
            continue

        meteo = {
            "temperature":    round(float(first["temperature"]), 2),
            "humidity":       round(float(first["humidity"]), 2),
            "pressure":       round(float(first["pressure"]), 2),
            "windSpeed":      round(float(first["wind_speed"]), 2),
            "windDirection":  str(first["wind_direction"]) if pd.notna(first.get("wind_direction")) else "N/A",
            "rainfall":       round(float(first["rainfall"]), 2) if pd.notna(first.get("rainfall")) else None,
        }

        stations.append({
            "id":        station_id,
            "name":      str(first["lcsqa_station_name"]),
            "lat":       float(first["lat"]),
            "lng":       float(first["lng"]),
            "pollutants": pollutants,
            "meteo":      meteo,
            "indices":    build_indices(pollutants),
            "timestamp":  latest_ts.isoformat(),
        })

    return stations


def _clean_nans(obj):
    """Recursively replace float NaN/inf with None so json.dumps doesn't choke."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _clean_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_nans(v) for v in obj]
    return obj


def send(stations: list[dict], url: str, batch_size: int) -> None:
    total = len(stations)
    sent = 0

    for i in range(0, total, batch_size):
        batch = stations[i:i + batch_size]
        try:
            r = requests.post(url, json=_clean_nans(batch), timeout=30)
            r.raise_for_status()
            sent += len(batch)
            logger.info("Sent %d/%d stations (batch %d–%d) → %s",
                        sent, total, i + 1, i + len(batch), r.status_code)
        except requests.HTTPError as e:
            logger.error("HTTP error on batch %d–%d: %s — %s", i + 1, i + len(batch), e, r.text[:200])
        except requests.RequestException as e:
            logger.error("Request failed on batch %d–%d: %s", i + 1, i + len(batch), e)

    logger.info("Done. %d/%d stations sent.", sent, total)


def main():
    parser = argparse.ArgumentParser(description="Push station data to an external app.")
    parser.add_argument("--url",   required=True, help="Target endpoint URL")
    parser.add_argument("--batch", type=int, default=100, help="Stations per POST request (default: 100)")
    parser.add_argument("--dry-run", action="store_true", help="Build data but do not send — prints JSON instead")
    args = parser.parse_args()

    candidates = sorted(PROCESSED_DIR.glob("combined_*.parquet"), reverse=True)
    if not candidates:
        logger.error("No combined parquet found in %s. Run clean_normalize.py first.", PROCESSED_DIR)
        sys.exit(1)

    logger.info("Loading %s", candidates[0])
    df = pd.read_parquet(candidates[0])
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    logger.info("Building station snapshots...")
    stations = build_stations(df)
    logger.info("Built %d stations", len(stations))

    if args.dry_run:
        print(json.dumps(stations[:2], ensure_ascii=False, indent=2))
        logger.info("Dry run — first 2 stations printed, nothing sent.")
        return

    send(stations, args.url, args.batch)


if __name__ == "__main__":
    main()
