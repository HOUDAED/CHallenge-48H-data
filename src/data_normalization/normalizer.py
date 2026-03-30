"""
Geospatial join + temporal alignment of LCSQA and SYNOP datasets.
"""
import io
import logging
import math
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

STATION_META_URL = (
    "https://static.data.gouv.fr/resources/"
    "donnees-temps-reel-de-mesure-des-concentrations-de-polluants-atmospheriques-reglementes-1/"
    "20251210-084445/fr-2025-d-lcsqa-ineris-20251209.xls"
)
STATIONS_CACHE = Path("data/processed/lcsqa_stations.csv")
MAX_KM = 50


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return distance in km between two lat/lon points."""
    R = 6371.0
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ = math.radians(lat2 - lat1)
    dλ = math.radians(lon2 - lon1)
    a = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def fetch_lcsqa_stations() -> pd.DataFrame:
    """Download LCSQA station metadata and return station_id → lat/lng mapping."""
    if STATIONS_CACHE.exists():
        logger.info("Loading LCSQA station coords from cache")
        return pd.read_csv(STATIONS_CACHE)

    logger.info("Downloading LCSQA station metadata...")
    r = requests.get(STATION_META_URL, timeout=30)
    r.raise_for_status()
    xls = pd.ExcelFile(io.BytesIO(r.content))
    df = xls.parse("AirQualityStations", usecols=["NatlStationCode", "Latitude", "Longitude"])
    df = df.rename(columns={"NatlStationCode": "station_id", "Latitude": "lat", "Longitude": "lng"})
    df = df.dropna(subset=["lat", "lng"]).drop_duplicates("station_id")

    STATIONS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(STATIONS_CACHE, index=False)
    logger.info("Saved %d LCSQA station coords", len(df))
    return df


def build_station_mapping(lcsqa_stations: pd.DataFrame, synop_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each LCSQA station find the nearest SYNOP station within MAX_KM.
    Returns DataFrame with columns: lcsqa_station_id, synop_station_id, distance_km.
    """
    synop_coords = (
        synop_df[["station_id", "station_name", "lat", "lng"]]
        .drop_duplicates("station_id")
        .reset_index(drop=True)
    )

    rows = []
    for _, lcsqa_row in lcsqa_stations.iterrows():
        best_dist = float("inf")
        best_synop_id = None
        best_synop_name = None

        for _, synop_row in synop_coords.iterrows():
            d = haversine(lcsqa_row["lat"], lcsqa_row["lng"], synop_row["lat"], synop_row["lng"])
            if d < best_dist:
                best_dist = d
                best_synop_id = synop_row["station_id"]
                best_synop_name = synop_row["station_name"]

        if best_dist <= MAX_KM:
            rows.append({
                "lcsqa_station_id": lcsqa_row["station_id"],
                "synop_station_id": best_synop_id,
                "synop_station_name": best_synop_name,
                "distance_km": round(best_dist, 2),
            })

    mapping = pd.DataFrame(rows)
    logger.info(
        "Station mapping: %d LCSQA stations matched to SYNOP within %d km (out of %d)",
        len(mapping), MAX_KM, len(lcsqa_stations),
    )
    return mapping


def align_synop_hourly(synop_df: pd.DataFrame) -> pd.DataFrame:
    """Round SYNOP timestamps to nearest hour and forward-fill to create hourly rows."""
    df = synop_df.copy()
    df["timestamp"] = df["timestamp"].dt.round("h")
    df = df.drop_duplicates(subset=["station_id", "timestamp"])

    # Build full hourly index per station and forward-fill (limit 3h gap)
    all_frames = []
    hourly_range = pd.date_range(df["timestamp"].min(), df["timestamp"].max(), freq="h")
    for station_id, group in df.groupby("station_id"):
        group = group.set_index("timestamp").reindex(hourly_range)
        group["station_id"] = station_id
        # Forward fill meteo values (max 3 hours)
        num_cols = ["temperature", "humidity", "pressure", "wind_speed", "rainfall"]
        for col in [c for c in num_cols if c in group.columns]:
            group[col] = group[col].ffill(limit=3)
        # Fill non-numeric cols
        for col in ["station_name", "lat", "lng", "wind_direction"]:
            if col in group.columns:
                group[col] = group[col].ffill().bfill()
        all_frames.append(group.reset_index().rename(columns={"index": "timestamp"}))

    return pd.concat(all_frames, ignore_index=True)


def join(lcsqa_df: pd.DataFrame, synop_df: pd.DataFrame) -> pd.DataFrame:
    """
    Full pipeline:
    1. Fetch LCSQA station coords
    2. Build geospatial station mapping
    3. Align SYNOP to hourly
    4. Merge LCSQA + SYNOP on (synop_station_id, timestamp_hour)
    """
    # 1. Station coordinates
    lcsqa_stations = fetch_lcsqa_stations()

    # Only keep stations present in the actual data
    lcsqa_stations = lcsqa_stations[
        lcsqa_stations["station_id"].isin(lcsqa_df["station_id"].unique())
    ]

    # 2. Geospatial mapping
    mapping = build_station_mapping(lcsqa_stations, synop_df)
    if mapping.empty:
        logger.error("No station matches found — check MAX_KM or data coverage")
        return pd.DataFrame()

    # 3. Attach coords + synop station id to LCSQA
    lcsqa_df = lcsqa_df.merge(lcsqa_stations, on="station_id", how="left")
    lcsqa_df = lcsqa_df.merge(
        mapping[["lcsqa_station_id", "synop_station_id", "synop_station_name", "distance_km"]],
        left_on="station_id",
        right_on="lcsqa_station_id",
        how="inner",
    ).drop(columns=["lcsqa_station_id"])

    # 4. Round LCSQA timestamps to hour
    lcsqa_df["timestamp_h"] = lcsqa_df["timestamp"].dt.round("h")

    # 5. Align SYNOP to hourly
    logger.info("Aligning SYNOP to hourly grid...")
    synop_hourly = align_synop_hourly(synop_df)
    synop_hourly = synop_hourly.rename(columns={"timestamp": "timestamp_h"})

    # 6. Merge
    meteo_cols = ["station_id", "timestamp_h", "temperature", "humidity",
                  "pressure", "wind_speed", "wind_direction", "rainfall"]
    synop_subset = synop_hourly[[c for c in meteo_cols if c in synop_hourly.columns]]
    synop_subset = synop_subset.rename(columns={"station_id": "synop_station_id"})

    combined = lcsqa_df.merge(synop_subset, on=["synop_station_id", "timestamp_h"], how="left")

    # Drop original timestamps, keep rounded hour as "timestamp"
    combined = combined.drop(columns=["timestamp"], errors="ignore")
    combined = combined.rename(columns={
        "station_id": "lcsqa_station_id",
        "station_name": "lcsqa_station_name",
        "timestamp_h": "timestamp",
    })

    # Reorder final columns
    final_cols = [
        "timestamp", "lcsqa_station_id", "lcsqa_station_name",
        "synop_station_id", "synop_station_name", "lat", "lng", "distance_km",
        "pollutant", "value", "unit",
        "temperature", "humidity", "pressure", "wind_speed", "wind_direction", "rainfall",
    ]
    combined = combined[[c for c in final_cols if c in combined.columns]]
    combined = combined.sort_values(["timestamp", "lcsqa_station_id", "pollutant"])

    logger.info("Combined dataset: %d rows, %d columns", len(combined), len(combined.columns))
    return combined.reset_index(drop=True)
