"""
FastAPI router — all REST endpoints.
"""
from __future__ import annotations

from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from .compute import compute_indices
from .models import CompositeIndex, MeteoData, PollutantData, Station
from .store import store

router = APIRouter()


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def _build_station(station_id: str, rows: pd.DataFrame) -> Station:
    """Build a Station model from a set of rows (same station, same timestamp)."""
    first = rows.iloc[0]

    # Pollutants
    pollutant_dict = store.pollutant_row_to_dict(rows)
    pollutants = PollutantData(**pollutant_dict)

    # Meteo
    def _safe(col: str):
        val = first.get(col)
        return float(val) if pd.notna(val) else None

    temp = _safe("temperature")
    humidity = _safe("humidity")
    pressure = _safe("pressure")
    wind_speed = _safe("wind_speed")
    wind_dir = first.get("wind_direction") or "N/A"
    if pd.isna(wind_dir):
        wind_dir = "N/A"

    if any(v is None for v in [temp, humidity, pressure, wind_speed]):
        raise ValueError(f"Missing meteo data for station {station_id}")

    meteo = MeteoData(
        temperature=round(temp, 2),
        humidity=round(humidity, 2),
        pressure=round(pressure, 2),
        windSpeed=round(wind_speed, 2),
        windDirection=str(wind_dir),
        rainfall=_safe("rainfall"),
    )

    # Indices
    indices = compute_indices(pollutants)

    return Station(
        id=station_id,
        name=str(first["lcsqa_station_name"]),
        lat=float(first["lat"]),
        lng=float(first["lng"]),
        pollutants=pollutants,
        meteo=meteo,
        indices=indices,
        timestamp=first["timestamp"].isoformat(),
    )


# ------------------------------------------------------------------ #
# Endpoints
# ------------------------------------------------------------------ #

@router.get("/stations", response_model=list[Station], summary="List all stations (latest snapshot)")
def list_stations():
    """Return every station with its most recent measurements."""
    result = []
    for station_id, rows in store.all_stations_latest().items():
        try:
            result.append(_build_station(station_id, rows))
        except (ValueError, KeyError):
            continue
    return result


@router.get("/stations/{station_id}", response_model=Station, summary="Get a single station (latest)")
def get_station(station_id: str):
    rows = store.latest_by_station(station_id)
    if rows is None or rows.empty:
        raise HTTPException(status_code=404, detail=f"Station '{station_id}' not found")
    try:
        return _build_station(station_id, rows)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.get(
    "/stations/{station_id}/history",
    summary="Get historical time-series for a station",
)
def get_station_history(
    station_id: str,
    start: Optional[str] = Query(None, description="ISO 8601 start datetime"),
    end: Optional[str] = Query(None, description="ISO 8601 end datetime"),
    pollutants: Optional[str] = Query(None, description="Comma-separated pollutant list, e.g. PM2.5,NO2"),
):
    """Return hourly time-series rows for a station, optionally filtered."""
    start_ts = pd.Timestamp(start) if start else None
    end_ts = pd.Timestamp(end) if end else None
    poll_list = [p.strip() for p in pollutants.split(",")] if pollutants else None

    df = store.history(station_id, start_ts, end_ts, poll_list)
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data found for station '{station_id}'")

    records = df.assign(timestamp=df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")).to_dict(orient="records")
    return {"station_id": station_id, "count": len(records), "rows": records}


@router.get("/indices/{station_id}", response_model=list[CompositeIndex], summary="Get composite indices for a station")
def get_indices(station_id: str):
    rows = store.latest_by_station(station_id)
    if rows is None or rows.empty:
        raise HTTPException(status_code=404, detail=f"Station '{station_id}' not found")

    pollutant_dict = store.pollutant_row_to_dict(rows)
    pollutants = PollutantData(**pollutant_dict)
    indices = compute_indices(pollutants)
    if not indices:
        raise HTTPException(status_code=422, detail="Not enough pollutant data to compute indices")
    return indices


@router.get("/station-ids", response_model=list[str], summary="List all station IDs")
def list_station_ids():
    return store.station_ids()


@router.get("/health", summary="Health check")
def health():
    try:
        count = len(store.station_ids())
        return {"status": "ok", "stations": count}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))
