"""
In-memory data store: loads the combined parquet and provides query methods.
Also manages the external JSON export store.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")

# Pollutant column name in the dataframe → PollutantData field
_POLLUTANT_MAP = {
    "PM2.5": "pm25",
    "PM10":  "pm10",
    "NO2":   "no2",
    "O3":    "o3",
    "CO":    "co",
    "SO2":   "so2",
}


class DataStore:
    def __init__(self):
        self._df: Optional[pd.DataFrame] = None

    def load(self, parquet_path: Optional[Path] = None) -> None:
        if parquet_path is None:
            # Pick the most recent combined parquet
            candidates = sorted(PROCESSED_DIR.glob("combined_*.parquet"), reverse=True)
            if not candidates:
                raise FileNotFoundError(f"No combined parquet found in {PROCESSED_DIR}")
            parquet_path = candidates[0]

        logger.info("Loading dataset from %s", parquet_path)
        self._df = pd.read_parquet(parquet_path)
        self._df["timestamp"] = pd.to_datetime(self._df["timestamp"])
        logger.info("Loaded %d rows", len(self._df))

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            raise RuntimeError("DataStore not loaded. Call load() first.")
        return self._df

    def reload(self, parquet_path: Optional[Path] = None) -> None:
        self.load(parquet_path)
        logger.info("DataStore reloaded.")

    # ------------------------------------------------------------------ #
    # Query helpers
    # ------------------------------------------------------------------ #

    def station_ids(self) -> list[str]:
        return sorted(self.df["lcsqa_station_id"].unique().tolist())

    def latest_by_station(self, station_id: str) -> Optional[pd.DataFrame]:
        """
        Return all pollutant rows for the station at its latest timestamp that
        also has meteo data. Falls back to absolute latest if none have meteo.
        """
        sub = self.df[self.df["lcsqa_station_id"] == station_id]
        if sub.empty:
            return None
        # Prefer the most recent timestamp that has temperature (meteo joined)
        with_meteo = sub[sub["temperature"].notna()]
        if not with_meteo.empty:
            latest_ts = with_meteo["timestamp"].max()
        else:
            latest_ts = sub["timestamp"].max()
        return sub[sub["timestamp"] == latest_ts]

    def history(
        self,
        station_id: str,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
        pollutants: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        sub = self.df[self.df["lcsqa_station_id"] == station_id].copy()
        if start is not None:
            sub = sub[sub["timestamp"] >= start]
        if end is not None:
            sub = sub[sub["timestamp"] <= end]
        if pollutants:
            norm = [p.upper() for p in pollutants]
            sub = sub[sub["pollutant"].str.upper().isin(norm)]
        return sub.sort_values("timestamp")

    def all_stations_latest(self) -> dict[str, pd.DataFrame]:
        """Return latest rows grouped by station, preferring timestamps with meteo data."""
        result: dict[str, pd.DataFrame] = {}
        for sid in self.df["lcsqa_station_id"].unique():
            rows = self.latest_by_station(sid)
            if rows is not None and not rows.empty:
                result[sid] = rows
        return result

    def pollutant_row_to_dict(self, rows: pd.DataFrame) -> dict[str, Optional[float]]:
        """Pivot pollutant rows into {field: value} dict matching PollutantData."""
        result: dict[str, Optional[float]] = {}
        for _, row in rows.iterrows():
            field = _POLLUTANT_MAP.get(row["pollutant"])
            if field and pd.notna(row["value"]):
                result[field] = round(float(row["value"]), 4)
        return result


# Module-level singleton
store = DataStore()
