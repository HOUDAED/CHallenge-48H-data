"""
SYNOP OMM meteorological data fetcher.
Dataset: https://www.data.gouv.fr/fr/datasets/archive-synop-omm/

Storage layout:
  base : https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/OBS/SYNOP/
  files: synop_{year}.csv.gz  (per-year, ~150 MB compressed)

CSV columns (semicolon-separated):
  lat, lon, geo_id_wmo, name, validity_time,
  t (K), u (%), pmer (Pa), ff (m/s), dd (deg),
  rr1 / rr3 / rr6 (mm)
"""
import gzip
import io
import logging
import os
from pathlib import Path

import pandas as pd

from .base_client import BaseClient

logger = logging.getLogger(__name__)

SYNOP_BASE = "https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/OBS/SYNOP"
RAW_DIR = Path(os.getenv("RAW_DATA_DIR", "data/raw")) / "synop"

COLUMN_MAP = {
    "geo_id_wmo": "station_id",
    "name": "station_name",
    "validity_time": "timestamp",
    "lat": "lat",
    "lon": "lng",
    "t": "temperature_k",
    "u": "humidity",
    "pmer": "pressure_pa",
    "ff": "wind_speed_ms",
    "dd": "wind_direction_deg",
    "rr1": "rainfall_1h",
    "rr3": "rainfall_3h",
    "rr6": "rainfall_6h",
}

WIND_DIRECTIONS = [
    "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
]


class SYNOPClient(BaseClient):

    def fetch_year(self, year: int) -> pd.DataFrame:
        """Download and parse the yearly CSV.GZ for SYNOP data."""
        filename = f"synop_{year}.csv.gz"
        url = f"{SYNOP_BASE}/{filename}"
        dest = RAW_DIR / filename
        dest.parent.mkdir(parents=True, exist_ok=True)

        if not dest.exists():
            logger.info("Downloading SYNOP %d", year)
            self.download_file(url, dest)

        return self._parse_gz(dest)

    def fetch_realtime(self) -> pd.DataFrame:
        """Fetch the current year's file (updated continuously by Météo-France)."""
        import datetime
        year = datetime.date.today().year
        filename = f"synop_{year}.csv.gz"
        url = f"{SYNOP_BASE}/{filename}"
        dest = RAW_DIR / f"synop_{year}_realtime.csv.gz"
        dest.parent.mkdir(parents=True, exist_ok=True)

        logger.info("Downloading SYNOP realtime (%d)", year)
        self.download_file.__wrapped__(self, url, dest)  # bypass cache, always re-fetch
        return self._parse_gz(dest)

    def _parse_gz(self, path: Path) -> pd.DataFrame | None:
        try:
            with gzip.open(path, "rb") as f:
                df = pd.read_csv(f, sep=";", encoding="utf-8-sig", low_memory=False)
        except Exception:
            try:
                with gzip.open(path, "rb") as f:
                    df = pd.read_csv(f, sep=";", encoding="latin-1", low_memory=False)
            except Exception as e:
                logger.error("Failed to parse %s: %s", path, e)
                return None

        df.columns = [c.strip().lower() for c in df.columns]
        df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        # Unit conversions
        if "temperature_k" in df.columns:
            df["temperature"] = pd.to_numeric(df["temperature_k"], errors="coerce") - 273.15

        if "pressure_pa" in df.columns:
            df["pressure"] = pd.to_numeric(df["pressure_pa"], errors="coerce") / 100  # Pa → hPa

        if "wind_speed_ms" in df.columns:
            df["wind_speed"] = pd.to_numeric(df["wind_speed_ms"], errors="coerce") * 3.6  # m/s → km/h

        if "wind_direction_deg" in df.columns:
            df["wind_direction"] = df["wind_direction_deg"].apply(self._deg_to_compass)

        if "humidity" in df.columns:
            df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce")

        # Rainfall: prefer 1h, fallback 3h
        if "rainfall_1h" in df.columns:
            df["rainfall"] = pd.to_numeric(df["rainfall_1h"], errors="coerce")
        elif "rainfall_3h" in df.columns:
            df["rainfall"] = pd.to_numeric(df["rainfall_3h"], errors="coerce")

        logger.info("SYNOP parsed: %d rows from %s", len(df), path.name)
        return df

    @staticmethod
    def _deg_to_compass(deg) -> str:
        try:
            idx = round(float(deg) / 22.5) % 16
            return WIND_DIRECTIONS[idx]
        except (TypeError, ValueError):
            return ""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from dotenv import load_dotenv
    load_dotenv()

    client = SYNOPClient()
    print("Fetching SYNOP 2024 sample...")
    df = client.fetch_year(2024)
    if df is not None:
        print(f"Rows: {len(df)}")
        print(df[["station_id", "station_name", "timestamp", "temperature", "humidity", "pressure", "wind_speed", "wind_direction"]].head())
