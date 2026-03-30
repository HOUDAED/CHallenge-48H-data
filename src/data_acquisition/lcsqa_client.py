"""
LCSQA air quality data fetcher.
Dataset: https://www.data.gouv.fr/fr/datasets/donnees-temps-reel-de-mesure-des-concentrations-de-polluants-atmospheriques-reglementes-1/

Storage layout (MinIO):
  bucket : ineris-prod
  prefix : lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/{year}/
  files  : FR_E2_YYYY-MM-DD.csv  (~11 MB each, daily, hourly measurements)

API endpoints used:
  list  : GET https://object.infra.data.gouv.fr/api/v1/buckets/ineris-prod/objects?prefix=...
  dl    : GET https://object.infra.data.gouv.fr/api/v1/buckets/ineris-prod/objects/download?prefix=...
"""
import io
import logging
import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

from .base_client import BaseClient

logger = logging.getLogger(__name__)

MINIO_BASE = "https://object.infra.data.gouv.fr/api/v1/buckets/ineris-prod"
LCSQA_PREFIX = "lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel"
RAW_DIR = Path(os.getenv("RAW_DATA_DIR", "data/raw")) / "lcsqa"

COLUMN_MAP = {
    "date de début": "timestamp",
    "date de fin": "timestamp_end",
    "code site": "station_id",
    "nom site": "station_name",
    "polluant": "pollutant",
    "valeur": "value",
    "unité de mesure": "unit",
    "code zas": "zone_id",
    "zas": "zone_name",
}


class LCSQAClient(BaseClient):

    def list_files(self, year: int) -> list[dict]:
        """Return list of objects for a given year from MinIO."""
        prefix = f"{LCSQA_PREFIX}/{year}/"
        resp = self.get_json(f"{MINIO_BASE}/objects", params={"prefix": prefix})
        objects = resp.get("objects") or []
        # Keep only CSV files
        return [o for o in objects if o["name"].endswith(".csv")]

    def fetch_date(self, d: date) -> pd.DataFrame:
        """Stream and parse the CSV for a single day directly into memory (no disk write)."""
        key = f"{LCSQA_PREFIX}/{d.year}/FR_E2_{d.isoformat()}.csv"
        logger.info("Downloading LCSQA %s", d.isoformat())
        r = self.session.get(
            f"{MINIO_BASE}/objects/download",
            params={"prefix": key},
            timeout=60,
        )
        r.raise_for_status()
        return self._parse_bytes(r.content, label=d.isoformat())

    def fetch_date_range(self, start: date, end: date) -> pd.DataFrame:
        """Stream all daily files between start and end (inclusive) into a single DataFrame."""
        frames = []
        current = start
        while current <= end:
            try:
                df = self.fetch_date(current)
                if df is not None and not df.empty:
                    frames.append(df)
            except requests.HTTPError as e:
                logger.warning("LCSQA %s: %s", current.isoformat(), e)
            current += timedelta(days=1)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True).drop_duplicates()

    def fetch_year(self, year: int) -> pd.DataFrame:
        """Stream all daily files for a full year."""
        end = date(year, 12, 31) if year < date.today().year else date.today()
        return self.fetch_date_range(date(year, 1, 1), end)

    def fetch_realtime(self) -> pd.DataFrame:
        """Fetch today's file (most recent available)."""
        today = date.today()
        for delta in range(0, 3):
            d = today - timedelta(days=delta)
            try:
                df = self.fetch_date(d)
                if df is not None and not df.empty:
                    logger.info("LCSQA realtime: %s (%d rows)", d, len(df))
                    return df
            except requests.HTTPError:
                continue
        return pd.DataFrame()

    def _parse_bytes(self, content: bytes, label: str = "") -> pd.DataFrame | None:
        for encoding in ("utf-8-sig", "latin-1"):
            try:
                df = pd.read_csv(io.BytesIO(content), sep=";", encoding=encoding, low_memory=False)
                break
            except Exception:
                continue
        else:
            logger.error("Failed to parse LCSQA %s", label)
            return None

        df.columns = [c.strip().lower() for c in df.columns]
        df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

        return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from dotenv import load_dotenv
    load_dotenv()

    client = LCSQAClient()
    files = client.list_files(2024)
    print(f"Files for 2024: {len(files)}")
    for f in files[:5]:
        print(f"  - {f['name'].split('/')[-1]}  ({f.get('size', '?')} bytes)")
