"""
Orchestrator: manages the full data pipeline.

Steps:
  1. Fetch raw data (LCSQA + SYNOP) for a date range
  2. Clean both datasets
  3. Normalize (geospatial join + temporal alignment)
  4. Persist processed files
  5. Reload the API store so new data is served immediately

Usage:
    from src.orchestrator.pipeline import Pipeline
    pipeline = Pipeline()
    pipeline.run_historical(2026)
    pipeline.run_realtime()
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from src.data_acquisition import LCSQAClient, SYNOPClient
from src.data_cleaning import clean_lcsqa, clean_synop
from src.data_normalization import join

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")


class Pipeline:
    def __init__(self, processed_dir: Path = PROCESSED_DIR):
        self.processed_dir = processed_dir
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self._lcsqa = LCSQAClient()
        self._synop = SYNOPClient()

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _save(self, df: pd.DataFrame, name: str) -> None:
        df.to_parquet(self.processed_dir / f"{name}.parquet", index=False)
        df.to_csv(self.processed_dir / f"{name}.csv", index=False)
        logger.info("Saved %s — %d rows", name, len(df))

    def _reload_store(self) -> None:
        try:
            from src.api.store import store
            # Pick latest combined parquet
            candidates = sorted(self.processed_dir.glob("combined_*.parquet"), reverse=True)
            if candidates:
                store.reload(candidates[0])
        except Exception as exc:
            logger.warning("Could not reload API store: %s", exc)

    # ------------------------------------------------------------------ #
    # Public pipeline methods
    # ------------------------------------------------------------------ #

    def run_date_range(
        self,
        start: date,
        end: date,
        label: Optional[str] = None,
        reload_store: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch, clean, normalize and persist data for a date range.
        Returns the combined DataFrame.
        """
        if label is None:
            label = f"{start.isoformat()}_{end.isoformat()}"

        logger.info("=== Pipeline: %s → %s ===", start, end)

        # 1. Fetch
        logger.info("Fetching LCSQA...")
        lcsqa_raw = self._lcsqa.fetch_date_range(start, end)
        if lcsqa_raw.empty:
            logger.warning("LCSQA: no data for %s → %s", start, end)
            return pd.DataFrame()

        logger.info("Fetching SYNOP...")
        synop_raw = self._synop.fetch_year(start.year)
        if synop_raw.empty:
            logger.warning("SYNOP: no data for year %d", start.year)
            return pd.DataFrame()

        # Filter SYNOP to date range
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end) + pd.Timedelta(hours=23, minutes=59)
        if synop_raw["timestamp"].dt.tz is not None:
            start_ts = start_ts.tz_localize("UTC")
            end_ts = end_ts.tz_localize("UTC")
        synop_raw = synop_raw[synop_raw["timestamp"].between(start_ts, end_ts)]

        # 2. Clean
        logger.info("Cleaning LCSQA...")
        lcsqa_clean = clean_lcsqa(lcsqa_raw)
        self._save(lcsqa_clean, f"lcsqa_{label}_clean")

        logger.info("Cleaning SYNOP...")
        synop_clean = clean_synop(synop_raw)
        self._save(synop_clean, f"synop_{label}_clean")

        # 3. Normalize + join
        logger.info("Joining datasets...")
        combined = join(lcsqa_clean, synop_clean)
        if combined.empty:
            logger.error("Combined dataset is empty — check station coverage")
            return pd.DataFrame()

        self._save(combined, f"combined_{label}")

        # 4. Reload API store
        if reload_store:
            self._reload_store()

        logger.info("Pipeline complete: %d rows in combined dataset", len(combined))
        return combined

    def run_historical(self, year: int) -> pd.DataFrame:
        """Fetch a full calendar year (up to today)."""
        start = date(year, 1, 1)
        end = min(date(year, 12, 31), date.today())
        return self.run_date_range(start, end, label=str(year))

    def run_realtime(self) -> pd.DataFrame:
        """Fetch and process the most recent available day."""
        today = date.today()
        # LCSQA data is typically available with a ~1 day lag
        target = today - timedelta(days=1)
        return self.run_date_range(target, target, label=f"realtime_{target.isoformat()}")

    def run_month(self, year: int, month: int) -> pd.DataFrame:
        """Convenience: fetch a single month."""
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        start = date(year, month, 1)
        end = date(year, month, last_day)
        return self.run_date_range(start, end, label=f"{year}_{month:02d}")
