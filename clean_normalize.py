"""
Clean and normalize LCSQA + SYNOP March 2026 data, then join by geospatial proximity.
Usage: python3 clean_normalize.py
"""
import logging
from pathlib import Path

import pandas as pd

from src.data_cleaning import clean_lcsqa, clean_synop
from src.data_normalization import join

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")


def save(df: pd.DataFrame, name: str):
    path_parquet = PROCESSED_DIR / f"{name}.parquet"
    path_csv = PROCESSED_DIR / f"{name}.csv"
    df.to_parquet(path_parquet, index=False)
    df.to_csv(path_csv, index=False)
    logger.info("Saved %s — %d rows (%s / %s)", name, len(df), path_parquet, path_csv)


def main():
    # Load raw parquets
    logger.info("Loading data...")
    lcsqa = pd.read_parquet(PROCESSED_DIR / "lcsqa_2026_march.parquet")
    synop = pd.read_parquet(PROCESSED_DIR / "synop_2026_march.parquet")
    logger.info("Loaded LCSQA: %d rows | SYNOP: %d rows", len(lcsqa), len(synop))

    # Clean
    logger.info("Cleaning LCSQA...")
    lcsqa_clean = clean_lcsqa(lcsqa)
    save(lcsqa_clean, "lcsqa_2026_march_clean")

    logger.info("Cleaning SYNOP...")
    synop_clean = clean_synop(synop)
    save(synop_clean, "synop_2026_march_clean")

    # Geospatial join + temporal alignment
    logger.info("Joining datasets...")
    combined = join(lcsqa_clean, synop_clean)
    if combined.empty:
        logger.error("Combined dataset is empty — check logs above")
        return
    save(combined, "combined_2026_march")

    # Summary
    logger.info("Done.")
    logger.info("  Pollutants: %s", combined["pollutant"].unique().tolist())
    logger.info("  Date range: %s → %s", combined["timestamp"].min(), combined["timestamp"].max())
    logger.info("  Distance stats (km): min=%.1f mean=%.1f max=%.1f",
                combined["distance_km"].min(),
                combined["distance_km"].mean(),
                combined["distance_km"].max())


if __name__ == "__main__":
    main()
