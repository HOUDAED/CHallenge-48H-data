"""
Fetch LCSQA + SYNOP data for March 2026 only.
Usage: python3 fetch_data.py
"""
import logging
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from src.data_acquisition import LCSQAClient, SYNOPClient

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

START = date(2026, 3, 1)
END   = date(2026, 3, 30)
PROCESSED_DIR = Path("data/processed")


def save(df, name: str):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    parquet_path = PROCESSED_DIR / f"{name}.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info("Saved parquet → %s (%d rows)", parquet_path, len(df))

    csv_path = PROCESSED_DIR / f"{name}.csv"
    df.to_csv(csv_path, index=False)
    logger.info("Saved csv    → %s", csv_path)


def main():
    lcsqa = LCSQAClient()
    synop = SYNOPClient()

    # LCSQA — stream daily CSVs for March 2026
    logger.info("=== LCSQA %s → %s ===", START, END)
    if (PROCESSED_DIR / "lcsqa_2026_march.parquet").exists():
        logger.info("LCSQA March 2026 already saved, skipping")
    else:
        df = lcsqa.fetch_date_range(START, END)
        if df is not None and not df.empty:
            save(df, "lcsqa_2026_march")
        else:
            logger.warning("LCSQA: no data for March 2026")

    # SYNOP — download yearly file and filter to March 2026
    logger.info("=== SYNOP %s → %s ===", START, END)
    df = synop.fetch_year(2026)
    if df is not None and not df.empty:
        df = df[df["timestamp"].between(
            pd.Timestamp("2026-03-01", tz="UTC"),
            pd.Timestamp("2026-03-30 23:59:59", tz="UTC"),
        )]
        save(df, "synop_2026_march")
    else:
        logger.warning("SYNOP: no data for 2026")


if __name__ == "__main__":
    main()
