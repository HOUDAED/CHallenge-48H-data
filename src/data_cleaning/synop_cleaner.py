"""
SYNOP meteorological data cleaner.
"""
import logging

import pandas as pd

logger = logging.getLogger(__name__)

KEEP_COLS = [
    "station_id", "station_name", "lat", "lng", "timestamp",
    "temperature", "humidity", "pressure", "wind_speed", "wind_direction", "rainfall",
]

RANGES = {
    "temperature": (-30, 50),
    "humidity":    (0, 100),
    "pressure":    (950, 1050),
    "wind_speed":  (0, 200),
}


def clean(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)

    # 1. Keep only relevant columns
    df = df[[c for c in KEEP_COLS if c in df.columns]].copy()

    # 2. Strip timezone → tz-naive UTC for join compatibility
    if pd.api.types.is_datetime64tz_dtype(df["timestamp"]):
        df["timestamp"] = df["timestamp"].dt.tz_localize(None)

    # 3. Clip physical ranges
    for col, (lo, hi) in RANGES.items():
        if col in df.columns:
            df[col] = df[col].clip(lower=lo, upper=hi)

    # 4. Remove duplicates
    df = df.drop_duplicates(subset=["timestamp", "station_id"])

    # 5. Interpolate missing values within each station (max 2 consecutive gaps)
    df = df.sort_values(["station_id", "timestamp"])
    num_cols = ["temperature", "humidity", "pressure", "wind_speed", "rainfall"]
    for col in num_cols:
        if col in df.columns:
            df[col] = (
                df.groupby("station_id")[col]
                .transform(lambda s: s.interpolate(method="linear", limit=2))
            )

    after = len(df)
    logger.info("SYNOP clean: %d → %d rows (-%d)", before, after, before - after)
    return df.reset_index(drop=True)
