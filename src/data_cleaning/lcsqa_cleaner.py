"""
LCSQA air quality data cleaner.
"""
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

COLS_TO_DROP = [
    "taux de saisie", "couverture temporelle", "couverture de données",
    "valeur brute", "discriminant", "timestamp_end",
    "type d'évaluation", "procédure de mesure", "réglementaire",
    "type d'influence", "type d'implantation", "organisme",
    "zone_id", "zone_name", "code qualité",
]

# Max physical thresholds per pollutant (values above → NaN)
POLLUTANT_CAPS = {
    "PM2.5": 500,
    "PM10":  600,
    "NO2":   400,
    "NO":    400,
    "O3":    400,
    "CO":    100,   # mg/m³
    "SO2":   500,
    "NOX":   1000,
    "H2S":   500,
}


def clean(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)

    # 1. Drop useless columns (ignore missing ones)
    df = df.drop(columns=[c for c in COLS_TO_DROP if c in df.columns])

    # 2. Keep only validated measurements
    if "validité" in df.columns:
        df = df[df["validité"] == 1].drop(columns=["validité"])

    # 3. Remove negative values
    if "value" in df.columns:
        df.loc[df["value"] < 0, "value"] = np.nan

    # 4. Cap extremes per pollutant
    if "value" in df.columns and "pollutant" in df.columns:
        for pollutant, cap in POLLUTANT_CAPS.items():
            mask = df["pollutant"].str.upper().str.contains(pollutant, na=False)
            df.loc[mask & (df["value"] > cap), "value"] = np.nan

    # 5. Remove duplicates
    df = df.drop_duplicates(subset=["timestamp", "station_id", "pollutant"])

    # 6. Fill missing values within (station_id, pollutant) group
    df = df.sort_values(["station_id", "pollutant", "timestamp"])
    df["value"] = (
        df.groupby(["station_id", "pollutant"])["value"]
        .transform(lambda s: s.ffill().bfill())
    )

    after = len(df)
    logger.info("LCSQA clean: %d → %d rows (-%d)", before, after, before - after)
    return df.reset_index(drop=True)
