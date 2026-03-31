#!/usr/bin/env python3
"""
Step 6 — Prévisions d'indices par régression linéaire simple.

Deux modèles complémentaires :

  Modèle A — AR global (court terme, 24h)
    Features(t) → IMD(t+1)
    Un seul modèle global entraîné sur toutes les stations et toutes les années.
    Prévision : dernier état météo connu → 24 prochaines heures.

  Modèle B — Tendance linéaire par station (long terme, 7 jours)
    day_number → daily_mean_IMD
    Répond à : "la qualité de l'air s'améliore-t-elle sur le long terme ?"

Dépendances : numpy (disponible via scipy), pas de nouvelles dépendances.
Input  : data/processed/meteo_normalized.jsonl
Output : data/processed/forecast.jsonl
         data/processed/forecast_quality_report.md
"""
import argparse
import json
import logging
import math
import pathlib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np


LOGGER = logging.getLogger("forecast_indices")

# ---------------------------------------------------------------------------
# IMD helpers — copied from calculate_indices.py to avoid import coupling
# ---------------------------------------------------------------------------

METEO_WEIGHTS = {
    "wind": 0.45,
    "rain": 0.20,
    "humidity": 0.20,
    "pressure": 0.15,
}


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def weighted_average(components: dict[str, float | None], weights: dict[str, float]) -> float | None:
    weighted_sum = 0.0
    available_weight = 0.0
    for key, weight in weights.items():
        value = components.get(key)
        if value is None:
            continue
        weighted_sum += value * weight
        available_weight += weight
    if available_weight == 0.0:
        return None
    return weighted_sum / available_weight


def compute_pressure_penalty(pressure_hpa: float) -> float:
    return clamp((pressure_hpa - 1005.0) / 20.0 * 100.0, 0.0, 100.0)


def compute_imd(meteo: dict | None) -> float | None:
    meteo = meteo or {}
    wind = meteo.get("windSpeed")
    humidity = meteo.get("humidity")
    pressure = meteo.get("pressure")
    rainfall = meteo.get("rainfall")

    components: dict[str, float | None] = {"wind": None, "rain": None, "humidity": None, "pressure": None}

    try:
        if wind is not None:
            components["wind"] = clamp(float(wind) / 30.0 * 100.0, 0.0, 100.0)
    except (TypeError, ValueError):
        pass
    try:
        if rainfall is not None:
            components["rain"] = clamp(float(rainfall) / 2.0 * 100.0, 0.0, 100.0)
    except (TypeError, ValueError):
        pass
    try:
        if humidity is not None:
            components["humidity"] = clamp(100.0 - float(humidity), 0.0, 100.0)
    except (TypeError, ValueError):
        pass
    try:
        if pressure is not None:
            components["pressure"] = clamp(100.0 - compute_pressure_penalty(float(pressure)), 0.0, 100.0)
    except (TypeError, ValueError):
        pass

    dispersion = weighted_average(components, METEO_WEIGHTS)
    if dispersion is None:
        return None
    return clamp(100.0 - dispersion, 0.0, 100.0)


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def build_features(meteo: dict, dt: datetime) -> list[float] | None:
    """
    Build the feature vector used by the AR global model.
    Returns None if any of the four critical meteo fields are missing.

    Features:
      [0] windSpeed / 30           (normalised, ~0–1 for typical values)
      [1] humidity / 100
      [2] (pressure - 980) / 40   (centred around ~1000 hPa)
      [3] temperature / 40        (rough normalisation for French climate)
      [4] rainfall / 2 clamped    (light rain = 1)
      [5] sin(2π * hour / 24)     cyclic hour-of-day
      [6] cos(2π * hour / 24)
      [7] sin(2π * doy / 365)     cyclic day-of-year (seasonality)
      [8] cos(2π * doy / 365)
      [9] 1.0                     bias term
    """
    wind = meteo.get("windSpeed")
    humidity = meteo.get("humidity")
    pressure = meteo.get("pressure")
    temp = meteo.get("temperature")
    rain = meteo.get("rainfall") or 0.0

    if any(v is None for v in (wind, humidity, pressure, temp)):
        return None

    hour = dt.hour + dt.minute / 60.0
    doy = dt.timetuple().tm_yday

    return [
        float(wind) / 30.0,
        float(humidity) / 100.0,
        (float(pressure) - 980.0) / 40.0,
        float(temp) / 40.0,
        clamp(float(rain) / 2.0, 0.0, 1.0),
        math.sin(2 * math.pi * hour / 24),
        math.cos(2 * math.pi * hour / 24),
        math.sin(2 * math.pi * doy / 365),
        math.cos(2 * math.pi * doy / 365),
        1.0,
    ]


# ---------------------------------------------------------------------------
# Linear regression helpers
# ---------------------------------------------------------------------------

def fit_lstsq(X: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, float]:
    """Fit y = X @ w. Returns (weights, R²)."""
    w, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    y_pred = X @ w
    ss_res = float(np.sum((y - y_pred) ** 2))
    ss_tot = float(np.sum((y - float(np.mean(y))) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0.0 else 0.0
    return w, r2


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def parse_timestamp(value: str) -> datetime:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_records(path: pathlib.Path) -> list[dict]:
    records = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


# ---------------------------------------------------------------------------
# Quality report
# ---------------------------------------------------------------------------

@dataclass
class Counters:
    total_input_records: int = 0
    records_with_valid_imd: int = 0
    records_skipped_missing_features: int = 0
    stations_total: int = 0
    stations_with_ar_forecast: int = 0
    stations_with_trend_forecast: int = 0
    ar_train_samples: int = 0
    ar_r2: float = 0.0
    trend_r2_values: list = field(default_factory=list)
    forecast_rows_written: int = 0


def write_quality_report(path: pathlib.Path, c: Counters) -> None:
    avg_trend_r2 = sum(c.trend_r2_values) / len(c.trend_r2_values) if c.trend_r2_values else 0.0
    lines = [
        "# Rapport qualité — Prévisions (Step 6)",
        "",
        "## Données d'entrée",
        f"- Enregistrements chargés : {c.total_input_records:,}",
        f"- Enregistrements avec IMD valide : {c.records_with_valid_imd:,}",
        f"- Enregistrements ignorés (features manquantes) : {c.records_skipped_missing_features:,}",
        f"- Stations uniques : {c.stations_total}",
        "",
        "## Modèle A — AR global (court terme 24h)",
        f"- Échantillons d'entraînement : {c.ar_train_samples:,}",
        f"- R² : {c.ar_r2:.4f}",
        f"- Stations avec prévision AR : {c.stations_with_ar_forecast}",
        "",
        "## Modèle B — Tendance linéaire par station (7 jours)",
        f"- Stations avec modèle de tendance : {c.stations_with_trend_forecast}",
        f"- R² moyen de tendance : {avg_trend_r2:.4f}",
        "",
        f"## Total lignes de prévision écrites : {c.forecast_rows_written:,}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="Forecast IMD using simple linear regression (Step 6)")
    parser.add_argument(
        "--input",
        default="data/processed/meteo_normalized.jsonl",
        help="Path to meteo_normalized.jsonl",
    )
    parser.add_argument(
        "--output",
        default="data/processed/forecast.jsonl",
        help="Path to output forecast JSONL",
    )
    parser.add_argument(
        "--quality-report",
        default="data/processed/forecast_quality_report.md",
        help="Path to markdown quality report",
    )
    parser.add_argument(
        "--horizon-hours",
        type=int,
        default=24,
        help="Number of hours to forecast ahead (Model A)",
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=7,
        help="Number of days to forecast ahead (Model B trend)",
    )
    parser.add_argument(
        "--min-trend-samples",
        type=int,
        default=10,
        help="Minimum daily data points required to fit per-station trend",
    )
    args = parser.parse_args()

    input_path = pathlib.Path(args.input)
    output_path = pathlib.Path(args.output)
    report_path = pathlib.Path(args.quality_report)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    counters = Counters()

    # -----------------------------------------------------------------------
    # 1. Load and group records by station
    # -----------------------------------------------------------------------
    LOGGER.info("Chargement des enregistrements depuis %s ...", input_path)
    raw_records = load_records(input_path)
    counters.total_input_records = len(raw_records)
    LOGGER.info("%d enregistrements chargés", counters.total_input_records)

    # Each entry: (datetime, feature_vector, imd_value, raw_meteo_dict)
    by_station: dict[str, list[tuple]] = defaultdict(list)

    for rec in raw_records:
        ts_raw = rec.get("timestamp")
        if not ts_raw:
            continue
        try:
            dt = parse_timestamp(str(ts_raw))
        except (ValueError, TypeError):
            continue

        meteo = rec.get("meteo") or {}
        imd = compute_imd(meteo)
        feats = build_features(meteo, dt)

        if imd is None or feats is None:
            counters.records_skipped_missing_features += 1
            continue

        counters.records_with_valid_imd += 1
        by_station[str(rec.get("stationId", "")).strip()].append((dt, feats, imd, meteo))

    # Sort each station's records by time
    for sid in by_station:
        by_station[sid].sort(key=lambda x: x[0])

    counters.stations_total = len(by_station)
    LOGGER.info("%d stations avec données valides", counters.stations_total)

    # -----------------------------------------------------------------------
    # 2. Model A — Build global AR training set: features(t) → IMD(t+1)
    # -----------------------------------------------------------------------
    LOGGER.info("Construction du jeu d'entraînement AR global ...")
    X_rows: list[list[float]] = []
    y_rows: list[float] = []

    for entries in by_station.values():
        for i in range(len(entries) - 1):
            X_rows.append(entries[i][1])   # features at t
            y_rows.append(entries[i + 1][2])  # IMD at t+1

    counters.ar_train_samples = len(X_rows)
    LOGGER.info("Échantillons d'entraînement AR : %d", counters.ar_train_samples)

    X_global = np.array(X_rows, dtype=float)
    y_global = np.array(y_rows, dtype=float)
    w_global, r2_global = fit_lstsq(X_global, y_global)
    counters.ar_r2 = r2_global
    LOGGER.info("Modèle AR global — R² = %.4f", r2_global)

    # -----------------------------------------------------------------------
    # 3. Model B — Per-station daily trend: day_number → daily_mean_IMD
    # -----------------------------------------------------------------------
    LOGGER.info("Ajustement des tendances linéaires par station ...")
    # trend entry: (slope, intercept, r2, last_day_index, reference_datetime)
    station_trends: dict[str, tuple] = {}

    for sid, entries in by_station.items():
        t0 = entries[0][0]
        daily_imd: dict[int, list[float]] = defaultdict(list)
        for dt, _, imd, _ in entries:
            day_idx = (dt - t0).days
            daily_imd[day_idx].append(imd)

        days = sorted(daily_imd.keys())
        if len(days) < args.min_trend_samples:
            continue

        d_arr = np.array(days, dtype=float)
        imd_arr = np.array([sum(daily_imd[d]) / len(daily_imd[d]) for d in days], dtype=float)

        A = np.column_stack([d_arr, np.ones(len(d_arr))])
        w_trend, r2_trend = fit_lstsq(A, imd_arr)

        station_trends[sid] = (
            float(w_trend[0]),   # slope (IMD units / day)
            float(w_trend[1]),   # intercept
            r2_trend,
            max(days),           # last known day index
            t0,                  # reference date (day 0)
        )

    counters.stations_with_trend_forecast = len(station_trends)
    counters.trend_r2_values = [v[2] for v in station_trends.values()]
    LOGGER.info("Tendances ajustées pour %d stations", counters.stations_with_trend_forecast)

    # -----------------------------------------------------------------------
    # 4. Generate forecasts and write output
    # -----------------------------------------------------------------------
    LOGGER.info("Génération des prévisions ...")
    output_rows: list[dict] = []

    for sid, entries in by_station.items():
        last_dt, _, _, last_meteo = entries[-1]

        # --- Model A: 24h AR forecast ---
        has_ar = False
        for h in range(1, args.horizon_hours + 1):
            target_dt = last_dt + timedelta(hours=h)
            feats_h = build_features(last_meteo, target_dt)
            if feats_h is None:
                continue
            predicted_imd = float(np.array(feats_h) @ w_global)
            predicted_imd = clamp(predicted_imd, 0.0, 100.0)
            output_rows.append({
                "stationId": sid,
                "timestamp": target_dt.isoformat(),
                "forecastHorizonH": h,
                "model": "AR_global",
                "predicted": {
                    "IMD": round(predicted_imd, 2),
                },
                "modelStats": {
                    "r2": round(r2_global, 4),
                    "trainSamples": counters.ar_train_samples,
                },
            })
            has_ar = True

        if has_ar:
            counters.stations_with_ar_forecast += 1

        # --- Model B: 7-day trend forecast ---
        if sid in station_trends:
            slope, intercept, r2_tr, last_day, t0 = station_trends[sid]
            for d in range(1, args.horizon_days + 1):
                future_day = last_day + d
                imd_trend = clamp(slope * future_day + intercept, 0.0, 100.0)
                target_dt = (t0 + timedelta(days=future_day)).replace(
                    hour=12, minute=0, second=0, microsecond=0
                )
                output_rows.append({
                    "stationId": sid,
                    "timestamp": target_dt.isoformat(),
                    "forecastHorizonDays": d,
                    "model": "trend_linear",
                    "predicted": {
                        "IMD": round(imd_trend, 2),
                    },
                    "modelStats": {
                        "r2": round(r2_tr, 4),
                        "slope": round(slope, 6),
                        "intercept": round(intercept, 4),
                    },
                })

    counters.forecast_rows_written = len(output_rows)

    with output_path.open("w", encoding="utf-8") as dst:
        for row in output_rows:
            dst.write(json.dumps(row, ensure_ascii=True) + "\n")

    write_quality_report(report_path, counters)

    LOGGER.info("Prévisions écrites : %d lignes → %s", counters.forecast_rows_written, output_path)
    LOGGER.info("Rapport qualité → %s", report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
