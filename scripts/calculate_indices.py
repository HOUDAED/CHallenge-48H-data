#!/usr/bin/env python3
import argparse
import json
import logging
import pathlib
from dataclasses import dataclass


LOGGER = logging.getLogger("calculate_indices")


POLLUTANT_LIMITS = {
    "pm25": 25.0,
    "pm10": 50.0,
    "no2": 200.0,
    "o3": 120.0,
    "so2": 125.0,
    "co": 10.0,
}

POLLUTION_WEIGHTS = {
    "pm25": 0.30,
    "pm10": 0.20,
    "no2": 0.20,
    "o3": 0.15,
    "so2": 0.10,
    "co": 0.05,
}

METEO_WEIGHTS = {
    "wind": 0.45,
    "rain": 0.20,
    "humidity": 0.20,
    "pressure": 0.15,
}

COMPOSITE_WEIGHTS = {
    "pollution": 0.75,
    "meteo": 0.25,
}


@dataclass
class Counters:
    total_rows: int = 0
    output_rows: int = 0
    missing_pollution_score: int = 0
    missing_meteo_score: int = 0
    missing_composite_score: int = 0


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


def compute_ipn(pollution: dict | None) -> float | None:
    pollution = pollution or {}
    pollutant_scores: dict[str, float | None] = {}
    for pollutant, limit in POLLUTANT_LIMITS.items():
        value = pollution.get(pollutant)
        if value is None:
            pollutant_scores[pollutant] = None
            continue
        try:
            score = 100.0 * float(value) / limit
        except (TypeError, ValueError):
            pollutant_scores[pollutant] = None
            continue
        pollutant_scores[pollutant] = clamp(score, 0.0, 100.0)
    return weighted_average(pollutant_scores, POLLUTION_WEIGHTS)


def compute_pressure_penalty(pressure_hpa: float) -> float:
    # Higher pressure often indicates more stable and stagnant conditions.
    return clamp((pressure_hpa - 1005.0) / 20.0 * 100.0, 0.0, 100.0)


def compute_imd(meteo: dict | None) -> float | None:
    meteo = meteo or {}

    wind = meteo.get("windSpeed")
    humidity = meteo.get("humidity")
    pressure = meteo.get("pressure")
    rainfall = meteo.get("rainfall")

    components: dict[str, float | None] = {
        "wind": None,
        "rain": None,
        "humidity": None,
        "pressure": None,
    }

    try:
        if wind is not None:
            wind_dispersion = clamp(float(wind) / 30.0 * 100.0, 0.0, 100.0)
            components["wind"] = wind_dispersion
    except (TypeError, ValueError):
        pass

    try:
        if rainfall is not None:
            rain_dispersion = clamp(float(rainfall) / 2.0 * 100.0, 0.0, 100.0)
            components["rain"] = rain_dispersion
    except (TypeError, ValueError):
        pass

    try:
        if humidity is not None:
            humidity_dispersion = clamp(100.0 - float(humidity), 0.0, 100.0)
            components["humidity"] = humidity_dispersion
    except (TypeError, ValueError):
        pass

    try:
        if pressure is not None:
            pressure_penalty = compute_pressure_penalty(float(pressure))
            pressure_dispersion = clamp(100.0 - pressure_penalty, 0.0, 100.0)
            components["pressure"] = pressure_dispersion
    except (TypeError, ValueError):
        pass

    dispersion_score = weighted_average(components, METEO_WEIGHTS)
    if dispersion_score is None:
        return None

    # IMD is a risk-oriented score: high value means meteo conditions are unfavorable.
    return clamp(100.0 - dispersion_score, 0.0, 100.0)


def compute_icam(ipn: float | None, imd: float | None) -> float | None:
    components = {
        "pollution": ipn,
        "meteo": imd,
    }
    score = weighted_average(components, COMPOSITE_WEIGHTS)
    if score is None:
        return None
    return clamp(score, 0.0, 100.0)


def write_quality_report(report_path: pathlib.Path, counters: Counters) -> None:
    lines = [
        "# Composite Indices Quality Report",
        "",
        f"- Input rows: {counters.total_rows}",
        f"- Output rows: {counters.output_rows}",
        f"- Missing IPN: {counters.missing_pollution_score}",
        f"- Missing IMD: {counters.missing_meteo_score}",
        f"- Missing ICAM: {counters.missing_composite_score}",
    ]
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description="Compute weighted pollution+meteo composite indices")
    parser.add_argument(
        "--input",
        default="data/processed/station_snapshots.jsonl",
        help="Path to joined station snapshots JSONL",
    )
    parser.add_argument(
        "--output",
        default="data/processed/indices_composite.jsonl",
        help="Path to indexed output JSONL",
    )
    parser.add_argument(
        "--quality-report",
        default="data/processed/indices_quality_report.md",
        help="Path to markdown quality report",
    )
    args = parser.parse_args()

    input_path = pathlib.Path(args.input)
    output_path = pathlib.Path(args.output)
    report_path = pathlib.Path(args.quality_report)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    counters = Counters()

    with input_path.open("r", encoding="utf-8") as src, output_path.open("w", encoding="utf-8") as dst:
        for line in src:
            line = line.strip()
            if not line:
                continue

            counters.total_rows += 1
            record = json.loads(line)

            ipn = compute_ipn(record.get("pollution"))
            imd = compute_imd(record.get("meteo"))
            icam = compute_icam(ipn, imd)

            if ipn is None:
                counters.missing_pollution_score += 1
            if imd is None:
                counters.missing_meteo_score += 1
            if icam is None:
                counters.missing_composite_score += 1

            indices = [
                {
                    "id": "IPN",
                    "label": "Indice Pollution Normalisee",
                    "value": None if ipn is None else round(ipn, 2),
                    "weights": POLLUTION_WEIGHTS,
                },
                {
                    "id": "IMD",
                    "label": "Indice Meteo Defavorable",
                    "value": None if imd is None else round(imd, 2),
                    "weights": METEO_WEIGHTS,
                },
                {
                    "id": "ICAM",
                    "label": "Indice Composite Air Meteo",
                    "value": None if icam is None else round(icam, 2),
                    "weights": COMPOSITE_WEIGHTS,
                },
            ]

            record["indices"] = indices
            dst.write(json.dumps(record, ensure_ascii=True) + "\n")
            counters.output_rows += 1

    write_quality_report(report_path, counters)

    LOGGER.info("Indexed snapshots written to: %s", output_path)
    LOGGER.info("Indices quality report written to: %s", report_path)
    LOGGER.info("Rows: input=%s, output=%s", counters.total_rows, counters.output_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())