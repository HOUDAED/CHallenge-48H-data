#!/usr/bin/env python3
import argparse
import csv
import gzip
import json
import logging
import pathlib
from dataclasses import dataclass
from datetime import timezone

from dateutil import parser as date_parser


LOGGER = logging.getLogger("transform_meteo_data")


CARDINALS = [
    "N",
    "NNE",
    "NE",
    "ENE",
    "E",
    "ESE",
    "SE",
    "SSE",
    "S",
    "SSW",
    "SW",
    "WSW",
    "W",
    "WNW",
    "NW",
    "NNW",
]


@dataclass
class Counters:
    total_rows: int = 0
    valid_rows: int = 0
    missing_station: int = 0
    missing_timestamp: int = 0
    conversion_errors: int = 0


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    val = value.strip()
    if not val or val.lower() in {"mq", "nan"}:
        return None
    try:
        return float(val.replace(",", "."))
    except ValueError:
        return None


def pick_value(row: dict[str, str], keys: list[str]) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is not None and value.strip() != "":
            return value
    return None


def to_cardinal(degrees: float | None) -> str:
    if degrees is None:
        return "N"
    index = round(degrees / 22.5) % 16
    return CARDINALS[index]


def normalize_timestamp(raw: str) -> str:
    dt = date_parser.parse(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def transform_row(row: dict[str, str], counters: Counters) -> dict | None:
    counters.total_rows += 1
    station_id = (pick_value(row, ["numer_sta", "geo_id_wmo", "station_id"]) or "").strip()
    if not station_id:
        counters.missing_station += 1
        return None

    date_raw = (pick_value(row, ["date", "validity_time", "timestamp"]) or "").strip()
    if not date_raw:
        counters.missing_timestamp += 1
        return None

    try:
        timestamp = normalize_timestamp(date_raw)
    except Exception:
        counters.conversion_errors += 1
        return None

    temperature_k = parse_float(row.get("t"))
    temperature = None if temperature_k is None else round(temperature_k - 273.15, 2)

    humidity = parse_float(row.get("u"))
    pressure_raw = parse_float(pick_value(row, ["pres", "pmer"]))
    pressure = pressure_raw
    if pressure is not None and pressure > 2000:
        pressure = round(pressure / 100.0, 2)

    wind_speed_ms = parse_float(row.get("ff"))
    wind_speed = None if wind_speed_ms is None else round(wind_speed_ms * 3.6, 2)

    wind_degrees = parse_float(row.get("dd"))
    wind_direction = to_cardinal(wind_degrees)

    rainfall = parse_float(row.get("rr1"))
    if rainfall is None:
        rainfall = parse_float(row.get("rr3"))

    normalized = {
        "stationId": station_id,
        "timestamp": timestamp,
        "meteo": {
            "temperature": temperature if temperature is not None else 0.0,
            "humidity": humidity if humidity is not None else 0.0,
            "pressure": pressure if pressure is not None else 0.0,
            "windSpeed": wind_speed if wind_speed is not None else 0.0,
            "windDirection": wind_direction,
            "rainfall": rainfall,
        },
    }

    counters.valid_rows += 1
    return normalized


def write_quality_report(report_path: pathlib.Path, counters: Counters) -> None:
    invalid = counters.total_rows - counters.valid_rows
    lines = [
        "# Meteo Ingestion Quality Report",
        "",
        f"- Total rows: {counters.total_rows}",
        f"- Valid rows: {counters.valid_rows}",
        f"- Invalid rows: {invalid}",
        f"- Missing station ID: {counters.missing_station}",
        f"- Missing timestamp: {counters.missing_timestamp}",
        f"- Conversion errors: {counters.conversion_errors}",
    ]
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description="Transform raw SYNOP CSV into normalized MeteoData records")
    parser.add_argument("--input", required=True, help="Path to synop_YYYY.csv.gz or synop_YYYY.csv")
    parser.add_argument(
        "--output",
        default="data/processed/meteo_normalized.jsonl",
        help="Path to normalized JSONL output",
    )
    parser.add_argument(
        "--quality-report",
        default="data/processed/meteo_quality_report.md",
        help="Path to markdown quality report",
    )
    args = parser.parse_args()

    input_path = pathlib.Path(args.input)
    output_path = pathlib.Path(args.output)
    report_path = pathlib.Path(args.quality_report)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    opener = gzip.open if input_path.suffix == ".gz" else open

    counters = Counters()
    with opener(input_path, "rt", encoding="utf-8", newline="") as src, output_path.open(
        "w", encoding="utf-8"
    ) as dst:
        reader = csv.DictReader(src, delimiter=";")
        for row in reader:
            normalized = transform_row(row, counters)
            if normalized is None:
                continue
            dst.write(json.dumps(normalized, ensure_ascii=True) + "\n")

    write_quality_report(report_path, counters)

    LOGGER.info("Normalized records written to: %s", output_path)
    LOGGER.info("Quality report written to: %s", report_path)
    LOGGER.info("Rows: total=%s, valid=%s", counters.total_rows, counters.valid_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
