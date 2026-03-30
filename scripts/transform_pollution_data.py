#!/usr/bin/env python3
import argparse
import csv
import json
import pathlib
from dataclasses import dataclass
from datetime import timezone

from dateutil import parser as date_parser


POLLUTANT_SYNONYMS = {
    "pm25": {"pm25", "pm2.5", "pm2_5", "pm 2.5", "particules pm2.5", "pm2,5"},
    "pm10": {"pm10", "pm 10", "particules pm10"},
    "no2": {"no2", "dioxyde d'azote", "dioxyde dazote"},
    "o3": {"o3", "ozone"},
    "co": {"co", "monoxyde de carbone"},
    "so2": {"so2", "dioxyde de soufre"},
}


@dataclass
class Counters:
    total_rows: int = 0
    grouped_rows: int = 0
    missing_station: int = 0
    missing_timestamp: int = 0
    unknown_pollutant: int = 0
    missing_value: int = 0


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    val = value.strip()
    if not val or val.lower() in {"mq", "na", "nan", "null"}:
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


def normalize_timestamp(raw: str) -> str:
    dt = date_parser.parse(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def normalize_pollutant_name(raw: str | None) -> str | None:
    if raw is None:
        return None
    val = raw.strip().lower()
    if not val:
        return None
    for normalized, aliases in POLLUTANT_SYNONYMS.items():
        if val in aliases:
            return normalized
    return None


def detect_delimiter(header_line: str) -> str:
    semicolon_count = header_line.count(";")
    comma_count = header_line.count(",")
    if semicolon_count >= comma_count:
        return ";"
    return ","


def write_quality_report(report_path: pathlib.Path, counters: Counters) -> None:
    invalid = (
        counters.missing_station
        + counters.missing_timestamp
        + counters.unknown_pollutant
        + counters.missing_value
    )
    lines = [
        "# Pollution Ingestion Quality Report",
        "",
        f"- Total rows read: {counters.total_rows}",
        f"- Grouped snapshots: {counters.grouped_rows}",
        f"- Missing station ID: {counters.missing_station}",
        f"- Missing timestamp: {counters.missing_timestamp}",
        f"- Unknown pollutant labels: {counters.unknown_pollutant}",
        f"- Missing concentration values: {counters.missing_value}",
        f"- Approx invalid signals: {invalid}",
    ]
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Transform pollution CSV into normalized PollutantData records"
    )
    parser.add_argument("--input", required=True, help="Path to FR_E2_YYYY-MM-DD.csv")
    parser.add_argument(
        "--output",
        default="data/processed/pollution_normalized.jsonl",
        help="Path to normalized JSONL output",
    )
    parser.add_argument(
        "--quality-report",
        default="data/processed/pollution_quality_report.md",
        help="Path to markdown quality report",
    )
    args = parser.parse_args()

    input_path = pathlib.Path(args.input)
    output_path = pathlib.Path(args.output)
    report_path = pathlib.Path(args.quality_report)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    counters = Counters()
    grouped: dict[tuple[str, str], dict] = {}

    with input_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as src:
        first_line = src.readline()
        if not first_line:
            raise SystemExit("Input CSV is empty")

        delimiter = detect_delimiter(first_line)
        src.seek(0)
        reader = csv.DictReader(src, delimiter=delimiter)

        for row in reader:
            counters.total_rows += 1

            station_id = (
                pick_value(
                    row,
                    [
                        "station_id",
                        "code site",
                        "codesite",
                        "code_station",
                        "id_station",
                        "identifiant_station",
                    ],
                )
                or ""
            ).strip()
            if not station_id:
                counters.missing_station += 1
                continue

            timestamp_raw = (
                pick_value(
                    row,
                    [
                        "timestamp",
                        "date_heure",
                        "date de début",
                        "date_debut",
                        "date",
                        "heure",
                    ],
                )
                or ""
            ).strip()
            if not timestamp_raw:
                counters.missing_timestamp += 1
                continue

            try:
                timestamp = normalize_timestamp(timestamp_raw)
            except Exception:
                counters.missing_timestamp += 1
                continue

            pollutant_raw = pick_value(row, ["polluant", "pollutant", "nom_poll", "libellé"]) or ""
            pollutant = normalize_pollutant_name(pollutant_raw)
            if pollutant is None:
                counters.unknown_pollutant += 1
                continue

            value = parse_float(
                pick_value(row, ["valeur", "concentration", "value", "resultat", "mesure"])
            )
            if value is None:
                counters.missing_value += 1
                continue

            lat = parse_float(pick_value(row, ["latitude", "lat"]))
            lon = parse_float(pick_value(row, ["longitude", "lon", "lng"]))

            key = (station_id, timestamp)
            if key not in grouped:
                grouped[key] = {
                    "stationId": station_id,
                    "timestamp": timestamp,
                    "coordinates": {
                        "latitude": lat,
                        "longitude": lon,
                    },
                    "pollution": {
                        "pm25": None,
                        "pm10": None,
                        "no2": None,
                        "o3": None,
                        "co": None,
                        "so2": None,
                    },
                }

            grouped[key]["pollution"][pollutant] = value

            if lat is not None and grouped[key]["coordinates"]["latitude"] is None:
                grouped[key]["coordinates"]["latitude"] = lat
            if lon is not None and grouped[key]["coordinates"]["longitude"] is None:
                grouped[key]["coordinates"]["longitude"] = lon

    with output_path.open("w", encoding="utf-8") as dst:
        for record in grouped.values():
            dst.write(json.dumps(record, ensure_ascii=True) + "\n")

    counters.grouped_rows = len(grouped)
    write_quality_report(report_path, counters)

    print(f"Normalized pollution records written to: {output_path}")
    print(f"Pollution quality report written to: {report_path}")
    print(f"Rows: raw={counters.total_rows}, grouped={counters.grouped_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
