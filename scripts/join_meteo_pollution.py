#!/usr/bin/env python3
import argparse
import bisect
import json
import logging
import math
import pathlib
from dataclasses import dataclass
from datetime import datetime, timezone

from scipy.spatial import cKDTree


LOGGER = logging.getLogger("join_meteo_pollution")


@dataclass
class Counters:
    total_pollution_rows: int = 0
    joined_rows: int = 0
    by_id_matches: int = 0
    nearest_matches: int = 0
    missing_pollution_timestamp: int = 0
    no_candidate_station: int = 0
    no_meteo_observation: int = 0


def parse_iso_to_epoch(value: str) -> float:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.timestamp()


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def load_meteo_stations(stations_geojson_path: pathlib.Path) -> dict[str, dict]:
    data = json.loads(stations_geojson_path.read_text(encoding="utf-8"))
    stations: dict[str, dict] = {}
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [])
        if len(coords) != 2:
            continue
        station_id = str(props.get("Id", "")).strip()
        if not station_id:
            continue
        lon, lat = float(coords[0]), float(coords[1])
        stations[station_id] = {
            "stationId": station_id,
            "name": props.get("Nom"),
            "latitude": lat,
            "longitude": lon,
        }
    return stations


def build_station_tree(
    station_coords: dict[str, dict],
    candidate_station_ids: set[str],
) -> tuple[cKDTree, list[str]]:
    points: list[tuple[float, float]] = []
    station_ids: list[str] = []
    for station_id, station in station_coords.items():
        if station_id not in candidate_station_ids:
            continue
        points.append((station["latitude"], station["longitude"]))
        station_ids.append(station_id)
    if not points:
        raise ValueError("No meteo station coordinates available to build spatial index")
    tree = cKDTree(points)
    return tree, station_ids


def load_meteo_by_station(meteo_jsonl_path: pathlib.Path) -> tuple[dict[str, list], dict[str, list]]:
    rows_by_station: dict[str, list] = {}
    epochs_by_station: dict[str, list] = {}

    with meteo_jsonl_path.open("r", encoding="utf-8") as src:
        for line in src:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            station_id = str(record.get("stationId", "")).strip()
            timestamp = record.get("timestamp")
            if not station_id or not timestamp:
                continue
            epoch = parse_iso_to_epoch(timestamp)
            rows_by_station.setdefault(station_id, []).append((epoch, record))

    for station_id, values in rows_by_station.items():
        values.sort(key=lambda item: item[0])
        epochs_by_station[station_id] = [item[0] for item in values]

    return rows_by_station, epochs_by_station


def nearest_meteo_station(
    pollution_lat: float,
    pollution_lon: float,
    station_coords: dict[str, dict],
    station_tree: cKDTree,
    station_id_by_tree_idx: list[str],
    max_distance_km: float,
) -> tuple[str | None, float | None]:
    _, idx = station_tree.query((pollution_lat, pollution_lon), k=1)
    station_id = station_id_by_tree_idx[int(idx)]
    station = station_coords[station_id]
    best_distance = haversine_km(
        pollution_lat,
        pollution_lon,
        station["latitude"],
        station["longitude"],
    )

    if best_distance > max_distance_km:
        return None, None
    return station_id, best_distance


def nearest_meteo_observation(
    target_epoch: float,
    station_id: str,
    rows_by_station: dict[str, list],
    epochs_by_station: dict[str, list],
    max_time_diff_seconds: float,
) -> tuple[dict | None, float | None]:
    epochs = epochs_by_station.get(station_id)
    if not epochs:
        return None, None

    i = bisect.bisect_left(epochs, target_epoch)
    candidates = []
    if i < len(epochs):
        candidates.append(i)
    if i > 0:
        candidates.append(i - 1)

    best_record = None
    best_diff = None
    for idx in candidates:
        epoch, record = rows_by_station[station_id][idx]
        diff = abs(epoch - target_epoch)
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best_record = record

    if best_diff is None or best_diff > max_time_diff_seconds:
        return None, None
    return best_record, best_diff


def write_quality_report(report_path: pathlib.Path, counters: Counters) -> None:
    lines = [
        "# Geo Join Quality Report",
        "",
        f"- Total pollution rows: {counters.total_pollution_rows}",
        f"- Joined rows: {counters.joined_rows}",
        f"- BY_ID matches: {counters.by_id_matches}",
        f"- NEAREST matches: {counters.nearest_matches}",
        f"- Missing pollution timestamp: {counters.missing_pollution_timestamp}",
        f"- No candidate meteo station: {counters.no_candidate_station}",
        f"- No meteo observation in time window: {counters.no_meteo_observation}",
    ]
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Join pollution and meteo datasets by station id or nearest GPS station"
    )
    parser.add_argument(
        "--meteo-input",
        default="data/processed/meteo_normalized.jsonl",
        help="Path to normalized meteo JSONL",
    )
    parser.add_argument(
        "--pollution-input",
        default="data/processed/pollution_normalized.jsonl",
        help="Path to normalized pollution JSONL",
    )
    parser.add_argument(
        "--stations-geojson",
        default="data/raw/postes_synop.geojson",
        help="Path to meteo stations GeoJSON",
    )
    parser.add_argument(
        "--output",
        default="data/processed/station_snapshots.jsonl",
        help="Path to joined output JSONL",
    )
    parser.add_argument(
        "--quality-report",
        default="data/processed/join_quality_report.md",
        help="Path to markdown quality report",
    )
    parser.add_argument(
        "--max-distance-km",
        type=float,
        default=50.0,
        help="Max distance for nearest station fallback",
    )
    parser.add_argument(
        "--max-time-diff-hours",
        type=float,
        default=6.0,
        help="Max time delta to match meteo observation",
    )
    args = parser.parse_args()

    meteo_input = pathlib.Path(args.meteo_input)
    pollution_input = pathlib.Path(args.pollution_input)
    stations_geojson = pathlib.Path(args.stations_geojson)
    output_path = pathlib.Path(args.output)
    report_path = pathlib.Path(args.quality_report)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    station_coords = load_meteo_stations(stations_geojson)
    rows_by_station, epochs_by_station = load_meteo_by_station(meteo_input)
    meteo_station_ids = set(rows_by_station.keys())
    station_tree, station_id_by_tree_idx = build_station_tree(station_coords, meteo_station_ids)
    LOGGER.info("Loaded %s meteo stations with observations", len(meteo_station_ids))

    counters = Counters()
    max_time_diff_seconds = args.max_time_diff_hours * 3600.0

    with pollution_input.open("r", encoding="utf-8") as src, output_path.open("w", encoding="utf-8") as dst:
        for line in src:
            line = line.strip()
            if not line:
                continue
            counters.total_pollution_rows += 1
            pollution = json.loads(line)

            pollution_station_id = str(pollution.get("stationId", "")).strip()
            pollution_timestamp = pollution.get("timestamp")
            if not pollution_timestamp:
                counters.missing_pollution_timestamp += 1
                continue

            try:
                pollution_epoch = parse_iso_to_epoch(pollution_timestamp)
            except Exception:
                counters.missing_pollution_timestamp += 1
                continue

            join_method = None
            meteo_station_id = None
            distance_km = None

            if pollution_station_id in meteo_station_ids:
                join_method = "BY_ID"
                meteo_station_id = pollution_station_id
                distance_km = 0.0
            else:
                coords = pollution.get("coordinates") or {}
                lat = coords.get("latitude")
                lon = coords.get("longitude")
                if lat is None or lon is None:
                    counters.no_candidate_station += 1
                    continue

                meteo_station_id, distance_km = nearest_meteo_station(
                    float(lat),
                    float(lon),
                    station_coords,
                    station_tree,
                    station_id_by_tree_idx,
                    args.max_distance_km,
                )
                if meteo_station_id is None:
                    counters.no_candidate_station += 1
                    continue
                join_method = "NEAREST"

            meteo_record, time_diff_seconds = nearest_meteo_observation(
                pollution_epoch,
                meteo_station_id,
                rows_by_station,
                epochs_by_station,
                max_time_diff_seconds,
            )
            if meteo_record is None:
                counters.no_meteo_observation += 1
                continue

            if join_method == "BY_ID":
                counters.by_id_matches += 1
            else:
                counters.nearest_matches += 1

            station_info = station_coords.get(meteo_station_id, {})
            joined = {
                "stationId": pollution_station_id,
                "stationName": station_info.get("name") or pollution_station_id,
                "timestamp": pollution_timestamp,
                "join": {
                    "method": join_method,
                    "meteoStationId": meteo_station_id,
                    "meteoStationName": station_info.get("name"),
                    "distanceKm": None if distance_km is None else round(distance_km, 3),
                    "timeDeltaMinutes": round((time_diff_seconds or 0.0) / 60.0, 2),
                },
                "coordinates": {
                    "pollution": pollution.get("coordinates"),
                    "meteo": {
                        "latitude": station_info.get("latitude"),
                        "longitude": station_info.get("longitude"),
                    },
                },
                "pollution": pollution.get("pollution"),
                "meteo": meteo_record.get("meteo"),
            }
            dst.write(json.dumps(joined, ensure_ascii=True) + "\n")
            counters.joined_rows += 1

    write_quality_report(report_path, counters)

    LOGGER.info("Joined snapshots written to: %s", output_path)
    LOGGER.info("Join quality report written to: %s", report_path)
    LOGGER.info(
        "Rows: "
        f"pollution={counters.total_pollution_rows}, "
        f"joined={counters.joined_rows}, "
        f"by_id={counters.by_id_matches}, "
        f"nearest={counters.nearest_matches}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
