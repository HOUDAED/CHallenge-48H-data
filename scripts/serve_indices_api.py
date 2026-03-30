#!/usr/bin/env python3
import argparse
import json
from datetime import date, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


def load_indices(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as src:
        for line in src:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def load_station_names(path: Path) -> dict[str, str]:
    names: dict[str, str] = {}
    if not path.exists():
        return names

    data = json.loads(path.read_text(encoding="utf-8"))
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        station_id = str(props.get("Id", "")).strip()
        station_name = str(props.get("Nom", "")).strip()
        if station_id and station_name:
            names[station_id] = station_name
    return names


def normalize_indices(indices: list[dict] | None) -> list[dict]:
    output: list[dict] = []
    for idx in indices or []:
        value = idx.get("value")
        if value is None:
            # Keep strict TS-like number contract for downstream clients.
            continue
        output.append(
            {
                "id": str(idx.get("id", "")).strip() or "custom",
                "label": str(idx.get("label", "")).strip() or "Composite index",
                "value": float(value),
                "weights": idx.get("weights") or {},
            }
        )
    return output


def parse_iso_datetime(value: str) -> datetime:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    return datetime.fromisoformat(raw)


def parse_query_date(value: str) -> date:
    return datetime.strptime(value.strip(), "%Y-%m-%d").date()


def extract_record_datetime(record: dict) -> datetime | None:
    timestamp = record.get("timestamp")
    if not timestamp:
        return None
    try:
        return parse_iso_datetime(str(timestamp))
    except ValueError:
        return None


def resolve_city_from_name(station_name: str) -> str:
    name = station_name.strip()
    if not name:
        return ""
    if "-" in name:
        return name.split("-", 1)[0].strip().upper()
    return name.upper()


def to_station_payload(record: dict, station_names: dict[str, str]) -> dict:
    station_id = str(record.get("stationId", "")).strip()
    join = record.get("join") or {}
    meteo_station_id = str(join.get("meteoStationId", "")).strip()

    station_name = station_names.get(meteo_station_id)
    if not station_name:
        station_name = f"Station {station_id or meteo_station_id or 'unknown'}"

    city = resolve_city_from_name(station_name)

    pollution_coords = (record.get("coordinates") or {}).get("pollution") or {}
    pollutants = record.get("pollution") or {}
    meteo = record.get("meteo") or {}

    return {
        "id": station_id,
        "name": station_name,
        "city": city,
        "lat": pollution_coords.get("latitude"),
        "lng": pollution_coords.get("longitude"),
        "pollutants": {
            "pm25": pollutants.get("pm25"),
            "pm10": pollutants.get("pm10"),
            "no2": pollutants.get("no2"),
            "o3": pollutants.get("o3"),
            "co": pollutants.get("co"),
            "so2": pollutants.get("so2"),
        },
        "meteo": {
            "temperature": meteo.get("temperature", 0.0),
            "humidity": meteo.get("humidity", 0.0),
            "pressure": meteo.get("pressure", 0.0),
            "windSpeed": meteo.get("windSpeed", 0.0),
            "windDirection": meteo.get("windDirection", "N"),
            "rainfall": meteo.get("rainfall"),
        },
        "indices": normalize_indices(record.get("indices") or []),
        "timestamp": record.get("timestamp"),
    }


def record_matches_filters(
    record: dict,
    station_names: dict[str, str],
    city_filter: str,
    date_filter: date | None,
    from_filter: datetime | None,
    to_filter: datetime | None,
) -> bool:
    if city_filter:
        join = record.get("join") or {}
        meteo_station_id = str(join.get("meteoStationId", "")).strip()
        station_name = station_names.get(meteo_station_id, "")
        city = resolve_city_from_name(station_name)
        if city_filter not in city and city_filter not in station_name.upper():
            return False

    if date_filter is not None or from_filter is not None or to_filter is not None:
        dt = extract_record_datetime(record)
        if dt is None:
            return False
        if date_filter is not None and dt.date() != date_filter:
            return False
        if from_filter is not None and dt < from_filter:
            return False
        if to_filter is not None and dt > to_filter:
            return False

    return True


def build_handler(data_path: Path, stations_geojson_path: Path):
    class IndicesHandler(BaseHTTPRequestHandler):
        def _send_json(self, status: int, payload: dict | list) -> None:
            encoded = json.dumps(payload, ensure_ascii=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def _not_found(self) -> None:
            self._send_json(
                HTTPStatus.NOT_FOUND,
                {
                    "error": "not_found",
                    "message": "Endpoint not found",
                },
            )

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            normalized_path = parsed.path.rstrip("/") or "/"

            if normalized_path == "/":
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "service": "indices-api",
                        "status": "ok",
                        "endpoints": [
                            "GET /health",
                            "GET /api/v1/indices",
                            "GET /api/v1/indices?stationId=ST_001&date=YYYY-MM-DD",
                            "GET /api/v1/indices?from=ISO_DATETIME&to=ISO_DATETIME",
                            "GET /api/v1/indices?city=PARIS&limit=10",
                            "GET /api/v1/stations",
                            "GET /api/v1/stations/{id}",
                            "GET /api/v1/stations?city=PARIS&date=YYYY-MM-DD",
                        ],
                    },
                )
                return

            if normalized_path == "/health":
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "status": "ok",
                        "service": "indices-api",
                    },
                )
                return

            if normalized_path != "/api/v1/indices":
                if normalized_path == "/api/v1/stations":
                    query = parse_qs(parsed.query)
                    station_id = (query.get("stationId", [""])[0] or "").strip()
                    city = (query.get("city", [""])[0] or "").strip().upper()

                    date_raw = (query.get("date", [""])[0] or "").strip()
                    from_raw = (query.get("from", [""])[0] or "").strip()
                    to_raw = (query.get("to", [""])[0] or "").strip()

                    try:
                        date_filter = parse_query_date(date_raw) if date_raw else None
                    except ValueError:
                        self._send_json(
                            HTTPStatus.BAD_REQUEST,
                            {
                                "error": "invalid_date",
                                "message": "Query param 'date' must be YYYY-MM-DD",
                            },
                        )
                        return

                    try:
                        from_filter = parse_iso_datetime(from_raw) if from_raw else None
                        to_filter = parse_iso_datetime(to_raw) if to_raw else None
                    except ValueError:
                        self._send_json(
                            HTTPStatus.BAD_REQUEST,
                            {
                                "error": "invalid_datetime",
                                "message": "Query params 'from' and 'to' must be ISO datetime",
                            },
                        )
                        return

                    if from_filter is not None and to_filter is not None and from_filter > to_filter:
                        self._send_json(
                            HTTPStatus.BAD_REQUEST,
                            {
                                "error": "invalid_range",
                                "message": "Query param 'from' must be <= 'to'",
                            },
                        )
                        return

                    try:
                        limit = int((query.get("limit", ["0"])[0] or "0").strip())
                    except ValueError:
                        limit = 0

                    station_names = load_station_names(stations_geojson_path)
                    raw_records = load_indices(data_path)

                    filtered_raw = [
                        r
                        for r in raw_records
                        if record_matches_filters(r, station_names, city, date_filter, from_filter, to_filter)
                    ]
                    records = [to_station_payload(r, station_names) for r in filtered_raw]

                    if station_id:
                        records = [r for r in records if str(r.get("id", "")).strip() == station_id]

                    if limit > 0:
                        records = records[:limit]

                    self._send_json(
                        HTTPStatus.OK,
                        {
                            "count": len(records),
                            "data": records,
                        },
                    )
                    return

                if normalized_path.startswith("/api/v1/stations/"):
                    station_id = normalized_path.split("/")[-1].strip()
                    query = parse_qs(parsed.query)
                    city = (query.get("city", [""])[0] or "").strip().upper()

                    date_raw = (query.get("date", [""])[0] or "").strip()
                    from_raw = (query.get("from", [""])[0] or "").strip()
                    to_raw = (query.get("to", [""])[0] or "").strip()

                    try:
                        date_filter = parse_query_date(date_raw) if date_raw else None
                    except ValueError:
                        self._send_json(
                            HTTPStatus.BAD_REQUEST,
                            {
                                "error": "invalid_date",
                                "message": "Query param 'date' must be YYYY-MM-DD",
                            },
                        )
                        return

                    try:
                        from_filter = parse_iso_datetime(from_raw) if from_raw else None
                        to_filter = parse_iso_datetime(to_raw) if to_raw else None
                    except ValueError:
                        self._send_json(
                            HTTPStatus.BAD_REQUEST,
                            {
                                "error": "invalid_datetime",
                                "message": "Query params 'from' and 'to' must be ISO datetime",
                            },
                        )
                        return

                    if from_filter is not None and to_filter is not None and from_filter > to_filter:
                        self._send_json(
                            HTTPStatus.BAD_REQUEST,
                            {
                                "error": "invalid_range",
                                "message": "Query param 'from' must be <= 'to'",
                            },
                        )
                        return

                    station_names = load_station_names(stations_geojson_path)

                    raw_records = [
                        r
                        for r in load_indices(data_path)
                        if record_matches_filters(r, station_names, city, date_filter, from_filter, to_filter)
                    ]

                    records = [to_station_payload(r, station_names) for r in raw_records]
                    for item in records:
                        if str(item.get("id", "")).strip() == station_id:
                            self._send_json(HTTPStatus.OK, item)
                            return
                    self._send_json(
                        HTTPStatus.NOT_FOUND,
                        {
                            "error": "station_not_found",
                            "message": f"No station found for id '{station_id}'",
                        },
                    )
                    return

                self._not_found()
                return

            query = parse_qs(parsed.query)
            station_id = (query.get("stationId", [""])[0] or "").strip()
            city = (query.get("city", [""])[0] or "").strip().upper()

            date_raw = (query.get("date", [""])[0] or "").strip()
            from_raw = (query.get("from", [""])[0] or "").strip()
            to_raw = (query.get("to", [""])[0] or "").strip()

            try:
                date_filter = parse_query_date(date_raw) if date_raw else None
            except ValueError:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "error": "invalid_date",
                        "message": "Query param 'date' must be YYYY-MM-DD",
                    },
                )
                return

            try:
                from_filter = parse_iso_datetime(from_raw) if from_raw else None
                to_filter = parse_iso_datetime(to_raw) if to_raw else None
            except ValueError:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "error": "invalid_datetime",
                        "message": "Query params 'from' and 'to' must be ISO datetime",
                    },
                )
                return

            if from_filter is not None and to_filter is not None and from_filter > to_filter:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "error": "invalid_range",
                        "message": "Query param 'from' must be <= 'to'",
                    },
                )
                return

            try:
                limit = int((query.get("limit", ["0"])[0] or "0").strip())
            except ValueError:
                limit = 0

            station_names = load_station_names(stations_geojson_path)
            records = [
                r
                for r in load_indices(data_path)
                if record_matches_filters(r, station_names, city, date_filter, from_filter, to_filter)
            ]

            if station_id:
                records = [r for r in records if str(r.get("stationId", "")).strip() == station_id]

            if limit > 0:
                records = records[:limit]

            self._send_json(
                HTTPStatus.OK,
                {
                    "count": len(records),
                    "data": records,
                },
            )

        def log_message(self, format: str, *args) -> None:
            # Keep server output clean in dev usage.
            return

    return IndicesHandler


def main() -> int:
    parser = argparse.ArgumentParser(description="Serve composite indices over HTTP")
    parser.add_argument(
        "--data",
        default="data/processed/indices_composite.jsonl",
        help="Path to indices JSONL file",
    )
    parser.add_argument(
        "--stations-geojson",
        default="data/raw/postes_synop.geojson",
        help="Path to stations GeoJSON used for station names",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind")
    args = parser.parse_args()

    data_path = Path(args.data)
    stations_geojson_path = Path(args.stations_geojson)
    handler = build_handler(data_path, stations_geojson_path)
    server = ThreadingHTTPServer((args.host, args.port), handler)

    print(f"Serving indices API on http://{args.host}:{args.port}")
    print("Endpoints: GET /health, GET /api/v1/indices, GET /api/v1/stations, GET /api/v1/stations/{id}")
    print(f"Data file: {data_path}")
    print(f"Stations file: {stations_geojson_path}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())