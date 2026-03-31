#!/usr/bin/env python3
"""
Serveur HTTP autonome pour les données de prévision (forecast.jsonl).
Séparé de serve_indices_api.py — dédié aux prévisions Step 6.

Endpoints :
  GET /                              Info service
  GET /health                        Health check
  GET /api/v1/forecast               Toutes les prévisions (filtres : stationId, model)
  GET /api/v1/forecast/stations      Liste des stations avec prévisions disponibles

Port par défaut : 8001
"""
import argparse
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


def load_forecast(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def build_handler(forecast_path: Path):
    class ForecastHandler(BaseHTTPRequestHandler):
        def _send_json(self, status: int, payload) -> None:
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            # CORS — allow React dev server
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def do_OPTIONS(self) -> None:
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query)

            if path == "/":
                self._send_json(HTTPStatus.OK, {
                    "service": "forecast-api",
                    "status": "ok",
                    "endpoints": [
                        "GET /health",
                        "GET /api/v1/forecast",
                        "GET /api/v1/forecast?stationId=07005",
                        "GET /api/v1/forecast?model=AR_global",
                        "GET /api/v1/forecast?model=trend_linear",
                        "GET /api/v1/forecast?stationId=07005&model=AR_global",
                        "GET /api/v1/forecast/stations",
                    ],
                })
                return

            if path == "/health":
                rows = load_forecast(forecast_path)
                self._send_json(HTTPStatus.OK, {
                    "status": "ok",
                    "service": "forecast-api",
                    "forecastRows": len(rows),
                    "dataFile": str(forecast_path),
                })
                return

            if path == "/api/v1/forecast/stations":
                rows = load_forecast(forecast_path)
                stations = sorted({r.get("stationId", "") for r in rows if r.get("stationId")})
                self._send_json(HTTPStatus.OK, {
                    "count": len(stations),
                    "stations": stations,
                })
                return

            if path == "/api/v1/forecast":
                station_id = (query.get("stationId", [""])[0] or "").strip()
                model = (query.get("model", [""])[0] or "").strip()

                try:
                    limit = int((query.get("limit", ["0"])[0] or "0").strip())
                except ValueError:
                    limit = 0

                rows = load_forecast(forecast_path)

                if station_id:
                    rows = [r for r in rows if r.get("stationId") == station_id]
                if model:
                    rows = [r for r in rows if r.get("model") == model]
                if limit > 0:
                    rows = rows[:limit]

                self._send_json(HTTPStatus.OK, {
                    "count": len(rows),
                    "data": rows,
                })
                return

            self._send_json(HTTPStatus.NOT_FOUND, {
                "error": "not_found",
                "message": "Endpoint not found",
            })

        def log_message(self, format: str, *args) -> None:
            return

    return ForecastHandler


def main() -> int:
    parser = argparse.ArgumentParser(description="Serve forecast data over HTTP")
    parser.add_argument("--data", default="data/processed/forecast.jsonl")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8001)
    args = parser.parse_args()

    forecast_path = Path(args.data)
    handler = build_handler(forecast_path)
    server = ThreadingHTTPServer((args.host, args.port), handler)

    print(f"Forecast API on http://{args.host}:{args.port}")
    print("Endpoints: GET /health, GET /api/v1/forecast, GET /api/v1/forecast/stations")
    print(f"Data file: {forecast_path}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
