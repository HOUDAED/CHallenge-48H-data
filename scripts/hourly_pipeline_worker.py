#!/usr/bin/env python3
import argparse
import json
import logging
import pathlib
import subprocess
import time
from datetime import datetime, timezone

import os

import requests
from dotenv import load_dotenv

load_dotenv()


# Nettoyage automatique des fichiers JSON de plus de 24h dans le dossier outbox
import os
import time

def cleanup_outbox(outbox_dir: pathlib.Path, keep_hours: int = 24):
    """Supprime les fichiers JSON plus vieux que keep_hours dans le dossier outbox."""
    now = time.time()
    for file in outbox_dir.glob("indices_payload_*.json"):
        if file.is_file():
            mtime = file.stat().st_mtime
            if now - mtime > keep_hours * 3600:
                file.unlink()
                LOGGER.info(f"Fichier supprimé (ancien): {file}")


LOGGER = logging.getLogger("hourly_pipeline_worker")


def run_cmd(command: list[str]) -> None:
    LOGGER.info("Running: %s", " ".join(command))
    subprocess.run(command, check=True)


def load_jsonl(path: pathlib.Path) -> list[dict]:
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


def _pick_station_coordinates(record: dict) -> tuple[float, float]:
    coords = record.get("coordinates") or {}
    pollution = coords.get("pollution") or {}
    meteo = coords.get("meteo") or {}

    lat = pollution.get("latitude")
    lng = pollution.get("longitude")

    if lat is None:
        lat = meteo.get("latitude")
    if lng is None:
        lng = meteo.get("longitude")

    return float(lat or 0.0), float(lng or 0.0)


def _to_station(record: dict) -> dict:
    station_id = str(record.get("stationId") or "")
    join = record.get("join") or {}
    station_name = (
        record.get("stationName")
        or join.get("meteoStationName")
        or station_id
    )
    lat, lng = _pick_station_coordinates(record)

    return {
        "id": station_id,
        "name": str(station_name),
        "lat": lat,
        "lng": lng,
        "pollutants": record.get("pollution") or {},
        "meteo": record.get("meteo") or {},
        "indices": record.get("indices") or [],
        "timestamp": record.get("timestamp"),
    }


def build_payload(indices_path: pathlib.Path) -> dict:
    records = load_jsonl(indices_path)
    stations = [_to_station(record) for record in records]
    generated_at = datetime.now(timezone.utc).isoformat()
    return {
        "status": "ok",
        "generatedAt": generated_at,
        "recordCount": len(stations),
        "stations": stations,
    }


def build_error_payload(indices_path: pathlib.Path, error_message: str) -> dict:
    records = load_jsonl(indices_path)
    stations = [_to_station(record) for record in records]
    generated_at = datetime.now(timezone.utc).isoformat()
    return {
        "status": "error",
        "error": error_message,
        "generatedAt": generated_at,
        "recordCount": len(stations),
        "stations": stations,
    }


def post_payload(endpoint_url: str, payload: dict, headers: dict, timeout_seconds: int) -> None:
    response = requests.post(endpoint_url, json=payload, timeout=timeout_seconds, headers=headers)
    response.raise_for_status()


def write_fallback(payload: dict, fallback_dir: pathlib.Path) -> pathlib.Path:
    fallback_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = fallback_dir / f"indices_payload_{ts}.json"
    target.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    return target


def execute_pipeline(run_meteo: bool, pollution_input_csv: str) -> None:
    stations_geojson = pathlib.Path("data/raw/postes_synop.geojson")
    meteo_normalized = pathlib.Path("data/processed/meteo_normalized.jsonl")
    if run_meteo or not stations_geojson.exists() or not meteo_normalized.exists():
        run_cmd(["bash", "scripts/run_step1_meteo.sh"])
    if pollution_input_csv.strip():
        run_cmd(["bash", "scripts/run_step2_pollution.sh", pollution_input_csv.strip()])
    else:
        run_cmd(["bash", "scripts/run_step2_pollution.sh"])
    run_cmd(["bash", "scripts/run_step3_join.sh"])
    run_cmd(["bash", "scripts/run_step4_indices.sh"])
    # [BONUS] Uncomment to enable forecast generation each cycle
    # run_cmd(["bash", "scripts/run_step6_forecast.sh"])


def publish_or_store(
    payload: dict,
    endpoint_url: str,
    timeout_seconds: int,
    fallback_dir: pathlib.Path,
    bearer_token: str = "",
) -> None:
    headers: dict = {}
    if bearer_token.strip():
        headers["Authorization"] = f"Bearer {bearer_token.strip()}"
    if endpoint_url.strip():
        try:
            post_payload(endpoint_url.strip(), payload, headers, timeout_seconds)
            LOGGER.info("POST success: %s | records=%s", endpoint_url.strip(), payload["recordCount"])
            return
        except Exception as exc:
            target = write_fallback(payload, fallback_dir)
            LOGGER.exception("POST failed, payload stored at %s: %s", target, exc)
            return

    target = write_fallback(payload, fallback_dir)
    LOGGER.info("No endpoint configured, payload stored at %s", target)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description="Run data pipeline every N minutes and publish payload")
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=int(os.environ.get("INTERVAL_MINUTES", "60")),
        help="Run frequency in minutes",
    )
    parser.add_argument(
        "--endpoint-url",
        default=os.environ.get("ENDPOINT_URL", "http://localhost:8000/api/ingest"),
        help="POST endpoint for publishing payload (if empty, payload is only written locally)",
    )

    parser.add_argument(
        "--indices-file",
        default="data/processed/indices_composite.jsonl",
        help="Path to pipeline output JSONL",
    )
    parser.add_argument(
        "--fallback-dir",
        default="data/processed/outbox",
        help="Directory for storing payloads when POST is disabled or fails",
    )
    parser.add_argument(
        "--post-timeout-seconds",
        type=int,
        default=int(os.environ.get("POST_TIMEOUT_SECONDS", "30")),
        help="Timeout for POST request",
    )
    parser.add_argument(
        "--run-meteo-each-cycle",
        action="store_true",
        default=os.environ.get("RUN_METEO_EACH_CYCLE", "").lower() in ("1", "true", "yes"),
        help="Also run meteorological ingestion each cycle",
    )
    parser.add_argument(
        "--pollution-input-csv",
        default="",
        help="Optional local pollution CSV to reuse every cycle (bypass remote download)",
    )
    parser.add_argument(
        "--bearer-token",
        default=os.environ.get("BEARER_TOKEN", ""),
        help="Bearer token added to the Authorization header of POST requests",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=int(os.environ.get("MAX_CYCLES", "0")),
        help="Max cycles to execute (0 means infinite)",
    )
    parser.add_argument(
        "--forecast-endpoint-url",
        default="",
        help="Optional POST endpoint for pushing forecast data after each cycle",
    )
    args = parser.parse_args()

    interval_seconds = max(args.interval_minutes, 1) * 60
    indices_path = pathlib.Path(args.indices_file)
    fallback_dir = pathlib.Path(args.fallback_dir)

    cycle = 0
    while True:
        cycle += 1
        started_at = datetime.now(timezone.utc).isoformat()
        LOGGER.info("Cycle %s started at %s", cycle, started_at)

        try:
            execute_pipeline(
                run_meteo=args.run_meteo_each_cycle,
                pollution_input_csv=args.pollution_input_csv,
            )
            payload = build_payload(indices_path)
            publish_or_store(payload, args.endpoint_url, args.post_timeout_seconds, fallback_dir, args.bearer_token)

            # [BONUS] Uncomment to enable forecast push after each cycle
            # if args.forecast_endpoint_url.strip():
            #     run_cmd([
            #         "python3", "scripts/push_forecast.py",
            #         "--url", args.forecast_endpoint_url.strip(),
            #     ])

        except subprocess.CalledProcessError as exc:
            LOGGER.error("Pipeline command failed with exit code %s", exc.returncode)
            payload = build_error_payload(indices_path, f"pipeline_failed_exit_{exc.returncode}")
            publish_or_store(payload, args.endpoint_url, args.post_timeout_seconds, fallback_dir, args.bearer_token)
        except Exception as exc:
            LOGGER.exception("Unexpected error: %s", exc)
            payload = build_error_payload(indices_path, str(exc))
            publish_or_store(payload, args.endpoint_url, args.post_timeout_seconds, fallback_dir, args.bearer_token)

        if args.max_cycles > 0 and cycle >= args.max_cycles:
            LOGGER.info("Max cycles reached, stopping")
            break


        # Nettoyage des fichiers JSON de plus de 24h dans outbox
        cleanup_outbox(fallback_dir, keep_hours=24)

        LOGGER.info("Sleeping %s minute(s)", args.interval_minutes)
        time.sleep(interval_seconds)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())