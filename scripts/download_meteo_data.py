#!/usr/bin/env python3
import argparse
import json
import logging
import pathlib
import sys

import requests


LOGGER = logging.getLogger("download_meteo_data")


def extract_year_urls(config: dict) -> list[tuple[int, str]]:
    # Backward compatible: support either single year/url or multiple years map.
    if "synop_sources" in config:
        sources = config.get("synop_sources") or {}
        pairs: list[tuple[int, str]] = []
        for year_raw, url in sources.items():
            try:
                year = int(year_raw)
            except (TypeError, ValueError):
                continue
            if isinstance(url, str) and url.strip():
                pairs.append((year, url.strip()))
        return sorted(pairs, key=lambda item: item[0])

    year = int(config["synop_year"])
    url = str(config["synop_csv_url"]).strip()
    return [(year, url)]


def download_file(url: str, destination: pathlib.Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_suffix(destination.suffix + ".part")
    try:
        with requests.get(url, stream=True, timeout=120) as response:
            response.raise_for_status()
            with tmp_path.open("wb") as target:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        target.write(chunk)
        if tmp_path.stat().st_size <= 0:
            raise OSError(f"Downloaded empty file from {url}")
        tmp_path.replace(destination)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description="Download meteorological data sources from data.gouv.fr")
    parser.add_argument(
        "--config",
        default="config/meteo_sources.json",
        help="Path to JSON config containing source URLs",
    )
    parser.add_argument(
        "--output-dir",
        default="data/raw",
        help="Directory where raw files are downloaded",
    )
    args = parser.parse_args()

    config_path = pathlib.Path(args.config)
    if not config_path.exists():
        LOGGER.error("Config not found: %s", config_path)
        return 1

    config = json.loads(config_path.read_text(encoding="utf-8"))
    output_dir = pathlib.Path(args.output_dir)

    geojson_filename = "postes_synop.geojson"
    geo_target = output_dir / geojson_filename

    year_urls = extract_year_urls(config)
    if not year_urls:
        LOGGER.error("No valid SYNOP source found in config")
        return 1

    try:
        for year, url in year_urls:
            synop_filename = f"synop_{year}.csv.gz"
            synop_target = output_dir / synop_filename
            LOGGER.info("Downloading %s -> %s", url, synop_target)
            download_file(url, synop_target)

        LOGGER.info("Downloading %s -> %s", config["synop_stations_geojson_url"], geo_target)
        download_file(config["synop_stations_geojson_url"], geo_target)
    except requests.RequestException as exc:
        LOGGER.exception("Network error while downloading meteorological data: %s", exc)
        return 2
    except OSError as exc:
        LOGGER.exception("Filesystem error while saving meteorological data: %s", exc)
        return 3

    LOGGER.info("Download completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
