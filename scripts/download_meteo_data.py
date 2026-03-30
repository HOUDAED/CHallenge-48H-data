#!/usr/bin/env python3
import argparse
import json
import pathlib
import sys

import requests


def download_file(url: str, destination: pathlib.Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with destination.open("wb") as target:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    target.write(chunk)


def main() -> int:
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
        print(f"Config not found: {config_path}", file=sys.stderr)
        return 1

    config = json.loads(config_path.read_text(encoding="utf-8"))
    output_dir = pathlib.Path(args.output_dir)

    year = int(config["synop_year"])
    synop_filename = f"synop_{year}.csv.gz"
    geojson_filename = "postes_synop.geojson"

    synop_target = output_dir / synop_filename
    geo_target = output_dir / geojson_filename

    print(f"Downloading {config['synop_csv_url']} -> {synop_target}")
    download_file(config["synop_csv_url"], synop_target)

    print(f"Downloading {config['synop_stations_geojson_url']} -> {geo_target}")
    download_file(config["synop_stations_geojson_url"], geo_target)

    print("Download completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
