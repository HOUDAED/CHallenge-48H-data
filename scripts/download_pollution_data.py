#!/usr/bin/env python3
import argparse
import json
import logging
import pathlib
import sys
from datetime import datetime, timedelta, timezone

import requests


LOGGER = logging.getLogger("download_pollution_data")


def is_html_payload(payload: bytes) -> bool:
    sample = payload.lstrip()[:200].lower()
    return sample.startswith(b"<!doctype html") or sample.startswith(b"<html")


def try_download_csv(url: str, destination: pathlib.Path) -> bool:
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_suffix(destination.suffix + ".part")
    try:
        with requests.get(url, stream=True, timeout=120) as response:
            if response.status_code != 200:
                LOGGER.warning("Download skipped with HTTP status %s for %s", response.status_code, url)
                return False

            first_chunk = b""
            with tmp_path.open("wb") as target:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    if not first_chunk:
                        first_chunk = chunk
                        if is_html_payload(first_chunk):
                            LOGGER.warning("HTML payload detected instead of CSV for %s", url)
                            return False
                    target.write(chunk)

        if tmp_path.stat().st_size <= 0:
            LOGGER.warning("Downloaded file is empty for %s", url)
            return False

        tmp_path.replace(destination)
        return True
    except requests.RequestException as exc:
        LOGGER.exception("Network error while downloading pollution CSV from %s: %s", url, exc)
        return False
    except OSError as exc:
        LOGGER.exception("Filesystem error while writing %s: %s", destination, exc)
        return False
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
    parser = argparse.ArgumentParser(
        description="Download daily pollution concentration CSV (FR_E2_YYYY-MM-DD.csv)"
    )
    parser.add_argument(
        "--config",
        default="config/pollution_sources.json",
        help="Path to JSON config containing the daily URL template",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Target date (YYYY-MM-DD). Defaults to yesterday (UTC)",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="Direct CSV URL override. If provided, date/lookback are ignored",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="How many days backward to try until a file is found",
    )
    parser.add_argument(
        "--output",
        default="data/raw/pollution/latest_pollution.csv",
        help="Where to save the downloaded CSV",
    )
    parser.add_argument(
        "--metadata-output",
        default="data/raw/pollution/latest_pollution_meta.json",
        help="Where to save metadata about the downloaded source",
    )
    args = parser.parse_args()

    config_path = pathlib.Path(args.config)
    if not config_path.exists():
        LOGGER.error("Config not found: %s", config_path)
        return 1

    config = json.loads(config_path.read_text(encoding="utf-8"))
    url_template = config["daily_csv_url_template"]
    latest_resource_url = config.get("latest_hourly_csv_resource_url")
    lookback_days = args.lookback_days
    if lookback_days is None:
        lookback_days = int(config.get("default_lookback_days", 14))

    if args.date:
        start_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        start_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    output_path = pathlib.Path(args.output)
    metadata_path = pathlib.Path(args.metadata_output)

    attempted = []

    if args.url:
        attempted.append(args.url)
        LOGGER.info("Trying direct URL: %s", args.url)
        ok = try_download_csv(args.url, output_path)
        if not ok:
            LOGGER.error("Unable to download pollution CSV from direct URL")
            return 2

        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata = {
            "source_url": args.url,
            "source_date": None,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "output_file": str(output_path),
        }
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")

        LOGGER.info("Downloaded pollution CSV to: %s", output_path)
        LOGGER.info("Metadata written to: %s", metadata_path)
        return 0

    for offset in range(max(1, lookback_days + 1)):
        candidate_date = start_date - timedelta(days=offset)
        date_str = candidate_date.isoformat()
        url = url_template.format(date=date_str)
        attempted.append(url)

        LOGGER.info("Trying: %s", url)
        ok = try_download_csv(url, output_path)
        if not ok:
            continue

        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata = {
            "source_url": url,
            "source_date": date_str,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "output_file": str(output_path),
        }
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")

        LOGGER.info("Downloaded pollution CSV to: %s", output_path)
        LOGGER.info("Metadata written to: %s", metadata_path)
        return 0

    if latest_resource_url:
        attempted.append(latest_resource_url)
        LOGGER.info("Trying fallback latest resource: %s", latest_resource_url)
        ok = try_download_csv(latest_resource_url, output_path)
        if ok:
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            metadata = {
                "source_url": latest_resource_url,
                "source_date": None,
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                "output_file": str(output_path),
            }
            metadata_path.write_text(
                json.dumps(metadata, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
            )
            LOGGER.info("Downloaded pollution CSV to: %s", output_path)
            LOGGER.info("Metadata written to: %s", metadata_path)
            return 0

    LOGGER.error("Unable to download a pollution CSV for requested date range")
    LOGGER.error("Attempted URLs:")
    for url in attempted:
        LOGGER.error("- %s", url)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
