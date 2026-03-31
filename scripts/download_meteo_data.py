#!/usr/bin/env python3
import argparse
import csv
import gzip
import json
import logging
import pathlib
import sys
from datetime import datetime, timedelta, timezone

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


def extract_recent_data(source_gz_path: pathlib.Path, dest_csv_path: pathlib.Path, days: int) -> None:
    """Lit le gros fichier .csv.gz et extrait les lignes des 'days' derniers jours."""
    # Calcul de la date limite en format UTC (Météo-France est en UTC)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    # Conversion au format texte Météo-France : YYYYMMDDHHMMSS
    cutoff_str = cutoff_date.strftime("%Y%m%d%H%M%S")
    
    dest_csv_path.parent.mkdir(parents=True, exist_ok=True)
    LOGGER.info("Extraction des %d derniers jours (>= %s) depuis %s vers %s", days, cutoff_str, source_gz_path.name, dest_csv_path.name)
    
    try:
        # Ouverture simultanée du fichier source compressé (lecture) et du fichier destination (écriture)
        with gzip.open(source_gz_path, 'rt', encoding='utf-8') as f_in, \
             open(dest_csv_path, 'w', encoding='utf-8', newline='') as f_out:
            
            reader = csv.reader(f_in, delimiter=';')
            writer = csv.writer(f_out, delimiter=';')
            
            # Lecture et écriture des en-têtes (noms des colonnes)
            header = next(reader)
            writer.writerow(header)
            
            try:
                date_idx = header.index('date')
            except ValueError:
                LOGGER.error("La colonne 'date' est introuvable dans le fichier source.")
                return
            
            # Parcours ligne par ligne pour le filtrage
            extracted_count = 0
            for row in reader:
                if len(row) > date_idx:
                    # Comparaison de chaînes chronologique
                    if row[date_idx] >= cutoff_str:
                        writer.writerow(row)
                        extracted_count += 1
                        
        LOGGER.info("Extraction terminée : %d lignes conservées pour les %d derniers jours.", extracted_count, days)
        
    except Exception as exc:
        LOGGER.error("Erreur lors de l'extraction : %s", exc)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description="Download and process meteorological data sources from data.gouv.fr")
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
    parser.add_argument(
        "--extract-days",
        type=int,
        default=10,
        help="Nombre de jours à extraire du fichier le plus récent (0 pour ignorer l'extraction)",
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

    # 1. TÉLÉCHARGEMENT DES FICHIERS
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

    # 2. EXTRACTION DES 10 DERNIERS JOURS
    if args.extract_days > 0:
        # On prend la dernière année de la liste (ex: 2026)
        latest_year = year_urls[-1][0]
        latest_gz_target = output_dir / f"synop_{latest_year}.csv.gz"
        recent_target = output_dir / f"latest_meteo_{args.extract_days}_days.csv"
        
        if latest_gz_target.exists():
            extract_recent_data(latest_gz_target, recent_target, args.extract_days)

    LOGGER.info("Processus terminé avec succès.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())