#!/usr/bin/env python3
"""
Lit forecast.jsonl et envoie les prévisions en POST vers une URL cible.

Usage :
  python3 scripts/push_forecast.py --url http://localhost:3000/api/forecast
  FORECAST_ENDPOINT_URL=http://... python3 scripts/push_forecast.py

Le payload envoyé :
  {
    "source": "forecast-pipeline",
    "generatedAt": "<ISO timestamp>",
    "count": 5859,
    "data": [ { ...forecast record... }, ... ]
  }
"""
import argparse
import json
import logging
import pathlib
import sys
from datetime import datetime, timezone

import requests


LOGGER = logging.getLogger("push_forecast")


def load_forecast(path: pathlib.Path) -> list[dict]:
    if not path.exists():
        LOGGER.error("Fichier introuvable : %s", path)
        return []
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="Push forecast data via POST")
    parser.add_argument(
        "--input",
        default="data/processed/forecast.jsonl",
        help="Chemin vers forecast.jsonl",
    )
    parser.add_argument(
        "--url",
        default="",
        help="URL cible (ex: http://localhost:3000/api/forecast). "
             "Peut aussi être définie via la variable d'environnement FORECAST_ENDPOINT_URL.",
    )
    parser.add_argument(
        "--station",
        default="",
        help="Filtrer par stationId avant l'envoi (optionnel)",
    )
    parser.add_argument(
        "--model",
        default="",
        choices=["", "AR_global", "trend_linear"],
        help="Filtrer par modèle avant l'envoi (optionnel)",
    )
    args = parser.parse_args()

    import os
    endpoint_url = args.url or os.environ.get("FORECAST_ENDPOINT_URL", "")
    if not endpoint_url:
        LOGGER.error(
            "URL cible manquante. Utilise --url ou la variable FORECAST_ENDPOINT_URL."
        )
        return 1

    rows = load_forecast(pathlib.Path(args.input))
    if not rows:
        LOGGER.error("Aucune donnée à envoyer.")
        return 1

    if args.station:
        rows = [r for r in rows if r.get("stationId") == args.station]
    if args.model:
        rows = [r for r in rows if r.get("model") == args.model]

    payload = {
        "source": "forecast-pipeline",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "count": len(rows),
        "data": rows,
    }

    LOGGER.info("Envoi de %d prévisions vers %s ...", len(rows), endpoint_url)

    try:
        response = requests.post(
            endpoint_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        LOGGER.info("Réponse %d : %s", response.status_code, response.text[:200])
        return 0
    except requests.exceptions.ConnectionError:
        LOGGER.error("Impossible de se connecter à %s", endpoint_url)
        return 1
    except requests.exceptions.HTTPError as e:
        LOGGER.error("Erreur HTTP : %s", e)
        return 1
    except requests.exceptions.Timeout:
        LOGGER.error("Timeout lors de la connexion à %s", endpoint_url)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
