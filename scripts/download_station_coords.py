"""
Download and extract pollution station coordinates from the LCSQA/INERIS
Dataset D XML file (AQD SamplingPoint metadata).

Station code mapping: SPO-{STATION_CODE}_{NUMBER} -> station code = STATION_CODE
Output: data/raw/pollution/station_coords.json  { "FR01011": {"lat": 49.5, "lon": 5.8}, ... }
"""
import argparse
import json
import logging
import pathlib
import urllib.request
import xml.etree.ElementTree as ET

LOGGER = logging.getLogger("download_station_coords")

DEFAULT_XML_URL = (
    "https://static.data.gouv.fr/resources/"
    "donnees-temps-reel-de-mesure-des-concentrations-de-polluants-"
    "atmospheriques-reglementes-1/20251210-083032/fr-2025-d-lcsqa-ineris-20251209.xml"
)

GML_POS = "{http://www.opengis.net/gml/3.2}pos"
EF_INSPIRE = "{http://inspire.ec.europa.eu/schemas/ef/3.0}inspireId"
BASE_LOCAL = "{http://inspire.ec.europa.eu/schemas/base/3.3}localId"


def parse_station_code(spo_code: str) -> str | None:
    """Extract station code from SPO-{STATION_CODE}_{NUMBER} format."""
    if not spo_code.startswith("SPO-"):
        return None
    rest = spo_code[4:]  # Remove "SPO-"
    idx = rest.rfind("_")
    if idx < 0:
        return None
    return rest[:idx]


def extract_coords(xml_url: str, timeout: int = 120) -> dict[str, dict]:
    """Stream-parse XML and return {station_code: {lat, lon}} mapping."""
    coords: dict[str, dict] = {}
    count = 0

    with urllib.request.urlopen(xml_url, timeout=timeout) as response:
        for event, elem in ET.iterparse(response, events=["end"]):
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag != "AQD_SamplingPoint":
                continue

            count += 1
            local_id_elem = elem.find(f".//{BASE_LOCAL}")
            pos_elem = elem.find(f".//{GML_POS}")

            if local_id_elem is not None and pos_elem is not None:
                spo_code = (local_id_elem.text or "").strip()
                station_code = parse_station_code(spo_code)
                if station_code and station_code not in coords:
                    parts = (pos_elem.text or "").strip().split()
                    if len(parts) == 2:
                        try:
                            coords[station_code] = {
                                "lat": float(parts[0]),
                                "lon": float(parts[1]),
                            }
                        except ValueError:
                            pass
            elem.clear()

    LOGGER.info("Parsed %d SamplingPoints, extracted %d station coords", count, len(coords))
    return coords


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description="Download ATMO station coordinates from LCSQA XML")
    parser.add_argument("--xml-url", default=DEFAULT_XML_URL)
    parser.add_argument("--output", default="data/raw/pollution/station_coords.json")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()

    output_path = pathlib.Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Skip download if already exists
    if output_path.exists() and output_path.stat().st_size > 0:
        LOGGER.info("Station coords already exist at %s, skipping download", output_path)
        return 0

    LOGGER.info("Downloading station coordinates from LCSQA XML...")
    coords = extract_coords(args.xml_url, timeout=args.timeout)
    output_path.write_text(json.dumps(coords, indent=2), encoding="utf-8")
    LOGGER.info("Saved %d station coords to %s", len(coords), output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
