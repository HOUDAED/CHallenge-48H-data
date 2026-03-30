import logging
import os
from pathlib import Path

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

DATA_GOUV_BASE = "https://www.data.gouv.fr/api/2"


class BaseClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "challenge-48h-data/0.1",
            "Accept": "application/json",
        })
        api_key = os.getenv("DATA_GOUV_API_KEY")
        if api_key:
            self.session.headers["X-API-KEY"] = api_key

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def get_json(self, url: str, params: dict = None) -> dict:
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def download_file(self, url: str, dest_path: Path) -> Path:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        if dest_path.exists():
            logger.info("Already downloaded: %s", dest_path.name)
            return dest_path

        logger.info("Downloading %s -> %s", url, dest_path)
        with self.session.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 64):
                    f.write(chunk)
        return dest_path
