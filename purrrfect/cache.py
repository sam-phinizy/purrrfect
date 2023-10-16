import hashlib
import pathlib

from purrrfect.prefect_api import prefect_client
import sqlitedict

def setup_cache(cache_path: pathlib.Path,prefect_url: str):
    cache_path.mkdir(parents=True, exist_ok=True)
