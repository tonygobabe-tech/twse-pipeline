import os, time, json, logging, hashlib
from typing import Any, Dict, Optional, Tuple

import requests

DEFAULT_HEADERS = {
    "User-Agent": "TWSE-OpenAPI-Example/1.0 (+https://example.local)"
}

class HttpClient:
    def __init__(self, timeout: int = 20, retries: int = 3):
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def get_json(self, url: str) -> Any:
        last_err = None
        for i in range(self.retries):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                # TWSE 有些端點回傳 JSON List，有些是 JSON Object
                return resp.json()
            except Exception as e:
                last_err = e
                time.sleep(1.5 * (i + 1))
        raise last_err

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def save_json(obj, path: str):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def slug(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:8]
