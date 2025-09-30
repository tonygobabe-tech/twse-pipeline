from typing import Dict, Any, Tuple
from utils import HttpClient, save_json, ensure_dir
import os

BASE = "https://openapi.twse.com.tw/v1"

ENDPOINTS = {
    "daily":   f"{BASE}/exchangeReport/STOCK_DAY_ALL",
    "monthly": f"{BASE}/exchangeReport/FMSRFK_ALL",
    "yearly":  f"{BASE}/exchangeReport/FMNPTK_ALL",
    "basics":  f"{BASE}/opendata/t187ap02_L",
    "news":    f"{BASE}/opendata/t187ap03_L",
    "holders": f"{BASE}/opendata/t187ap14_L",
}

def fetch(dataset: str, client: HttpClient, out_dir: str) -> str:
    if dataset not in ENDPOINTS:
        raise ValueError(f"Unknown dataset: {dataset}")
    url = ENDPOINTS[dataset]
    data = client.get_json(url)
    raw_path = os.path.join(out_dir, "raw", f"{dataset}.json")
    save_json(data, raw_path)
    return raw_path
