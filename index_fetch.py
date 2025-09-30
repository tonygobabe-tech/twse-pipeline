# index_fetch.py — 取得加權(TAIEX)與櫃買(OTC)指數；含容錯，避免 pipeline 因回傳非 JSON 而失敗
from utils import HttpClient, save_json
from datetime import datetime, timedelta, timezone
import os
import json
import requests


def _taipei_yyyymmdd() -> str:
    """將 UTC 轉台北時間；週末回推到最近的週五，回傳 YYYYMMDD"""
    now = datetime.now(timezone.utc) + timedelta(hours=8)
    if now.weekday() == 5:  # Sat
        now -= timedelta(days=1)
    elif now.weekday() == 6:  # Sun
        now -= timedelta(days=2)
    return now.strftime("%Y%m%d")


def fetch_taiex(client: HttpClient, out_root: str, date_yyyymmdd: str | None = None):
    """
    取得加權(台股)指數日資料（盤後）
    來源：TWSE MI_INDEX (type=IND)
    產出：data/raw/taiex_<yyyymmdd>.json
    """
    d = date_yyyymmdd or _taipei_yyyymmdd()
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={d}&type=IND"
    data = client.get_json(url)

    raw_dir = os.path.join(out_root, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    path = os.path.join(raw_dir, f"taiex_{d}.json")
    save_json(data, path)
    return path, d


def fetch_otc(client: HttpClient, out_root: str, date_yyyymmdd: str | None = None):
    """
    取得櫃買(OTC)指數日資料（盤後）—「強韌版」
    - TPEX 偶爾回傳空白/HTML 或 429，導致 .json() 失敗
    - 策略：主端點 → 失敗則原樣 requests 取回並嘗試 json.loads → 再失敗寫入 []，保證不讓 pipeline 中斷
    產出：data/raw/otc_<yyyymmdd>.json
    """
    d = date_yyyymmdd or _taipei_yyyymmdd()
    raw_dir = os.path.join(out_root, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    path = os.path.join(raw_dir, f"otc_{d}.json")

    primary = f"https://www.tpex.org.tw/openapi/v1/tpex_mainboard_index?date={d}"

    data = None
    # 1) 先用共用 HttpClient 嘗試
    try:
        data = client.get_json(primary)
    except Exception:
        data = None

    # 2) 主端點失敗 → 用 requests 直接抓文字並嘗試解析
    if data is None:
        try:
            r = requests.get(
                primary,
                timeout=20,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; twse-pipeline/1.0)"
                },
            )
            txt = (r.text or "").strip()
            # 只在可能是 JSON 的情況下解析；否則當空資料
            if txt.startswith("[") or txt.startswith("{"):
                data = json.loads(txt)
            else:
                data = []
        except Exception:
            data = []

    # 3) 最終保存（即使是 [] 也寫入，避免後續步驟找不到檔案而失敗）
    try:
        save_json(data, path)
    except Exception:
        # 極端情況下仍保底
        with open(path, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False)

    return path, d
