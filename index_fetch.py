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
    取得櫃買(OTC)指數日資料（盤後）— 強韌版 v2
    策略：
      1) 先打帶 date 的端點（常見會回空陣列）
      2) 如果拿不到，再打「不帶日期」的全量端點，然後用多種日期格式去過濾
      3) 最後仍無資料 → 輸出 []，避免 pipeline 中斷
    輸出：data/raw/otc_<yyyymmdd>.json
    """
    import json, requests
    d = date_yyyymmdd or _taipei_yyyymmdd()

    raw_dir = os.path.join(out_root, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    path = os.path.join(raw_dir, f"otc_{d}.json")

    # 1) 主端點（常見情況會回空 list）
    url_primary = f"https://www.tpex.org.tw/openapi/v1/tpex_mainboard_index?date={d}"
    data = None
    try:
        data = client.get_json(url_primary)
    except Exception:
        data = None

    # 2) 若主端點拿不到或為空 → 用全量端點，再做日期過濾
    if not data:  # None 或 空 list 都進來
        url_all = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_index"
        try:
            r = requests.get(
                url_all, timeout=20,
                headers={"User-Agent": "Mozilla/5.0 (compatible; twse-pipeline/1.0)"}
            )
            txt = (r.text or "").strip()
            if txt.startswith("[") or txt.startswith("{"):
                data = json.loads(txt)
            else:
                data = []
        except Exception:
            data = []

        # 目標日期多種寫法（做寬鬆比對）
        yyyy = d[:4]; mm = d[4:6]; dd = d[6:8]
        date_patterns = {
            f"{yyyy}{mm}{dd}",
            f"{yyyy}-{mm}-{dd}",
            f"{yyyy}/{mm}/{dd}",
        }
        # 也放一個民國年（保險，但多半用不到）
        try:
            roc = f"{int(yyyy)-1911}/{mm}/{dd}"
            date_patterns.add(roc)
        except Exception:
            pass

        # 在常見鍵名上做比對
        filtered = []
        for row in data if isinstance(data, list) else []:
            cand = str(
                row.get("date")
                or row.get("Date")
                or row.get("tradeDate")
                or row.get("日期")
                or row.get("time")
                or ""
            ).strip()
            cand = cand.replace(".", "-").replace(".", "/")
            if cand in date_patterns:
                filtered.append(row)
        data = filtered

    # 3) 最終保存（即使是 [] 也寫入）
    try:
        save_json(data or [], path)
    except Exception:
        with open(path, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False)

    # Debug（會出現在 Actions log）
    try:
        print(f"DEBUG[OTC] after fetch: rows={len(data or [])}, sample={(data or [])[:2]}")
    except Exception:
        pass

    return path, d
