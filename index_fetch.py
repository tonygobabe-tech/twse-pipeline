# index_fetch.py — 指數抓取（TAIEX/OTC）強韌回推版（max_backtrack=10＋強化 Log）
# 特點：
# - 以台北時間判斷；<16:00 預設抓前一個交易日
# - 自動跳過週末
# - 先抓「指定日」；失敗則回推前幾天（預設最多 10 天，應付長假）
# - OTC 另加「全量端點 + 多日期格式」過濾
# - 永遠落地 raw 檔（就算是空檔），避免後續流程中斷
# - Log 一致：DEBUG[...] 每次嘗試；INFO[...] 超過回推上限

from utils import HttpClient, save_json
from datetime import datetime, timedelta, timezone
import os
import json
import requests


# ---------- 共用工具 ----------
def _tz_now_tw() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=8)

def _yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")

def _start_base_date(date_yyyymmdd: str | None) -> datetime:
    """
    若未指定日期，且台北時間 < 16:00，先抓前一日（避免當天盤後尚未出資料）。
    """
    if date_yyyymmdd:
        return datetime.strptime(date_yyyymmdd, "%Y%m%d")
    now_tw = _tz_now_tw()
    base = now_tw if now_tw.hour >= 16 else (now_tw - timedelta(days=1))
    return base

def _skip_weekend(dt: datetime) -> datetime:
    while dt.weekday() >= 5:  # 5=Sat, 6=Sun
        dt = dt - timedelta(days=1)
    return dt


# ---------- TAIEX（加權指數） ----------
def fetch_taiex(
    client: HttpClient,
    out_root: str,
    date_yyyymmdd: str | None = None,
    max_backtrack: int = 10,   # ← 調整為 10 天
):
    """
    TWSE MI_INDEX (type=IND)
    回推策略：指定日 → 若表格空或取用失敗，往前回推，最多 max_backtrack 天。
    raw 檔：data/raw/taiex_<yyyymmdd>.json
    """
    raw_dir = os.path.join(out_root, "raw")
    os.makedirs(raw_dir, exist_ok=True)

    base = _skip_weekend(_start_base_date(date_yyyymmdd))
    tried = 0

    while tried <= max_backtrack:
        d = _skip_weekend(base - timedelta(days=tried))
        ymd = _yyyymmdd(d)
        url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={ymd}&type=IND"

        data = None
        try:
            data = client.get_json(url)
        except Exception:
            data = None

        # 判斷是否有「發行量加權」那張表，且有資料
        ok = False
        if isinstance(data, dict):
            tables = data.get("tables") or []
            for t in tables:
                title = (t.get("title") or "")
                if "發行量加權" in title:
                    rows = t.get("data") or []
                    ok = len(rows) > 0
                    break

        path = os.path.join(raw_dir, f"taiex_{ymd}.json")
        save_json(data or {}, path)
        print(f"DEBUG[TAIEX] try={tried:>2}, date={ymd}, ok={ok}")

        if ok:
            return path, ymd

        tried += 1

    # 回推到上限仍無資料 → 落地空檔
    final = _yyyymmdd(_skip_weekend(base))
    empty_path = os.path.join(raw_dir, f"taiex_{final}.json")
    save_json({}, empty_path)
    print(f"INFO[TAIEX] no data after backtrack {max_backtrack} days; saved empty for {final}")
    return empty_path, final


# ---------- OTC（櫃買指數） ----------
def fetch_otc(
    client: HttpClient,
    out_root: str,
    date_yyyymmdd: str | None = None,
    max_backtrack: int = 10,   # ← 調整為 10 天
):
    """
    TPEX openapi：先打帶 date 端點，若空再打不帶日期的全量端點並以多種日期格式過濾。
    回推策略：指定日 → 空就往前回推，最多 max_backtrack 天。
    raw 檔：data/raw/otc_<yyyymmdd>.json
    """
    raw_dir = os.path.join(out_root, "raw")
    os.makedirs(raw_dir, exist_ok=True)

    base = _skip_weekend(_start_base_date(date_yyyymmdd))
    tried = 0

    while tried <= max_backtrack:
        d = _skip_weekend(base - timedelta(days=tried))
        ymd = _yyyymmdd(d)

        # 1) 主端點（常見回空陣列）
        primary = f"https://www.tpex.org.tw/openapi/v1/tpex_mainboard_index?date={ymd}"
        data = None
        try:
            data = client.get_json(primary)
        except Exception:
            data = None

        # 2) 備援：不帶日期端點，取全量後依多種日期格式過濾
        if not data:
            url_all = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_index"
            try:
                r = requests.get(
                    url_all,
                    timeout=20,
                    headers={"User-Agent": "Mozilla/5.0 (compatible; twse-pipeline/1.0)"},
                )
                txt = (r.text or "").strip()
                data = json.loads(txt) if (txt.startswith("[") or txt.startswith("{")) else []
            except Exception:
                data = []

            yyyy, mm, dd = ymd[:4], ymd[4:6], ymd[6:8]
            patterns = {f"{yyyy}{mm}{dd}", f"{yyyy}-{mm}-{dd}", f"{yyyy}/{mm}/{dd}"}
            # 民國年（保險）
            try:
                patterns.add(f"{int(yyyy)-1911}/{mm}/{dd}")
            except Exception:
                pass

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
                if cand in patterns:
                    filtered.append(row)
            data = filtered

        path = os.path.join(raw_dir, f"otc_{ymd}.json")
        save_json(data or [], path)
        print(f"DEBUG[OTC]   try={tried:>2}, date={ymd}, rows={len(data or [])}")

        if data:  # 有資料就停
            return path, ymd

        tried += 1

    # 回推到上限仍無資料 → 落地空檔
    final = _yyyymmdd(_skip_weekend(base))
    empty_path = os.path.join(raw_dir, f"otc_{final}.json")
    save_json([], empty_path)
    print(f"INFO[OTC] no data after backtrack {max_backtrack} days; saved empty for {final}")
    return empty_path, final
