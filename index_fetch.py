# index_fetch.py — 加權(TAIEX)/櫃買(OTC) 指數抓取：回推 + 快取最後一次有資料
from utils import HttpClient, save_json
from datetime import datetime, timedelta, timezone
import os
import json
import requests

# ---- 可調參數 ----
MAX_BACKTRACK = 10              # 最多往前嘗試天數
ANNOUNCE_HOUR_LOCAL = 16        # 當地時間（台北）幾點前視為尚未公布，先從前一交易日起算

# ---- 時間 & 檔案工具 ----
def _now_tw():
    return datetime.now(timezone.utc) + timedelta(hours=8)

def _ymd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")

def _backtrack_dates(start: datetime, days: int):
    """從 start 往前推回傳工作日清單（跳過週末），含 start 本身。"""
    out = []
    d = start
    for _ in range(days + 1):
        while d.weekday() >= 5:  # 5,6 = 六、日
            d = d - timedelta(days=1)
        out.append(_ymd(d))
        d = d - timedelta(days=1)
    return out

def _ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def _cache_dir(out_root):
    d = os.path.join(out_root, "cache")
    _ensure_dir(d)
    return d

def _cache_path(out_root, market: str):
    return os.path.join(_cache_dir(out_root), f"{market}_last.json")

def _save_cache(out_root, market: str, date_str: str, payload):
    path = _cache_path(out_root, market)
    data = {"date": date_str, "data": payload}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"[CACHE] saved {market}={date_str} -> {path}")

def _load_cache(out_root, market: str):
    path = _cache_path(out_root, market)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        date_str = obj.get("date")
        data = obj.get("data")
        if data:
            return date_str, data
    except Exception:
        pass
    return None

# ---- 判斷資料是否為「非空」 ----
def _non_empty_taiex(raw_json) -> bool:
    """MI_INDEX response 是否含『發行量加權股價指數』且 data 有列。"""
    tables = raw_json.get("tables") if isinstance(raw_json, dict) else None
    if not tables:
        return False
    for t in tables:
        title = (t.get("title") or "")
        if "發行量加權" in title:
            rows = t.get("data") or []
            return len(rows) > 0
    return False

def _non_empty_otc(raw_obj) -> bool:
    """TPEX 主板指數 openapi：list 非空；或 dict 含 data 且非空。"""
    if isinstance(raw_obj, list):
        return len(raw_obj) > 0
    if isinstance(raw_obj, dict):
        rows = raw_obj.get("data") or []
        return len(rows) > 0
    return False

# ---- 抓取：TAIEX ----
def fetch_taiex(client: HttpClient, out_root: str, date_yyyymmdd: str | None = None,
                max_backtrack: int = MAX_BACKTRACK, use_cache: bool = True):
    """
    抓 TWSE 加權指數（MI_INDEX?type=IND）
    先回推工作日；若都空，使用 cache 回填，raw 會標註 _cached 與 _cached_from。
    """
    raw_dir = os.path.join(out_root, "raw"); _ensure_dir(raw_dir)

    # 起始日：未指定且未到公布時間，先從前一工作日
    if date_yyyymmdd:
        start = datetime.strptime(date_yyyymmdd, "%Y%m%d")
    else:
        start = _now_tw()
        if _now_tw().hour < ANNOUNCE_HOUR_LOCAL:
            start = start - timedelta(days=1)
    dates = _backtrack_dates(start, max_backtrack)

    for i, ymd in enumerate(dates):
        url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={ymd}&type=IND"
        ok = False
        try:
            data = client.get_json(url)
            ok = _non_empty_taiex(data)
        except Exception:
            ok = False
            data = {}

        print(f"DEBUG[TAIEX] try={i}, date={ymd}, ok={ok}")
        path = os.path.join(raw_dir, f"taiex_{ymd}.json")
        if ok:
            save_json(data, path)
            _save_cache(out_root, "taiex", ymd, data)
            return path, ymd

    # 都抓不到 → 用 cache 回填
    if use_cache:
        cached = _load_cache(out_root, "taiex")
        if cached:
            cdate, cdata = cached
            ymd = dates[0]  # 以預期日命名
            out_obj = {"_cached": True, "_cached_from": cdate,
                       **(cdata if isinstance(cdata, dict) else {"data": cdata})}
            path = os.path.join(raw_dir, f"taiex_{ymd}.json")
            save_json(out_obj, path)
            print(f"INFO[TAIEX] used cache from {cdate} -> {path}")
            return path, ymd

    # 仍無 → 存空
    ymd = dates[0]
    path = os.path.join(raw_dir, f"taiex_{ymd}.json")
    save_json({}, path)
    print(f"INFO[TAIEX] no data after backtrack {max_backtrack} days; saved empty for {ymd}")
    return path, ymd

# ---- 抓取：OTC ----
def fetch_otc(client: HttpClient, out_root: str, date_yyyymmdd: str | None = None,
              max_backtrack: int = MAX_BACKTRACK, use_cache: bool = True):
    """
    抓 TPEX 主板指數：
      1) 打 ?date=YYYYMMDD
      2) 空 → 打全量端點過濾
      3) 回推工作日
      4) 最後用 cache 回填
    """
    raw_dir = os.path.join(out_root, "raw"); _ensure_dir(raw_dir)

    # 起始日
    if date_yyyymmdd:
        start = datetime.strptime(date_yyyymmdd, "%Y%m%d")
    else:
        start = _now_tw()
        if _now_tw().hour < ANNOUNCE_HOUR_LOCAL:
            start = start - timedelta(days=1)
    dates = _backtrack_dates(start, max_backtrack)

    for i, ymd in enumerate(dates):
        # 先主端點
        primary = f"https://www.tpex.org.tw/openapi/v1/tpex_mainboard_index?date={ymd}"
        obj = None
        try:
            obj = client.get_json(primary)
        except Exception:
            obj = None

        # 失敗/空 → 打全量端點並過濾
        if not _non_empty_otc(obj):
            try:
                r = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_index",
                                 timeout=20,
                                 headers={"User-Agent": "Mozilla/5.0 (compatible; twse-pipeline/1.0)"})
                txt = (r.text or "").strip()
                if txt.startswith("[") or txt.startswith("{"):
                    data = json.loads(txt)
                else:
                    data = []
            except Exception:
                data = []

            yyyy, mm, dd = ymd[:4], ymd[4:6], ymd[6:8]
            patterns = {f"{yyyy}{mm}{dd}", f"{yyyy}-{mm}-{dd}", f"{yyyy}/{mm}/{dd}"}
            try:  # 民國年
                patterns.add(f"{int(yyyy)-1911}/{mm}/{dd}")
            except Exception:
                pass

            filtered = []
            for row in data if isinstance(data, list) else []:
                cand = str(
                    row.get("date") or row.get("Date") or row.get("tradeDate") or
                    row.get("日期") or row.get("time") or ""
                ).strip()
                cand = cand.replace(".", "-").replace(".", "/")
                if cand in patterns:
                    filtered.append(row)

            obj = filtered

        rows = (len(obj) if isinstance(obj, list) else len(obj.get("data", [])) if isinstance(obj, dict) else 0)
        print(f"DEBUG[OTC] try={i}, date={ymd}, rows={rows}")
        path = os.path.join(raw_dir, f"otc_{ymd}.json")
        if _non_empty_otc(obj):
            save_json(obj, path)
            _save_cache(out_root, "otc", ymd, obj)
            return path, ymd

    # 都抓不到 → 用 cache 回填
    if use_cache:
        cached = _load_cache(out_root, "otc")
        if cached:
            cdate, cdata = cached
            ymd = dates[0]
            out_obj = {"_cached": True, "_cached_from": cdate,
                       "data": cdata if isinstance(cdata, list) else cdata.get("data", [])}
            path = os.path.join(raw_dir, f"otc_{ymd}.json")
            save_json(out_obj, path)
            print(f"INFO[OTC] used cache from {cdate} -> {path}")
            return path, ymd

    # 仍無 → 存空
    ymd = dates[0]
    path = os.path.join(raw_dir, f"otc_{ymd}.json")
    save_json([], path)
    print(f"INFO[OTC] no data after backtrack {max_backtrack} days; saved empty for {ymd}")
    return path, ymd

