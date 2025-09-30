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


def fetch_otc(client: HttpClient, out_root: str, date_yyyymmdd: str | None = None,
              max_backtrack: int = 5):
    """
    取得櫃買(OTC)指數日資料（盤後）— 強韌回推版
    規則：
      1) 以台北時間為基準；若在盤後公布前（建議 < 16:00），直接以「前一個交易日」為起點
      2) 先打帶 date 的端點；若為空，再打不帶日期的全量端點並依多種日期格式過濾
      3) 若仍為空 → 回推到前一日（跳過週末），最多回推 max_backtrack 天
      4) 總是輸出 raw/otc_<實際抓到的日期>.json（即使是 []），避免 pipeline 中斷
    """
    import json, requests
    from datetime import datetime, timedelta, timezone

    def _tz_now_tw():
        return datetime.now(timezone.utc) + timedelta(hours=8)

    def _ymd(dt):
        return dt.strftime("%Y%m%d")

    # 1) 決定起始日期：如果未指定且現在時間太早，先預設抓「前一個交易日」
    if date_yyyymmdd:
        base = datetime.strptime(date_yyyymmdd, "%Y%m%d")
    else:
        now_tw = _tz_now_tw()
        base = now_tw
        # 公布時間前先抓昨天（避免今天尚未出資料）
        if now_tw.hour < 16:
            base = base - timedelta(days=1)

    # 跳過週末
    while base.weekday() >= 5:  # 5=Sat, 6=Sun
        base = base - timedelta(days=1)

    tried = 0
    out_dir = os.path.join(out_root, "raw")
    os.makedirs(out_dir, exist_ok=True)

    while tried <= max_backtrack:
        d = base - timedelta(days=tried)
        # 遇到週末就再往前
        while d.weekday() >= 5:
            d = d - timedelta(days=1)
        ymd = _ymd(d)

        # ---- 主端點（常見會回空）----
        url_primary = f"https://www.tpex.org.tw/openapi/v1/tpex_mainboard_index?date={ymd}"
        data = None
        try:
            data = client.get_json(url_primary)
        except Exception:
            data = None

        # ---- 備援：不帶日期，全量下載後篩選 ----
        if not data:
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

            # 多種日期格式（寬鬆匹配）
            yyyy, mm, dd = ymd[:4], ymd[4:6], ymd[6:8]
            patterns = {
                f"{yyyy}{mm}{dd}",
                f"{yyyy}-{mm}-{dd}",
                f"{yyyy}/{mm}/{dd}",
            }
            # 民國年（保險用）
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

        # 若這一天拿到資料就保存並結束；否則回推一天重試
        path = os.path.join(out_dir, f"otc_{ymd}.json")
        save_json(data or [], path)
        print(f"DEBUG[OTC] try={tried}, date={ymd}, rows={len(data or [])}")

        if data:
            return path, ymd

        tried += 1

    # 全部回推都沒有，就以起始日輸出空陣列
    final_path = os.path.join(out_dir, f"otc_{_ymd(base)}.json")
    save_json([], final_path)
    print(f"INFO[OTC] no data after backtrack {max_backtrack} days; saved empty for {_ymd(base)}")
    return final_path, _ymd(base)
