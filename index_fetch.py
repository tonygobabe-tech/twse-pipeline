# index_fetch.py —— 取得加權(台股)與櫃買指數日資料（盤後）
from utils import HttpClient, save_json
from datetime import datetime, timedelta, timezone
import os

def _taipei_yyyymmdd():
    now = datetime.now(timezone.utc) + timedelta(hours=8)
    # 週末回推到最近交易日（簡化：六日回推到週五）
    if now.weekday() == 5: now -= timedelta(days=1)
    if now.weekday() == 6: now -= timedelta(days=2)
    return now.strftime("%Y%m%d")

def fetch_taiex(client: HttpClient, out_root: str, date_yyyymmdd: str | None = None):
    d = date_yyyymmdd or _taipei_yyyymmdd()
    # TWSE 加權指數日報（JSON）
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={d}&type=IND"
    data = client.get_json(url)
    path = os.path.join(out_root, "raw", f"taiex_{d}.json")
    save_json(data, path)
    return path, d

def fetch_otc(client: HttpClient, out_root: str, date_yyyymmdd: str | None = None):
    d = date_yyyymmdd or _taipei_yyyymmdd()
    # 櫃買指數（OTC）日報（JSON）
    url = f"https://www.tpex.org.tw/openapi/v1/tpex_mainboard_index?date={d}"
    data = client.get_json(url)
    path = os.path.join(out_root, "raw", f"otc_{d}.json")
    save_json(data, path)
    return path, d
