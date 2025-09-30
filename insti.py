# insti.py —— 取得 TWSE 三大法人（T86）
from datetime import datetime, timedelta, timezone
import os
from utils import HttpClient, save_json

def _taipei_yyyymmdd():
    # Actions 跑在 UTC；轉台北時區並處理週末（六日退到最近週五）
    now = datetime.now(timezone.utc) + timedelta(hours=8)
    if now.weekday() == 5:      # Sat
        now -= timedelta(days=1)
    elif now.weekday() == 6:    # Sun
        now -= timedelta(days=2)
    return now.strftime("%Y%m%d")

def fetch_insti(client: HttpClient, out_root: str, date_yyyymmdd: str | None = None):
    date_yyyymmdd = date_yyyymmdd or _taipei_yyyymmdd()
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_yyyymmdd}&selectType=ALL"
    data = client.get_json(url)
    raw_path = os.path.join(out_root, "raw", f"insti_{date_yyyymmdd}.json")
    save_json(data, raw_path)
    return raw_path, date_yyyymmdd
