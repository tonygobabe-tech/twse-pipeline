from typing import List, Dict, Any
import pandas as pd
import os, re, datetime as dt

def _to_num(x):
    if pd.isna(x):
        return None
    if isinstance(x, (int, float)):
        return x
    s = str(x).replace(",", "").strip()
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        if "." in s:
            return float(s)
        return int(s)
    except:
        return None

def normalize_daily(raw_json) -> pd.DataFrame:
    # STOCK_DAY_ALL 範例欄位名稱可能為中文；容錯處理
    df = pd.DataFrame(raw_json)
    # 嘗試猜測欄位
    col_map = {
        "Code":"code", "證券代號":"code",
        "Name":"name", "證券名稱":"name",
        "TradeVolume":"volume", "成交股數":"volume",
        "TradeValue":"turnover", "成交金額":"turnover",
        "OpeningPrice":"open", "開盤價":"open",
        "HighestPrice":"high", "最高價":"high",
        "LowestPrice":"low", "最低價":"low",
        "ClosingPrice":"close", "收盤價":"close",
        "Date":"date", "成交日期":"date",
    }
    for k,v in col_map.items():
        if k in df.columns:
            df.rename(columns={k:v}, inplace=True)
    # 基本型態清洗
    for c in ["open","high","low","close","volume","turnover"]:
        if c in df.columns:
            df[c] = df[c].map(_to_num)
    # 日期轉換（民國/西元兼容）
    if "date" in df.columns:
        def fix_date(x):
            s = str(x).replace("/", "-")
            # 民國年轉西元
            m = re.match(r"^(\d{2,3})-(\d{1,2})-(\d{1,2})$", s)
            if m and int(m.group(1)) < 1911:
                y = int(m.group(1)) + 1911
                return f"{y:04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
            return s
        df["date"] = df["date"].map(fix_date)
    return df[["code","name","date","open","high","low","close","volume","turnover"]].dropna(how="all")

def normalize_basics(raw_json) -> pd.DataFrame:
    df = pd.DataFrame(raw_json)
    col_map = {
        "公司代號":"code","CompanyCode":"code",
        "公司名稱":"name","CompanyName":"name",
        "產業別":"industry","Industry":"industry",
        "營利事業統一編號":"vat","UnifiedBusinessNumber":"vat",
        "上市日期":"listed_date","ListingDate":"listed_date",
    }
    for k,v in col_map.items():
        if k in df.columns:
            df.rename(columns={k:v}, inplace=True)
    return df

def normalize_news(raw_json) -> pd.DataFrame:
    df = pd.DataFrame(raw_json)
    col_map = {
        "公司代號":"code","CompanyCode":"code",
        "公司名稱":"name","CompanyName":"name",
        "主旨":"title","Subject":"title",
        "發言日期":"date","SpokeDate":"date",
    }
    for k,v in col_map.items():
        if k in df.columns:
            df.rename(columns={k:v}, inplace=True)
    return df

def normalize_generic(raw_json) -> pd.DataFrame:
    return pd.DataFrame(raw_json)
def normalize_insti(raw_json) -> pd.DataFrame:
    """
    將 /fund/T86 轉成欄位：
    code, name, date, net_foreign, net_invest, net_dealer, net_total
    """
    fields = raw_json.get("fields") or []
    rows = raw_json.get("data") or []
    if not fields or not rows:
        return pd.DataFrame(columns=["code","name","date","net_foreign","net_invest","net_dealer","net_total"])

    df = pd.DataFrame(rows, columns=fields)

    # 欄位對應（中文抬頭常見值）
    rename_map = {}
    for col in df.columns:
        if col in ["證券代號", "Code"]:
            rename_map[col] = "code"
        elif col in ["證券名稱", "Name"]:
            rename_map[col] = "name"
        elif "外陸資買賣超股數" in col:
            rename_map[col] = "net_foreign"
        elif "投信買賣超股數" in col:
            rename_map[col] = "net_invest"
    df.rename(columns=rename_map, inplace=True)

    def _to_int(x):
        try:
            s = str(x).replace(",", "").strip()
            if s in ("", "—"): return 0
            return int(float(s))
        except Exception:
            return 0

    # 自營商欄位可能分拆（自行/避險/合計），統一相加
    dealer_cols = [c for c in df.columns if c.startswith("自營商買賣超股數")]
    df["net_dealer"] = df[dealer_cols].applymap(_to_int).sum(axis=1) if dealer_cols else 0

    df["net_foreign"] = df["net_foreign"].map(_to_int) if "net_foreign" in df else 0
    df["net_invest"]  = df["net_invest"].map(_to_int)  if "net_invest"  in df else 0
    df["net_total"]   = df["net_foreign"] + df["net_invest"] + df["net_dealer"]

    raw_date = (raw_json.get("date") or "").replace("/", "-")[:10]  # YYYY-MM-DD
    df["date"] = raw_date

    out_cols = ["code","name","date","net_foreign","net_invest","net_dealer","net_total"]
    for c in out_cols:
        if c not in df.columns: df[c] = None
    return df[out_cols]
