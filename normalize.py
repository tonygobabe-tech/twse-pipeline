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
