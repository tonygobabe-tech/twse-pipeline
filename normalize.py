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
def normalize_taiex(raw_json) -> pd.DataFrame:
    """
    解析 TWSE MI_INDEX 指數表格，輸出：
      market, date, open, high, low, close, volume, turnover, is_cached, source_date
    """
    import pandas as pd, re

    is_cached = 1 if (isinstance(raw_json, dict) and raw_json.get("_cached")) else 0
    source_date = (raw_json.get("_cached_from") if isinstance(raw_json, dict) else None)

    # TWSE 回傳多個 tables，找加權指數那張
    tables = raw_json.get("tables") if isinstance(raw_json, dict) else None
    if not tables:
        # 空資料也要回傳 schema，避免後續出錯
        cols = ["market","date","open","high","low","close","volume","turnover","is_cached","source_date"]
        return pd.DataFrame(columns=cols)

    target = None
    for t in tables:
        if "發行量加權" in (t.get("title") or ""):
            target = t
            break
    if not target:
        cols = ["market","date","open","high","low","close","volume","turnover","is_cached","source_date"]
        return pd.DataFrame(columns=cols)

    cols = target.get("fields") or []
    rows = target.get("data") or []
    df = pd.DataFrame(rows, columns=cols)

    def _num(x):
        s = str(x).replace(",", "").strip()
        try:
            return float(re.sub(r"[^\d\.\-]", "", s)) if s else None
        except:
            return None

    cmap = {}
    for c in df.columns:
        if "開盤" in c: cmap[c] = "open"
        elif "最高" in c: cmap[c] = "high"
        elif "最低" in c: cmap[c] = "low"
        elif "收盤" in c or "收市" in c: cmap[c] = "close"
        elif "成交股數" in c or "成交量" in c: cmap[c] = "volume"
        elif "成交金額" in c: cmap[c] = "turnover"
    df = df.rename(columns=cmap)

    for c in ["open","high","low","close","volume","turnover"]:
        if c in df: df[c] = df[c].map(_num)

    date_str = (raw_json.get("reportDate") or raw_json.get("date") or "") if isinstance(raw_json, dict) else ""
    date_str = date_str.replace("/", "-")[:10] if date_str else None

    df["date"] = date_str
    df["market"] = "TAIEX"
    df["is_cached"] = is_cached
    df["source_date"] = source_date

    out_cols = ["market","date","open","high","low","close","volume","turnover","is_cached","source_date"]
    return df[out_cols]


def normalize_otc(raw_obj) -> pd.DataFrame:
    """
    解析 TPEX 主板指數，輸出：
      date, open, high, low, close, volume, turnover, is_cached, source_date
    支援 raw 為 list 或 dict（含 _cached 標記）。
    """
    import pandas as pd, re

    def _num(x):
        s = str(x).replace(",", "").strip()
        try:
            return float(re.sub(r"[^\d\.\-]", "", s)) if s else None
        except:
            return None

    is_cached = 1 if (isinstance(raw_obj, dict) and raw_obj.get("_cached")) else 0
    source_date = (raw_obj.get("_cached_from") if isinstance(raw_obj, dict) else None)

    # 1) 抽出 rows
    if isinstance(raw_obj, dict):
        if "data" in raw_obj and isinstance(raw_obj["data"], list):
            df = pd.DataFrame(raw_obj["data"])
        elif "data" in raw_obj and isinstance(raw_obj["data"], dict):
            df = pd.DataFrame(raw_obj["data"].get("data") or [])
        else:
            df = pd.DataFrame()  # 不認得的結構
    elif isinstance(raw_obj, list):
        df = pd.DataFrame(raw_obj)
    else:
        df = pd.DataFrame()

    if df.empty:
        cols = ["date","open","high","low","close","volume","turnover","is_cached","source_date"]
        return pd.DataFrame(columns=cols)

    # 2) 找日期欄位並統一欄名
    date_col = None
    for cand in ["date","Date","tradeDate","日期","time"]:
        if cand in df.columns:
            date_col = cand
            break
    if date_col:
        df.rename(columns={date_col: "date"}, inplace=True)
    else:
        df["date"] = None

    rename_map = {
        "開盤價": "open",
        "最高價": "high",
        "最低價": "low",
        "收盤價": "close",
        "成交股數": "volume",
        "成交金額": "turnover",
    }
    for k, v in rename_map.items():
        if k in df.columns:
            df.rename(columns={k: v}, inplace=True)

    for c in ["open","high","low","close","volume","turnover"]:
        if c in df.columns:
            df[c] = df[c].map(_num)

    df["is_cached"] = is_cached
    df["source_date"] = source_date

    keep = [c for c in ["date","open","high","low","close","volume","turnover","is_cached","source_date"] if c in df.columns]
    df = df[keep].dropna(how="all")
    return df


