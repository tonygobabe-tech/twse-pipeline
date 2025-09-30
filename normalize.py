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
    解析 TWSE MI_INDEX 指數表格，輸出：market, date, open, high, low, close, volume, turnover
    """
    import pandas as pd
    # TWSE 回傳多個 tables，找加權指數那張
    tables = raw_json.get("tables") or []
    target = None
    for t in tables:
        if t.get("title") and ("發行量加權股價指數" in t["title"] or "發行量加權" in t["title"]):
            target = t
            break
    if not target:
        return pd.DataFrame(columns=["market","date","open","high","low","close","volume","turnover"])

    cols = target.get("fields") or []
    rows = target.get("data") or []
    df = pd.DataFrame(rows, columns=cols)

    def _num(x):
        import re
        s = str(x).replace(",", "").strip()
        try:
            return float(re.sub(r"[^\d\.\-]", "", s)) if s else None
        except: return None

    # 嘗試常見中文欄位
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

    date = (raw_json.get("reportDate") or raw_json.get("date") or "").replace("/", "-")[:10]
    df["date"] = date
    df["market"] = "TAIEX"
    return df[["market","date","open","high","low","close","volume","turnover"]]

def normalize_otc(raw_obj) -> pd.DataFrame:
    """
    DEBUG 版：把 OTC 原始欄位印到 log，並且彈性對應欄位。
    就算抓不到任何欄位，也會回傳合法 schema，避免整體 pipeline 失敗。
    """
    import pandas as pd
    import json

    # ---- 1) 把原始結構/欄位印出來（到 GitHub Actions 的 log）----
    print("DEBUG[OTC] raw type:", type(raw_obj))
    if isinstance(raw_obj, dict):
        print("DEBUG[OTC] raw keys:", list(raw_obj.keys()))
        data = (
            raw_obj.get("data")
            or raw_obj.get("records")
            or raw_obj.get("items")
            or raw_obj.get("Data")
            or raw_obj.get("result")
            or raw_obj.get("payload")
        )
    else:
        data = raw_obj  # 有些端點直接就是 list

    if data is None:
        # 盡量不要把整包印出來，避免太長；只截前 500 字
        print("DEBUG[OTC] no obvious data key; raw head:", str(raw_obj)[:500])
        data = []

    df = pd.DataFrame(data)
    print("DEBUG[OTC] columns:", list(df.columns))
    try:
        print("DEBUG[OTC] sample rows:", df.head(2).to_dict(orient="records"))
    except Exception:
        pass

    # ---- 2) 嘗試把各種常見欄位名「對應」成標準欄位 ----
    candidates = {
        "date":     ["date", "Date", "日期", "交易日期"],
        "open":     ["open", "Open", "開盤", "開盤指數", "開盤價"],
        "high":     ["high", "High", "最高", "最高指數", "最高價"],
        "low":      ["low", "Low", "最低", "最低指數", "最低價"],
        "close":    ["close", "Close", "收盤", "收盤指數", "收盤價"],
        "volume":   ["volume", "Volume", "成交股數", "成交量"],
        "turnover": ["turnover", "Turnover", "成交金額", "成交值", "成交金額(千元)"],
    }

    rename_map = {}
    for std, opts in candidates.items():
        for c in opts:
            if c in df.columns:
                rename_map[c] = std
                break
    if rename_map:
        df = df.rename(columns=rename_map)

    # ---- 3) 數字/日期標準化（能轉就轉，轉不了留空）----
    def _num(x):
        try:
            s = str(x).replace(",", "").strip()
            if s == "" or s == "—":
                return None
            return float(s)
        except Exception:
            return None

    for c in ["open", "high", "low", "close", "volume", "turnover"]:
        if c in df.columns:
            df[c] = df[c].map(_num)

    if "date" in df.columns:
        df["date"] = df["date"].astype(str).str.replace("/", "-").str[:10]

    # ---- 4) 固定欄位輸出，缺的補 None；並標 market=OTC ----
    df["market"] = "OTC"
    cols = ["market", "date", "open", "high", "low", "close", "volume", "turnover"]
    for c in cols:
        if c not in df.columns:
            df[c] = None

    out = df[cols]
    print("DEBUG[OTC] normalized rows:", len(out))
    return out
