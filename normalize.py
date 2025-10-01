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
    解析 TWSE MI_INDEX 指數表格，統一輸出：
    ['market','date','open','high','low','close','volume','turnover']
    會印出原始 keys / sample 以利除錯。
    """
    import pandas as pd
    import re
    from datetime import datetime

    def _num2(x):
        # 用你檔案上方的 _to_num 規則，但容許百分比/字尾
        s = str(x).strip().replace(",", "")
        s = re.sub(r"[^\d\.\-]", "", s)
        try:
            return float(s) if s else None
        except:
            return None

    def _date_any(s):
        if s is None:
            return None
        s = str(s).strip()
        # 民國年 112/09/30
        m = re.match(r"^(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})$", s)
        if m and int(m.group(1)) < 1911:
            try:
                y = int(m.group(1)) + 1911
                return f"{y:04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
            except:
                pass
        for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except:
                continue
        return s  # 先保留原樣

    # ---- DEBUG：原始結構 ----
    if isinstance(raw_json, dict):
        print("DEBUG[TAIEX-RAW] keys:", list(raw_json.keys()))
        if "tables" in raw_json and isinstance(raw_json["tables"], list) and raw_json["tables"]:
            t0 = raw_json["tables"][0]
            print("DEBUG[TAIEX-RAW] tables[0] keys:", list(t0.keys()))
            d0 = (t0.get("data") or t0.get("rows") or [])
            if isinstance(d0, list) and d0:
                print("DEBUG[TAIEX-RAW] tables[0].data[0]:", d0[0])
    else:
        print("DEBUG[TAIEX-RAW] type:", type(raw_json))

    rows = []

    try:
        if isinstance(raw_json, dict):
            tables = raw_json.get("tables") or []
            target = None
            for t in tables:
                title = str(t.get("title") or t.get("name") or "")
                if ("發行量加權" in title) or ("加權指數" in title):
                    target = t
                    break

            if target:
                # 可能是 columns + data，也可能只有 data（list 或 list[dict]）
                cols = target.get("fields") or target.get("columns") or []
                data = target.get("data") or target.get("rows") or []

                if cols and isinstance(data, list) and data and isinstance(data[0], (list, tuple)):
                    # 形式：fields + data(row = list)
                    df = pd.DataFrame(data, columns=cols)
                else:
                    # 形式：data 為 list[dict] 或其他寬鬆型
                    df = pd.DataFrame(data)

                # DEBUG 欄位檢視
                print("DEBUG[TAIEX-NORM] cand columns:", df.columns.tolist()[:20])

                # 嘗試欄位映射（中文/英文）
                cmap = {}
                for c in df.columns:
                    cs = str(c)
                    if any(k in cs for k in ["開盤", "open", "Open"]):        cmap[c] = "open"
                    elif any(k in cs for k in ["最高", "high", "High"]):      cmap[c] = "high"
                    elif any(k in cs for k in ["最低", "low", "Low"]):        cmap[c] = "low"
                    elif any(k in cs for k in ["收盤", "收市", "close", "Close", "Index"]): cmap[c] = "close"
                    elif any(k in cs for k in ["成交量", "成交股數", "Volume"]): cmap[c] = "volume"
                    elif any(k in cs for k in ["成交金額", "Turnover", "Value"]): cmap[c] = "turnover"
                    elif any(k in cs for k in ["日期", "date", "Date", "time"]): cmap[c] = "date"

                if cmap:
                    df = df.rename(columns=cmap)

                # 如果沒有 date 欄，嘗試從報表日期或 raw_json 頭階資訊拿
                if "date" not in df.columns:
                    rpt_date = (raw_json.get("reportDate") or raw_json.get("date") or "").replace("/", "-")[:10]
                    df["date"] = rpt_date

                # 數字清洗
                for c in ["open", "high", "low", "close", "volume", "turnover"]:
                    if c in df.columns:
                        df[c] = df[c].map(_num2)

                # 日期轉換
                df["date"] = df["date"].map(_date_any)

                # 固定欄位
                for c in ["open","high","low","close","volume","turnover"]:
                    if c not in df.columns: df[c] = None
                df["market"] = "TAIEX"
                df = df[["market","date","open","high","low","close","volume","turnover"]]
                df = df.dropna(how="all")
                print("DEBUG[TAIEX-NORM] rows:", len(df))
                return df
    except Exception as e:
        print("DEBUG[TAIEX-NORM] exception:", repr(e))

    # 全部失敗 → 回空但欄位正確
    print("DEBUG[TAIEX-NORM] parsed rows=0 -> return empty df")
    return pd.DataFrame(columns=["market","date","open","high","low","close","volume","turnover"])

def normalize_otc(raw_obj) -> pd.DataFrame:
    """
    解析 TPEX 櫃買指數，統一輸出：
    ['market','date','open','high','low','close','volume','turnover']
    加入原始 keys / sample debug、寬鬆欄位對應、數字/日期清洗。
    """
    import pandas as pd
    import re
    from datetime import datetime

    def _num2(x):
        s = str(x).strip().replace(",", "")
        s = re.sub(r"[^\d\.\-]", "", s)
        try:
            return float(s) if s else None
        except:
            return None

    def _date_any(s):
        if s is None:
            return None
        s = str(s).strip()
        m = re.match(r"^(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})$", s)
        if m and int(m.group(1)) < 1911:
            try:
                y = int(m.group(1)) + 1911
                return f"{y:04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
            except:
                pass
        for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except:
                continue
        return s

    # ---- DEBUG 原始結構 ----
    if isinstance(raw_obj, list):
        print("DEBUG[OTC-RAW] type=list, len:", len(raw_obj))
        if raw_obj[:1]:
            print("DEBUG[OTC-RAW] first keys:", list(raw_obj[0].keys()))
            print("DEBUG[OTC-RAW] first item:", raw_obj[0])
    elif isinstance(raw_obj, dict):
        print("DEBUG[OTC-RAW] type=dict keys:", list(raw_obj.keys()))
    else:
        print("DEBUG[OTC-RAW] type:", type(raw_obj))

    # 支援 list[dict] / dict{data:list}
    if isinstance(raw_obj, dict) and "data" in raw_obj:
        df = pd.DataFrame(raw_obj.get("data") or [])
    elif isinstance(raw_obj, list):
        df = pd.DataFrame(raw_obj)
    else:
        print("DEBUG[OTC-NORM] unexpected raw -> empty")
        return pd.DataFrame(columns=["market","date","open","high","low","close","volume","turnover"])

    if df.empty:
        print("DEBUG[OTC-NORM] df empty -> return empty")
        return pd.DataFrame(columns=["market","date","open","high","low","close","volume","turnover"])

    print("DEBUG[OTC-NORM] cand columns:", df.columns.tolist()[:20])

    # 欄位對應（盡量寬鬆）
    cmap = {}
    for c in df.columns:
        cs = str(c)
        if cs in ["date","Date","日期","time"]:              cmap[c] = "date"
        elif any(k in cs for k in ["open","OpeningIndex","開盤"]):  cmap[c] = "open"
        elif any(k in cs for k in ["high","最高"]):                 cmap[c] = "high"
        elif any(k in cs for k in ["low","最低"]):                  cmap[c] = "low"
        elif any(k in cs for k in ["close","ClosingIndex","收盤","index"]): cmap[c] = "close"
        elif any(k in cs for k in ["volume","Volume","成交量","tradeVolume"]): cmap[c] = "volume"
        elif any(k in cs for k in ["turnover","TradeValue","成交金額","tradeValue"]): cmap[c] = "turnover"
    if cmap:
        df = df.rename(columns=cmap)

    # 數字/日期清洗
    for c in ["open","high","low","close","volume","turnover"]:
        if c in df.columns:
            df[c] = df[c].map(_num2)
    if "date" in df.columns:
        df["date"] = df["date"].map(_date_any)
    else:
        print("DEBUG[OTC-NORM] no 'date' column -> fill NA")
        df["date"] = None

    for c in ["open","high","low","close","volume","turnover"]:
        if c not in df.columns: df[c] = None

    df["market"] = "OTC"
    df = df[["market","date","open","high","low","close","volume","turnover"]]
    df = df.dropna(how="all")

    print("DEBUG[OTC-NORM] rows:", len(df), "cols:", list(df.columns))
    return df


