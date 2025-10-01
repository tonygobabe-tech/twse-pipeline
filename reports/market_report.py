# reports/market_report.py
import os
import pandas as pd

OUT_DIR = os.path.join("data", "reports")
os.makedirs(OUT_DIR, exist_ok=True)
OUT_CSV = os.path.join(OUT_DIR, "market_overview.csv")

# 讀取來源（各檔存在則讀）
paths = {
    "taiex": os.path.join("data", "normalized", "taiex.csv"),
    "otc":   os.path.join("data", "normalized", "otc.csv"),
    "insti": os.path.join("data", "normalized", "insti.csv"),
}
dfs = {}
for k, p in paths.items():
    dfs[k] = pd.read_csv(p) if os.path.exists(p) and os.path.getsize(p) > 0 else pd.DataFrame()

# 取最新一筆 TAIEX 與 OTC（若空則用 None）
def latest_one(df: pd.DataFrame, market_name: str):
    if df.empty:
        return pd.DataFrame([{
            "market": market_name, "date": None,
            "open": None, "high": None, "low": None, "close": None,
            "volume": None, "turnover": None
        }])
    cols = [c for c in ["market","date","open","high","low","close","volume","turnover"] if c in df.columns]
    d = df.sort_values("date").tail(1)[cols].copy()
    # 確保 market 欄位
    if "market" not in d.columns:
        d["market"] = market_name
    return d

taiex_last = latest_one(dfs["taiex"], "TAIEX")
otc_last   = latest_one(dfs["otc"],   "OTC")
index_df   = pd.concat([taiex_last, otc_last], ignore_index=True)

# 法人近一日合計（若空則補 None）
if not dfs["insti"].empty and "date" in dfs["insti"].columns:
    insti_latest_date = dfs["insti"]["date"].max()
    insti_last = dfs["insti"][dfs["insti"]["date"] == insti_latest_date]
    agg = {
        "net_foreign": insti_last["net_foreign"].sum() if "net_foreign" in insti_last else None,
        "net_invest":  insti_last["net_invest"].sum()  if "net_invest"  in insti_last else None,
        "net_dealer":  insti_last["net_dealer"].sum()  if "net_dealer"  in insti_last else None,
        "net_total":   insti_last["net_total"].sum()   if "net_total"   in insti_last else None,
    }
else:
    insti_latest_date = None
    agg = {"net_foreign": None, "net_invest": None, "net_dealer": None, "net_total": None}

overview = index_df.copy()
overview["insti_date"] = insti_latest_date
overview["net_foreign"] = agg["net_foreign"]
overview["net_invest"]  = agg["net_invest"]
overview["net_dealer"]  = agg["net_dealer"]
overview["net_total"]   = agg["net_total"]

# 定義固定欄位順序（就算沒資料也要有表頭）
cols = [
    "market","date","open","high","low","close","volume","turnover",
    "insti_date","net_foreign","net_invest","net_dealer","net_total"
]
for c in cols:
    if c not in overview.columns:
        overview[c] = None
overview = overview[cols]

# 寫檔（就算是空資料，也會寫出表頭）
overview.to_csv(OUT_CSV, index=False)
print(f"[OK] report -> {OUT_CSV} (rows={len(overview)})")

