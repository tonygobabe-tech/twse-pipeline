# reports/market_report.py
import os
import pandas as pd

DATA_DIR = "data/normalized"
OUTPUT = "data/market_overview.csv"

def _read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"[WARN] missing file: {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        print(f"[OK] read {os.path.basename(path)} -> rows={len(df)}")
        return df
    except Exception as e:
        print(f"[ERROR] read csv failed: {path} -> {e}")
        return pd.DataFrame()

def _pick_first(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _prepare_insti(insti: pd.DataFrame) -> pd.DataFrame:
    """將 insti(三大法人)彙總到「每日合計」：date, net_foreign, net_invest, net_dealer, net_total"""
    if insti.empty:
        return pd.DataFrame(columns=["date","net_foreign","net_invest","net_dealer","net_total"])

    # 自動對應欄位（支援 net_* 或英文/中文）
    f_col = _pick_first(insti, ["net_foreign", "foreign", "外陸資買賣超股數合計", "外資買賣超股數"])
    i_col = _pick_first(insti, ["net_invest",  "invest",  "投信買賣超股數"])
    d_col = _pick_first(insti, ["net_dealer",  "dealer",  "自營商買賣超股數", "自營商買賣超股數(合計)"])

    # 沒偵測到也不要爆，先補 0
    for col, newname in [(f_col,"foreign"), (i_col,"invest"), (d_col,"dealer")]:
        if col is None:
            insti[newname] = 0
        else:
            insti[newname] = pd.to_numeric(insti[col], errors="coerce").fillna(0)

    if "date" not in insti.columns:
        # 沒有日期就沒法彙總
        return pd.DataFrame(columns=["date","net_foreign","net_invest","net_dealer","net_total"])

    g = insti.groupby("date")[["foreign","invest","dealer"]].sum().reset_index()
    g.rename(columns={
        "foreign":"net_foreign",
        "invest":"net_invest",
        "dealer":"net_dealer"
    }, inplace=True)
    g["net_total"] = g["net_foreign"] + g["net_invest"] + g["net_dealer"]
    print(f"[OK] insti grouped -> rows={len(g)}")
    return g[["date","net_foreign","net_invest","net_dealer","net_total"]]

def main():
    taiex = _read_csv(os.path.join(DATA_DIR, "taiex.csv"))
    otc   = _read_csv(os.path.join(DATA_DIR, "otc.csv"))
    insti = _read_csv(os.path.join(DATA_DIR, "insti.csv"))

    # 統一欄位存在與型別
    for df, mkt in [(taiex,"TAIEX"), (otc,"OTC")]:
        if not df.empty:
            if "market" not in df.columns:
                df["market"] = mkt
            # 確保關鍵價量欄位存在
            for c in ["open","high","low","close","volume","turnover"]:
                if c not in df.columns:
                    df[c] = None
        else:
            # 允許空表，後面 concat 會得到空表
            pass

    market_df = pd.concat([taiex, otc], ignore_index=True) if not (taiex.empty and otc.empty) else pd.DataFrame()
    print(f"[INFO] market rows -> {len(market_df)}")

    # 預設法人欄位
    base_cols = [
        "date","market","open","high","low","close","volume","turnover",
        "net_foreign","net_invest","net_dealer","net_total"
    ]

    if market_df.empty:
        # 沒有任何 TAIEX/OTC 資料，仍輸出空表（但保留欄位），讓下游不會壞
        out = pd.DataFrame(columns=base_cols)
        out.to_csv(OUTPUT, index=False, encoding="utf-8-sig")
        print(f"[OK] Saved market overview -> {OUTPUT}, rows=0 (empty market)")
        return

    # 準備法人合計（按日）
    insti_daily = _prepare_insti(insti)

    # 將法人欄位 merge 進兩個市場（相同日期共用同一組法人數）
    for c in ["net_foreign","net_invest","net_dealer","net_total"]:
        market_df[c] = 0

    if not insti_daily.empty:
        market_df = market_df.merge(insti_daily, on="date", how="left", suffixes=("", "_y"))
        for c in ["net_foreign","net_invest","net_dealer","net_total"]:
            fill = market_df[f"{c}_y"].fillna(0)
            # 若原本欄位是 0，就用合併的值覆蓋
            market_df[c] = fill
            market_df.drop(columns=[f"{c}_y"], inplace=True)

    # 最終欄位順序
    for c in base_cols:
        if c not in market_df.columns:
            market_df[c] = None
    market_df = market_df[base_cols]

    market_df.to_csv(OUTPUT, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved market overview -> {OUTPUT}, rows={len(market_df)}")

if __name__ == "__main__":
    main()

