# reports/market_report.py
# 目的：把 TAIEX / OTC 的指數 (normalized) 與三大法人 T86 (normalized) 合併成「市場概覽」
# 產出：
#   - data/market_overview.csv
#   - twse.db -> market_overview 表（自動建表，覆寫當日同鍵資料）

import os
import sqlite3
import pandas as pd

DATA_DIR = "data"
NORM_DIR = os.path.join(DATA_DIR, "normalized")
CSV_OUT = os.path.join(DATA_DIR, "market_overview.csv")
DB_PATH = "twse.db"
TABLE = "market_overview"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(NORM_DIR, exist_ok=True)


def _read_csv_safe(path: str, expect_cols=None) -> pd.DataFrame:
    """讀 CSV，不在就回空表；如果只有表頭也視為空表。"""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame(columns=expect_cols or [])
    try:
        df = pd.read_csv(path, dtype=str)
        # 若只有表頭或完全空
        if df.empty or (len(df.columns) == 0):
            return pd.DataFrame(columns=expect_cols or [])
        return df
    except Exception:
        return pd.DataFrame(columns=expect_cols or [])


def _to_num(s):
    try:
        s = str(s).replace(",", "").strip()
        if s == "" or s.lower() == "nan":
            return None
        return float(s)
    except Exception:
        return None


def load_index() -> pd.DataFrame:
    """載入 TAIEX/OTC 的 normalized 指數資料，合併成同一張表。"""
    expect_cols = ["market", "date", "open", "high", "low", "close", "volume", "turnover"]

    taiex = _read_csv_safe(os.path.join(NORM_DIR, "taiex.csv"), expect_cols=expect_cols)
    otc   = _read_csv_safe(os.path.join(NORM_DIR, "otc.csv"),   expect_cols=expect_cols)

    df = pd.concat([taiex, otc], ignore_index=True, sort=False)
    if df.empty:
        return pd.DataFrame(columns=expect_cols)

    # 數值欄位轉型
    for c in ["open", "high", "low", "close", "volume", "turnover"]:
        if c in df.columns:
            df[c] = df[c].map(_to_num)

    # 只留必要欄位
    keep = [c for c in expect_cols if c in df.columns]
    df = df[keep].copy()

    # 清掉完全空白列
    if {"open","high","low","close","volume","turnover"}.issubset(df.columns):
        df = df.dropna(subset=["open","high","low","close"], how="all")

    return df


def load_insti_daily() -> pd.DataFrame:
    """
    載入 normalized 的 T86（insti.csv）並聚合為 market 層級（全市場合計）。
    期待欄位：date, net_foreign, net_invest, net_dealer, net_total
    """
    expect_cols = ["date", "net_foreign", "net_invest", "net_dealer", "net_total"]
    path = os.path.join(NORM_DIR, "insti.csv")
    df = _read_csv_safe(path, expect_cols=expect_cols)

    if df.empty:
        return pd.DataFrame(columns=expect_cols)

    # 確保欄位存在
    for c in expect_cols:
        if c not in df.columns:
            df[c] = 0

    # 數值轉型
    for c in ["net_foreign", "net_invest", "net_dealer", "net_total"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # 以 date 聚合為全市場合計
    grouped = df.groupby("date")[["net_foreign", "net_invest", "net_dealer", "net_total"]].sum().reset_index()
    return grouped


def build_report():
    idx = load_index()        # TAIEX/OTC 指數（market+價量）
    insti = load_insti_daily()  # 三大法人加總（僅 date + net_*）

    # 如果指數兩張都空，仍輸出表頭 CSV，避免 workflow 後續步驟報錯
    if idx.empty:
        cols = ["date","market","open","high","low","close","volume","turnover",
                "net_foreign","net_invest","net_dealer","net_total"]
        pd.DataFrame(columns=cols).to_csv(CSV_OUT, index=False, encoding="utf-8")
        # 也寫進 DB 空表（確保存在）
        with sqlite3.connect(DB_PATH) as conn:
            pd.DataFrame(columns=cols).to_sql(TABLE, conn, if_exists="replace", index=False)
        print("[INFO] index empty -> wrote empty market_overview")
        return

    # left merge（index 左，避免 insti 空導致資料消失）
    out = idx.merge(insti, on="date", how="left")

    # 填 NA → 0（法人欄位）
    for c in ["net_foreign","net_invest","net_dealer","net_total"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype("int64")

    # 欄位順序
    cols = ["date","market","open","high","low","close","volume","turnover",
            "net_foreign","net_invest","net_dealer","net_total"]
    for c in cols:
        if c not in out.columns:
            out[c] = None
    out = out[cols].copy()

    # 依日期與市場排序
    out = out.sort_values(["date","market"], ascending=[True, True])

    # 寫 CSV
    out.to_csv(CSV_OUT, index=False, encoding="utf-8")
    print(f"[OK] wrote {CSV_OUT} rows={len(out)}")

    # 寫 SQLite（全量覆寫）
    with sqlite3.connect(DB_PATH) as conn:
        out.to_sql(TABLE, conn, if_exists="replace", index=False)
        # 可選的索引（加速查詢）
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_market_overview_date ON market_overview(date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_market_overview_mkt_date ON market_overview(market, date)")
        except Exception:
            pass
    print(f"[OK] wrote table {TABLE} into {DB_PATH}, rows={len(out)}")


def main():
    build_report()


if __name__ == "__main__":
    main()
