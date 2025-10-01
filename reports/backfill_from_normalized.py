# reports/backfill_from_normalized.py
# 目的：把現有的 normalized/taiex.csv 與 normalized/otc.csv
# 合併成 market_overview.csv 並 upsert 進 data/twse.db 的 market_overview 表

import os
import sqlite3
import pandas as pd

DB_PATH = "data/twse.db"
OUT_CSV = "data/reports/market_overview.csv"
TABLE   = "market_overview"

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  date TEXT NOT NULL,
  market TEXT NOT NULL,
  open REAL,
  high REAL,
  low REAL,
  close REAL,
  volume REAL,
  turnover REAL,
  net_foreign REAL,
  net_invest REAL,
  net_dealer REAL,
  net_total REAL,
  PRIMARY KEY (date, market)
);
"""

def _read_csv_safe(path: str) -> pd.DataFrame:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def main():
    os.makedirs("data/reports", exist_ok=True)

    tai = _read_csv_safe("data/normalized/taiex.csv")
    otc = _read_csv_safe("data/normalized/otc.csv")

    frames = []
    if not tai.empty:
        # 需要欄位：date,open,high,low,close,volume,turnover
        keep = [c for c in ["date","open","high","low","close","volume","turnover"] if c in tai.columns]
        df = tai[keep].copy()
        df["market"] = "TAIEX"
        frames.append(df)

    if not otc.empty:
        keep = [c for c in ["date","open","high","low","close","volume","turnover"] if c in otc.columns]
        df = otc[keep].copy()
        df["market"] = "OTC"
        frames.append(df)

    if not frames:
        print("[WARN] no normalized TAIEX/OTC csv; nothing to backfill.")
        return

    out = pd.concat(frames, ignore_index=True)

    # 清理型別與欄位
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for c in ["open","high","low","close","volume","turnover"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    # 加上法人欄位（目前沒有市場層級的 T86，所以先給空值）
    for c in ["net_foreign","net_invest","net_dealer","net_total"]:
        out[c] = None

    # 欄位順序
    cols = ["date","market","open","high","low","close","volume","turnover",
            "net_foreign","net_invest","net_dealer","net_total"]
    for c in cols:
        if c not in out.columns:
            out[c] = None
    out = out[cols].dropna(subset=["date","market"], how="any").drop_duplicates(subset=["date","market"], keep="last")

    # 1) 產 CSV（覆蓋）
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"[OK] wrote market_overview.csv -> {OUT_CSV}, rows={len(out)}")

    # 2) 寫入 SQLite（upsert）
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(DDL)
        conn.commit()
        rows = [tuple(x) for x in out.itertuples(index=False, name=None)]
        conn.executemany(f"""
            INSERT OR REPLACE INTO {TABLE}
            (date, market, open, high, low, close, volume, turnover,
             net_foreign, net_invest, net_dealer, net_total)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
        conn.commit()
        print(f"[OK] backfilled {len(rows)} rows into {DB_PATH}#{TABLE}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
