# reports/to_sql.py
# 將 data/reports/market_overview.csv 寫入 SQLite: data/twse.db (表: market_overview)
import os
import sqlite3
import pandas as pd

CSV_PATH = "data/reports/market_overview.csv"
DB_PATH  = "data/twse.db"
TABLE    = "market_overview"

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

INSERT_SQL = f"""
INSERT OR REPLACE INTO {TABLE}
(date, market, open, high, low, close, volume, turnover,
 net_foreign, net_invest, net_dealer, net_total)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
"""

def main():
    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        print(f"[WARN] CSV not found or empty: {CSV_PATH}")
        return

    try:
        df = pd.read_csv(CSV_PATH)
    except Exception as e:
        print(f"[ERROR] failed to read {CSV_PATH}: {e}")
        return

    if df.empty:
        print("[WARN] market_overview.csv is empty, skip DB write")
        return

    # 欄位健檢與轉型
    need_cols = ["date","market","open","high","low","close","volume","turnover",
                 "net_foreign","net_invest","net_dealer","net_total"]
    for c in need_cols:
        if c not in df.columns:
            df[c] = None

    # 日期/數值型態歸一
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for c in ["open","high","low","close","volume","turnover",
              "net_foreign","net_invest","net_dealer","net_total"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 去重（避免重複 append）
    df = df.drop_duplicates(subset=["date","market"], keep="last")

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(DDL)
        conn.commit()

        rows = [tuple(x) for x in df[need_cols].itertuples(index=False, name=None)]
        conn.executemany(INSERT_SQL, rows)
        conn.commit()
        print(f"[OK] upserted {len(rows)} rows into {DB_PATH}#{TABLE}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
