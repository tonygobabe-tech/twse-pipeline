# reports/to_sql.py
import os
import sqlite3
import pandas as pd

DB_PATH = "data/twse.db"
CSV_PATH = "data/market_overview.csv"
TABLE = "market_overview"

# 表格 schema
SCHEMA = """
CREATE TABLE IF NOT EXISTS market_overview (
    date TEXT,
    market TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    turnover REAL,
    net_foreign REAL,
    net_invest REAL,
    net_dealer REAL,
    net_total REAL
);
"""

def main():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(SCHEMA)
    conn.commit()

    # 如果 CSV 有資料就寫入
    if os.path.exists(CSV_PATH):
        try:
            df = pd.read_csv(CSV_PATH)
            if not df.empty:
                df.to_sql(TABLE, conn, if_exists="append", index=False)
                print(f"[OK] inserted {len(df)} rows into {TABLE}")
            else:
                print(f"[WARN] {CSV_PATH} is empty, no rows inserted (table exists).")
        except Exception as e:
            print(f"[ERROR] failed to load {CSV_PATH}: {e}")
    else:
        print(f"[WARN] {CSV_PATH} not found, skipped insert.")

    conn.close()

if __name__ == "__main__":
    main()
