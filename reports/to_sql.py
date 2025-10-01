# reports/to_sql.py
import os
import sqlite3
import pandas as pd

DB_PATH = "data/twse.db"
CSV_PATH = "data/market_overview.csv"
TABLE = "market_overview"

def main():
    if not os.path.exists(CSV_PATH):
        print(f"[WARN] CSV not found: {CSV_PATH}")
        return

    # 嘗試讀取 CSV
    try:
        df = pd.read_csv(CSV_PATH)
    except Exception as e:
        print(f"[ERROR] Failed to read CSV: {e}")
        return

    # 確認有資料
    if df.empty:
        print(f"[WARN] CSV is empty: {CSV_PATH}")
        return

    # 補齊欄位（避免 SQLite 建表缺欄位）
    expected_cols = [
        "date", "market", "open", "high", "low", "close",
        "volume", "turnover", "net_foreign", "net_invest",
        "net_dealer", "net_total"
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    df = df[expected_cols]

    # 寫進 SQLite
    conn = sqlite3.connect(DB_PATH)
    try:
        df.to_sql(TABLE, conn, if_exists="append", index=False)
        print(f"[OK] Appended {len(df)} rows into {TABLE} at {DB_PATH}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
