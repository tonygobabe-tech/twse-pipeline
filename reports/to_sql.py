# reports/to_sql.py
import os
import sqlite3
import pandas as pd

DB_PATH = os.path.join("data", "twse.db")
CSV_PATH = os.path.join("data", "reports", "market_overview.csv")
TABLE = "market_overview"

SCHEMA_COLS = [
    ("market", "TEXT"), ("date", "TEXT"),
    ("open", "REAL"), ("high", "REAL"), ("low", "REAL"), ("close", "REAL"),
    ("volume", "REAL"), ("turnover", "REAL"),
    ("insti_date", "TEXT"),
    ("net_foreign", "REAL"), ("net_invest", "REAL"),
    ("net_dealer", "REAL"), ("net_total", "REAL"),
]

def ensure_table(conn):
    cols_sql = ", ".join([f"{c} {t}" for c,t in SCHEMA_COLS])
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {cols_sql}
        )
    """)

def append_df(conn, df: pd.DataFrame):
    # 僅保留 schema 定義的欄位
    keep = [c for c,_ in SCHEMA_COLS]
    for c in keep:
        if c not in df.columns: df[c] = None
    df = df[keep]
    df.to_sql(TABLE, conn, if_exists="append", index=False)
    print(f"[OK] appended {len(df)} rows into table {TABLE}")

def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_table(conn)

        if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
            # CSV 不存在或為空檔：仍確保表存在，直接結束
            print(f"[WARN] CSV not found or empty: {CSV_PATH}. Table ensured, no rows appended.")
            return

        try:
            df = pd.read_csv(CSV_PATH)
        except pd.errors.EmptyDataError:
            print(f"[WARN] Empty CSV (no header): {CSV_PATH}. Table ensured, no rows appended.")
            return

        if df.empty:
            print(f"[WARN] CSV has only header (no rows): {CSV_PATH}. Skipping append.")
            return

        append_df(conn, df)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
