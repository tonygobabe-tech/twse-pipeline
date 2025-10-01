import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("data/twse.db")
CSV_PATH = Path("data/market_overview.csv")

def main():
    if not CSV_PATH.exists():
        print(f"❌ {CSV_PATH} 不存在，跳過")
        return

    # 讀取 CSV
    df = pd.read_csv(CSV_PATH)

    # 如果空的就跳過
    if df.empty:
        print("⚠️ market_overview.csv 是空的，沒有資料寫入")
        return

    # 連線 SQLite
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 建表（確保存在）
    cursor.execute("""
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
    )
    """)

    # 寫入（append 模式）
    df.to_sql("market_overview", conn, if_exists="append", index=False)

    conn.commit()
    conn.close()
    print(f"✅ 已寫入 {len(df)} 筆到 market_overview")

if __name__ == "__main__":
    main()
