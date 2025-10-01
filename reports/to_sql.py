# reports/to_sql.py
from pathlib import Path
import sqlite3
import pandas as pd

DB = Path("data") / "twse.db"
CSV = Path("data") / "market_overview.csv"

DDL = """
CREATE TABLE IF NOT EXISTS market_overview (
  date TEXT NOT NULL,
  market TEXT NOT NULL,
  open REAL, high REAL, low REAL, close REAL,
  volume REAL, turnover REAL,
  net_foreign REAL, net_invest REAL, net_dealer REAL, net_total REAL,
  PRIMARY KEY (date, market)
);
"""

def main():
    if not CSV.exists():
        print("⚠️ market_overview.csv 不存在，跳過寫入")
        return

    df = pd.read_csv(CSV)
    if df.empty:
        print("⚠️ market_overview.csv 為空，跳過寫入")
        return

    with sqlite3.connect(DB) as con:
        con.execute(DDL)

        # 逐筆 upsert（避免重複），速度也足夠
        df.to_sql("_tmp_market_overview", con, if_exists="replace", index=False)
        con.execute("""
        INSERT OR REPLACE INTO market_overview
        SELECT * FROM _tmp_market_overview;
        """)
        con.execute("DROP TABLE _tmp_market_overview;")
        con.commit()

    print(f"✅ 已寫入 SQLite：{DB} → market_overview（{len(df)} 筆）")

if __name__ == "__main__":
    main()
