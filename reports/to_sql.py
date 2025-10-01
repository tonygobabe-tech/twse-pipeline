# reports/to_sql.py  — 強化版：先建表、再有資料才寫入，並避免重複
import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("data/twse.db")
CSV_PATH = Path("data/market_overview.csv")

SCHEMA_SQL = """
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

def ensure_table(conn):
    conn.execute(SCHEMA_SQL)
    conn.commit()
    print("✅ ensured table: market_overview")

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    # 步驟 1）先建表（即使今天沒資料也建立）
    ensure_table(conn)

    # 步驟 2）CSV 存在才讀
    if not CSV_PATH.exists():
        print(f"⚠️ {CSV_PATH} 不存在，今天只建立了空表，不寫入資料。")
        conn.close()
        return

    # 讀 CSV
    try:
        df = pd.read_csv(CSV_PATH)
    except Exception as e:
        print(f"❌ 讀取 {CSV_PATH} 失敗：{e}")
        conn.close()
        return

    if df.empty:
        print("⚠️ market_overview.csv 是空的，今天不寫入任何資料（但表已存在）。")
        conn.close()
        return

    # 標準欄位檢查（少欄位就補 None）
    required_cols = [
        "date","market","open","high","low","close",
        "volume","turnover","net_foreign","net_invest","net_dealer","net_total"
    ]
    for c in required_cols:
        if c not in df.columns:
            df[c] = None
    df = df[required_cols].copy()

    # 先刪舊的（避免重複）：取今天（CSV 內所有日期）與市場，逐一清除舊紀錄
    dates = tuple(sorted(set(df["date"].astype(str).tolist())))
    markets = tuple(sorted(set(df["market"].astype(str).tolist())))
    # 使用 IN 搭配日期+市場刪除
    # 注意：SQLite 不支援 tuple-of-tuples 的複合 IN；改成逐行刪
    cur = conn.cursor()
    for d in dates:
        for m in markets:
            cur.execute("DELETE FROM market_overview WHERE date=? AND market=?", (d, m))
    conn.commit()

    # 寫入（append）
    df.to_sql("market_overview", conn, if_exists="append", index=False)
    print(f"✅ 寫入完成：{len(df)} 筆（日期們：{', '.join(sorted(set(df['date'])))}；市場們：{', '.join(sorted(set(df['market'])))}）")

    conn.close()

if __name__ == "__main__":
    main()

