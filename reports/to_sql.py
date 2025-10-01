# reports/to_sql.py
# 將 market_overview.csv 寫入 data/twse.db 的 market_overview 表（自動建表、去重、UPSERT）
import os
import sqlite3
import pandas as pd

DB_PATH = "data/twse.db"
CSV_CANDIDATES = [
    "reports/market_overview.csv",  # 主要輸出位置
    "data/market_overview.csv",     # 回填腳本可能寫在 data/
]

TABLE = "market_overview"

SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    date        TEXT    NOT NULL,
    market      TEXT    NOT NULL,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    volume      REAL,
    turnover    REAL,
    net_foreign REAL,
    net_invest  REAL,
    net_dealer  REAL,
    net_total   REAL,
    PRIMARY KEY (date, market)
);
"""

COLS = [
    "date","market","open","high","low","close",
    "volume","turnover","net_foreign","net_invest","net_dealer","net_total",
]

def _find_csv() -> str | None:
    for p in CSV_CANDIDATES:
        if os.path.exists(p) and os.path.isfile(p):
            return p
    return None

def _read_csv(path: str) -> pd.DataFrame:
    try:
        # 用 python 引擎容錯，避免奇怪分隔/空白行
        df = pd.read_csv(path, engine="python")
        # 只有表頭或全空 → 視為空
        if df.empty or df.dropna(how="all").shape[0] == 0:
            return pd.DataFrame(columns=COLS)
        # 只取我們需要的欄位（缺的補 None）
        for c in COLS:
            if c not in df.columns:
                df[c] = None
        return df[COLS]
    except Exception as e:
        print(f"[ERROR] read_csv failed: {e}")
        return pd.DataFrame(columns=COLS)

def _ensure_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(SCHEMA_SQL)
        conn.commit()

def _upsert(df: pd.DataFrame):
    if df.empty:
        print("[WARN] market_overview.csv has no rows (only header or empty) — skip DB write.")
        return

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        sql = f"""
        INSERT INTO {TABLE}
            (date, market, open, high, low, close, volume, turnover,
             net_foreign, net_invest, net_dealer, net_total)
        VALUES
            (:date, :market, :open, :high, :low, :close, :volume, :turnover,
             :net_foreign, :net_invest, :net_dealer, :net_total)
        ON CONFLICT(date, market) DO UPDATE SET
            open=excluded.open,
            high=excluded.high,
            low=excluded.low,
            close=excluded.close,
            volume=excluded.volume,
            turnover=excluded.turnover,
            net_foreign=excluded.net_foreign,
            net_invest=excluded.net_invest,
            net_dealer=excluded.net_dealer,
            net_total=excluded.net_total;
        """
        rows = df.to_dict(orient="records")
        cur.executemany(sql, rows)
        conn.commit()
        print(f"[OK] Upserted {len(rows)} rows into {TABLE} @ {DB_PATH}")

def main():
    csv_path = _find_csv()
    if not csv_path:
        print(f"[WARN] CSV not found: tried {CSV_CANDIDATES} — skip.")
        return

    print(f"[INFO] Using CSV: {csv_path}")
    df = _read_csv(csv_path)
    print(f"[INFO] CSV rows: {len(df)}")

    _ensure_db()
    _upsert(df)

if __name__ == "__main__":
    main()

