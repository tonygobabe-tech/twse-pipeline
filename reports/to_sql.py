# reports/to_sql.py
# 將 data/reports/market_overview.csv 寫入 twse.db 的 market_overview 表
# 設計：PRIMARY KEY(date, market)，使用 INSERT OR REPLACE 避免重覆

import os
import sqlite3
import pandas as pd

ROOT = "data"
DB_PATH = os.path.join(ROOT, "twse.db")
CSV_PATH = os.path.join(ROOT, "reports", "market_overview.csv")

def _read_csv_safe(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"[WARN] csv not found: {path}")
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.read_csv(path, engine="python")

def _ensure_schema(conn: sqlite3.Connection):
    # 市場概覽表：指數 + 法人 + 技術指標 + 緩存狀態
    # 注意：date, market 做 PK，利於 upsert
    sql = """
    CREATE TABLE IF NOT EXISTS market_overview (
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
        ret_1d REAL,
        ma5 REAL,
        ma20 REAL,
        vol_ma20 REAL,
        signal TEXT,
        is_cached INTEGER,
        source_date TEXT,
        PRIMARY KEY (date, market)
    );
    """
    conn.execute(sql)
    conn.commit()

def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    # 型別規範，容錯處理
    if df.empty:
        return df
    # 欄位存在即轉型
    num_cols = [
        "open","high","low","close","volume","turnover",
        "net_foreign","net_invest","net_dealer","net_total",
        "ret_1d","ma5","ma20","vol_ma20",
        "is_cached"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "date" in df.columns:
        # 寫 DB 用 TEXT(YYYY-MM-DD)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    # 填空值（避免 NaN 造成 INSERT 失敗）
    for c in df.columns:
        df[c] = df[c].where(pd.notna(df[c]), None)
    return df

def upsert_overview():
    df = _read_csv_safe(CSV_PATH)
    if df.empty:
        print("[WARN] market_overview.csv is empty; skip to_sql")
        return

    df = _coerce_types(df)

    # 僅保留 schema 中有定義的欄位
    keep_cols = [
        "date","market","open","high","low","close","volume","turnover",
        "net_foreign","net_invest","net_dealer","net_total",
        "ret_1d","ma5","ma20","vol_ma20","signal","is_cached","source_date"
    ]
    df = df[[c for c in keep_cols if c in df.columns]].copy()

    # 連線 + 建表
    os.makedirs(ROOT, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_schema(conn)

        # 準備 upsert
        placeholders = ",".join(["?"] * len(keep_cols))
        col_list = ",".join(keep_cols)
        sql = f"""
            INSERT OR REPLACE INTO market_overview ({col_list})
            VALUES ({placeholders})
        """

        rows = [tuple(r.get(c) for c in keep_cols) for _, r in df.iterrows()]
        conn.executemany(sql, rows)
        conn.commit()
        print(f"[OK] upsert {len(rows)} rows into market_overview")
        print(f"[OK] SQLite -> {DB_PATH}")
    finally:
        conn.close()

if __name__ == "__main__":
    upsert_overview()
