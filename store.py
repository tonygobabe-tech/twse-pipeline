# store.py  —— CSV + SQLite 雙存版（穩定版）
import os
import pandas as pd
from store_sqlite import save_sqlite  # 同目錄下的 store_sqlite.py

def save_csv(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")

def save(df: pd.DataFrame, storage: str, out_root: str, name: str):
    """
    無視 storage，總是同時輸出：
      1) SQLite：data/twse.db 的 <name> 表（append）
      2) CSV   ：data/normalized/<name>.csv
    回傳 CSV 路徑（給呼叫端列印用）
    """
    os.makedirs(out_root, exist_ok=True)

    # 1) 寫入 SQLite
    db = os.path.join(out_root, "twse.db")
    save_sqlite(df, db, name)

    # 2) 寫入 normalized CSV
    csv_path = os.path.join(out_root, "normalized", f"{name}.csv")
    save_csv(df, csv_path)
    return csv_path
