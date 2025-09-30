import os, sqlite3
import pandas as pd
from store_sqlite import save_sqlite

def save_csv(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")

def save(df: pd.DataFrame, storage: str, out_root: str, name: str):
    if storage == "sqlite":
        db = os.path.join(out_root, "twse.db")
        save_sqlite(df, db, name)
    else:
        # default csv
        csv_path = os.path.join(out_root, "normalized", f"{name}.csv")
        save_csv(df, csv_path)
        return csv_path
