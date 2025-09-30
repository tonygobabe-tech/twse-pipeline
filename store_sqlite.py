import os, sqlite3
import pandas as pd

def save_sqlite(df: pd.DataFrame, db_path: str, table: str):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql(table, conn, if_exists="append", index=False)
    finally:
        conn.close()
