def save(df: pd.DataFrame, storage: str, out_root: str, name: str):
    # 一次存兩份：SQLite + CSV（無視 storage，兩邊都存）
    from store_sqlite import save_sqlite
    import os
    os.makedirs(out_root, exist_ok=True)

    # 寫 SQLite
    db = os.path.join(out_root, "twse.db")
    save_sqlite(df, db, name)

    # 寫 normalized CSV
    csv_path = os.path.join(out_root, "normalized", f"{name}.csv")
    save_csv(df, csv_path)
    return csv_path
