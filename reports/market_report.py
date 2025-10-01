import pandas as pd
import sqlite3
from pathlib import Path

def main():
    data_dir = Path("data/normalized")

    # === 讀取大盤日行情 (taiex + otc) ===
    taiex = pd.read_csv(data_dir / "taiex.csv")
    otc = pd.read_csv(data_dir / "otc.csv")

    taiex["market"] = "taiex"
    otc["market"] = "otc"
    market = pd.concat([taiex, otc], ignore_index=True)

    # === 讀取法人買賣超 (T86) ===
    insti = pd.read_csv(data_dir / "insti.csv")

    # 修正欄位名稱，對應到程式需要的
    insti = insti.rename(columns={
        "net_foreig": "foreign",   # 拼字錯誤修正
        "net_invest": "invest",
        "net_dealer": "dealer"
    })

    # 聚合成每日「全市場合計」法人買賣超
    grouped = insti.groupby("date")[["foreign", "invest", "dealer"]].sum().reset_index()
    grouped["net_total"] = grouped["foreign"] + grouped["invest"] + grouped["dealer"]

    # === 合併到大盤行情 ===
    market_overview = market.merge(grouped, on="date", how="left")

    # === 存成 CSV ===
    output_csv = Path("data/market_overview.csv")
    market_overview.to_csv(output_csv, index=False, encoding="utf-8-sig")

    # === 存進 SQLite ===
    conn = sqlite3.connect("twse.db")
    market_overview.to_sql("market_overview", conn, if_exists="replace", index=False)
    conn.close()

    print("✅ market_overview 已更新！")

if __name__ == "__main__":
    main()
