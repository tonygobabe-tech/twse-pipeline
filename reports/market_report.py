# reports/market_report.py
import pandas as pd
import os

DATA_DIR = "data/normalized"
OUTPUT = "data/market_overview.csv"

def main():
    taiex_path = os.path.join(DATA_DIR, "taiex.csv")
    otc_path = os.path.join(DATA_DIR, "otc.csv")
    insti_path = os.path.join(DATA_DIR, "insti.csv")

    if not (os.path.exists(taiex_path) and os.path.exists(otc_path)):
        print("[ERROR] missing taiex/otc files, cannot build report")
        return

    # 讀取大盤指數
    taiex = pd.read_csv(taiex_path)
    otc = pd.read_csv(otc_path)

    taiex["market"] = "TAIEX"
    otc["market"] = "OTC"

    # 合併兩個市場
    df = pd.concat([taiex, otc], ignore_index=True)

    # 預設法人流向欄位
    df["net_foreign"] = 0
    df["net_invest"] = 0
    df["net_dealer"] = 0
    df["net_total"] = 0

    # 如果有法人資料
    if os.path.exists(insti_path):
        insti = pd.read_csv(insti_path)
        # 假設 insti.csv 有欄位：date, foreign, invest, dealer
        grouped = insti.groupby("date")[["foreign", "invest", "dealer"]].sum().reset_index()
        grouped["net_total"] = grouped["foreign"] + grouped["invest"] + grouped["dealer"]

        # 左合併 (TAIEX/OTC 各一筆，法人流向相同套上去)
        df = df.merge(grouped, how="left", left_on="date", right_on="date")

        # 覆蓋欄位
        df["net_foreign"] = df["foreign"].fillna(0)
        df["net_invest"] = df["invest"].fillna(0)
        df["net_dealer"] = df["dealer"].fillna(0)
        df["net_total"] = df["net_total"].fillna(0)

        # 丟掉多餘欄位
        df = df.drop(columns=["foreign", "invest", "dealer"])

    # 輸出欄位順序固定
    cols = [
        "date", "market", "open", "high", "low", "close",
        "volume", "turnover", "net_foreign", "net_invest",
        "net_dealer", "net_total"
    ]
    df = df[cols]

    # 寫出報表
    df.to_csv(OUTPUT, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved market overview -> {OUTPUT}, rows={len(df)}")

if __name__ == "__main__":
    main()
