# reports/market_report.py
import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")
OUTPUT = DATA_DIR / "market_overview.csv"

def main():
    taiex_file = DATA_DIR / "taiex.csv"
    otc_file = DATA_DIR / "otc.csv"
    insti_file = DATA_DIR / "insti.csv"

    if not taiex_file.exists() or not otc_file.exists() or not insti_file.exists():
        print("⚠️ 缺少必要的輸入檔案，無法產生 market_overview")
        return

    # 載入大盤指數（上市/上櫃）
    taiex = pd.read_csv(taiex_file)
    otc = pd.read_csv(otc_file)

    # 統一欄位名稱
    taiex["market"] = "TAIEX"
    otc["market"] = "OTC"

    # 載入法人資料
    insti = pd.read_csv(insti_file)

    # 合併（這裡直接用日期 + 市場對齊）
    merged = pd.concat([taiex, otc], ignore_index=True)

    # 左連法人流向
    merged = merged.merge(insti, on=["date", "market"], how="left")

    # 若法人欄位不存在，就補 0
    for col in ["net_foreign", "net_invest", "net_dealer"]:
        if col not in merged.columns:
            merged[col] = 0

    merged["net_total"] = merged["net_foreign"] + merged["net_invest"] + merged["net_dealer"]

    # 排序
    merged = merged.sort_values(["date", "market"])

    # 輸出
    merged.to_csv(OUTPUT, index=False, encoding="utf-8-sig")
    print(f"✅ 已輸出 {OUTPUT}, 共 {len(merged)} 筆")

if __name__ == "__main__":
    main()
