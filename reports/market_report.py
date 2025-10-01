# reports/market_report.py
# 合併 TAIEX / OTC 指數 + 三大法人淨買賣，輸出 data/reports/market_overview.csv
import os
import pandas as pd

BASE_NORM = "data/normalized"
OUT_DIR = "data/reports"
OUT_FILE = os.path.join(OUT_DIR, "market_overview.csv")

def _safe_read_csv(path: str) -> pd.DataFrame:
    if os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            return pd.read_csv(path)
        except Exception as e:
            print(f"[WARN] failed to read {path}: {e}")
    return pd.DataFrame()

def _keep_cols(df: pd.DataFrame, need):
    cols = [c for c in need if c in df.columns]
    return df[cols].copy() if cols else pd.DataFrame(columns=need)

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    taiex = _safe_read_csv(os.path.join(BASE_NORM, "taiex.csv"))
    otc   = _safe_read_csv(os.path.join(BASE_NORM, "otc.csv"))
    insti = _safe_read_csv(os.path.join(BASE_NORM, "insti.csv"))

    # 統一欄位：日線常用欄
    price_cols = ["date", "open", "high", "low", "close", "volume", "turnover"]

    frames = []
    if not taiex.empty:
        taiex = _keep_cols(taiex, price_cols)
        taiex["market"] = "TAIEX"
        frames.append(taiex)

    if not otc.empty:
        otc = _keep_cols(otc, price_cols)
        otc["market"] = "OTC"
        frames.append(otc)

    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=price_cols + ["market"])

    # 三大法人：按日彙總
    # 欄位: date, net_foreign, net_invest, net_dealer, net_total
    if not insti.empty and not merged.empty:
        for c in ["net_foreign","net_invest","net_dealer","net_total"]:
            if c in insti.columns:
                insti[c] = pd.to_numeric(insti[c], errors="coerce")
        insti["date"] = pd.to_datetime(insti["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        g = insti.groupby("date")[["net_foreign","net_invest","net_dealer","net_total"]].sum(min_count=1)
        g = g.reset_index()

        merged["date"] = pd.to_datetime(merged["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        merged = merged.merge(g, on="date", how="left")

    # 排序、輸出
    if "date" in merged.columns:
        merged = merged.sort_values(["date","market"]).reset_index(drop=True)

    merged.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")
    print(f"[OK] market report -> {OUT_FILE}, rows={len(merged)}")

if __name__ == "__main__":
    main()
