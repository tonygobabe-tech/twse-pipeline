# reports/market_report.py
from pathlib import Path
import pandas as pd

DATA = Path("data")
OUT = DATA / "market_overview.csv"
CACHE_DIR = DATA / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHE_DIR / "market_overview_cache.csv"

COLS_IDX = ["date", "market", "open", "high", "low", "close", "volume", "turnover"]
COLS_NET = ["net_foreign", "net_invest", "net_dealer"]
COLS_ALL = COLS_IDX + COLS_NET + ["net_total"]

def _read_first_nonempty(candidates: list[pd.DataFrame]) -> pd.DataFrame:
    for df in candidates:
        if df is not None and len(df) > 0:
            return df
    return pd.DataFrame(columns=COLS_IDX)

def _read_csv_safe(p: Path, usecols=None) -> pd.DataFrame | None:
    try:
        if not p.exists():
            return None
        return pd.read_csv(p, usecols=usecols)
    except Exception:
        return None

def build_index_df() -> pd.DataFrame:
    # 優先 data/*.csv；如果沒資料再用 data/normalized/*.csv
    taiex = _read_first_nonempty([
        _read_csv_safe(DATA / "taiex.csv"),
        _read_csv_safe(DATA / "normalized" / "taiex.csv"),
    ])
    otc = _read_first_nonempty([
        _read_csv_safe(DATA / "otc.csv"),
        _read_csv_safe(DATA / "normalized" / "otc.csv"),
    ])

    if "market" not in taiex.columns and len(taiex):
        taiex["market"] = "TAIEX"
    if "market" not in otc.columns and len(otc):
        otc["market"] = "OTC"

    taiex = taiex.reindex(columns=COLS_IDX).dropna(how="all")
    otc   = otc.reindex(columns=COLS_IDX).dropna(how="all")

    idx_df = pd.concat([taiex, otc], ignore_index=True)
    return idx_df

def build_insti_df() -> pd.DataFrame:
    # 讀取法人（T86 聚合到全市場；與市場非對齊，作為參考值）
    insti = _read_first_nonempty([
        _read_csv_safe(DATA / "insti.csv"),
        _read_csv_safe(DATA / "normalized" / "insti.csv"),
    ])
    if len(insti) == 0:
        return pd.DataFrame(columns=["date"] + COLS_NET)

    # 兼容可能不同欄名
    rename = {}
    for k in insti.columns:
        kk = k.strip().lower()
        if "net_foreign" in kk or "外" in k:
            rename[k] = "net_foreign"
        elif "net_invest" in kk or "投" in k:
            rename[k] = "net_invest"
        elif "net_dealer" in kk or "自營商" in k:
            rename[k] = "net_dealer"
    if rename:
        insti = insti.rename(columns=rename)

    for c in COLS_NET:
        if c not in insti.columns:
            insti[c] = 0

    if "date" not in insti.columns:
        return pd.DataFrame(columns=["date"] + COLS_NET)

    insti_agg = (
        insti.groupby("date", as_index=False)[COLS_NET].sum()
        .sort_values("date")
        .reset_index(drop=True)
    )
    return insti_agg

def main():
    idx_df = build_index_df()
    insti_agg = build_insti_df()

    # 若兩邊都空 → 試著用快取
    if len(idx_df) == 0:
        if CACHE_FILE.exists():
            cache = pd.read_csv(CACHE_FILE)
            cache.to_csv(OUT, index=False, encoding="utf-8-sig")
            print(f"⚠️ 指數來源皆為空，使用快取：{CACHE_FILE} → {OUT}（{len(cache)} 筆）")
            return
        else:
            # 沒有任何可用資料
            pd.DataFrame(columns=COLS_ALL).to_csv(OUT, index=False, encoding="utf-8-sig")
            print("⚠️ 指數來源皆為空，且無快取；輸出空檔")
            return

    # 合併法人（用日期 join；同一天 TAIEX/OTC 會帶入相同 net_* 參考值）
    merged = idx_df.copy()
    if len(insti_agg) > 0:
        merged = merged.merge(insti_agg, on="date", how="left")
    else:
        for c in COLS_NET:
            merged[c] = 0

    for c in COLS_NET:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)

    merged["net_total"] = merged["net_foreign"] + merged["net_invest"] + merged["net_dealer"]
    merged = merged.reindex(columns=COLS_ALL).sort_values(["date", "market"])

    # 寫出 + 更新快取（僅在非空時）
    merged.to_csv(OUT, index=False, encoding="utf-8-sig")
    if len(merged) > 0:
        merged.to_csv(CACHE_FILE, index=False, encoding="utf-8-sig")
        print(f"✅ 產出報表 {OUT}（{len(merged)} 筆），並更新快取 {CACHE_FILE}")
    else:
        print("⚠️ 產出為空，快取不更新")

if __name__ == "__main__":
    main()

