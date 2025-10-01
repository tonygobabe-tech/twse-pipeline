# reports/market_report.py
# 產出三份報表：
# - data/reports/market_index.csv      (TAIEX/OTC 合併後的指數時序)
# - data/reports/insti_agg.csv         (三大法人全市場聚合淨買賣)
# - data/reports/market_overview.csv   (指數 + 法人流向 + 技術指標 + 風險燈號)
# - data/reports/market_overview.md    (人類可讀摘要)

import os
import pandas as pd

ROOT = "data"
NORM = os.path.join(ROOT, "normalized")
REPO = os.path.join(ROOT, "reports")
os.makedirs(REPO, exist_ok=True)

def _read_csv_safe(path: str, **kw) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path, **kw)
    except Exception:
        # 有時候空檔案或 BOM：再試一次
        return pd.read_csv(path, engine="python", **kw)

def build_market_index():
    """合併 taiex.csv 與 otc.csv，欄位對齊並加上 market 欄位。"""
    taiex = _read_csv_safe(os.path.join(NORM, "taiex.csv"))
    otc   = _read_csv_safe(os.path.join(NORM, "otc.csv"))

    # 容錯：有些欄位可能不存在，先補齊
    base_cols = ["market","date","open","high","low","close","volume","turnover","is_cached","source_date"]
    for df, mkt in [(taiex, "TAIEX"), (otc, "OTC")]:
        if df.empty:
            continue
        if "market" not in df.columns:
            df["market"] = mkt
        for c in base_cols:
            if c not in df.columns:
                if c in ["is_cached"]:
                    df[c] = 0
                else:
                    df[c] = None

        # 型別與日期規範
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for c in ["open","high","low","close","volume","turnover"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

    all_df = pd.concat([taiex, otc], ignore_index=True).sort_values(["market","date"])
    out = os.path.join(REPO, "market_index.csv")
    all_df.to_csv(out, index=False)
    return all_df

def build_insti_agg():
    """將 insti.csv 聚合到日粒度（全市場合計 & 個別法人）。"""
    insti = _read_csv_safe(os.path.join(NORM, "insti.csv"))
    if insti.empty:
        out = os.path.join(REPO, "insti_agg.csv")
        pd.DataFrame(columns=["date","net_foreign","net_invest","net_dealer","net_total"]).to_csv(out, index=False)
        return pd.DataFrame()

    # 容錯/型別
    insti["date"] = pd.to_datetime(insti["date"], errors="coerce")
    for c in ["net_foreign","net_invest","net_dealer","net_total"]:
        if c not in insti.columns:
            insti[c] = 0
        insti[c] = pd.to_numeric(insti[c], errors="coerce").fillna(0)

    agg = (insti
           .groupby("date", as_index=False)[["net_foreign","net_invest","net_dealer","net_total"]]
           .sum())
    out = os.path.join(REPO, "insti_agg.csv")
    agg.to_csv(out, index=False)
    return agg

def _add_tech(df: pd.DataFrame):
    """加一些簡單技術指標與風險燈號。"""
    if df.empty:
        return df
    df = df.sort_values(["market","date"]).copy()
    # 報酬與均線
    df["ret_1d"] = df.groupby("market")["close"].pct_change()
    df["ma5"]    = df.groupby("market")["close"].transform(lambda s: s.rolling(5).mean())
    df["ma20"]   = df.groupby("market")["close"].transform(lambda s: s.rolling(20).mean())
    df["vol_ma20"] = df.groupby("market")["volume"].transform(lambda s: s.rolling(20).mean())

    # 風險燈號（極簡示例）
    def _signal(row):
        if pd.isna(row.get("close")) or pd.isna(row.get("ma20")):
            return "NEUTRAL"
        # 均線上且外資/總體淨買為正 → 多
        if row["close"] >= row["ma20"] and row.get("net_total", 0) > 0:
            return "BULL"
        # 均線下且外資/總體淨賣為負 → 空
        if row["close"] < row["ma20"] and row.get("net_total", 0) < 0:
            return "BEAR"
        return "NEUTRAL"

    df["signal"] = df.apply(_signal, axis=1)
    return df

def build_overview():
    idx  = build_market_index()
    inst = build_insti_agg()

    if idx.empty:
        out = os.path.join(REPO, "market_overview.csv")
        pd.DataFrame().to_csv(out, index=False)
        open(os.path.join(REPO, "market_overview.md"), "w", encoding="utf-8").write("# Market Overview\n\n(資料不足)\n")
        return

    # 依日期合併（法人是全市場等級，對兩個市場同一日套用同一筆淨流向）
    idx["date"] = pd.to_datetime(idx["date"], errors="coerce")
    merged = idx.merge(inst, on="date", how="left")

    merged = _add_tech(merged)

    out_csv = os.path.join(REPO, "market_overview.csv")
    merged.to_csv(out_csv, index=False)

    # 輕量可讀摘要
    def _fmt(v):
        try:
            return f"{v:,.0f}"
        except Exception:
            return str(v)

    latest = merged.sort_values("date").groupby("market").tail(1)
    lines = ["# Market Overview\n"]
    for _, r in latest.iterrows():
        lines.append(
            f"- **{r['market']}** {r['date'].date()}  "
            f"Close: {_fmt(r.get('close'))}  |  "
            f"MA5/MA20: {_fmt(r.get('ma5'))}/{_fmt(r.get('ma20'))}  |  "
            f"外資/投信/自營商/合計(張): "
            f"{_fmt(r.get('net_foreign'))}/"
            f"{_fmt(r.get('net_invest'))}/"
            f"{_fmt(r.get('net_dealer'))}/"
            f"{_fmt(r.get('net_total'))}  |  "
            f"Signal: {r.get('signal')}"
        )
    md_path = os.path.join(REPO, "market_overview.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

if __name__ == "__main__":
    build_overview()
    print("[OK] built reports -> data/reports/*.csv & .md")
