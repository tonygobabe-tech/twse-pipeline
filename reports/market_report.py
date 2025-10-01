# reports/market_report.py
# 產出「大盤 + 全市場合計法人流向」的報表
# 輸出：data/reports/market_overview.csv
#
# 欄位：
# date, market, open, high, low, close, volume, turnover,
# net_foreign, net_invest, net_dealer, net_total
#
# 備註：
# - 法人流向為「全市場合計」（當天 T86 全部股票加總），同一日期會併到 TAIEX/OTC 兩筆
# - 之後若要市值加權或分 TWSE/OTC，我們再升級邏輯即可

import os
import pandas as pd

BASE = "data"
NORM = os.path.join(BASE, "normalized")
REPORT_DIR = os.path.join(BASE, "reports")
os.makedirs(REPORT_DIR, exist_ok=True)

def _read_csv(path: str) -> pd.DataFrame:
    try:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return pd.read_csv(path)
    except Exception:
        pass
    return pd.DataFrame()

def _ensure_cols(df: pd.DataFrame, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df

def build_market_overview():
    # 1) 讀兩個指數
    taiex = _read_csv(os.path.join(NORM, "taiex.csv"))
    otc   = _read_csv(os.path.join(NORM, "otc.csv"))

    # 只保留共用欄位並標 market
    keep = ["date","open","high","low","close","volume","turnover"]
    frames = []

    if not taiex.empty:
        taiex = _ensure_cols(taiex, keep)[keep].copy()
        taiex["market"] = "TAIEX"
        frames.append(taiex)

    if not otc.empty:
        otc = _ensure_cols(otc, keep)[keep].copy()
        otc["market"] = "OTC"
        frames.append(otc)

    if frames:
        idx = pd.concat(frames, ignore_index=True)
    else:
        # 沒有任何指數資料 → 仍輸出空殼報表
        cols = keep + ["market","net_foreign","net_invest","net_dealer","net_total"]
        out = pd.DataFrame(columns=cols)
        out.to_csv(os.path.join(REPORT_DIR, "market_overview.csv"), index=False, encoding="utf-8-sig")
        print("[OK] market_overview.csv -> empty (no index rows)")
        return

    # 2) 讀三大法人（T86）並作「全市場合計」
    insti = _read_csv(os.path.join(NORM, "insti.csv"))
    flow_cols = ["net_foreign","net_invest","net_dealer","net_total"]

    if not insti.empty:
        for c in flow_cols + ["date"]:
            if c not in insti.columns:
                insti[c] = 0 if c in flow_cols else None

        # 嘗試把法人數字轉成 int（若為空或格式亂則當 0）
        def _to_int(x):
            try:
                s = str(x).replace(",", "").strip()
                return int(float(s)) if s not in ("", "—", "None", "nan") else 0
            except Exception:
                return 0

        for c in flow_cols:
            insti[c] = insti[c].map(_to_int)

        # 以日期加總（全市場合計）
        flows = (
            insti.groupby("date", dropna=True)[flow_cols]
                 .sum(min_count=1)  # all-NaN -> NaN
                 .reset_index()
        )
    else:
        flows = pd.DataFrame(columns=["date"] + flow_cols)

    # 3) 依日期合併（全市場合計 → 同一天併到兩筆 market）
    out = idx.merge(flows, on="date", how="left")

    # 若法人欄位缺 → 補 0，避免空值影響 downstream
    for c in flow_cols:
        if c not in out.columns:
            out[c] = 0
        out[c] = out[c].fillna(0).astype(int)

    # 排序：日期新到舊、再以 market 排（TAIEX 在前）
    market_order = {"TAIEX": 0, "OTC": 1}
    out["_m"] = out["market"].map(market_order).fillna(9)
    out = out.sort_values(by=["date", "_m"], ascending=[False, True]).drop(columns=["_m"])

    # 4) 輸出
    out_path = os.path.join(REPORT_DIR, "market_overview.csv")
    out.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"[OK] market_overview.csv rows={len(out)} -> {out_path}")

if __name__ == "__main__":
    build_market_overview()
