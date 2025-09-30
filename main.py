# main.py — 加入「大盤指數 TAIEX / OTC」，並保留 insti（三大法人）
# 變更標記：# NEW / # CHG

import argparse, os, yaml, sys, json
from utils import HttpClient
from fetcher import fetch as fetch_openapi
from normalize import (
    normalize_daily, normalize_basics, normalize_news, normalize_generic,
    normalize_insti,
    normalize_taiex,            # NEW
    normalize_otc               # NEW
)
from store import save, save_csv
from insti import fetch_insti
from index_fetch import fetch_taiex, fetch_otc   # NEW
import pandas as pd

# CHG: 擴充資料集清單
DATASETS = ["daily","monthly","yearly","basics","news","holders","insti","taiex","otc"]  # CHG

def load_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def run_fetch(datasets, cfg):
    client = HttpClient(timeout=cfg.get("timeout_sec",20), retries=cfg.get("retries",3))
    out_root = cfg.get("output_dir","data")
    raw_paths = {}
    for ds in datasets:
        # NEW: 依資料集呼叫不同抓取器
        if ds == "insti":
            raw_path, the_date = fetch_insti(client, out_root)
        elif ds == "taiex":   # NEW
            raw_path, the_date = fetch_taiex(client, out_root)
        elif ds == "otc":     # NEW
            raw_path, the_date = fetch_otc(client, out_root)
        else:
            raw_path = fetch_openapi(ds, client, out_root)

        print(f"[OK] fetched {ds} -> {raw_path}")
        raw_paths[ds] = raw_path
    return raw_paths

def run_normalize(datasets, cfg):
    out_root = cfg.get("output_dir","data")
    storage = cfg.get("storage","csv")
    for ds in datasets:
        # CHG: insti/taiex/otc 的 raw 檔名都帶日期，要從 raw 資料夾找「最新一個」
        if ds in ["insti","taiex","otc"]:   # CHG
            raw_dir = os.path.join(out_root, "raw")
            prefix = {"insti":"insti_", "taiex":"taiex_", "otc":"otc_"}[ds]
            candidates = sorted([p for p in os.listdir(raw_dir) if p.startswith(prefix)], reverse=True)
            if not candidates:
                print(f"[WARN] no raw {ds} file found, skip")
                continue
            raw_path = os.path.join(raw_dir, candidates[0])
        else:
            raw_path = os.path.join(out_root, "raw", f"{ds}.json")

        if not os.path.exists(raw_path):
            print(f"[WARN] raw not found: {raw_path}, skip")
            continue

        with open(raw_path, "r", encoding="utf-8") as f:
            raw_obj = json.load(f)

        # NEW: 指數的 normalize
        if ds == "taiex":
            df = normalize_taiex(raw_obj)
        elif ds == "otc":
            df = normalize_otc(raw_obj)
        elif ds == "daily":
            df = normalize_daily(raw_obj)
        elif ds == "basics":
            df = normalize_basics(raw_obj)
        elif ds == "news":
            df = normalize_news(raw_obj)
        elif ds == "insti":
            df = normalize_insti(raw_obj)
        else:
            df = normalize_generic(raw_obj)

        out = save(df, storage, out_root, ds)
        if out:
            print(f"[OK] normalized {ds} -> {out}")
        else:
            print(f"[OK] normalized {ds} -> saved to SQLite")

        # watchlist 過濾（僅對含 code 欄位的表）
        wl = set([str(x) for x in cfg.get("watchlist", [])])
        if len(wl) > 0 and "code" in df.columns:
            wdf = df[df["code"].astype(str).isin(wl)].copy()
            wpath = os.path.join(out_root, "watchlist", f"{ds}.csv")
            save_csv(wdf, wpath)
            print(f"[OK] watchlist filtered {ds} -> {wpath}")

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")

    p_fetch = sub.add_parser("fetch", help="抓取資料集")
    p_fetch.add_argument(
        "datasets",
        nargs="*",
        default=["daily"],
        help="可多選: daily monthly yearly basics news holders insti taiex otc"  # CHG
    )

    sub.add_parser("fetch-all", help="一鍵抓取全部資料集")

    args = parser.parse_args()
    cfg = load_config()

    if args.cmd == "fetch":
        ds = [d for d in args.datasets if d in DATASETS]
        if not ds:
            print("No valid dataset specified.")
            sys.exit(1)
        run_fetch(ds, cfg)
        run_normalize(ds, cfg)
    elif args.cmd == "fetch-all":
        run_fetch(DATASETS, cfg)
        run_normalize(DATASETS, cfg)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()


