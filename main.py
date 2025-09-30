import argparse, os, yaml, sys
from utils import HttpClient
from fetcher import fetch
from normalize import normalize_daily, normalize_basics, normalize_news, normalize_generic
from store import save, save_csv
import pandas as pd

DATASETS = ["daily","monthly","yearly","basics","news","holders"]

def load_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def run_fetch(datasets, cfg):
    client = HttpClient(timeout=cfg.get("timeout_sec",20), retries=cfg.get("retries",3))
    out_root = cfg.get("output_dir","data")
    raw_paths = {}
    for ds in datasets:
        raw_path = fetch(ds, client, out_root)
        print(f"[OK] fetched {ds} -> {raw_path}")
        raw_paths[ds] = raw_path
    return raw_paths

def run_normalize(datasets, cfg):
    out_root = cfg.get("output_dir","data")
    storage = cfg.get("storage","csv")
    for ds in datasets:
        raw_path = os.path.join(out_root, "raw", f"{ds}.json")
        if not os.path.exists(raw_path):
            print(f"[WARN] raw not found: {raw_path}, skip")
            continue
        raw = pd.read_json(raw_path)
        raw = raw.to_dict(orient="records")

        if ds=="daily":
            df = normalize_daily(raw)
        elif ds=="basics":
            df = normalize_basics(raw)
        elif ds=="news":
            df = normalize_news(raw)
        else:
            df = normalize_generic(raw)

        # Save to configured storage
        out = save(df, storage, out_root, ds)
        if out:
            print(f"[OK] normalized {ds} -> {out}")
        else:
            print(f"[OK] normalized {ds} -> saved to SQLite")

        # watchlist 過濾 (only for csv path to keep it simple)
        wl = set([str(x) for x in cfg.get("watchlist", [])])
        if len(wl)>0 and "code" in df.columns:
            wdf = df[df["code"].astype(str).isin(wl)].copy()
            wpath = os.path.join(out_root, "watchlist", f"{ds}.csv")
            save_csv(wdf, wpath)
            print(f"[OK] watchlist filtered {ds} -> {wpath}")

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")

    p_fetch = sub.add_parser("fetch", help="抓取資料集")
    p_fetch.add_argument("datasets", nargs="*", default=["daily"], help="可多選: daily monthly yearly basics news holders")

    p_all = sub.add_parser("fetch-all", help="一鍵抓取全部資料集")

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
