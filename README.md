# TWSE OpenAPI Example (可直接落地的骨架)

這是一個**可執行的 Python 範例專案**，示範如何以「批次」抓取台灣證券交易所 TWSE OpenAPI（盤後資料為主），並將資料清洗、存成 CSV / SQLite。

> 對齊你的使用情境：先抓**全市場**彙整（ALL 端點），在本地以**watchlist**或條件篩出關注標的（例如 2330、0050），再與其他來源（例：三大法人網站報表）做 Join。

---

## 支援資料集（預設）
- 個股**日成交**（全市場）：`/v1/exchangeReport/STOCK_DAY_ALL`
- 個股**月成交**（全市場）：`/v1/exchangeReport/FMSRFK_ALL`
- 個股**年成交**（全市場）：`/v1/exchangeReport/FMNPTK_ALL`
- 上市公司**基本資料**：`/v1/opendata/t187ap02_L`
- 上市公司**重大訊息（每日）**：`/v1/opendata/t187ap03_L`
- 上市公司**持股逾 10% 大股東**：`/v1/opendata/t187ap14_L`

> 備註：**三大法人買賣/金額統計**、**盤中逐筆**不在 OpenAPI，請改抓 TWSE 官方網站報表 API 或行情供應商 API。

---

## 快速開始

### 1) 安裝依賴
```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2) 設定觀察清單
在 `config.yaml` 修改：
```yaml
watchlist:
  - "2330"   # 台積電
  - "0050"   # 元大台灣50
output_dir: "data"
storage: "csv"  # csv 或 sqlite
```

### 3) 一鍵抓取
```bash
python main.py fetch-all
```

### 4) 只抓單一資料集
```bash
python main.py fetch daily   # STOCK_DAY_ALL
python main.py fetch monthly # FMSRFK_ALL
python main.py fetch yearly  # FMNPTK_ALL
python main.py fetch basics  # t187ap02_L
python main.py fetch news    # t187ap03_L
python main.py fetch holders # t187ap14_L
```

### 5) 產出（預設）
- `data/raw/*.json`：原始 API 回傳（保留觀測）
- `data/normalized/*.csv`：清洗後標準欄位
- `data/watchlist/*.csv`：僅保留 watchlist 之標的

---

## 自動化排程（範例）

### Linux (cron)
```cron
# 每日 18:05 抓日成交與重大訊息
5 18 * * * /usr/bin/bash -lc 'cd /path/to/twse_openapi_example && . .venv/bin/activate && python main.py fetch daily news'

# 每月 1 號 06:00 抓月度資料
0 6 1 * * /usr/bin/bash -lc 'cd /path/to/twse_openapi_example && . .venv/bin/activate && python main.py fetch monthly'

# 每年 1 月 2 日 06:30 抓年度資料
30 6 2 1 * /usr/bin/bash -lc 'cd /path/to/twse_openapi_example && . .venv/bin/activate && python main.py fetch yearly'
```

### GitHub Actions（節錄）
```yaml
name: TWSE Pipeline
on:
  schedule:
    - cron: "5 10 * * *"  # UTC 10:05 -> 台北 18:05
jobs:
  run:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r requirements.txt
      - run: python main.py fetch daily news
      - uses: actions/upload-artifact@v4
        with: { name: data, path: data }
```

---

## 欄位說明（簡化）
清洗後欄位盡量標準化為：
- `code`（證券代碼）、`name`（名稱）
- `date`（YYYY-MM-DD）
- `open`, `high`, `low`, `close`, `volume`, `turnover`（金額）
- 其他欄位依各資料集補充（如產業別、重大訊息標題等）

> 實務上 TWSE 回傳欄位名稱可能為中文、數字含逗號，已於 normalize 階段處理。

---

## 重要聲明
- 本範例僅示範**盤後公開資料**抓取，請遵守 TWSE/證交所使用規範。
- 若需**即時行情或盤中逐筆**，請改接有授權的行情商/券商 API。
- 若要商業使用，請自行評估法遵、版權與流量限制。


---

## 進階：改用 SQLite 儲存
在 `config.yaml` 設定：
```yaml
storage: "sqlite"
```
輸出會寫入 `data/twse.db`，每個資料集對應一張表（`daily`, `monthly`, `yearly`, `basics`, `news`, `holders`）。

---

## 一鍵執行腳本

### Windows
雙擊執行：`scripts\run_daily.bat`
```bat
@echo off
cd /d %~dp0\..
python -m venv .venv
call .venv\Scripts\activate
pip install -r requirements.txt
python main.py fetch daily news
```

### macOS / Linux
```bash
bash scripts/run_daily.sh
```
`run_daily.sh` 內容：
```bash
#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py fetch daily news
```

---

## GitHub Actions（已附工作流程檔）
`.github/workflows/pipeline.yml` 會在台北時間每日 18:05 自動跑 `daily + news`，並把 `data/` 存成 artifact。
