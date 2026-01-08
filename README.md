# Financial Fraud Data Pipeline

## 專案簡介

本專案使用 **Credit Card Fraud Detection** 資料集，建立一條由原始交易資料到分析層資料的完整資料處理管線。  
透過分層式資料架構，將原始資料轉換為可直接用於分析與風險決策的結構化資料表，並以 Parquet 格式儲存。
此專案使用 DuckDB 直接對 Parquet 檔案執行分析型 SQL 查詢，無需建立或維護傳統資料庫。


目前專案已完成以下資料層級：

Bronze → Silver → Gold
本專案已進一步完成 Feature Engineering、基準模型訓練，以及使用 DuckDB 對 Parquet 檔案進行 SQL 分析，完整呈現從資料處理到分析與風險決策支援的流程。


---

## 專案架構

financial-data-pipeline/

├─ data/

│ ├─ raw/ # 原始 CSV 資料（不納入版本控制）

│ ├─ silver/ # 清洗後的事實表與維度表（Parquet）
│ ├─ gold/ # 分析與決策用彙總資料（Parquet）
│ └─ feature/ # 模型訓練用特徵資料（Parquet）
├─ scripts/
│ ├─ bronze_to_silver.py
│ ├─ silver_to_gold.py
│ ├─ silver_to_feature.py
│ ├─ feature_to_model.py
│ └─ gold_sql_analysis.py
└─ README.md

---

## 資料分層說明

### Bronze Layer（原始資料層）

Bronze layer 用於保存原始交易資料，不進行任何清洗或轉換，確保資料可回溯性。

特點：
- 原始 CSV 格式
- 不修改資料內容
- 不納入 Git 版本控制

---

### Silver Layer（清洗與結構化資料層）

Silver layer 將原始交易資料轉換為乾淨且結構一致的資料表，作為後續分析與彙總的基礎。

處理內容：
- 欄位標準化與重新命名
- 拆分為事實表與維度表
- 統一資料型別
- 移除不必要欄位

輸出資料：
- `transactions.parquet`：交易事實表
- `customers.parquet`：客戶維度表

對應程式：
- `scripts/bronze_to_silver.py`

Silver layer 完成後即進行版本控管，作為穩定的資料基礎。

---

### Gold Layer（分析層）

Gold layer 專注於分析與決策需求，不再進行資料清洗，而是透過彙總與指標計算，產生可直接使用的分析資料表。

主要輸出表如下：

- `country_kpi.parquet`  
  - 各國交易筆數  
  - 總交易金額  
  - 詐欺率（fraud rate）

- `risk_level_kpi.parquet`  
  - 各風險等級的交易行為  
  - 平均交易金額  
  - 詐欺率

- `customer_summary.parquet`  
  - 每位客戶的交易次數  
  - 累積交易金額  
  - 個人詐欺率

對應程式：
- `scripts/silver_to_gold.py`

Gold layer 可直接用於 SQL 查詢、BI 視覺化工具，或作為後續特徵工程的輸入來源。

## Feature Engineering

在完成 Gold layer 後，本專案進一步建立 Feature layer，將交易資料與客戶資料整合為可直接用於模型訓練的數值型特徵表。

處理內容：
- 合併交易事實表與客戶維度表
- 類別欄位進行 one-hot encoding
- 移除識別用欄位（如 ID）
- 保留詐欺標籤作為模型目標變數

輸出資料：
- `features.parquet`

對應程式：
- `scripts/silver_to_feature.py`
## Baseline Fraud Detection Model

本專案使用 Logistic Regression 作為基準模型，以驗證特徵資料是否具有詐欺預測能力。

模型設計重點：
- 使用數值化後的特徵資料進行訓練
- 考量詐欺資料高度不平衡，採用 class weight 調整
- 以 precision、recall 與 ROC-AUC 作為主要評估指標

此模型主要用於驗證資料與特徵工程的有效性，而非追求模型效能最佳化。

對應程式：
- `scripts/feature_to_model.py`

## SQL 分析（DuckDB）

本專案使用 **DuckDB** 直接對 Parquet 檔案執行分析型 SQL 查詢，無需建立或維護傳統關聯式資料庫。  
此作法可進行輕量、高效率的資料分析，符合現代資料分析與 analytics engineering 的實務做法。

### 為何選擇 DuckDB
- 可直接使用 SQL 查詢 Parquet 檔案
- 無需資料庫伺服器或 schema 建立流程
- 適合分析與探索性資料處理（analytical / exploratory workloads）
- SQL 查詢邏輯可直接移植至 MySQL、PostgreSQL 等系統

### SQL 查詢範例

**各國詐欺率比較**
```sql
SELECT
  country,
  total_transactions,
  total_amount,
  fraud_rate
FROM country_kpi
ORDER BY fraud_rate DESC;
不同風險等級的詐欺表現
SELECT
  risk_level,
  total_transactions,
  avg_transaction_amount,
  fraud_rate
FROM risk_level_kpi
ORDER BY fraud_rate DESC;
高風險客戶篩選
SELECT
  cust_id,
  total_transactions,
  total_amount,
  fraud_rate
FROM customer_summary
WHERE fraud_rate > 0.2
ORDER BY fraud_rate DESC
LIMIT 10;
上述查詢示範如何透過 SQL 直接對分析層 Parquet 資料進行查詢，以支援詐欺分析與風險決策。




## 設計重點

- 採用 Bronze / Silver / Gold 分層資料架構
- 清楚區分資料清洗、分析與決策用途
- 使用 Parquet 作為處理後資料格式
- 每一層資料完成後即進行 Git 版本控管
- 架構可無縫延伸至 Feature Engineering 與模型訓練

---

## 目前進度



✓ Bronze layer completed  
✓ Silver layer finalized and versioned  
✓ Gold layer finalized and versioned  
✓ Feature engineering completed  
✓ Baseline model training completed  
✓ SQL analytics on Parquet using DuckDB completed

---

## 資料集來源

- Credit Card Fraud Detection Dataset


