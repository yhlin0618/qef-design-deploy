# Period Comparison Module

## 概述

Period Comparison 模組提供時間段比較功能，解決 ISSUE_118「缺少上期相比功能」的需求。此模組支援多種時間粒度的比較，包括環比（與上期比較）、同比（與去年同期比較）和自訂期間比較。

## 功能特性

- **多時間粒度支援**：日、週、月、季、年
- **靈活比較模式**：
  - 環比（Period over Period）：與上一個時間段比較
  - 同比（Year over Year）：與去年同期比較
  - 自訂比較：可指定往回滾動的期數
- **視覺化呈現**：
  - 資訊卡片顯示當期、上期和變化率
  - 詳細比較表格
  - 趨勢圖表
- **自動計算**：
  - 差異值（absolute difference）
  - 變化率（percentage change）
  - 趨勢指示（上升/下降/持平）

## 檔案結構

```
periodComparison/
├── periodComparisonUI.R       # UI 元件
├── periodComparisonServer.R   # Server 邏輯
├── periodComparisonDefaults.R # 預設設定
└── README.md                   # 本文件
```

相關工具函數：
```
04_utils/
└── fn_calculate_period_comparison.R  # 計算工具函數
```

## 使用方式

### 基本整合

```r
# 在主應用的 UI 中
ui <- fluidPage(
  periodComparisonUI("sales_comparison")
)

# 在主應用的 Server 中
server <- function(input, output, session) {
  # 假設已有資料庫連線 db_conn

  comparison_result <- periodComparisonServer(
    id = "sales_comparison",
    db_connection = db_conn,
    data_source = "sales_data",
    metrics = c("revenue", "customers", "orders"),
    group_by = "region"  # 可選：按區域分組
  )
}
```

### 進階配置

```r
# 使用自訂預設值
defaults <- periodComparisonDefaults()
defaults$default_period_type <- "weekly"
defaults$chart_colors <- c("#FF0000", "#00FF00", "#0000FF")

# 將預設值傳遞給模組
comparison_result <- periodComparisonServer(
  id = "custom_comparison",
  db_connection = db_conn,
  data_source = "custom_data",
  metrics = c("sales", "profit"),
  defaults = defaults
)
```

## 資料需求

輸入資料表必須包含以下欄位：

- `date`：日期欄位（Date 或 DateTime 格式）
- 一個或多個數值欄位用於比較（如 revenue、customers、orders）

範例資料結構：
```r
data.frame(
  date = as.Date("2024-01-01"),
  revenue = 10000,
  customers = 100,
  orders = 150
)
```

## 計算邏輯

### 環比計算
```r
# 與上一個時間段比較
previous_value <- lag(current_value, n = 1)
difference <- current_value - previous_value
change_rate <- difference / previous_value
```

### 同比計算
```r
# 與去年同期比較
# 月度數據：往前 12 個月
# 季度數據：往前 4 個季度
# 年度數據：往前 1 年
previous_value <- lag(current_value, n = periods_in_year)
```

### 變化率上限
為避免極端值影響顯示，變化率預設上限為 150%（可調整）：
```r
capped_rate <- min(change_rate, 1.5)
```

## 與 kitchenMAMA 的差異

本實作參考了 kitchenMAMA 的時間比較邏輯，但進行了以下改進：

1. **模組化設計**：獨立的 UI-Server-Defaults 三元組（符合 R09 原則）
2. **更靈活的配置**：支援自訂時間粒度和比較方式
3. **更好的效能**：使用向量化運算（符合 MP30 原則）
4. **國際化支援**：完整的繁體中文介面
5. **響應式更新**：使用 reactive 確保資料即時更新

## 測試

執行測試套件：
```r
source("scripts/global_scripts/98_test/test_period_comparison.R")
```

執行互動式測試應用：
```r
# 在 R 互動環境中
source("scripts/global_scripts/98_test/test_period_comparison.R")
# 測試應用會自動啟動
```

## 效能考量

- 大數據集會自動進行聚合以提升效能
- 使用 `tbl2()` 進行資料庫查詢優化（符合 R92 原則）
- 計算結果會快取，避免重複計算

## 相關 Issues

- ISSUE_118: 缺少上期相比功能（本模組解決）
- ISSUE_120: 相關的指標缺失問題

## 維護者

Created: 2025-09-23
Principle Explorer for MAMBA Enterprise Platform

## 授權

內部使用，遵循 MAMBA 專案授權條款