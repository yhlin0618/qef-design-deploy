# microMacroKPI 計算方法說明

## 組件功能概述

`microMacroKPI` 是一個客戶DNA宏觀指標監控組件，透過9個關鍵績效指標（KPI）提供客戶價值的全面性儀表板視圖。

## 資料來源與計算優化

- **資料表**: `df_dna_by_customer`
- **連接方式**: 使用 `tbl2()` 函數進行增強式資料存取 (R116原則)
- **篩選邏輯**: 支援平台篩選，但不支援產品線篩選（客戶DNA資料為跨產品線聚合）
- **計算優化**: 使用 DuckDB SQL 聚合計算，避免將原始資料載入 R 記憶體

## 9個KPI指標計算方法

### 第一排：客戶價值指標

#### 1. 金額價值 (Monetary Value - RFM-M)
- **資料欄位**: `m_value`
- **計算方式**: 平均值與總和
- **顯示格式**: 貨幣格式 ($XX,XXX.XX)
- **業務意義**: 客戶在RFM分析中的金額貢獻度
- **公式**: 
  ```r
  mean_val <- mean(m_value, na.rm = TRUE)
  total_val <- sum(m_value, na.rm = TRUE)
  ```

#### 2. 交易頻率 (Frequency - RFM-F)
- **資料欄位**: `f_value`
- **計算方式**: 平均值與總和
- **顯示格式**: 小數格式 (XX.XX)
- **業務意義**: 客戶交易頻繁程度
- **公式**:
  ```r
  mean_val <- mean(f_value, na.rm = TRUE)
  total_val <- sum(f_value, na.rm = TRUE)
  ```

#### 3. 客戶活躍指數 (Customer Activity Index)
- **資料欄位**: `cai`
- **計算方式**: 平均值，含最小值與最大值範圍
- **顯示格式**: 小數格式 (XX.XX)
- **業務意義**: 客戶整體活躍程度評估
- **公式**:
  ```r
  mean_val <- mean(cai, na.rm = TRUE)
  min_val <- min(cai, na.rm = TRUE)
  max_val <- max(cai, na.rm = TRUE)
  ```

### 第二排：客戶行為指標

#### 4. 購買間隔時間 (Inter-Purchase Time)
- **資料欄位**: `ipt_mean`
- **計算方式**: 平均值（天數）
- **顯示格式**: 小數格式 + "days"
- **業務意義**: 客戶兩次購買之間的平均間隔天數
- **公式**:
  ```r
  mean_val <- mean(ipt_mean, na.rm = TRUE)
  ```

#### 5. 過往客戶價值 (Past Customer Value)
- **資料欄位**: `pcv`
- **計算方式**: 平均值與總和
- **顯示格式**: 貨幣格式 ($XX,XXX.XX)
- **業務意義**: 客戶歷史貢獻價值總計
- **公式**:
  ```r
  mean_val <- mean(pcv, na.rm = TRUE)
  total_val <- sum(pcv, na.rm = TRUE)
  ```

#### 6. 客戶終身價值 (Customer Lifetime Value)
- **資料欄位**: `clv`
- **計算方式**: 平均值與總和
- **顯示格式**: 貨幣格式 ($XX,XXX.XX)
- **業務意義**: 客戶預期終身貢獻價值
- **公式**:
  ```r
  mean_val <- mean(clv, na.rm = TRUE)
  total_val <- sum(clv, na.rm = TRUE)
  ```

### 第三排：客戶忠誠度與交易指標

#### 7. 客戶留存指數 (Customer Retention Index)
- **資料欄位**: `cri`
- **計算方式**: 平均值
- **顯示格式**: 小數格式 (XX.XX)
- **業務意義**: 客戶留存概率評估
- **公式**:
  ```r
  mean_val <- mean(cri, na.rm = TRUE)
  ```

#### 8. 新客戶交易價值 (New Customer Transaction)
- **資料欄位**: `nt`
- **計算方式**: 平均值與總和
- **顯示格式**: 貨幣格式 ($XX,XXX.XX)
- **業務意義**: 新客戶首次交易平均價值
- **公式**:
  ```r
  mean_val <- mean(nt, na.rm = TRUE)
  total_val <- sum(nt, na.rm = TRUE)
  ```

#### 9. 既有客戶交易價值 (Existing Customer Transaction)
- **資料欄位**: `e0t`
- **計算方式**: 平均值與總和
- **顯示格式**: 貨幣格式 ($XX,XXX.XX)
- **業務意義**: 既有客戶重複購買平均價值
- **公式**:
  ```r
  mean_val <- mean(e0t, na.rm = TRUE)
  total_val <- sum(e0t, na.rm = TRUE)
  ```

## 資料處理邏輯

### DuckDB SQL 聚合計算
```sql
-- 所有統計值在 SQL 層計算，避免載入原始資料
kpi_summary <- tbl %>%
  summarise(
    # 客戶價值指標
    m_value_mean = mean(m_value, na.rm = TRUE),
    m_value_sum = sum(m_value, na.rm = TRUE),
    m_value_count = sum(case_when(!is.na(m_value) ~ 1, TRUE ~ 0)),
    
    # 其他 KPI 指標...
    total_records = n()
  ) %>%
  collect()
```

### 平台篩選
```r
# 現代化平台ID處理（直接使用字符串）
# 平台ID格式：
# - "eby" = eBay
# - "amz" = Amazon  
# - "all" = 所有平台

# 套用篩選（在 SQL 層執行）
if (!is.null(platform_val) && platform_val != "all") {
  tbl <- tbl %>% dplyr::filter(platform_id == platform_val)
}
```

### 產品線篩選
```r
# 注意：df_dna_by_customer 不包含產品線資訊
# 客戶DNA資料是跨所有產品線的聚合結果
# 因此產品線篩選功能已被註釋掉，避免錯誤
```

### 安全統計值提取
```r
safeKPIValue <- function(summary_data, field_name, default = 0) {
  if (is.null(summary_data) || nrow(summary_data) == 0 || 
      !field_name %in% names(summary_data)) {
    return(default)
  }
  
  value <- summary_data[[field_name]][1]  # 取第一列（唯一列）
  
  if (is.na(value) || !is.finite(value)) {
    return(default)
  }
  
  return(value)
}
```

## 數值格式化

### 格式類型自動判斷
```r
get_format_type <- function(metric_name) {
  metric_lower <- tolower(metric_name)
  
  if (grepl("revenue|cost|spend|price", metric_lower)) {
    return("currency")     # 貨幣格式
  } else if (grepl("rate|conversion|percentage|pct", metric_lower)) {
    return("percentage")   # 百分比格式
  } else if (grepl("count|sales|impressions|clicks|views", metric_lower)) {
    return("integer")      # 整數格式
  } else {
    return("decimal")      # 小數格式
  }
}
```

### 格式化函數
```r
format_kpi_value <- function(value, type = "decimal") {
  switch(type,
    "currency" = paste0("$", format(round(value, 2), big.mark = ",", nsmall = 2)),
    "percentage" = paste0(format(round(value * 100, 1), nsmall = 1), "%"),
    "decimal" = format(round(value, 2), big.mark = ",", nsmall = 2),
    "integer" = format(round(value, 0), big.mark = ","),
    format(round(value, 2), big.mark = ",", nsmall = 2)
  )
}
```

## ValueBox 視覺化

每個KPI使用不同顏色的ValueBox呈現：
- **成功色系** (success): 金額價值
- **主要色系** (primary): 交易頻率
- **警告色系** (warning): 客戶活躍指數
- **危險色系** (danger): 購買間隔時間
- **青綠色系** (teal): 過往客戶價值
- **淺藍色系** (lightblue): 客戶終身價值
- **橘色系** (orange): 客戶留存指數
- **深藍色系** (navy): 新客戶交易價值
- **橄欖色系** (olive): 既有客戶交易價值

## 狀態追蹤

組件使用 `component_status` 追蹤執行狀態：
- `idle`: 準備進行KPI分析
- `loading`: 載入KPI資料中...
- `ready`: 分析完成 - X個KPI已計算
- `computing`: 計算KPI統計中...
- `error`: KPI分析發生錯誤

## 技術架構

### 遵循原則
- **MP56**: Connected Component Principle (組件結構)
- **MP81**: Explicit Parameter Specification (函數參數)
- **R116**: Enhanced Data Access with tbl2 (資料存取)
- **R09**: UI-Server-Defaults Triple (組件組織)
- **MP88**: Immediate Feedback (即時回饋)
- **MP47**: Functional Programming (資料轉換函數)

### 優化後資料流程
1. 從 `df_dna_by_customer` 資料表建立 tbl2 連接
2. 根據平台ID進行篩選（SQL 層）
3. **DuckDB 聚合計算**：所有統計值在 SQL 層完成計算
4. `collect()` 只傳回一列統計結果（而非原始資料）
5. R 層格式化數值並產生 ValueBox
6. 即時更新狀態與視覺化結果

### 效能優化效果
- ✅ **記憶體使用**: 只載入統計結果，不載入原始資料
- ✅ **計算速度**: DuckDB 向量化聚合比 R 迴圈快
- ✅ **網路傳輸**: 減少資料傳輸量（1列 vs N列）
- ✅ **擴展性**: 支援處理超過記憶體大小的資料集

---

**最後更新**: 2025-08-04  
**檔案位置**: `/micro/microMacroKPI/microMacroKPI.R`  
**組件類型**: 宏觀指標監控組件  
**優化版本**: DuckDB SQL 聚合計算版本